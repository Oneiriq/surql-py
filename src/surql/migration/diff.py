"""Schema diffing utilities for migration generation.

This module provides functions for comparing schema definitions and generating
SQL statements for schema changes.
"""

import re

import structlog

from surql.migration.models import DiffOperation, SchemaDiff
from surql.schema.bucket import BucketDefinition
from surql.schema.edge import EdgeDefinition, EdgeMode
from surql.schema.fields import FieldDefinition, FieldType, _detect_target_table_from_value
from surql.schema.table import (
  DISKANN_DEFAULT_ALPHA,
  DISKANN_DEFAULT_DEGREE,
  DISKANN_DEFAULT_L_BUILD,
  DiskAnnDistanceType,
  EventDefinition,
  IndexDefinition,
  IndexType,
  MTreeVectorType,
  TableDefinition,
)

logger = structlog.get_logger(__name__)


# Pattern matching safe SurrealDB default values:
# - Function calls: time::now(), rand::uuid(), math::floor(1.5)
# - Numeric literals: 42, 3.14, -1
# - Boolean literals: true, false
# - String literals in single quotes: 'hello'
# - NONE/NULL
_SAFE_DEFAULT_PATTERN = re.compile(
  r'^('
  r'[a-zA-Z_][a-zA-Z0-9_]*(?:::[a-zA-Z_][a-zA-Z0-9_]*)*\([^;]*\)'  # function calls
  r'|-?\d+(?:\.\d+)?[fd]?'  # numeric literals (optional f/d suffix: SurrealDB v3 normalises float/double defaults to `1f` / `0.5f` / `1d` via INFO FOR TABLE round-trip)
  r'|true|false'  # boolean literals
  r'|NONE|NULL'  # null values
  r"|'(?:[^'\\]|\\.)*'"  # single-quoted strings
  r'|\$[a-zA-Z_][a-zA-Z0-9_]*'  # parameter references
  r')$'
)


# Matches a numeric literal with optional sign + decimal + f/d suffix.
# Used to recognise default values that round-trip through SurrealDB v3's
# `INFO FOR TABLE` as `1f` / `0.5f` / `-1d` (the suffix flags float/double
# storage type) but compare semantically equal to their plain `1.0` / `0.5`
# / `-1` form on the in-code side.
_NUMERIC_DEFAULT_RE = re.compile(r'^-?\d+(?:\.\d+)?[fd]?$')


def _canonicalise_default(expr: str) -> str:
  """Normalise a numeric default value to a single canonical string form.

  `1.0` ≡ `1f` ≡ `1.0f` all canonicalise to `1.0`. Non-numeric expressions
  pass through unchanged so function calls / boolean literals / parameter
  references compare via their raw form.
  """
  stripped = expr.strip()
  if not _NUMERIC_DEFAULT_RE.match(stripped):
    return stripped
  # Strip trailing f/d suffix and re-format as a float string so `1`, `1.0`,
  # `1f`, `1.0f` all become `1.0`. This loses no precision because the
  # suffix is a storage-type flag, not a value qualifier.
  return repr(float(stripped.rstrip('fd')))


def _validate_event_expression(expr: str, label: str) -> None:
  """Validate that an event expression does not contain SQL injection patterns.

  Args:
    expr: The event condition or action expression to validate
    label: Label for error messages (e.g. 'condition', 'action')

  Raises:
    ValueError: If the expression contains dangerous SQL patterns
  """
  stripped = expr.strip()
  if '; ' in stripped or ';--' in stripped or stripped.endswith(';'):
    raise ValueError(
      f'Unsafe event {label}: {expr!r}. Event {label}s must not contain statement separators.'
    )
  if '--' in stripped:
    raise ValueError(
      f'Unsafe event {label}: {expr!r}. Event {label}s must not contain SQL comments.'
    )


def _validate_default_value(default: str) -> None:
  """Validate that a field default is a safe SurrealDB expression.

  Args:
    default: The default value expression to validate

  Raises:
    ValueError: If the default contains potentially unsafe SQL
  """
  if not _SAFE_DEFAULT_PATTERN.match(default.strip()):
    raise ValueError(
      f'Unsafe default value expression: {default!r}. '
      'Defaults must be function calls, literals, or parameter references.'
    )


def diff_tables(
  old_table: TableDefinition | None,
  new_table: TableDefinition | None,
) -> list[SchemaDiff]:
  """Compare two table definitions and generate diff operations.

  Args:
    old_table: Previous table definition (None if table is new)
    new_table: New table definition (None if table is removed)

  Returns:
    List of SchemaDiff operations

  Examples:
    >>> diffs = diff_tables(None, new_table)
    >>> diffs[0].operation
    DiffOperation.ADD_TABLE
  """
  diffs: list[SchemaDiff] = []

  # Table added
  if old_table is None and new_table is not None:
    diffs.extend(_generate_add_table_diffs(new_table))
    return diffs

  # Table removed
  if old_table is not None and new_table is None:
    diffs.extend(_generate_drop_table_diffs(old_table))
    return diffs

  # Both exist, compare
  if old_table is not None and new_table is not None:
    # Compare fields
    diffs.extend(diff_fields(old_table, new_table))

    # Compare indexes
    diffs.extend(diff_indexes(old_table, new_table))

    # Compare events
    diffs.extend(diff_events(old_table, new_table))

    # Compare permissions
    diffs.extend(diff_permissions(old_table, new_table))

  return diffs


def diff_fields(
  old_table: TableDefinition,
  new_table: TableDefinition,
) -> list[SchemaDiff]:
  """Compare field definitions between two table versions.

  Args:
    old_table: Previous table definition
    new_table: New table definition

  Returns:
    List of field-related SchemaDiff operations
  """
  diffs: list[SchemaDiff] = []

  # Create field mappings
  old_fields = {f.name: f for f in old_table.fields}
  new_fields = {f.name: f for f in new_table.fields}

  # Find added fields
  for field_name, field_def in new_fields.items():
    if field_name not in old_fields:
      diffs.append(_generate_add_field_diff(new_table.name, field_def))

  # Find removed fields
  for field_name, field_def in old_fields.items():
    if field_name not in new_fields:
      diffs.append(_generate_drop_field_diff(new_table.name, field_def))

  # Find modified fields
  for field_name in old_fields.keys() & new_fields.keys():
    old_field = old_fields[field_name]
    new_field = new_fields[field_name]

    if not _fields_equal(old_field, new_field):
      diffs.append(_generate_modify_field_diff(new_table.name, old_field, new_field))

  return diffs


def diff_indexes(
  old_table: TableDefinition,
  new_table: TableDefinition,
) -> list[SchemaDiff]:
  """Compare index definitions between two table versions.

  Args:
    old_table: Previous table definition
    new_table: New table definition

  Returns:
    List of index-related SchemaDiff operations
  """
  diffs: list[SchemaDiff] = []

  # Create index mappings
  old_indexes = {idx.name: idx for idx in old_table.indexes}
  new_indexes = {idx.name: idx for idx in new_table.indexes}

  # Find added indexes
  for index_name, index_def in new_indexes.items():
    if index_name not in old_indexes:
      diffs.append(_generate_add_index_diff(new_table.name, index_def))

  # Find removed indexes
  for index_name, index_def in old_indexes.items():
    if index_name not in new_indexes:
      diffs.append(_generate_drop_index_diff(new_table.name, index_def))

  return diffs


def diff_events(
  old_table: TableDefinition,
  new_table: TableDefinition,
) -> list[SchemaDiff]:
  """Compare event definitions between two table versions.

  Args:
    old_table: Previous table definition
    new_table: New table definition

  Returns:
    List of event-related SchemaDiff operations
  """
  diffs: list[SchemaDiff] = []

  # Create event mappings
  old_events = {evt.name: evt for evt in old_table.events}
  new_events = {evt.name: evt for evt in new_table.events}

  # Find added events
  for event_name, event_def in new_events.items():
    if event_name not in old_events:
      diffs.append(_generate_add_event_diff(new_table.name, event_def))

  # Find removed events
  for event_name, event_def in old_events.items():
    if event_name not in new_events:
      diffs.append(_generate_drop_event_diff(new_table.name, event_def))

  return diffs


def diff_permissions(
  old_table: TableDefinition,
  new_table: TableDefinition,
) -> list[SchemaDiff]:
  """Compare permission definitions between two table versions.

  Normalises empty / None permissions before comparing so a parsed live
  table with `permissions=None` (which is what the parser returns for
  the SurrealDB `PERMISSIONS NONE` / `PERMISSIONS FULL` default the v3
  server stores on every table that wasn't created with an explicit
  per-action permissions block) does NOT spuriously diff against a
  code-side `permissions={}` or `permissions=None`.

  Pre-1.6.2 a raw `!=` comparison on `dict | None` reported every
  PERMISSIONS-bearing table as drifted because the parser always returned
  `None` regardless of what the live DB stored; the 1.6.2 parser now
  extracts the per-action rules and this helper compares them
  symmetrically.

  Args:
    old_table: Previous table definition
    new_table: New table definition

  Returns:
    List of permission-related SchemaDiff operations
  """
  diffs: list[SchemaDiff] = []

  if _permissions_equal(old_table.permissions, new_table.permissions):
    return diffs

  diffs.append(
    _generate_modify_permissions_diff(old_table.name, new_table.permissions, old_table.permissions)
  )

  return diffs


def _permissions_equal(
  left: dict[str, str] | None,
  right: dict[str, str] | None,
) -> bool:
  """Compare two PERMISSIONS dicts for semantic equality.

  Treats `None`, `{}`, and the live-DB shape (which is also `None` after
  parser normalisation of `PERMISSIONS NONE` / `PERMISSIONS FULL`) as
  equivalent. For non-empty dicts, normalises action keys to lowercase
  and rule whitespace before comparing — matches how the emitter
  serialises permissions clauses.
  """
  left_normalised = _normalise_permissions(left)
  right_normalised = _normalise_permissions(right)
  return left_normalised == right_normalised


def _normalise_permissions(
  permissions: dict[str, str] | None,
) -> dict[str, str]:
  """Normalise a permissions dict for stable comparison.

  - `None` or empty -> `{}` (equivalent to "no per-action rules").
  - Lowercases action keys (SurrealDB grammar is case-insensitive but
    the emitter writes lowercase).
  - Collapses whitespace in rule expressions.
  """
  if not permissions:
    return {}
  return {action.lower(): ' '.join(rule.split()) for action, rule in permissions.items()}


def diff_edges(
  old_edge: EdgeDefinition | None,
  new_edge: EdgeDefinition | None,
) -> list[SchemaDiff]:
  """Compare two edge definitions and generate diff operations.

  Args:
    old_edge: Previous edge definition (None if edge is new)
    new_edge: New edge definition (None if edge is removed)

  Returns:
    List of SchemaDiff operations
  """
  diffs: list[SchemaDiff] = []

  # Edge added
  if old_edge is None and new_edge is not None:
    diffs.extend(_generate_add_edge_diffs(new_edge))
    return diffs

  # Edge removed
  if old_edge is not None and new_edge is None:
    diffs.extend(_generate_drop_edge_diffs(old_edge))
    return diffs

  # Both exist - compare fields, indexes, events, and permissions
  if old_edge is not None and new_edge is not None:
    old_proxy = _edge_to_table_proxy(old_edge)
    new_proxy = _edge_to_table_proxy(new_edge)
    diffs.extend(diff_fields(old_proxy, new_proxy))
    diffs.extend(diff_indexes(old_proxy, new_proxy))
    diffs.extend(diff_events(old_proxy, new_proxy))
    diffs.extend(diff_permissions(old_proxy, new_proxy))

  return diffs


def diff_buckets(
  old_bucket: BucketDefinition | None,
  new_bucket: BucketDefinition | None,
) -> list[SchemaDiff]:
  """Compare two bucket definitions and generate diff operations.

  Mirrors :func:`diff_tables` / :func:`diff_edges`:

  - added (``old`` is None) -> ``ADD_BUCKET`` (forward ``DEFINE BUCKET``,
    backward ``REMOVE BUCKET``).
  - removed (``new`` is None) -> ``DROP_BUCKET`` (forward ``REMOVE BUCKET``,
    backward ``DEFINE BUCKET`` to restore it).
  - changed -> ``MODIFY_BUCKET`` (forward ``ALTER BUCKET`` to the new shape,
    backward ``ALTER BUCKET`` back to the old shape). No diff is produced when
    the two definitions are equal.

  Args:
    old_bucket: Previous bucket definition (None if the bucket is new)
    new_bucket: New bucket definition (None if the bucket is removed)

  Returns:
    List of bucket-related SchemaDiff operations (possibly empty)
  """
  if old_bucket is None and new_bucket is not None:
    return [_generate_add_bucket_diff(new_bucket)]

  if old_bucket is not None and new_bucket is None:
    return [_generate_drop_bucket_diff(old_bucket)]

  if old_bucket is not None and new_bucket is not None and old_bucket != new_bucket:
    return _generate_modify_bucket_diff(old_bucket, new_bucket)

  return []


def _generate_add_bucket_diff(bucket: BucketDefinition) -> SchemaDiff:
  """Generate diff for adding a new bucket."""
  from surql.schema.sql import generate_bucket_sql, generate_remove_bucket_sql

  return SchemaDiff(
    operation=DiffOperation.ADD_BUCKET,
    bucket=bucket.name,
    description=f'Add bucket {bucket.name}',
    forward_sql=generate_bucket_sql(bucket)[0],
    backward_sql=generate_remove_bucket_sql(bucket)[0],
  )


def _generate_drop_bucket_diff(bucket: BucketDefinition) -> SchemaDiff:
  """Generate diff for dropping a bucket."""
  from surql.schema.sql import generate_bucket_sql, generate_remove_bucket_sql

  return SchemaDiff(
    operation=DiffOperation.DROP_BUCKET,
    bucket=bucket.name,
    description=f'Drop bucket {bucket.name}',
    forward_sql=generate_remove_bucket_sql(bucket)[0],
    backward_sql=generate_bucket_sql(bucket)[0],
  )


def _generate_modify_bucket_diff(
  old_bucket: BucketDefinition,
  new_bucket: BucketDefinition,
) -> list[SchemaDiff]:
  """Generate diff for modifying a bucket via ALTER BUCKET.

  Returns an empty list if the ALTER emitter found nothing to change (should
  not happen when callers gate on ``old != new``, but stays defensive).
  """
  from surql.schema.sql import generate_alter_bucket_sql

  forward = generate_alter_bucket_sql(old_bucket, new_bucket)
  backward = generate_alter_bucket_sql(new_bucket, old_bucket)
  if not forward:
    return []

  return [
    SchemaDiff(
      operation=DiffOperation.MODIFY_BUCKET,
      bucket=new_bucket.name,
      description=f'Modify bucket {new_bucket.name}',
      forward_sql=forward[0],
      backward_sql=backward[0] if backward else '',
    )
  ]


# Helper functions to generate specific diff types


def _edge_to_table_proxy(edge: EdgeDefinition) -> TableDefinition:
  """Wrap an EdgeDefinition as a TableDefinition for diff comparison.

  Both models share name, fields, indexes, events, and permissions attributes.
  This allows reusing diff_fields/diff_indexes/diff_events/diff_permissions.
  """
  return TableDefinition(
    name=edge.name,
    fields=list(edge.fields),
    indexes=list(edge.indexes),
    events=list(edge.events),
    permissions=edge.permissions,
  )


def _generate_add_table_diffs(table: TableDefinition) -> list[SchemaDiff]:
  """Generate diffs for adding a new table."""
  diffs: list[SchemaDiff] = []

  # Main table definition — fold PERMISSIONS into the DEFINE TABLE statement per
  # SurrealDB v3 grammar. Emitting them as a separate `DEFINE FIELD PERMISSIONS`
  # statement is not valid SurrealQL and is rejected at apply time with a parse error.
  permissions_clause = _permissions_clause_sql(table.permissions)
  forward_sql = f'DEFINE TABLE {table.name} {table.mode.value}{permissions_clause};'
  backward_sql = f'REMOVE TABLE {table.name};'

  diffs.append(
    SchemaDiff(
      operation=DiffOperation.ADD_TABLE,
      table=table.name,
      description=f'Add table {table.name}',
      forward_sql=forward_sql,
      backward_sql=backward_sql,
    )
  )

  # Add all fields
  for field in table.fields:
    diffs.append(_generate_add_field_diff(table.name, field))

  # Add all indexes
  for index in table.indexes:
    diffs.append(_generate_add_index_diff(table.name, index))

  # Add all events
  for event in table.events:
    diffs.append(_generate_add_event_diff(table.name, event))

  return diffs


def _generate_drop_table_diffs(table: TableDefinition) -> list[SchemaDiff]:
  """Generate diffs for dropping a table."""
  return [
    SchemaDiff(
      operation=DiffOperation.DROP_TABLE,
      table=table.name,
      description=f'Drop table {table.name}',
      forward_sql=f'REMOVE TABLE {table.name};',
      backward_sql=f'DEFINE TABLE {table.name} {table.mode.value};',
    )
  ]


def _generate_add_field_diff(table_name: str, field: FieldDefinition) -> SchemaDiff:
  """Generate diff for adding a field.

  When a field has a default value, includes a backfill UPDATE statement
  to apply the default to existing records where the field is NONE.
  """
  forward_sql = _field_to_sql(table_name, field)

  # Add backfill SQL for fields with defaults to update existing records
  if field.default:
    _validate_default_value(field.default)
    backfill_sql = (
      f'UPDATE {table_name} SET {field.name} = {field.default} WHERE {field.name} IS NONE;'
    )
    forward_sql = f'{forward_sql}\n{backfill_sql}'

  backward_sql = f'REMOVE FIELD {field.name} ON TABLE {table_name};'

  return SchemaDiff(
    operation=DiffOperation.ADD_FIELD,
    table=table_name,
    field=field.name,
    description=f'Add field {field.name} to {table_name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
    details={'type': field.type.value},
  )


def _generate_drop_field_diff(table_name: str, field: FieldDefinition) -> SchemaDiff:
  """Generate diff for dropping a field."""
  forward_sql = f'REMOVE FIELD {field.name} ON TABLE {table_name};'
  backward_sql = _field_to_sql(table_name, field)

  return SchemaDiff(
    operation=DiffOperation.DROP_FIELD,
    table=table_name,
    field=field.name,
    description=f'Drop field {field.name} from {table_name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
  )


def _generate_modify_field_diff(
  table_name: str,
  old_field: FieldDefinition,
  new_field: FieldDefinition,
) -> SchemaDiff:
  """Generate diff for modifying a field."""
  forward_sql = _field_to_sql(table_name, new_field)
  backward_sql = _field_to_sql(table_name, old_field)

  return SchemaDiff(
    operation=DiffOperation.MODIFY_FIELD,
    table=table_name,
    field=new_field.name,
    description=f'Modify field {new_field.name} in {table_name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
    details={'old_type': old_field.type.value, 'new_type': new_field.type.value},
  )


def _generate_add_index_diff(table_name: str, index: IndexDefinition) -> SchemaDiff:
  """Generate diff for adding an index."""

  # MTREE/HNSW indexes use different syntax
  if index.type == IndexType.MTREE:
    forward_sql = _mtree_index_to_sql(table_name, index)
  elif index.type == IndexType.HNSW:
    forward_sql = _hnsw_index_to_sql(table_name, index)
  elif index.type == IndexType.DISKANN:
    forward_sql = _diskann_index_to_sql(table_name, index)
  else:
    forward_sql = _index_to_sql(table_name, index)

  backward_sql = f'REMOVE INDEX {index.name} ON TABLE {table_name};'

  return SchemaDiff(
    operation=DiffOperation.ADD_INDEX,
    table=table_name,
    index=index.name,
    description=f'Add index {index.name} to {table_name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
  )


def _generate_drop_index_diff(table_name: str, index: IndexDefinition) -> SchemaDiff:
  """Generate diff for dropping an index."""

  forward_sql = f'REMOVE INDEX {index.name} ON TABLE {table_name};'

  # MTREE/HNSW indexes use different syntax for recreation
  if index.type == IndexType.MTREE:
    backward_sql = _mtree_index_to_sql(table_name, index)
  elif index.type == IndexType.HNSW:
    backward_sql = _hnsw_index_to_sql(table_name, index)
  elif index.type == IndexType.DISKANN:
    backward_sql = _diskann_index_to_sql(table_name, index)
  else:
    backward_sql = _index_to_sql(table_name, index)

  return SchemaDiff(
    operation=DiffOperation.DROP_INDEX,
    table=table_name,
    index=index.name,
    description=f'Drop index {index.name} from {table_name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
  )


def _generate_add_event_diff(table_name: str, event: EventDefinition) -> SchemaDiff:
  """Generate diff for adding an event."""
  _validate_event_expression(event.condition, 'condition')
  _validate_event_expression(event.action, 'action')
  forward_sql = f'DEFINE EVENT {event.name} ON TABLE {table_name} WHEN {event.condition} THEN {{ {event.action} }};'
  backward_sql = f'REMOVE EVENT {event.name} ON TABLE {table_name};'

  return SchemaDiff(
    operation=DiffOperation.ADD_EVENT,
    table=table_name,
    event=event.name,
    description=f'Add event {event.name} to {table_name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
  )


def _generate_drop_event_diff(table_name: str, event: EventDefinition) -> SchemaDiff:
  """Generate diff for dropping an event."""
  _validate_event_expression(event.condition, 'condition')
  _validate_event_expression(event.action, 'action')
  forward_sql = f'REMOVE EVENT {event.name} ON TABLE {table_name};'
  backward_sql = f'DEFINE EVENT {event.name} ON TABLE {table_name} WHEN {event.condition} THEN {{ {event.action} }};'

  return SchemaDiff(
    operation=DiffOperation.DROP_EVENT,
    table=table_name,
    event=event.name,
    description=f'Drop event {event.name} from {table_name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
  )


def _permissions_clause_sql(permissions: dict[str, str] | None) -> str:
  """Render a SurrealDB ``PERMISSIONS FOR <action> WHERE <rule>`` clause body.

  Returns a leading-space string for direct concatenation into a `DEFINE TABLE`
  statement, or empty string when no permissions are defined.
  """
  if not permissions:
    return ''
  clauses = ' '.join(f'FOR {action.lower()} WHERE {rule}' for action, rule in permissions.items())
  return f' PERMISSIONS {clauses}'


def _generate_modify_permissions_diff(
  table_name: str,
  permissions: dict[str, str] | None,
  old_permissions: dict[str, str] | None = None,
) -> SchemaDiff:
  """Generate diff for modifying permissions.

  SurrealDB has no `ALTER TABLE` syntax; permissions are modified by re-issuing
  a `DEFINE TABLE` statement that includes the new PERMISSIONS clause. The
  table's mode (SCHEMAFULL/SCHEMALESS/RELATION) must be re-stated — for safety
  we default to SCHEMAFULL and the caller is responsible for ensuring this
  matches the table's actual mode.

  Either or both of `permissions` / `old_permissions` may be None: a None
  forward permits dropping the clause (re-DEFINE with no PERMISSIONS);
  a None rollback means the table previously had no permissions.
  """
  forward_sql = (
    f'DEFINE TABLE {table_name} SCHEMAFULL{_permissions_clause_sql(permissions)};'
    if permissions is not None
    else f'DEFINE TABLE {table_name} SCHEMAFULL;'
  )
  backward_sql = (
    f'DEFINE TABLE {table_name} SCHEMAFULL{_permissions_clause_sql(old_permissions)};'
    if old_permissions is not None
    else f'DEFINE TABLE {table_name} SCHEMAFULL;'
  )

  return SchemaDiff(
    operation=DiffOperation.MODIFY_PERMISSIONS,
    table=table_name,
    description=f'Modify permissions for {table_name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
  )


def _generate_add_edge_diffs(edge: EdgeDefinition) -> list[SchemaDiff]:
  """Generate diffs for adding a new edge."""

  diffs: list[SchemaDiff] = []

  # Edge table definition - varies by mode; PERMISSIONS clause folds in on whichever shape
  # so edges with `with_edge_permissions(...)` apply isolation at the DB layer.
  permissions_clause = _permissions_clause_sql(edge.permissions)

  if edge.mode == EdgeMode.RELATION:
    # TYPE RELATION syntax
    forward_sql = f'DEFINE TABLE {edge.name} TYPE RELATION'

    if edge.from_table:
      forward_sql += f' FROM {edge.from_table}'

    if edge.to_table:
      forward_sql += f' TO {edge.to_table}'

    forward_sql += f'{permissions_clause};'
  elif edge.mode == EdgeMode.SCHEMAFULL:
    # Traditional SCHEMAFULL table
    forward_sql = f'DEFINE TABLE {edge.name} SCHEMAFULL{permissions_clause};'
  else:  # SCHEMALESS
    forward_sql = f'DEFINE TABLE {edge.name} SCHEMALESS{permissions_clause};'

  backward_sql = f'REMOVE TABLE {edge.name};'

  diffs.append(
    SchemaDiff(
      operation=DiffOperation.ADD_TABLE,
      table=edge.name,
      description=f'Add edge {edge.name}',
      forward_sql=forward_sql,
      backward_sql=backward_sql,
    )
  )

  # Add edge fields
  for field in edge.fields:
    diffs.append(_generate_add_field_diff(edge.name, field))

  # Add edge indexes
  for index in edge.indexes:
    diffs.append(_generate_add_index_diff(edge.name, index))

  # Add edge events
  for event in edge.events:
    diffs.append(_generate_add_event_diff(edge.name, event))

  return diffs


def _generate_drop_edge_diffs(edge: EdgeDefinition) -> list[SchemaDiff]:
  """Generate diffs for dropping an edge."""
  return [
    SchemaDiff(
      operation=DiffOperation.DROP_TABLE,
      table=edge.name,
      description=f'Drop edge {edge.name}',
      forward_sql=f'REMOVE TABLE {edge.name};',
      backward_sql='',
    )
  ]


def _field_to_sql(table_name: str, field: FieldDefinition) -> str:
  """Convert a field definition to SQL statement.

  Args:
    table_name: Name of the table
    field: Field definition

  Returns:
    SQL statement string
  """
  # Mirror schema/sql.py: RECORD fields with target_table emit `record<X>`
  # and drop the redundant `VALUE type::record("X", $value)` coercion.
  if field.type == FieldType.RECORD and field.target_table:
    base_type = f'record<{field.target_table}>'
    drop_value = bool(
      field.value and _detect_target_table_from_value(field.value) == field.target_table
    )
  else:
    base_type = field.type.value
    drop_value = False

  type_clause = f'option<{base_type}>' if field.nullable else base_type
  sql = f'DEFINE FIELD {field.name} ON TABLE {table_name} TYPE {type_clause}'

  if field.assertion:
    sql += f' ASSERT {field.assertion}'

  if field.default:
    _validate_default_value(field.default)
    sql += f' DEFAULT {field.default}'

  if field.value and not drop_value:
    _validate_default_value(field.value)
    sql += f' VALUE {field.value}'

  if field.readonly:
    sql += ' READONLY'

  if field.flexible:
    sql += ' FLEXIBLE'

  sql += ';'

  return sql


def _fields_equal(field1: FieldDefinition, field2: FieldDefinition) -> bool:
  """Check if two field definitions are equal.

  Compares the seven attributes that produce observable SurrealDB schema
  differences: name, type, nullable (option<X>), target_table (record<X>),
  assertion, default, value, readonly, flexible.

  Pre-1.6.2 this missed `nullable` and `target_table` — which made every
  parsed live field that the emitter produced via `TYPE option<X>` or
  `TYPE record<target>` look unequal to the code declaration, since the
  parser also failed to lift those out of `none | X` / `record<target>`
  textual forms. Both are addressed in 1.6.2: parser sets nullable +
  target_table when present, and this comparison includes them.

  Args:
    field1: First field definition
    field2: Second field definition

  Returns:
    True if fields are equal, False otherwise
  """
  return (
    field1.name == field2.name
    and field1.type == field2.type
    and field1.nullable == field2.nullable
    and field1.target_table == field2.target_table
    and _expressions_equal(field1.assertion, field2.assertion)
    and _expressions_equal(field1.default, field2.default)
    and _expressions_equal(field1.value, field2.value)
    and field1.readonly == field2.readonly
    and field1.flexible == field2.flexible
  )


def _expressions_equal(left: str | None, right: str | None) -> bool:
  """Compare two SurrealQL expressions for semantic equality.

  Normalises both sides through:
    - whitespace collapsing (cosmetic differences in spacing don't trip the diff)
    - numeric-default canonicalisation (`1.0` ≡ `1f` ≡ `1.0f`, since
      SurrealDB v3 round-trips float defaults through `INFO FOR TABLE` with
      a `f`/`d` storage-type suffix that's not present on the in-code side)
  This mirrors the same normalisation `surql.schema.validator` applies
  to expression comparisons, so the two code paths agree on what counts
  as "drift".
  """
  if left is None and right is None:
    return True
  if left is None or right is None:
    return False
  left_norm = _canonicalise_default(' '.join(left.split()))
  right_norm = _canonicalise_default(' '.join(right.split()))
  return left_norm == right_norm


def _index_to_sql(table_name: str, index: IndexDefinition) -> str:
  """Convert a non-vector index definition (UNIQUE / STANDARD / FULLTEXT) to SQL.

  Full-text (``SEARCH``) indexes render the SurrealDB 3.x ``FULLTEXT`` keyword
  plus their analyzer and optional ``BM25`` / ``HIGHLIGHTS`` clauses; the v1/v2
  ``SEARCH`` spelling was renamed in 3.0 and is a parse error there. See
  ``docs/v3-patterns.md``.

  Args:
    table_name: Name of the table the index belongs to
    index: Index definition (must not be MTREE/HNSW)

  Returns:
    SQL statement string ending in a semicolon
  """

  columns_str = ', '.join(index.columns)
  sql = f'DEFINE INDEX {index.name} ON TABLE {table_name} COLUMNS {columns_str}'

  if index.type == IndexType.UNIQUE:
    sql += ' UNIQUE'
  elif index.type == IndexType.SEARCH:
    analyzer = index.analyzer or 'ascii'
    sql += f' FULLTEXT ANALYZER {analyzer}'
    if index.bm25:
      sql += ' BM25'
    if index.highlights:
      sql += ' HIGHLIGHTS'

  sql += ';'
  return sql


def _mtree_index_to_sql(table_name: str, index: IndexDefinition) -> str:
  """Convert an MTREE index definition to SQL statement.

  Args:
    table_name: Name of the table
    index: MTREE index definition

  Returns:
    SQL statement string for MTREE index

  Examples:
    >>> _mtree_index_to_sql('documents', mtree_index('emb_idx', 'embedding', 1536))
    'DEFINE INDEX emb_idx ON TABLE documents COLUMNS embedding MTREE DIMENSION 1536 DIST EUCLIDEAN TYPE F64;'
  """
  if not index.dimension:
    msg = f'MTREE index {index.name} must have dimension specified'
    raise ValueError(msg)

  # MTREE indexes only support single column
  field_name = index.columns[0] if index.columns else ''

  sql = f'DEFINE INDEX {index.name} ON TABLE {table_name} COLUMNS {field_name} MTREE DIMENSION {index.dimension}'

  # Add optional distance metric
  if index.distance:
    sql += f' DIST {index.distance.value}'

  # Add optional vector type
  if index.vector_type:
    sql += f' TYPE {index.vector_type.value}'

  sql += ';'

  return sql


def _diskann_index_to_sql(table_name: str, index: IndexDefinition) -> str:
  """Convert a DISKANN index definition to SQL statement.

  The engine echoes DIST / TYPE / DEGREE / L_BUILD / ALPHA back with its
  defaults filled in even when the definition never stated them, so this spells
  them all. A migration that omitted one would render a statement the next
  reconcile reads back as different, and re-apply the index on every boot.

  Args:
    table_name: Name of the table
    index: DISKANN index definition

  Returns:
    SQL statement string for DISKANN index

  Examples:
    >>> _diskann_index_to_sql('documents', diskann_index('emb_idx', 'embedding', 1536))
    'DEFINE INDEX emb_idx ON TABLE documents COLUMNS embedding DISKANN DIMENSION 1536 DIST EUCLIDEAN TYPE F32 DEGREE 64 L_BUILD 100 ALPHA 1.2;'
  """
  if not index.dimension:
    msg = f'DISKANN index {index.name} must have dimension specified'
    raise ValueError(msg)

  # DISKANN indexes only support single column
  field_name = index.columns[0] if index.columns else ''
  distance = index.diskann_distance or DiskAnnDistanceType.EUCLIDEAN
  vector_type = index.vector_type or MTreeVectorType.F32
  degree = index.degree if index.degree is not None else DISKANN_DEFAULT_DEGREE
  l_build = index.l_build if index.l_build is not None else DISKANN_DEFAULT_L_BUILD
  alpha = index.alpha or DISKANN_DEFAULT_ALPHA

  sql = (
    f'DEFINE INDEX {index.name} ON TABLE {table_name} COLUMNS {field_name}'
    f' DISKANN DIMENSION {index.dimension} DIST {distance.value}'
    f' TYPE {vector_type.value} DEGREE {degree} L_BUILD {l_build} ALPHA {alpha}'
  )

  if index.hashed_vector:
    sql += ' HASHED_VECTOR'

  sql += ';'

  return sql


def _hnsw_index_to_sql(table_name: str, index: IndexDefinition) -> str:
  """Convert an HNSW index definition to SQL statement.

  Args:
    table_name: Name of the table
    index: HNSW index definition

  Returns:
    SQL statement string for HNSW index

  Examples:
    >>> _hnsw_index_to_sql('documents', hnsw_index('emb_idx', 'embedding', 1536))
    'DEFINE INDEX emb_idx ON TABLE documents COLUMNS embedding HNSW DIMENSION 1536 DIST EUCLIDEAN TYPE F64;'
  """
  if not index.dimension:
    msg = f'HNSW index {index.name} must have dimension specified'
    raise ValueError(msg)

  # HNSW indexes only support single column
  field_name = index.columns[0] if index.columns else ''

  sql = f'DEFINE INDEX {index.name} ON TABLE {table_name} COLUMNS {field_name} HNSW DIMENSION {index.dimension}'

  # Add optional distance metric
  if index.hnsw_distance:
    sql += f' DIST {index.hnsw_distance.value}'

  # Add optional vector type
  if index.vector_type:
    sql += f' TYPE {index.vector_type.value}'

  # Add optional HNSW tuning parameters
  if index.efc is not None:
    sql += f' EFC {index.efc}'

  if index.m is not None:
    sql += f' M {index.m}'

  sql += ';'

  return sql
