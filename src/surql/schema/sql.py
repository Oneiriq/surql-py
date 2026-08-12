"""SQL generation from schema definitions.

Generates SurrealQL DEFINE statements from TableDefinition, EdgeDefinition,
and AccessDefinition objects. This enables consumers to create database schemas
directly from surql schema definitions without using the migration system.
"""

from surql.schema.access import AccessDefinition, AccessType
from surql.schema.analyzer import AnalyzerDefinition
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


def _ine_clause(if_not_exists: bool) -> str:
  """Return the IF NOT EXISTS clause when enabled.

  Args:
    if_not_exists: Whether to include the clause

  Returns:
    ' IF NOT EXISTS' or empty string
  """
  return ' IF NOT EXISTS' if if_not_exists else ''


def _permissions_clause(permissions: dict[str, str] | None) -> str:
  """Render a SurrealDB PERMISSIONS clause for inclusion in DEFINE TABLE.

  SurrealDB v3 syntax: ``PERMISSIONS FOR <action> WHERE <rule> [FOR <action> WHERE <rule>]...``
  where actions are lowercase ``select``, ``create``, ``update``, ``delete``.

  Args:
    permissions: Action -> rule mapping (e.g. ``{'select': 'id = $auth.id'}``).
      Accepts mixed-case keys; emits them lowercased per SurrealDB grammar.

  Returns:
    ' PERMISSIONS FOR select WHERE ... FOR create WHERE ...' or empty string.
  """
  if not permissions:
    return ''
  clauses = ' '.join(f'FOR {action.lower()} WHERE {rule}' for action, rule in permissions.items())
  return f' PERMISSIONS {clauses}'


def _generate_field_sql(
  table_name: str,
  field_def: FieldDefinition,
  *,
  if_not_exists: bool = False,
) -> str:
  """Generate DEFINE FIELD statement for a single field.

  Args:
    table_name: Name of the table
    field_def: Field definition
    if_not_exists: When True, adds IF NOT EXISTS clause

  Returns:
    SurrealQL DEFINE FIELD statement
  """
  ine = _ine_clause(if_not_exists)
  base_type, drop_value = _resolve_type_clause(field_def)
  type_clause = f'option<{base_type}>' if field_def.nullable else base_type
  sql = f'DEFINE FIELD{ine} {field_def.name} ON TABLE {table_name} TYPE {type_clause}'

  if field_def.assertion:
    sql += f' ASSERT {field_def.assertion}'

  if field_def.default:
    sql += f' DEFAULT {field_def.default}'

  if field_def.value and not drop_value:
    sql += f' VALUE {field_def.value}'

  if field_def.readonly:
    sql += ' READONLY'

  if field_def.flexible:
    sql += ' FLEXIBLE'

  sql += ';'
  return sql


def _resolve_type_clause(field_def: FieldDefinition) -> tuple[str, bool]:
  """Return `(base_type_clause, drop_value)` honoring RECORD target_table.

  For RECORD fields with a `target_table`, emit `record<target>` instead of
  bare `record` so SurrealDB's introspection (and Surrealist's graph designer)
  can render cross-table relationships. If the value arg was the canonical
  `type::record("target", $value)` coercion, it's now redundant — the type
  parameter enforces the same thing — so signal back to drop it.
  """
  if field_def.type == FieldType.RECORD and field_def.target_table:
    drop_value = bool(
      field_def.value and _detect_target_table_from_value(field_def.value) == field_def.target_table
    )
    return f'record<{field_def.target_table}>', drop_value
  return field_def.type.value, False


def _generate_diskann_sql(table_name: str, index_def: IndexDefinition, ine: str) -> str:
  """Generate the DISKANN form of a DEFINE INDEX statement.

  The engine always echoes DIST / TYPE / DEGREE / L_BUILD / ALPHA back with its
  defaults filled in, even when the definition never stated them, so this
  spells them all. A definition that omitted one would never compare equal to
  its own echo, and a reconcile would re-apply the index on every boot.

  Args:
    table_name: Name of the table
    index_def: Index definition
    ine: Rendered IF NOT EXISTS clause

  Returns:
    SurrealQL DEFINE INDEX statement
  """
  field_name = index_def.columns[0] if index_def.columns else ''
  distance = index_def.diskann_distance or DiskAnnDistanceType.EUCLIDEAN
  vector_type = index_def.vector_type or MTreeVectorType.F32
  degree = index_def.degree if index_def.degree is not None else DISKANN_DEFAULT_DEGREE
  l_build = index_def.l_build if index_def.l_build is not None else DISKANN_DEFAULT_L_BUILD
  alpha = index_def.alpha or DISKANN_DEFAULT_ALPHA

  sql = (
    f'DEFINE INDEX{ine} {index_def.name} ON TABLE {table_name}'
    f' COLUMNS {field_name} DISKANN DIMENSION {index_def.dimension}'
    f' DIST {distance.value} TYPE {vector_type.value}'
    f' DEGREE {degree} L_BUILD {l_build} ALPHA {alpha}'
  )
  if index_def.hashed_vector:
    sql += ' HASHED_VECTOR'
  return sql + ';'


def _generate_index_sql(
  table_name: str,
  index_def: IndexDefinition,
  *,
  if_not_exists: bool = False,
) -> str:
  """Generate DEFINE INDEX statement for a single index.

  Args:
    table_name: Name of the table
    index_def: Index definition
    if_not_exists: When True, adds IF NOT EXISTS clause

  Returns:
    SurrealQL DEFINE INDEX statement
  """
  ine = _ine_clause(if_not_exists)
  columns = ', '.join(index_def.columns)

  if index_def.type == IndexType.MTREE:
    field_name = index_def.columns[0] if index_def.columns else ''
    sql = (
      f'DEFINE INDEX{ine} {index_def.name} ON TABLE {table_name}'
      f' COLUMNS {field_name} MTREE DIMENSION {index_def.dimension}'
    )
    if index_def.distance:
      sql += f' DIST {index_def.distance.value}'
    if index_def.vector_type:
      sql += f' TYPE {index_def.vector_type.value}'
    sql += ';'
    return sql

  if index_def.type == IndexType.HNSW:
    field_name = index_def.columns[0] if index_def.columns else ''
    sql = (
      f'DEFINE INDEX{ine} {index_def.name} ON TABLE {table_name}'
      f' COLUMNS {field_name} HNSW DIMENSION {index_def.dimension}'
    )
    if index_def.hnsw_distance:
      sql += f' DIST {index_def.hnsw_distance.value}'
    if index_def.vector_type:
      sql += f' TYPE {index_def.vector_type.value}'
    if index_def.efc is not None:
      sql += f' EFC {index_def.efc}'
    if index_def.m is not None:
      sql += f' M {index_def.m}'
    sql += ';'
    return sql

  if index_def.type == IndexType.DISKANN:
    return _generate_diskann_sql(table_name, index_def, ine)

  sql = f'DEFINE INDEX{ine} {index_def.name} ON TABLE {table_name} COLUMNS {columns}'

  if index_def.type == IndexType.UNIQUE:
    sql += ' UNIQUE'
  elif index_def.type == IndexType.SEARCH:
    # SurrealDB 3.x renamed the full-text keyword from SEARCH to FULLTEXT (the
    # v1/v2 `SEARCH ANALYZER ascii` form is a parse error on v3). An unset
    # analyzer renders the historical `ascii` default. See docs/v3-patterns.md.
    analyzer = index_def.analyzer or 'ascii'
    sql += f' FULLTEXT ANALYZER {analyzer}'
    if index_def.bm25:
      sql += ' BM25'
    if index_def.highlights:
      sql += ' HIGHLIGHTS'

  sql += ';'
  return sql


def _generate_event_sql(
  table_name: str,
  event_def: EventDefinition,
  *,
  if_not_exists: bool = False,
) -> str:
  """Generate DEFINE EVENT statement for a single event.

  Args:
    table_name: Name of the table
    event_def: Event definition
    if_not_exists: When True, adds IF NOT EXISTS clause

  Returns:
    SurrealQL DEFINE EVENT statement
  """
  ine = _ine_clause(if_not_exists)
  sql = (
    f'DEFINE EVENT{ine} {event_def.name} ON TABLE {table_name}'
    f' WHEN {event_def.condition} THEN {event_def.action};'
  )
  return sql


def generate_table_sql(
  table: TableDefinition,
  *,
  if_not_exists: bool = False,
) -> list[str]:
  """Generate SurrealQL DEFINE statements for a table and its components.

  Args:
    table: Table definition to generate SQL for
    if_not_exists: When True, adds IF NOT EXISTS to all DEFINE statements

  Returns:
    List of SurrealQL statements (DEFINE TABLE, DEFINE FIELD, etc.)

  Examples:
    >>> from surql.schema.table import table_schema, TableMode
    >>> from surql.schema.fields import string_field
    >>> t = table_schema('user', mode=TableMode.SCHEMAFULL, fields=[string_field('name')])
    >>> stmts = generate_table_sql(t)
    >>> stmts[0]
    'DEFINE TABLE user SCHEMAFULL;'
  """
  ine = _ine_clause(if_not_exists)
  permissions = _permissions_clause(table.permissions)
  statements: list[str] = []

  # Table definition — permissions fold into the DEFINE TABLE statement itself per
  # SurrealDB v3 grammar (DEFINE TABLE ... PERMISSIONS FOR <action> WHERE <rule> ...).
  statements.append(f'DEFINE TABLE{ine} {table.name} {table.mode.value}{permissions};')

  # Field definitions
  for field_def in table.fields:
    statements.append(_generate_field_sql(table.name, field_def, if_not_exists=if_not_exists))

  # Index definitions
  for index_def in table.indexes:
    statements.append(_generate_index_sql(table.name, index_def, if_not_exists=if_not_exists))

  # Event definitions
  for event_def in table.events:
    statements.append(_generate_event_sql(table.name, event_def, if_not_exists=if_not_exists))

  return statements


def generate_edge_sql(
  edge: EdgeDefinition,
  *,
  if_not_exists: bool = False,
) -> list[str]:
  """Generate SurrealQL DEFINE statements for an edge table.

  Args:
    edge: Edge definition to generate SQL for
    if_not_exists: When True, adds IF NOT EXISTS to all DEFINE statements

  Returns:
    List of SurrealQL statements

  Examples:
    >>> from surql.schema.edge import edge_schema
    >>> e = edge_schema('likes', from_table='user', to_table='post')
    >>> stmts = generate_edge_sql(e)
    >>> stmts[0]
    'DEFINE TABLE likes TYPE RELATION FROM user TO post;'
  """
  if edge.mode == EdgeMode.RELATION and (not edge.from_table or not edge.to_table):
    raise ValueError(f'Edge {edge.name!r} with RELATION mode requires both from_table and to_table')

  ine = _ine_clause(if_not_exists)
  permissions = _permissions_clause(edge.permissions)
  statements: list[str] = []

  if edge.mode == EdgeMode.RELATION:
    table_sql = f'DEFINE TABLE{ine} {edge.name} TYPE RELATION'
    if edge.from_table:
      table_sql += f' FROM {edge.from_table}'
    if edge.to_table:
      table_sql += f' TO {edge.to_table}'
    table_sql += f'{permissions};'
  elif edge.mode == EdgeMode.SCHEMAFULL:
    table_sql = f'DEFINE TABLE{ine} {edge.name} SCHEMAFULL{permissions};'
  else:
    table_sql = f'DEFINE TABLE{ine} {edge.name} SCHEMALESS{permissions};'

  statements.append(table_sql)

  for field_def in edge.fields:
    statements.append(_generate_field_sql(edge.name, field_def, if_not_exists=if_not_exists))

  for index_def in edge.indexes:
    statements.append(_generate_index_sql(edge.name, index_def, if_not_exists=if_not_exists))

  for event_def in edge.events:
    statements.append(_generate_event_sql(edge.name, event_def, if_not_exists=if_not_exists))

  return statements


def generate_access_sql(access: AccessDefinition) -> list[str]:
  """Generate SurrealQL DEFINE ACCESS statement.

  Args:
    access: Access definition to generate SQL for

  Returns:
    List containing the DEFINE ACCESS statement

  Examples:
    >>> from surql.schema.access import jwt_access
    >>> a = jwt_access('api', key='secret')
    >>> stmts = generate_access_sql(a)
    >>> stmts[0]
    "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';"
  """
  sql = f'DEFINE ACCESS {access.name} ON DATABASE TYPE {access.type.value}'

  if access.type == AccessType.JWT and access.jwt:
    sql += f' ALGORITHM {access.jwt.algorithm}'
    if access.jwt.key:
      sql += f" KEY '{access.jwt.key}'"
    if access.jwt.url:
      sql += f" URL '{access.jwt.url}'"
    if access.jwt.issuer:
      sql += f" WITH ISSUER '{access.jwt.issuer}'"

  if access.type == AccessType.RECORD and access.record:
    if access.record.signup:
      sql += f' SIGNUP ({access.record.signup})'
    if access.record.signin:
      sql += f' SIGNIN ({access.record.signin})'

  if access.duration_session or access.duration_token:
    duration_parts: list[str] = []
    if access.duration_session:
      duration_parts.append(f'FOR SESSION {access.duration_session}')
    if access.duration_token:
      duration_parts.append(f'FOR TOKEN {access.duration_token}')
    sql += f' DURATION {", ".join(duration_parts)}'

  sql += ';'
  return [sql]


def _surql_string(value: str) -> str:
  """Render ``value`` as a double-quoted SurrealQL string literal.

  Escapes embedded backslashes and double quotes so a backend path or comment
  containing them cannot break out of the literal. Mirrors how the sibling
  ports quote ``BACKEND`` / ``COMMENT`` operands.
  """
  escaped = value.replace('\\', '\\\\').replace('"', '\\"')
  return f'"{escaped}"'


def generate_bucket_sql(
  bucket: BucketDefinition,
  *,
  if_not_exists: bool = False,
  overwrite: bool = False,
) -> list[str]:
  """Generate the ``DEFINE BUCKET`` statement for a bucket.

  SurrealDB v3 grammar::

    DEFINE BUCKET [OVERWRITE | IF NOT EXISTS] <name>
      [BACKEND "<backend>"] [READONLY] [PERMISSIONS ...] [COMMENT "..."]

  Args:
    bucket: Bucket definition to generate SQL for
    if_not_exists: When True, adds ``IF NOT EXISTS`` for idempotent re-apply
    overwrite: When True, adds ``OVERWRITE`` (mutually exclusive with
      ``if_not_exists``; ``if_not_exists`` wins if both are set)

  Returns:
    List containing the single DEFINE BUCKET statement

  Examples:
    >>> from surql.schema.bucket import memory_bucket
    >>> generate_bucket_sql(memory_bucket('avatars'))[0]
    'DEFINE BUCKET avatars BACKEND "memory";'
  """
  if if_not_exists:
    prefix = ' IF NOT EXISTS'
  elif overwrite:
    prefix = ' OVERWRITE'
  else:
    prefix = ''

  sql = f'DEFINE BUCKET{prefix} {bucket.name} BACKEND {_surql_string(bucket.backend)}'

  if bucket.readonly:
    sql += ' READONLY'

  sql += _permissions_clause(bucket.permissions)

  if bucket.comment:
    sql += f' COMMENT {_surql_string(bucket.comment)}'

  sql += ';'
  return [sql]


def generate_remove_bucket_sql(
  bucket: BucketDefinition | str,
  *,
  if_exists: bool = False,
) -> list[str]:
  """Generate the ``REMOVE BUCKET`` statement for a bucket.

  Args:
    bucket: Bucket definition or bucket name
    if_exists: When True, adds ``IF EXISTS`` so removing an absent bucket is a
      no-op rather than an error

  Returns:
    List containing the single REMOVE BUCKET statement

  Examples:
    >>> generate_remove_bucket_sql('avatars')[0]
    'REMOVE BUCKET avatars;'
  """
  name = bucket.name if isinstance(bucket, BucketDefinition) else bucket
  ife = ' IF EXISTS' if if_exists else ''
  return [f'REMOVE BUCKET{ife} {name};']


def generate_alter_bucket_sql(
  old: BucketDefinition,
  new: BucketDefinition,
  *,
  if_exists: bool = False,
) -> list[str]:
  """Generate the ``ALTER BUCKET`` statement transforming ``old`` into ``new``.

  SurrealDB v3 grammar::

    ALTER BUCKET [IF EXISTS] <name>
      [READONLY | DROP READONLY]
      [BACKEND "<backend>" | DROP BACKEND]
      [PERMISSIONS ...]
      [COMMENT "..." | DROP COMMENT]

  Only the clauses whose values actually changed between ``old`` and ``new`` are
  emitted. When nothing changed, an empty list is returned.

  Args:
    old: Previous bucket definition
    new: Desired bucket definition (must share ``old.name``)
    if_exists: When True, adds ``IF EXISTS``

  Returns:
    List with a single ALTER BUCKET statement, or empty list if no change

  Examples:
    >>> from surql.schema.bucket import memory_bucket
    >>> a = memory_bucket('b')
    >>> b = memory_bucket('b', readonly=True)
    >>> generate_alter_bucket_sql(a, b)[0]
    'ALTER BUCKET b READONLY;'
  """
  ife = ' IF EXISTS' if if_exists else ''
  clauses: list[str] = []

  if old.readonly != new.readonly:
    clauses.append('READONLY' if new.readonly else 'DROP READONLY')

  if old.backend != new.backend:
    clauses.append(f'BACKEND {_surql_string(new.backend)}')

  if old.permissions != new.permissions:
    # SurrealDB has no DROP PERMISSIONS on a bucket; re-stating an empty
    # permissions clause is the closest analogue, but the common case is
    # supplying a new clause. When new is None we emit nothing for permissions.
    perm_clause = _permissions_clause(new.permissions)
    if perm_clause:
      clauses.append(perm_clause.lstrip())

  if old.comment != new.comment:
    clauses.append(f'COMMENT {_surql_string(new.comment)}' if new.comment else 'DROP COMMENT')

  if not clauses:
    return []

  return [f'ALTER BUCKET{ife} {new.name} {" ".join(clauses)};']


def generate_analyzer_sql(
  analyzer: AnalyzerDefinition,
  *,
  if_not_exists: bool = False,
) -> list[str]:
  """Generate the ``DEFINE ANALYZER`` statement for an analyzer.

  Validation runs first; an invalid definition raises ``ValueError``.

  Args:
    analyzer: Analyzer definition to generate SQL for
    if_not_exists: When True, adds IF NOT EXISTS for idempotent re-application

  Returns:
    List containing the single DEFINE ANALYZER statement

  Examples:
    >>> from surql.schema.analyzer import standard_analyzer
    >>> stmts = generate_analyzer_sql(standard_analyzer('text_en'))
    >>> stmts[0]
    'DEFINE ANALYZER text_en TOKENIZERS class FILTERS lowercase,ascii;'
  """
  analyzer.validate_definition()
  return [analyzer.to_surql_with_options(if_not_exists=if_not_exists)]


def generate_analyzer_sql_with_options(
  analyzer: AnalyzerDefinition,
  *,
  if_not_exists: bool = False,
) -> list[str]:
  """Generate the ``DEFINE ANALYZER`` statement, optionally with ``IF NOT EXISTS``.

  Alias of :func:`generate_analyzer_sql` kept for parity with the sibling ports
  (e.g. a persistent store applying its schema on every connect).

  Args:
    analyzer: Analyzer definition to generate SQL for
    if_not_exists: When True, adds IF NOT EXISTS for idempotent re-application

  Returns:
    List containing the single DEFINE ANALYZER statement
  """
  return generate_analyzer_sql(analyzer, if_not_exists=if_not_exists)


def generate_schema_sql(
  tables: dict[str, TableDefinition] | None = None,
  edges: dict[str, EdgeDefinition] | None = None,
  analyzers: dict[str, AnalyzerDefinition] | None = None,
  buckets: dict[str, BucketDefinition] | None = None,
  *,
  if_not_exists: bool = False,
) -> str:
  """Generate complete SurrealQL schema from the supplied definitions.

  Buckets and analyzers render first (a ``file`` field or full-text index can
  only reference one that already exists), then tables, then edges. Each
  definition block is separated by a blank line for readability.

  Args:
    tables: Dict of table name to TableDefinition
    edges: Dict of edge name to EdgeDefinition
    analyzers: Dict of analyzer name to AnalyzerDefinition (emitted before tables)
    buckets: Dict of bucket name to BucketDefinition (emitted before tables)
    if_not_exists: When True, adds IF NOT EXISTS to all DEFINE statements

  Returns:
    Complete SurrealQL schema as a single string

  Examples:
    >>> sql = generate_schema_sql(tables={'user': user_table}, edges={'likes': likes_edge})
  """
  all_statements: list[str] = []

  if buckets:
    for bucket in buckets.values():
      all_statements.extend(generate_bucket_sql(bucket, if_not_exists=if_not_exists))
      all_statements.append('')  # blank line between buckets

  if analyzers:
    for analyzer in analyzers.values():
      all_statements.extend(generate_analyzer_sql(analyzer, if_not_exists=if_not_exists))
      all_statements.append('')  # blank line between analyzers

  if tables:
    for table in tables.values():
      all_statements.extend(generate_table_sql(table, if_not_exists=if_not_exists))
      all_statements.append('')  # blank line between tables

  if edges:
    for edge in edges.values():
      all_statements.extend(generate_edge_sql(edge, if_not_exists=if_not_exists))
      all_statements.append('')

  return '\n'.join(all_statements).strip()
