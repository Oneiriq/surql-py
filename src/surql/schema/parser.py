"""Database schema parser.

This module parses SurrealDB INFO responses into TableDefinition objects.
This enables comparison between code-defined schemas and database schemas.

# Round-trip symmetry (1.6.2)

`parse_table_info` is the inverse of `surql.schema.sql._generate_field_sql`
and `surql.migration.diff._field_to_sql`. After a `migrate_up`, the
INFO FOR TABLE response stored by SurrealDB v3 differs textually from
what the emitter produced:

  emitted               -> stored / returned by INFO FOR TABLE
  TYPE option<X>        -> TYPE none | X            (option unfolded)
  TYPE option<record<Y>> -> TYPE none | record<Y>
  TYPE record<Y>         -> TYPE record<Y>          (unchanged)
  TYPE object FLEXIBLE   -> FLEXIBLE TYPE object    (FLEXIBLE clause moved)
  no PERMISSIONS         -> PERMISSIONS FULL        (per-field DB default)
  TYPE array             -> TYPE array + a separate <field>[*] (or <field>.*)
                            entry holding the element-type spec.

The parser normalises these back into the same FieldDefinition / TableDefinition
shape the emitter started from, so a fresh `diff_tables(parsed_db, code_table)`
returns `[]` when DB and code match. This is exercised by
`tests/test_diff_round_trip.py`.
"""

import re
from typing import Any

import structlog

from surql.schema.edge import EdgeDefinition, EdgeMode
from surql.schema.fields import FieldDefinition, FieldType
from surql.schema.table import (
  EventDefinition,
  HnswDistanceType,
  IndexDefinition,
  IndexType,
  MTreeDistanceType,
  MTreeVectorType,
  TableDefinition,
  TableMode,
)

logger = structlog.get_logger(__name__)


class SchemaParseError(Exception):
  """Error parsing database schema."""


# Tokens that mark the start of a top-level clause in a DEFINE FIELD statement.
# Used to slice the definition into (TYPE, ASSERT, DEFAULT, VALUE) bodies so
# each extractor only sees its own clause body — not a substring of a later
# clause or of the field name (which is what the pre-1.6.2 regexes did, hence
# the `Unsafe default value expression: 'ON ... TYPE ...'` crash on tables
# with a field named `default`).
#
# Keywords that may appear in either DEFINE FIELD or DEFINE TABLE position.
# Order matters only when one keyword is a prefix of another — none are here.
_FIELD_CLAUSE_KEYWORDS = (
  'TYPE',
  'ASSERT',
  'DEFAULT',
  'VALUE',
  'READONLY',
  'FLEXIBLE',
  'PERMISSIONS',
  'COMMENT',
)

# Pattern matching `record<TableName>` (and `record<TableName, ...>` if SurrealDB
# ever emits comma-separated record-link variants — we take the first target).
_RECORD_TYPE_PATTERN = re.compile(r'\brecord\s*<\s*([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE)

# Pattern matching a SurrealDB array sub-field entry — the per-element type
# spec that `INFO FOR TABLE` emits alongside the parent array field.
# Recognises both observed forms:
#   `unresolved_refs[*]`   (bracket form)
#   `jurisdiction.*`       (dot-star form)
_ARRAY_SUBFIELD_PATTERN = re.compile(r'(?:\[\*\]|\.\*)\s*$')


def parse_table_info(
  table_name: str,
  info: dict[str, Any],
  define_table: str | None = None,
) -> TableDefinition:
  """Parse SurrealDB INFO FOR TABLE response into TableDefinition.

  Args:
    table_name: Name of the table
    info: Raw INFO FOR TABLE response dictionary
    define_table: Optional ``DEFINE TABLE <name> ...`` statement string. When
      provided, used as the source of table-level mode + PERMISSIONS. Pass this
      when introspecting against SurrealDB v3, which does NOT include the
      table-level DEFINE statement in ``INFO FOR TABLE`` (only ``INFO FOR DB``'s
      ``tables.<name>`` dict carries it). Without this, table-level PERMISSIONS
      are silently lost on round-trip and every consumer that declares them
      sees a false-positive ``MODIFY_PERMISSIONS`` diff.

  Returns:
    Parsed TableDefinition

  Raises:
    SchemaParseError: If parsing fails

  Examples:
    >>> # SurrealDB v2 — tb is inside INFO FOR TABLE
    >>> info = await client.execute(f'INFO FOR TABLE {table_name};')
    >>> table_def = parse_table_info(table_name, info[0]['result'])

    >>> # SurrealDB v3 — fetch the DEFINE TABLE string from INFO FOR DB and
    >>> # pass it alongside the per-table info dict.
    >>> db_info = await client.execute('INFO FOR DB;')
    >>> info = await client.execute(f'INFO FOR TABLE {table_name};')
    >>> table_def = parse_table_info(
    ...     table_name,
    ...     info[0]['result'],
    ...     define_table=db_info[0]['result']['tables'].get(table_name),
    ... )
  """
  try:
    logger.debug('parsing_table_info', table=table_name)

    # Caller-supplied DEFINE TABLE statement wins; fall back to the
    # legacy `tb` key inside the INFO FOR TABLE response (SurrealDB v2 shape).
    tb_definition = define_table if define_table is not None else info.get('tb', '')

    # Parse table mode from tb field
    mode = _parse_table_mode(tb_definition)

    # Parse table-level permissions from tb field (1.6.2 — previously
    # always `None`, which made every PERMISSIONS-bearing code table
    # report a false-positive `MODIFY_PERMISSIONS` diff).
    permissions = _parse_table_permissions(tb_definition)

    # Parse fields - support both 'fields' and 'fd' keys.
    # Skip array sub-field entries (`<field>[*]` / `<field>.*`) — they're
    # part of the parent array type spec, not standalone fields, so the
    # diff should NOT see them as orphan drops.
    fields_dict = info.get('fields') or info.get('fd') or {}
    fields = _parse_fields(fields_dict)

    # Parse indexes - support both 'indexes' and 'ix' keys
    indexes_dict = info.get('indexes') or info.get('ix') or {}
    indexes = _parse_indexes(indexes_dict)

    # Parse events - support both 'events' and 'ev' keys
    events_dict = info.get('events') or info.get('ev') or {}
    events = _parse_events(events_dict)

    return TableDefinition(
      name=table_name,
      mode=mode,
      fields=fields,
      indexes=indexes,
      events=events,
      permissions=permissions,
    )

  except Exception as e:
    logger.error('parse_table_info_failed', table=table_name, error=str(e))
    raise SchemaParseError(f'Failed to parse table {table_name}: {e}') from e


def parse_edge_info(
  edge_name: str,
  info: dict[str, Any],
  define_table: str | None = None,
) -> EdgeDefinition:
  """Parse an edge table's ``INFO FOR TABLE`` response into an EdgeDefinition.

  Counterpart to :func:`parse_table_info` for graph-edge tables defined via
  ``edge_schema`` / :class:`~surql.schema.edge.EdgeDefinition`. Edges round-trip
  through SurrealDB as regular tables in ``INFO FOR DB.tables``; the only thing
  that makes them edges is the ``TYPE RELATION FROM <x> TO <y>`` clause on the
  ``DEFINE TABLE`` statement. Without an edge-aware parser, a drift detector
  using :func:`parse_table_info` against an edge table would see it as a
  SCHEMALESS table missing every field-level diff signal an edge expects (mode,
  from/to constraints, auto ``in``/``out`` proxies).

  Args:
    edge_name: Edge table name.
    info: Raw ``INFO FOR TABLE`` response dictionary (same shape as the
      ``parse_table_info`` input — typically the inner result dict).
    define_table: Optional ``DEFINE TABLE <name> ...`` statement string. When
      provided, the source of edge mode + FROM/TO + PERMISSIONS. Pass this on
      SurrealDB v3, where ``INFO FOR TABLE`` does not include the table-level
      DEFINE — only ``INFO FOR DB``'s ``tables.<name>`` dict does.

  Returns:
    Parsed EdgeDefinition with mode, from_table, to_table, fields, indexes,
    events, and permissions populated. For ``RELATION``-mode edges the auto
    ``in`` and ``out`` fields SurrealDB emits are skipped (they are implicit
    when ``TYPE RELATION`` is set, so the code-side EdgeDefinition does not
    declare them either).

  Raises:
    SchemaParseError: If parsing fails.

  Examples:
    >>> db_info = await client.execute('INFO FOR DB;')
    >>> info = await client.execute(f'INFO FOR TABLE {edge_name};')
    >>> edge_def = parse_edge_info(
    ...     edge_name,
    ...     info[0]['result'],
    ...     define_table=db_info[0]['result']['tables'].get(edge_name),
    ... )
  """
  try:
    logger.debug('parsing_edge_info', edge=edge_name)

    tb_definition = define_table if define_table is not None else info.get('tb', '')

    mode = _parse_edge_mode(tb_definition)
    from_table, to_table = _parse_edge_from_to(tb_definition)
    permissions = _parse_table_permissions(tb_definition)

    fields_dict = info.get('fields') or info.get('fd') or {}
    fields = _parse_fields(fields_dict)
    # `in` and `out` are auto-emitted by SurrealDB for TYPE RELATION edges and
    # are not part of the code-side EdgeDefinition's `fields` list. Strip them
    # so round-trip diffs do not flag them as orphan additions.
    if mode == EdgeMode.RELATION:
      fields = [f for f in fields if f.name not in ('in', 'out')]

    indexes_dict = info.get('indexes') or info.get('ix') or {}
    indexes = _parse_indexes(indexes_dict)

    events_dict = info.get('events') or info.get('ev') or {}
    events = _parse_events(events_dict)

    return EdgeDefinition(
      name=edge_name,
      mode=mode,
      from_table=from_table,
      to_table=to_table,
      fields=fields,
      indexes=indexes,
      events=events,
      permissions=permissions,
    )

  except Exception as e:
    logger.error('parse_edge_info_failed', edge=edge_name, error=str(e))
    raise SchemaParseError(f'Failed to parse edge {edge_name}: {e}') from e


# Pattern matching `TYPE RELATION` (case-insensitive, word-boundary anchored).
_TYPE_RELATION_PATTERN = re.compile(r'\bTYPE\s+RELATION\b', re.IGNORECASE)

# Pattern matching `FROM <ident>` and `TO <ident>` in a DEFINE TABLE statement.
# Identifier characters mirror SurrealDB's table-name grammar (letter/digit/_).
_EDGE_FROM_PATTERN = re.compile(r'\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)', re.IGNORECASE)
_EDGE_TO_PATTERN = re.compile(r'\bTO\s+([A-Za-z_][A-Za-z0-9_]*)', re.IGNORECASE)


def _parse_edge_mode(tb_definition: str) -> EdgeMode:
  """Return EdgeMode for an edge `DEFINE TABLE` string.

  `TYPE RELATION` wins. If absent, fall back to the same SCHEMAFULL/SCHEMALESS
  keyword check `_parse_table_mode` uses; SCHEMALESS is the default when no
  keyword is present.
  """
  if not tb_definition:
    return EdgeMode.SCHEMALESS

  if _TYPE_RELATION_PATTERN.search(tb_definition):
    return EdgeMode.RELATION

  definition_upper = tb_definition.upper()
  if 'SCHEMAFULL' in definition_upper:
    return EdgeMode.SCHEMAFULL
  return EdgeMode.SCHEMALESS


def _parse_edge_from_to(tb_definition: str) -> tuple[str | None, str | None]:
  """Extract `FROM <table>` and `TO <table>` from an edge DEFINE TABLE string.

  Returns ``(None, None)`` for non-RELATION edges or if either clause is
  missing. The emitter requires both for RELATION mode, but this parser stays
  permissive on read so a malformed live definition surfaces as missing-clause
  drift instead of a parse failure.
  """
  if not tb_definition:
    return (None, None)
  from_match = _EDGE_FROM_PATTERN.search(tb_definition)
  to_match = _EDGE_TO_PATTERN.search(tb_definition)
  return (
    from_match.group(1) if from_match else None,
    to_match.group(1) if to_match else None,
  )


def _parse_table_mode(tb_definition: str) -> TableMode:
  """Parse table mode from DEFINE TABLE statement.

  Args:
    tb_definition: DEFINE TABLE statement string

  Returns:
    TableMode enum value
  """
  if not tb_definition:
    return TableMode.SCHEMALESS

  definition_upper = tb_definition.upper()

  if 'SCHEMAFULL' in definition_upper:
    return TableMode.SCHEMAFULL
  if 'SCHEMALESS' in definition_upper:
    return TableMode.SCHEMALESS
  if 'DROP' in definition_upper:
    return TableMode.DROP

  return TableMode.SCHEMALESS


def _parse_table_permissions(tb_definition: str) -> dict[str, str] | None:
  """Extract per-action PERMISSIONS clauses from a DEFINE TABLE statement.

  Recognises three shapes SurrealDB v3 emits:

  - ``PERMISSIONS NONE`` → returns ``None`` (no per-action rules: the table
    has the default-deny posture, which is the same shape the code-side
    helper emits when no ``permissions=`` kwarg was passed).
  - ``PERMISSIONS FULL`` → returns ``None`` for the same reason (the code-side
    helper has no representation for "all actions = full"; this normalisation
    avoids a permanent false-positive `MODIFY_PERMISSIONS` diff on every table
    whose code declaration omitted permissions).
  - ``PERMISSIONS FOR select WHERE <r1> FOR create WHERE <r2> ...`` (expanded
    form) → returns ``{'select': '<r1>', 'create': '<r2>', ...}``.
  - ``PERMISSIONS FOR select, create, update, delete WHERE <rule>`` (compact
    comma-joined form — what SurrealDB v3's emitter actually produces when
    multiple actions share a single rule) → returns
    ``{'select': '<rule>', 'create': '<rule>', 'update': '<rule>', 'delete': '<rule>'}``.
  - Mixed forms (some actions grouped via comma, others split) are handled
    by parsing each ``FOR <action-list> WHERE <rule>`` clause independently
    and exploding the action list into per-action entries.

  Returns ``None`` when no PERMISSIONS clause is present.
  """
  if not tb_definition:
    return None

  # Locate the PERMISSIONS clause body (everything after the keyword up to end-of-string
  # or to a trailing semicolon).
  perm_match = re.search(
    r'\bPERMISSIONS\b(.*?)(?:\s*;|\s*$)',
    tb_definition,
    re.IGNORECASE | re.DOTALL,
  )
  if not perm_match:
    return None

  body = perm_match.group(1).strip()
  if not body:
    return None

  # Bare `NONE` / `FULL` → no per-action rules to compare. Code-side
  # `permissions=None` is the canonical match.
  if body.upper() in ('NONE', 'FULL'):
    return None

  # Per-action form: each clause is `FOR <action-list> WHERE <rule>` where
  # `<action-list>` is one or more comma-separated `select|create|update|delete`
  # keywords. Capture both list-of-actions and rule, then explode.
  rules: dict[str, str] = {}
  for action_match in re.finditer(
    r'\bFOR\s+((?:select|create|update|delete)(?:\s*,\s*(?:select|create|update|delete))*)\s+WHERE\s+(.*?)(?=\s+FOR\s+(?:select|create|update|delete)\b|\s*;|\s*$)',
    body,
    re.IGNORECASE | re.DOTALL,
  ):
    actions_raw = action_match.group(1)
    rule = action_match.group(2).strip()
    for action in re.split(r'\s*,\s*', actions_raw):
      rules[action.lower()] = rule

  return rules or None


def _parse_fields(fd_dict: dict[str, str]) -> list[FieldDefinition]:
  """Parse field definitions from fd dictionary.

  Args:
    fd_dict: Dictionary of field name to DEFINE FIELD statement

  Returns:
    List of FieldDefinition objects (sub-field array element entries
    like `<field>[*]` and `<field>.*` are skipped — they're part of
    the parent array's type spec, not standalone fields).
  """
  fields = []

  for field_name, definition in fd_dict.items():
    if _is_array_subfield_name(field_name):
      logger.debug('skipping_array_subfield', field=field_name)
      continue
    try:
      field_def = _parse_field_definition(field_name, definition)
      if field_def:
        fields.append(field_def)
    except Exception as e:
      logger.warning('field_parse_warning', field=field_name, error=str(e))

  return fields


def _is_array_subfield_name(name: str) -> bool:
  """Return True for SurrealDB array sub-field entries.

  SurrealDB v3 reports the per-element type spec for an array field as
  its own entry in the `INFO FOR TABLE` `fields` dict, with names like
  `unresolved_refs[*]` or `jurisdiction.*`. These are NOT standalone
  fields — they're the typed-element spec for the parent array — and
  the diff should not treat them as orphan drops.
  """
  return bool(_ARRAY_SUBFIELD_PATTERN.search(name))


def _parse_field_definition(field_name: str, definition: str) -> FieldDefinition | None:
  """Parse a single field definition.

  Args:
    field_name: Field name
    definition: DEFINE FIELD statement

  Returns:
    FieldDefinition or None if parsing fails
  """
  if not definition:
    return None

  logger.debug('parsing_field', field=field_name, definition=definition)

  # Slice the DEFINE FIELD statement into clause bodies so each extractor
  # only sees its own clause text — never a substring of the field name
  # nor a token from a later clause. This is the 1.6.2 root-cause fix for
  # the `Unsafe default value expression: 'ON ... TYPE ...'` crash on
  # tables with a field named `default` (and similarly-named hazards).
  clauses = _split_field_clauses(definition)

  type_clause = clauses.get('TYPE', '')
  field_type, nullable, target_table = _parse_type_clause(type_clause)

  assertion = clauses.get('ASSERT') or None
  default = clauses.get('DEFAULT') or None
  value = clauses.get('VALUE') or None

  # READONLY / FLEXIBLE are flag clauses (no body). `clauses` includes
  # them as empty strings if they were present; absent keys mean the
  # flag was not in the definition.
  readonly = 'READONLY' in clauses
  flexible = 'FLEXIBLE' in clauses

  return FieldDefinition(
    name=field_name,
    type=field_type,
    assertion=assertion,
    default=default,
    value=value,
    readonly=readonly,
    flexible=flexible,
    nullable=nullable,
    target_table=target_table,
  )


def _split_field_clauses(definition: str) -> dict[str, str]:
  """Split a DEFINE FIELD statement into clause bodies keyed by keyword.

  Returns a dict like ``{'TYPE': 'none | record<x>', 'PERMISSIONS': 'FULL'}``
  containing each clause body verbatim (trailing whitespace stripped).

  Implementation note: we scan for clause keywords as standalone tokens
  (delimited by whitespace or the start/end of the definition string)
  AFTER skipping past the `DEFINE FIELD <name> ON [TABLE] <table>` prefix.
  This ensures `default` as a field name does not match the DEFAULT clause
  keyword, and that `<name>.*` and `[*]` sub-fields are handled by the
  caller's name filter rather than blowing up here.

  Flag-only clauses (READONLY, FLEXIBLE) get an empty-string body but ARE
  present in the returned dict so callers can use `key in clauses` to test
  presence.
  """
  # Find the end of the `ON [TABLE] <name>` prefix so we don't scan the
  # field/table name itself for clause keywords. SurrealDB v3 emits
  # `DEFINE FIELD <name> ON <table>` (no `TABLE` token); older SurrealDB
  # / our own emitter writes `ON TABLE <name>`. Skip past either form.
  prefix_match = re.match(
    r'\s*DEFINE\s+FIELD\s+\S+\s+ON\s+(?:TABLE\s+)?\S+\s*',
    definition,
    re.IGNORECASE,
  )
  scan_start = prefix_match.end() if prefix_match else 0
  body = definition[scan_start:]

  # Locate each top-level clause keyword (word-boundary anchored) and
  # record (keyword, start_index_of_body, end_index_of_keyword).
  keyword_alt = '|'.join(_FIELD_CLAUSE_KEYWORDS)
  matches = list(re.finditer(rf'\b({keyword_alt})\b', body, re.IGNORECASE))

  clauses: dict[str, str] = {}
  for i, m in enumerate(matches):
    keyword = m.group(1).upper()
    body_start = m.end()
    body_end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
    clause_body = body[body_start:body_end].strip()
    # Drop a trailing semicolon if present (only meaningful at the very end).
    if clause_body.endswith(';'):
      clause_body = clause_body[:-1].rstrip()
    # Last-clause-wins is intentional: SurrealDB never emits the same
    # top-level clause twice, so duplicates would indicate a parse bug.
    clauses[keyword] = clause_body

  return clauses


def _parse_type_clause(
  type_clause: str,
) -> tuple[FieldType, bool, str | None]:
  """Parse the body of a TYPE clause into (field_type, nullable, target_table).

  Recognises:
    `string`                     -> (STRING, False, None)
    `option<string>`             -> (STRING, True, None)
    `none | string`              -> (STRING, True, None)
    `record<user>`               -> (RECORD, False, 'user')
    `option<record<user>>`       -> (RECORD, True, 'user')
    `none | record<user>`        -> (RECORD, True, 'user')
    `object`                     -> (OBJECT, False, None)
    empty / unknown              -> (ANY, False, None)
  """
  if not type_clause:
    return FieldType.ANY, False, None

  type_str = type_clause.strip()
  nullable = False

  # `option<X>` -> X, nullable
  option_match = re.match(r'\s*option\s*<\s*(.+)\s*>\s*$', type_str, re.IGNORECASE)
  if option_match:
    nullable = True
    type_str = option_match.group(1).strip()

  # `none | X` and `X | none` -> X, nullable. SurrealDB v3 stores
  # `option<X>` as `none | X`; the emitter writes `option<X>` — both must
  # parse to the same field type so round-trip diffs are clean.
  union_parts = [p.strip() for p in re.split(r'\s*\|\s*', type_str) if p.strip()]
  if any(p.lower() == 'none' for p in union_parts):
    nullable = True
    non_none = [p for p in union_parts if p.lower() != 'none']
    if non_none:
      # Pick the first non-none branch as the canonical type. Multi-branch
      # unions other than `none | X` aren't representable in the current
      # FieldDefinition model — fall back to ANY for safety.
      if len(non_none) == 1:
        type_str = non_none[0]
      else:
        return FieldType.ANY, nullable, None

  # `record<target>` -> RECORD with target_table=target
  record_match = _RECORD_TYPE_PATTERN.match(type_str)
  if record_match:
    return FieldType.RECORD, nullable, record_match.group(1)

  # Bare type name — match the leading word.
  type_word_match = re.match(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)', type_str)
  if not type_word_match:
    return FieldType.ANY, nullable, None

  return _field_type_from_word(type_word_match.group(1)), nullable, None


def _field_type_from_word(type_word: str) -> FieldType:
  """Map a SurrealDB type word to a FieldType enum value."""
  type_mapping = {
    'string': FieldType.STRING,
    'int': FieldType.INT,
    'float': FieldType.FLOAT,
    'bool': FieldType.BOOL,
    'datetime': FieldType.DATETIME,
    'duration': FieldType.DURATION,
    'decimal': FieldType.DECIMAL,
    'number': FieldType.NUMBER,
    'object': FieldType.OBJECT,
    'array': FieldType.ARRAY,
    'record': FieldType.RECORD,
    'geometry': FieldType.GEOMETRY,
    'any': FieldType.ANY,
  }
  return type_mapping.get(type_word.lower(), FieldType.ANY)


def _parse_indexes(ix_dict: dict[str, str]) -> list[IndexDefinition]:
  """Parse index definitions from ix dictionary.

  Args:
    ix_dict: Dictionary of index name to DEFINE INDEX statement

  Returns:
    List of IndexDefinition objects
  """
  indexes = []

  for index_name, definition in ix_dict.items():
    try:
      index_def = _parse_index_definition(index_name, definition)
      if index_def:
        indexes.append(index_def)
    except Exception as e:
      logger.warning('index_parse_warning', index=index_name, error=str(e))

  return indexes


def _parse_index_definition(index_name: str, definition: str) -> IndexDefinition | None:
  """Parse a single index definition.

  Args:
    index_name: Index name
    definition: DEFINE INDEX statement

  Returns:
    IndexDefinition or None if parsing fails
  """
  if not definition:
    return None

  logger.debug('parsing_index', index=index_name, definition=definition)

  # Extract columns
  columns = _extract_index_columns(definition)
  if not columns:
    columns = _extract_index_fields(definition)

  # Determine index type
  index_type = _extract_index_type(definition)

  # For MTREE indexes, extract additional parameters
  dimension = None
  distance = None
  vector_type = None

  if index_type == IndexType.MTREE:
    dimension = _extract_mtree_dimension(definition)
    distance = _extract_mtree_distance(definition)
    vector_type = _extract_mtree_vector_type(definition)

  # For HNSW indexes, extract additional parameters
  hnsw_distance = None
  efc = None
  m = None

  if index_type == IndexType.HNSW:
    dimension = _extract_mtree_dimension(definition)
    vector_type = _extract_mtree_vector_type(definition)
    hnsw_distance = _extract_hnsw_distance(definition)
    efc = _extract_hnsw_efc(definition)
    m = _extract_hnsw_m(definition)

  return IndexDefinition(
    name=index_name,
    columns=columns,
    type=index_type,
    dimension=dimension,
    distance=distance,
    vector_type=vector_type,
    hnsw_distance=hnsw_distance,
    efc=efc,
    m=m,
  )


def _extract_index_columns(definition: str) -> list[str]:
  """Extract COLUMNS from DEFINE INDEX statement.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    List of column names
  """
  # Match COLUMNS followed by comma-separated column names
  columns_pattern = r'COLUMNS\s+([^;]+?)(?:UNIQUE|SEARCH|HNSW|MTREE|\s*;|\s*$)'
  match = re.search(columns_pattern, definition, re.IGNORECASE)

  if match:
    columns_str = match.group(1).strip()
    columns = [c.strip() for c in columns_str.split(',')]
    return [c for c in columns if c]

  return []


def _extract_index_fields(definition: str) -> list[str]:
  """Extract FIELDS from DEFINE INDEX statement (alternative syntax).

  Args:
    definition: DEFINE INDEX statement

  Returns:
    List of field names
  """
  # Match FIELDS followed by comma-separated field names
  fields_pattern = r'FIELDS\s+([^;]+?)(?:UNIQUE|SEARCH|HNSW|MTREE|\s*;|\s*$)'
  match = re.search(fields_pattern, definition, re.IGNORECASE)

  if match:
    fields_str = match.group(1).strip()
    fields = [f.strip() for f in fields_str.split(',')]
    return [f for f in fields if f]

  return []


def _extract_index_type(definition: str) -> IndexType:
  """Extract index type from DEFINE INDEX statement.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    IndexType enum value
  """
  definition_upper = definition.upper()

  if 'UNIQUE' in definition_upper:
    return IndexType.UNIQUE
  if 'SEARCH' in definition_upper:
    return IndexType.SEARCH
  if 'HNSW' in definition_upper:
    return IndexType.HNSW
  if 'MTREE' in definition_upper:
    return IndexType.MTREE

  return IndexType.STANDARD


def _extract_mtree_dimension(definition: str) -> int | None:
  """Extract DIMENSION from MTREE index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    Dimension value or None
  """
  dim_pattern = r'DIMENSION\s+(\d+)'
  match = re.search(dim_pattern, definition, re.IGNORECASE)

  if match:
    return int(match.group(1))

  return None


def _extract_mtree_distance(definition: str) -> MTreeDistanceType | None:
  """Extract DIST/DISTANCE from MTREE index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    MTreeDistanceType or None
  """
  dist_pattern = r'(?:DIST|DISTANCE)\s+(\w+)'
  match = re.search(dist_pattern, definition, re.IGNORECASE)

  if not match:
    return None

  dist_str = match.group(1).upper()

  distance_mapping = {
    'COSINE': MTreeDistanceType.COSINE,
    'EUCLIDEAN': MTreeDistanceType.EUCLIDEAN,
    'MANHATTAN': MTreeDistanceType.MANHATTAN,
    'MINKOWSKI': MTreeDistanceType.MINKOWSKI,
  }

  return distance_mapping.get(dist_str)


def _extract_mtree_vector_type(definition: str) -> MTreeVectorType | None:
  """Extract TYPE from MTREE index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    MTreeVectorType or None
  """
  # MTREE index has type for vector component type
  type_pattern = r'TYPE\s+(\w+)'
  match = re.search(type_pattern, definition, re.IGNORECASE)

  if not match:
    return None

  type_str = match.group(1).upper()

  type_mapping = {
    'F64': MTreeVectorType.F64,
    'F32': MTreeVectorType.F32,
    'I64': MTreeVectorType.I64,
    'I32': MTreeVectorType.I32,
    'I16': MTreeVectorType.I16,
  }

  return type_mapping.get(type_str)


def _extract_hnsw_distance(definition: str) -> HnswDistanceType | None:
  """Extract DIST/DISTANCE from HNSW index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    HnswDistanceType or None
  """
  dist_pattern = r'(?:DIST|DISTANCE)\s+(\w+)'
  match = re.search(dist_pattern, definition, re.IGNORECASE)

  if not match:
    return None

  dist_str = match.group(1).upper()

  distance_mapping = {
    'CHEBYSHEV': HnswDistanceType.CHEBYSHEV,
    'COSINE': HnswDistanceType.COSINE,
    'EUCLIDEAN': HnswDistanceType.EUCLIDEAN,
    'HAMMING': HnswDistanceType.HAMMING,
    'JACCARD': HnswDistanceType.JACCARD,
    'MANHATTAN': HnswDistanceType.MANHATTAN,
    'MINKOWSKI': HnswDistanceType.MINKOWSKI,
    'PEARSON': HnswDistanceType.PEARSON,
  }

  return distance_mapping.get(dist_str)


def _extract_hnsw_efc(definition: str) -> int | None:
  """Extract EFC from HNSW index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    EFC value or None
  """
  efc_pattern = r'EFC\s+(\d+)'
  match = re.search(efc_pattern, definition, re.IGNORECASE)

  if match:
    return int(match.group(1))

  return None


def _extract_hnsw_m(definition: str) -> int | None:
  """Extract M from HNSW index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    M value or None
  """
  m_pattern = r'\bM\s+(\d+)'
  match = re.search(m_pattern, definition, re.IGNORECASE)

  if match:
    return int(match.group(1))

  return None


def _parse_events(ev_dict: dict[str, str]) -> list[EventDefinition]:
  """Parse event definitions from ev dictionary.

  Args:
    ev_dict: Dictionary of event name to DEFINE EVENT statement

  Returns:
    List of EventDefinition objects
  """
  events = []

  for event_name, definition in ev_dict.items():
    try:
      event_def = _parse_event_definition(event_name, definition)
      if event_def:
        events.append(event_def)
    except Exception as e:
      logger.warning('event_parse_warning', event=event_name, error=str(e))

  return events


def _parse_event_definition(event_name: str, definition: str) -> EventDefinition | None:
  """Parse a single event definition.

  Args:
    event_name: Event name
    definition: DEFINE EVENT statement

  Returns:
    EventDefinition or None if parsing fails
  """
  if not definition:
    return None

  logger.debug('parsing_event', event=event_name, definition=definition)

  condition = _extract_event_condition(definition)
  action = _extract_event_action(definition)

  if not condition or not action:
    return None

  return EventDefinition(
    name=event_name,
    condition=condition,
    action=action,
  )


def _extract_event_condition(definition: str) -> str | None:
  """Extract WHEN condition from DEFINE EVENT statement.

  Args:
    definition: DEFINE EVENT statement

  Returns:
    Condition expression or None
  """
  # Match WHEN followed by condition
  when_pattern = r'WHEN\s+(.+?)\s+THEN'
  match = re.search(when_pattern, definition, re.IGNORECASE | re.DOTALL)

  if match:
    return match.group(1).strip()

  return None


def _extract_event_action(definition: str) -> str | None:
  """Extract THEN action from DEFINE EVENT statement.

  Args:
    definition: DEFINE EVENT statement

  Returns:
    Action expression or None
  """
  # Match THEN followed by action (can include braces)
  then_pattern = r'THEN\s+(?:\{(.+?)\}|(.+?))(?:\s*;|\s*$)'
  match = re.search(then_pattern, definition, re.IGNORECASE | re.DOTALL)

  if match:
    # Return whichever group matched
    action = match.group(1) or match.group(2)
    return action.strip() if action else None

  return None


def parse_db_info(info: dict[str, Any]) -> dict[str, TableDefinition]:
  """Parse SurrealDB INFO FOR DB response into table definitions.

  Args:
    info: Raw INFO FOR DB response dictionary

  Returns:
    Dictionary of table name to TableDefinition

  Examples:
    >>> info = await client.execute('INFO FOR DB;')
    >>> tables = parse_db_info(info[0]['result'])
  """
  tables = {}

  # Extract tables from tb field
  tb_dict = info.get('tb', {})

  for table_name, definition in tb_dict.items():
    try:
      # We need to fetch additional info for each table
      # For now, create a basic definition
      mode = _parse_table_mode(definition)
      tables[table_name] = TableDefinition(
        name=table_name,
        mode=mode,
      )
    except Exception as e:
      logger.warning('table_parse_warning', table=table_name, error=str(e))

  return tables
