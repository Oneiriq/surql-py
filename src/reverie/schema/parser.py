"""Database schema parser.

This module parses SurrealDB INFO responses into TableDefinition objects.
This enables comparison between code-defined schemas and database schemas.
"""

import re
from typing import Any

import structlog

from reverie.schema.fields import FieldDefinition, FieldType
from reverie.schema.table import (
  EventDefinition,
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


def parse_table_info(
  table_name: str,
  info: dict[str, Any],
) -> TableDefinition:
  """Parse SurrealDB INFO FOR TABLE response into TableDefinition.

  Args:
    table_name: Name of the table
    info: Raw INFO FOR TABLE response dictionary

  Returns:
    Parsed TableDefinition

  Raises:
    SchemaParseError: If parsing fails

  Examples:
    >>> info = await client.execute(f'INFO FOR TABLE {table_name};')
    >>> table_def = parse_table_info(table_name, info[0]['result'])
  """
  try:
    logger.debug('parsing_table_info', table=table_name)

    # Parse table mode from tb field
    mode = _parse_table_mode(info.get('tb', ''))

    # Parse fields - support both 'fields' and 'fd' keys
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
      permissions=None,  # Permissions are complex to parse, leave for later
    )

  except Exception as e:
    logger.error('parse_table_info_failed', table=table_name, error=str(e))
    raise SchemaParseError(f'Failed to parse table {table_name}: {e}') from e


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


def _parse_fields(fd_dict: dict[str, str]) -> list[FieldDefinition]:
  """Parse field definitions from fd dictionary.

  Args:
    fd_dict: Dictionary of field name to DEFINE FIELD statement

  Returns:
    List of FieldDefinition objects
  """
  fields = []

  for field_name, definition in fd_dict.items():
    try:
      field_def = _parse_field_definition(field_name, definition)
      if field_def:
        fields.append(field_def)
    except Exception as e:
      logger.warning('field_parse_warning', field=field_name, error=str(e))

  return fields


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

  field_type = _extract_field_type(definition)
  assertion = _extract_assertion(definition)
  default = _extract_default(definition)
  value = _extract_value(definition)
  readonly = _extract_readonly(definition)
  flexible = _extract_flexible(definition)

  return FieldDefinition(
    name=field_name,
    type=field_type,
    assertion=assertion,
    default=default,
    value=value,
    readonly=readonly,
    flexible=flexible,
  )


def _extract_field_type(definition: str) -> FieldType:
  """Extract field type from DEFINE FIELD statement.

  Args:
    definition: DEFINE FIELD statement

  Returns:
    FieldType enum value
  """
  # Match TYPE keyword followed by type name
  type_pattern = r'TYPE\s+(\w+)'
  match = re.search(type_pattern, definition, re.IGNORECASE)

  if not match:
    return FieldType.ANY

  type_str = match.group(1).lower()

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

  return type_mapping.get(type_str, FieldType.ANY)


def _extract_assertion(definition: str) -> str | None:
  """Extract ASSERT clause from definition.

  Args:
    definition: DEFINE FIELD statement

  Returns:
    Assertion expression or None
  """
  # Match ASSERT followed by the assertion expression
  assert_pattern = r'ASSERT\s+(.+?)(?:DEFAULT|VALUE|READONLY|FLEXIBLE|PERMISSIONS|\s*;|\s*$)'
  match = re.search(assert_pattern, definition, re.IGNORECASE | re.DOTALL)

  if match:
    return match.group(1).strip()

  # Simpler pattern if no following keyword
  assert_pattern_simple = r'ASSERT\s+(.+?)(?:\s*;|\s*$)'
  match = re.search(assert_pattern_simple, definition, re.IGNORECASE | re.DOTALL)

  if match:
    return match.group(1).strip()

  return None


def _extract_default(definition: str) -> str | None:
  """Extract DEFAULT clause from definition.

  Args:
    definition: DEFINE FIELD statement

  Returns:
    Default expression or None
  """
  # Match DEFAULT followed by the default value
  default_pattern = r'DEFAULT\s+(.+?)(?:VALUE|READONLY|FLEXIBLE|PERMISSIONS|ASSERT|\s*;|\s*$)'
  match = re.search(default_pattern, definition, re.IGNORECASE | re.DOTALL)

  if match:
    return match.group(1).strip()

  return None


def _extract_value(definition: str) -> str | None:
  """Extract VALUE clause (computed field) from definition.

  Args:
    definition: DEFINE FIELD statement

  Returns:
    Value expression or None
  """
  # Match VALUE followed by the computed value
  value_pattern = r'VALUE\s+(.+?)(?:DEFAULT|READONLY|FLEXIBLE|PERMISSIONS|ASSERT|\s*;|\s*$)'
  match = re.search(value_pattern, definition, re.IGNORECASE | re.DOTALL)

  if match:
    return match.group(1).strip()

  return None


def _extract_readonly(definition: str) -> bool:
  """Check if field is readonly.

  Args:
    definition: DEFINE FIELD statement

  Returns:
    True if readonly, False otherwise
  """
  return bool(re.search(r'\bREADONLY\b', definition, re.IGNORECASE))


def _extract_flexible(definition: str) -> bool:
  """Check if field is flexible.

  Args:
    definition: DEFINE FIELD statement

  Returns:
    True if flexible, False otherwise
  """
  return bool(re.search(r'\bFLEXIBLE\b', definition, re.IGNORECASE))


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

  return IndexDefinition(
    name=index_name,
    columns=columns,
    type=index_type,
    dimension=dimension,
    distance=distance,
    vector_type=vector_type,
  )


def _extract_index_columns(definition: str) -> list[str]:
  """Extract COLUMNS from DEFINE INDEX statement.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    List of column names
  """
  # Match COLUMNS followed by comma-separated column names
  columns_pattern = r'COLUMNS\s+([^;]+?)(?:UNIQUE|SEARCH|MTREE|\s*;|\s*$)'
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
  fields_pattern = r'FIELDS\s+([^;]+?)(?:UNIQUE|SEARCH|MTREE|\s*;|\s*$)'
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
