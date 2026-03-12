"""SQL generation from schema definitions.

Generates SurrealQL DEFINE statements from TableDefinition, EdgeDefinition,
and AccessDefinition objects. This enables consumers to create database schemas
directly from reverie schema definitions without using the migration system.
"""

from reverie.schema.access import AccessDefinition, AccessType
from reverie.schema.edge import EdgeDefinition, EdgeMode
from reverie.schema.fields import FieldDefinition
from reverie.schema.table import (
  EventDefinition,
  IndexDefinition,
  IndexType,
  TableDefinition,
)


def _generate_field_sql(table_name: str, field_def: FieldDefinition) -> str:
  """Generate DEFINE FIELD statement for a single field.

  Args:
    table_name: Name of the table
    field_def: Field definition

  Returns:
    SurrealQL DEFINE FIELD statement
  """
  sql = f'DEFINE FIELD {field_def.name} ON TABLE {table_name} TYPE {field_def.type.value}'

  if field_def.assertion:
    sql += f' ASSERT {field_def.assertion}'

  if field_def.default:
    sql += f' DEFAULT {field_def.default}'

  if field_def.value:
    sql += f' VALUE {field_def.value}'

  if field_def.readonly:
    sql += ' READONLY'

  if field_def.flexible:
    sql += ' FLEXIBLE'

  sql += ';'
  return sql


def _generate_index_sql(table_name: str, index_def: IndexDefinition) -> str:
  """Generate DEFINE INDEX statement for a single index.

  Args:
    table_name: Name of the table
    index_def: Index definition

  Returns:
    SurrealQL DEFINE INDEX statement
  """
  columns = ', '.join(index_def.columns)

  if index_def.type == IndexType.MTREE:
    field_name = index_def.columns[0] if index_def.columns else ''
    sql = (
      f'DEFINE INDEX {index_def.name} ON TABLE {table_name}'
      f' COLUMNS {field_name} MTREE DIMENSION {index_def.dimension}'
    )
    if index_def.distance:
      sql += f' DIST {index_def.distance.value}'
    if index_def.vector_type:
      sql += f' TYPE {index_def.vector_type.value}'
    sql += ';'
    return sql

  sql = f'DEFINE INDEX {index_def.name} ON TABLE {table_name} COLUMNS {columns}'

  if index_def.type == IndexType.UNIQUE:
    sql += ' UNIQUE'
  elif index_def.type == IndexType.SEARCH:
    sql += ' SEARCH ANALYZER ascii'

  sql += ';'
  return sql


def _generate_event_sql(table_name: str, event_def: EventDefinition) -> str:
  """Generate DEFINE EVENT statement for a single event.

  Args:
    table_name: Name of the table
    event_def: Event definition

  Returns:
    SurrealQL DEFINE EVENT statement
  """
  sql = (
    f'DEFINE EVENT {event_def.name} ON TABLE {table_name}'
    f' WHEN {event_def.condition} THEN {event_def.action};'
  )
  return sql


def generate_table_sql(table: TableDefinition) -> list[str]:
  """Generate SurrealQL DEFINE statements for a table and its components.

  Args:
    table: Table definition to generate SQL for

  Returns:
    List of SurrealQL statements (DEFINE TABLE, DEFINE FIELD, etc.)

  Examples:
    >>> from reverie.schema.table import table_schema, TableMode
    >>> from reverie.schema.fields import string_field
    >>> t = table_schema('user', mode=TableMode.SCHEMAFULL, fields=[string_field('name')])
    >>> stmts = generate_table_sql(t)
    >>> stmts[0]
    'DEFINE TABLE user SCHEMAFULL;'
  """
  statements: list[str] = []

  # Table definition
  statements.append(f'DEFINE TABLE {table.name} {table.mode.value};')

  # Field definitions
  for field_def in table.fields:
    statements.append(_generate_field_sql(table.name, field_def))

  # Index definitions
  for index_def in table.indexes:
    statements.append(_generate_index_sql(table.name, index_def))

  # Event definitions
  for event_def in table.events:
    statements.append(_generate_event_sql(table.name, event_def))

  # Permission definitions
  if table.permissions:
    for action, rule in table.permissions.items():
      statements.append(
        f'DEFINE FIELD PERMISSIONS FOR {action.upper()} ON TABLE {table.name} WHERE {rule};'
      )

  return statements


def generate_edge_sql(edge: EdgeDefinition) -> list[str]:
  """Generate SurrealQL DEFINE statements for an edge table.

  Args:
    edge: Edge definition to generate SQL for

  Returns:
    List of SurrealQL statements

  Examples:
    >>> from reverie.schema.edge import edge_schema
    >>> e = edge_schema('likes', from_table='user', to_table='post')
    >>> stmts = generate_edge_sql(e)
    >>> stmts[0]
    'DEFINE TABLE likes TYPE RELATION FROM user TO post;'
  """
  if edge.mode == EdgeMode.RELATION and (not edge.from_table or not edge.to_table):
    raise ValueError(f'Edge {edge.name!r} with RELATION mode requires both from_table and to_table')

  statements: list[str] = []

  if edge.mode == EdgeMode.RELATION:
    table_sql = f'DEFINE TABLE {edge.name} TYPE RELATION'
    if edge.from_table:
      table_sql += f' FROM {edge.from_table}'
    if edge.to_table:
      table_sql += f' TO {edge.to_table}'
    table_sql += ';'
  elif edge.mode == EdgeMode.SCHEMAFULL:
    table_sql = f'DEFINE TABLE {edge.name} SCHEMAFULL;'
  else:
    table_sql = f'DEFINE TABLE {edge.name} SCHEMALESS;'

  statements.append(table_sql)

  for field_def in edge.fields:
    statements.append(_generate_field_sql(edge.name, field_def))

  for index_def in edge.indexes:
    statements.append(_generate_index_sql(edge.name, index_def))

  for event_def in edge.events:
    statements.append(_generate_event_sql(edge.name, event_def))

  return statements


def generate_access_sql(access: AccessDefinition) -> list[str]:
  """Generate SurrealQL DEFINE ACCESS statement.

  Args:
    access: Access definition to generate SQL for

  Returns:
    List containing the DEFINE ACCESS statement

  Examples:
    >>> from reverie.schema.access import jwt_access
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


def generate_schema_sql(
  tables: dict[str, TableDefinition] | None = None,
  edges: dict[str, EdgeDefinition] | None = None,
) -> str:
  """Generate complete SurrealQL schema from table and edge definitions.

  Args:
    tables: Dict of table name to TableDefinition
    edges: Dict of edge name to EdgeDefinition

  Returns:
    Complete SurrealQL schema as a single string

  Examples:
    >>> sql = generate_schema_sql(tables={'user': user_table}, edges={'likes': likes_edge})
  """
  all_statements: list[str] = []

  if tables:
    for table in tables.values():
      all_statements.extend(generate_table_sql(table))
      all_statements.append('')  # blank line between tables

  if edges:
    for edge in edges.values():
      all_statements.extend(generate_edge_sql(edge))
      all_statements.append('')

  return '\n'.join(all_statements).strip()
