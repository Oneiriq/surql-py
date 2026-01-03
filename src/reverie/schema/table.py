"""Table schema definition functions.

This module provides functions for defining table schemas with fields, indexes,
permissions, and events.
"""

from enum import Enum

from pydantic import BaseModel, ConfigDict

from reverie.schema.fields import FieldDefinition


class TableMode(Enum):
  """Table schema modes.

  Defines whether a table enforces strict schema validation.
  """

  SCHEMAFULL = 'SCHEMAFULL'
  SCHEMALESS = 'SCHEMALESS'
  DROP = 'DROP'


class IndexType(Enum):
  """Index types for table fields.

  Defines the type of index to create on table fields.
  """

  UNIQUE = 'UNIQUE'
  SEARCH = 'SEARCH'
  STANDARD = 'INDEX'
  MTREE = 'MTREE'


class MTreeDistanceType(Enum):
  """Distance metric types for MTREE vector indexes.

  Defines the distance metric used for vector similarity search.
  """

  COSINE = 'COSINE'
  EUCLIDEAN = 'EUCLIDEAN'
  MANHATTAN = 'MANHATTAN'
  MINKOWSKI = 'MINKOWSKI'


class MTreeVectorType(Enum):
  """Vector data types for MTREE indexes.

  Defines the numeric type used for vector components.
  """

  F64 = 'F64'
  F32 = 'F32'
  I64 = 'I64'
  I32 = 'I32'
  I16 = 'I16'


class IndexDefinition(BaseModel):
  """Immutable index definition.

  Represents an index on one or more fields in a table.

  Examples:
    Standard index:
    >>> idx = IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE)

    MTREE vector index:
    >>> idx = IndexDefinition(
    ...   name='embedding_idx',
    ...   columns=['embedding'],
    ...   type=IndexType.MTREE,
    ...   dimension=1536,
    ...   distance=MTreeDistanceType.COSINE,
    ...   vector_type=MTreeVectorType.F32
    ... )
  """

  name: str
  columns: list[str]
  type: IndexType = IndexType.STANDARD
  # MTREE-specific parameters
  dimension: int | None = None
  distance: MTreeDistanceType | None = None
  vector_type: MTreeVectorType | None = None

  model_config = ConfigDict(frozen=True)


class EventDefinition(BaseModel):
  """Immutable event/trigger definition.

  Represents a database event that executes when a condition is met.

  Examples:
    >>> event = EventDefinition(
    ...   name='email_changed',
    ...   condition='$before.email != $after.email',
    ...   action='CREATE audit_log SET ...'
    ... )
  """

  name: str
  condition: str
  action: str

  model_config = ConfigDict(frozen=True)


class TableDefinition(BaseModel):
  """Immutable table schema definition.

  Represents a complete table schema with fields, indexes, permissions, and events.

  Examples:
    >>> table = TableDefinition(
    ...   name='user',
    ...   mode=TableMode.SCHEMAFULL,
    ...   fields=[
    ...     FieldDefinition(name='email', type=FieldType.STRING),
    ...   ],
    ...   indexes=[
    ...     IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE),
    ...   ]
    ... )
  """

  name: str
  mode: TableMode = TableMode.SCHEMAFULL
  fields: list[FieldDefinition] = []
  indexes: list[IndexDefinition] = []
  events: list[EventDefinition] = []
  permissions: dict[str, str] | None = None
  drop: bool = False

  model_config = ConfigDict(frozen=True)


# Table builder functions


def table_schema(
  name: str,
  *,
  mode: TableMode = TableMode.SCHEMAFULL,
  fields: list[FieldDefinition] | None = None,
  indexes: list[IndexDefinition] | None = None,
  events: list[EventDefinition] | None = None,
  permissions: dict[str, str] | None = None,
  drop: bool = False,
) -> TableDefinition:
  """Create a table schema definition.

  Pure function to create an immutable table definition.

  Args:
    name: Table name
    mode: Schema mode (SCHEMAFULL, SCHEMALESS, or DROP)
    fields: List of field definitions
    indexes: List of index definitions
    events: List of event definitions
    permissions: Dict of permission rules (select, create, update, delete)
    drop: If True, marks table for deletion

  Returns:
    Immutable TableDefinition instance

  Examples:
    Basic table:
    >>> table = table_schema('user')

    Table with fields and indexes:
    >>> table = table_schema(
    ...   'user',
    ...   mode=TableMode.SCHEMAFULL,
    ...   fields=[
    ...     string_field('email'),
    ...     int_field('age'),
    ...   ],
    ...   indexes=[
    ...     index('email_idx', ['email'], IndexType.UNIQUE),
    ...   ]
    ... )
  """
  return TableDefinition(
    name=name,
    mode=mode,
    fields=fields or [],
    indexes=indexes or [],
    events=events or [],
    permissions=permissions,
    drop=drop,
  )


def index(
  name: str,
  columns: list[str],
  index_type: IndexType = IndexType.STANDARD,
) -> IndexDefinition:
  """Create an index definition.

  Pure function to create an immutable index definition.

  Args:
    name: Index name
    columns: List of column names to index
    index_type: Type of index (UNIQUE, SEARCH, or STANDARD)

  Returns:
    Immutable IndexDefinition instance

  Examples:
    >>> index('email_idx', ['email'], IndexType.UNIQUE)
    IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE)

    >>> index('name_search', ['name.first', 'name.last'], IndexType.SEARCH)
    IndexDefinition(name='name_search', columns=['name.first', 'name.last'], type=IndexType.SEARCH)
  """
  return IndexDefinition(
    name=name,
    columns=columns,
    type=index_type,
  )


def unique_index(
  name: str,
  columns: list[str],
) -> IndexDefinition:
  """Create a unique index definition.

  Convenience function for creating unique indexes.

  Args:
    name: Index name
    columns: List of column names to index

  Returns:
    Immutable IndexDefinition with UNIQUE type

  Examples:
    >>> unique_index('email_idx', ['email'])
    IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE)
  """
  return index(name, columns, IndexType.UNIQUE)


def search_index(
  name: str,
  columns: list[str],
) -> IndexDefinition:
  """Create a search index definition.

  Convenience function for creating full-text search indexes.

  Args:
    name: Index name
    columns: List of column names to index

  Returns:
    Immutable IndexDefinition with SEARCH type

  Examples:
    >>> search_index('content_search', ['title', 'content'])
    IndexDefinition(name='content_search', columns=['title', 'content'], type=IndexType.SEARCH)
  """
  return index(name, columns, IndexType.SEARCH)


def mtree_index(
  name: str,
  column: str,
  dimension: int,
  *,
  distance: MTreeDistanceType = MTreeDistanceType.EUCLIDEAN,
  vector_type: MTreeVectorType = MTreeVectorType.F64,
) -> IndexDefinition:
  """Create an MTREE vector index definition.

  Convenience function for creating MTREE vector similarity search indexes.

  Args:
    name: Index name
    column: Column name containing the vector data
    dimension: Number of dimensions in the vector
    distance: Distance metric (COSINE, EUCLIDEAN, MANHATTAN, MINKOWSKI)
    vector_type: Vector component data type (F64, F32, I64, I32, I16)

  Returns:
    Immutable IndexDefinition with MTREE type

  Examples:
    OpenAI embeddings with cosine similarity:
    >>> mtree_index('embedding_idx', 'embedding', 1536, distance=MTreeDistanceType.COSINE, vector_type=MTreeVectorType.F32)

    Custom vector with Euclidean distance:
    >>> mtree_index('feature_idx', 'features', 128)
  """
  return IndexDefinition(
    name=name,
    columns=[column],
    type=IndexType.MTREE,
    dimension=dimension,
    distance=distance,
    vector_type=vector_type,
  )


def event(
  name: str,
  condition: str,
  action: str,
) -> EventDefinition:
  """Create an event/trigger definition.

  Pure function to create an immutable event definition.

  Args:
    name: Event name
    condition: SurrealQL condition expression that triggers the event
    action: SurrealQL statements to execute when triggered

  Returns:
    Immutable EventDefinition instance

  Examples:
    >>> event(
    ...   'email_changed',
    ...   '$before.email != $after.email',
    ...   'CREATE audit_log SET user = $value.id, changed_at = time::now()'
    ... )
    EventDefinition(name='email_changed', condition='$before.email != $after.email', action='...')
  """
  return EventDefinition(
    name=name,
    condition=condition,
    action=action,
  )


# Functional composition helpers


def with_fields(
  table: TableDefinition,
  *fields: FieldDefinition,
) -> TableDefinition:
  """Add fields to a table definition.

  Pure function that returns a new table with additional fields.

  Args:
    table: Existing table definition
    fields: Field definitions to add

  Returns:
    New TableDefinition with added fields

  Examples:
    >>> table = table_schema('user')
    >>> table = with_fields(
    ...   table,
    ...   string_field('email'),
    ...   int_field('age'),
    ... )
  """
  return table.model_copy(update={'fields': [*table.fields, *fields]})


def with_indexes(
  table: TableDefinition,
  *indexes: IndexDefinition,
) -> TableDefinition:
  """Add indexes to a table definition.

  Pure function that returns a new table with additional indexes.

  Args:
    table: Existing table definition
    indexes: Index definitions to add

  Returns:
    New TableDefinition with added indexes

  Examples:
    >>> table = table_schema('user', fields=[string_field('email')])
    >>> table = with_indexes(
    ...   table,
    ...   unique_index('email_idx', ['email']),
    ... )
  """
  return table.model_copy(update={'indexes': [*table.indexes, *indexes]})


def with_events(
  table: TableDefinition,
  *events: EventDefinition,
) -> TableDefinition:
  """Add events to a table definition.

  Pure function that returns a new table with additional events.

  Args:
    table: Existing table definition
    events: Event definitions to add

  Returns:
    New TableDefinition with added events

  Examples:
    >>> table = table_schema('user')
    >>> table = with_events(
    ...   table,
    ...   event('email_changed', '$before.email != $after.email', '...'),
    ... )
  """
  return table.model_copy(update={'events': [*table.events, *events]})


def with_permissions(
  table: TableDefinition,
  permissions: dict[str, str],
) -> TableDefinition:
  """Add permissions to a table definition.

  Pure function that returns a new table with permissions.

  Args:
    table: Existing table definition
    permissions: Dict of permission rules (select, create, update, delete)

  Returns:
    New TableDefinition with permissions

  Examples:
    >>> table = table_schema('user')
    >>> table = with_permissions(
    ...   table,
    ...   {
    ...     'select': '$auth.id = id OR $auth.admin = true',
    ...     'update': '$auth.id = id',
    ...     'delete': '$auth.admin = true',
    ...   }
    ... )
  """
  return table.model_copy(update={'permissions': permissions})


def set_mode(
  table: TableDefinition,
  mode: TableMode,
) -> TableDefinition:
  """Set the schema mode for a table.

  Pure function that returns a new table with the specified mode.

  Args:
    table: Existing table definition
    mode: Schema mode to set

  Returns:
    New TableDefinition with updated mode

  Examples:
    >>> table = table_schema('user')
    >>> table = set_mode(table, TableMode.SCHEMALESS)
  """
  return table.model_copy(update={'mode': mode})
