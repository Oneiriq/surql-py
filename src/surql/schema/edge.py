"""Edge/relationship schema definition functions.

This module provides functions for defining edge schemas for graph relationships
in SurrealDB.

Two approaches are supported:

1. TYPE RELATION (default): Modern SurrealDB graph edges with automatic in/out fields
   Example: DEFINE TABLE likes TYPE RELATION FROM user TO post

2. SCHEMAFULL with explicit in/out fields: Traditional approach (driftnet-compatible)
   Example: DEFINE TABLE likes SCHEMAFULL;
           DEFINE FIELD in ON TABLE likes TYPE record<user>;
           DEFINE FIELD out ON TABLE likes TYPE record<post>;
"""

import enum

from pydantic import BaseModel, ConfigDict

from surql.schema.fields import FieldDefinition
from surql.schema.table import EventDefinition, IndexDefinition


class EdgeMode(str, enum.Enum):
  """Edge table mode.

  Defines how edge tables are created in SurrealDB.
  """

  RELATION = 'RELATION'  # TYPE RELATION (default, automatic in/out)
  SCHEMAFULL = 'SCHEMAFULL'  # Explicit schema with in/out fields
  SCHEMALESS = 'SCHEMALESS'  # Flexible schema


class EdgeDefinition(BaseModel):
  """Immutable edge/relationship schema definition.

  Represents a graph edge between tables with optional constraints and fields.

  Two modes are supported:

  1. RELATION (default): Uses TYPE RELATION for automatic in/out fields
     - from_table/to_table constrain the edge endpoints
     - in/out fields are automatically created by SurrealDB
     - Additional fields can be added for edge properties

  2. SCHEMAFULL: Traditional table with explicit in/out field definitions
     - Compatible with driftnet and traditional SurrealDB schemas
     - Requires explicit in/out fields in the fields list
     - Provides more control over field constraints

  Examples:
    TYPE RELATION edge (default):
    >>> edge = EdgeDefinition(
    ...   name='likes',
    ...   from_table='user',
    ...   to_table='post',
    ...   fields=[
    ...     datetime_field('created_at', default='time::now()'),
    ...   ]
    ... )

    SCHEMAFULL edge (driftnet-compatible):
    >>> edge = EdgeDefinition(
    ...   name='entity_relation',
    ...   mode=EdgeMode.SCHEMAFULL,
    ...   fields=[
    ...     record_field('in', table='entity'),
    ...     record_field('out', table='entity'),
    ...     string_field('relation_type'),
    ...     float_field('confidence'),
    ...   ]
    ... )
  """

  name: str
  mode: EdgeMode = EdgeMode.RELATION
  from_table: str | None = None  # Used with RELATION mode
  to_table: str | None = None  # Used with RELATION mode
  fields: list[FieldDefinition] = []
  indexes: list[IndexDefinition] = []
  events: list[EventDefinition] = []
  permissions: dict[str, str] | None = None

  model_config = ConfigDict(frozen=True)


# Edge builder functions


def edge_schema(
  name: str,
  *,
  mode: EdgeMode = EdgeMode.RELATION,
  from_table: str | None = None,
  to_table: str | None = None,
  fields: list[FieldDefinition] | None = None,
  indexes: list[IndexDefinition] | None = None,
  events: list[EventDefinition] | None = None,
  permissions: dict[str, str] | None = None,
) -> EdgeDefinition:
  """Create an edge schema definition.

  Pure function to create an immutable edge definition for graph relationships.

  Args:
    name: Edge table name
    mode: Edge mode (RELATION, SCHEMAFULL, or SCHEMALESS)
    from_table: Optional constraint on source table (used with RELATION mode)
    to_table: Optional constraint on target table (used with RELATION mode)
    fields: List of field definitions for edge properties
    indexes: List of index definitions
    events: List of event definitions
    permissions: Dict of permission rules (select, create, update, delete)

  Returns:
    Immutable EdgeDefinition instance

  Examples:
    TYPE RELATION edge (default):
    >>> edge = edge_schema('likes', from_table='user', to_table='post')

    SCHEMAFULL edge (driftnet-compatible):
    >>> edge = edge_schema(
    ...   'entity_relation',
    ...   mode=EdgeMode.SCHEMAFULL,
    ...   fields=[
    ...     record_field('in', table='entity'),
    ...     record_field('out', table='entity'),
    ...     string_field('relation_type'),
   ... ]
    ... )

    Edge with fields:
    >>> edge = edge_schema(
    ...   'likes',
    ...   from_table='user',
    ...   to_table='post',
    ...   fields=[
    ...     datetime_field('created_at', default='time::now()'),
    ...     int_field('weight', default='1'),
    ...   ]
    ... )
  """
  return EdgeDefinition(
    name=name,
    mode=mode,
    from_table=from_table,
    to_table=to_table,
    fields=fields or [],
    indexes=indexes or [],
    events=events or [],
    permissions=permissions,
  )


# Functional composition helpers


def with_edge_mode(
  edge: EdgeDefinition,
  mode: EdgeMode,
) -> EdgeDefinition:
  """Set the edge table mode.

  Pure function that returns a new edge with the mode set.

  Args:
    edge: Existing edge definition
    mode: Edge mode (RELATION, SCHEMAFULL, or SCHEMALESS)

  Returns:
    New EdgeDefinition with mode set

  Examples:
    >>> edge = edge_schema('likes')
    >>> edge = with_edge_mode(edge, EdgeMode.SCHEMAFULL)
  """
  return edge.model_copy(update={'mode': mode})


def with_from_table(
  edge: EdgeDefinition,
  from_table: str,
) -> EdgeDefinition:
  """Set the source table constraint for an edge.

  Pure function that returns a new edge with the from_table constraint.

  Args:
    edge: Existing edge definition
    from_table: Table name constraint for source

  Returns:
    New EdgeDefinition with from_table set

  Examples:
    >>> edge = edge_schema('follows')
    >>> edge = with_from_table(edge, 'user')
  """
  return edge.model_copy(update={'from_table': from_table})


def with_to_table(
  edge: EdgeDefinition,
  to_table: str,
) -> EdgeDefinition:
  """Set the target table constraint for an edge.

  Pure function that returns a new edge with the to_table constraint.

  Args:
    edge: Existing edge definition
    to_table: Table name constraint for target

  Returns:
    New EdgeDefinition with to_table set

  Examples:
    >>> edge = edge_schema('likes')
    >>> edge = with_to_table(edge, 'post')
  """
  return edge.model_copy(update={'to_table': to_table})


def with_edge_fields(
  edge: EdgeDefinition,
  *fields: FieldDefinition,
) -> EdgeDefinition:
  """Add fields to an edge definition.

  Pure function that returns a new edge with additional fields.

  Args:
    edge: Existing edge definition
    fields: Field definitions to add

  Returns:
    New EdgeDefinition with added fields

  Examples:
    >>> edge = edge_schema('likes', from_table='user', to_table='post')
    >>> edge = with_edge_fields(
    ...   edge,
    ...   datetime_field('created_at', default='time::now()'),
    ...   int_field('weight', default='1'),
    ... )
  """
  return edge.model_copy(update={'fields': [*edge.fields, *fields]})


def with_edge_indexes(
  edge: EdgeDefinition,
  *indexes: IndexDefinition,
) -> EdgeDefinition:
  """Add indexes to an edge definition.

  Pure function that returns a new edge with additional indexes.

  Args:
    edge: Existing edge definition
    indexes: Index definitions to add

  Returns:
    New EdgeDefinition with added indexes

  Examples:
    >>> edge = edge_schema('likes', from_table='user', to_table='post')
    >>> edge = with_edge_indexes(
    ...   edge,
    ...   index('created_idx', ['created_at']),
    ... )
  """
  return edge.model_copy(update={'indexes': [*edge.indexes, *indexes]})


def with_edge_events(
  edge: EdgeDefinition,
  *events: EventDefinition,
) -> EdgeDefinition:
  """Add events to an edge definition.

  Pure function that returns a new edge with additional events.

  Args:
    edge: Existing edge definition
    events: Event definitions to add

  Returns:
    New EdgeDefinition with added events

  Examples:
    >>> edge = edge_schema('likes')
    >>> edge = with_edge_events(
    ...   edge,
    ...   event('like_created', '$event = "CREATE"', '...'),
    ... )
  """
  return edge.model_copy(update={'events': [*edge.events, *events]})


def with_edge_permissions(
  edge: EdgeDefinition,
  permissions: dict[str, str],
) -> EdgeDefinition:
  """Add permissions to an edge definition.

  Pure function that returns a new edge with permissions.

  Args:
    edge: Existing edge definition
    permissions: Dict of permission rules (select, create, update, delete)

  Returns:
    New EdgeDefinition with permissions

  Examples:
    >>> edge = edge_schema('follows', from_table='user', to_table='user')
    >>> edge = with_edge_permissions(
    ...   edge,
    ...   {
    ...     'create': '$auth.id = in',
    ...     'delete': '$auth.id = in',
    ...   }
    ... )
  """
  return edge.model_copy(update={'permissions': permissions})


# Convenience functions for common edge patterns


def bidirectional_edge(
  name: str,
  table: str,
  *,
  fields: list[FieldDefinition] | None = None,
  indexes: list[IndexDefinition] | None = None,
  events: list[EventDefinition] | None = None,
  permissions: dict[str, str] | None = None,
) -> EdgeDefinition:
  """Create a bidirectional edge (same table for both from and to).

  Convenience function for creating self-referential edges like
  'follows', 'friends', etc.

  Args:
    name: Edge table name
    table: Table name for both source and target
    fields: List of field definitions for edge properties
    indexes: List of index definitions
    events: List of event definitions
    permissions: Dict of permission rules

  Returns:
    EdgeDefinition with from_table and to_table set to the same table

  Examples:
    >>> edge = bidirectional_edge(
    ...   'follows',
    ...   'user',
    ...   fields=[
    ...     datetime_field('since', default='time::now()'),
    ...   ]
    ... )
  """
  return edge_schema(
    name,
    from_table=table,
    to_table=table,
    fields=fields,
    indexes=indexes,
    events=events,
    permissions=permissions,
  )


def typed_edge(
  name: str,
  from_table: str,
  to_table: str,
  *,
  fields: list[FieldDefinition] | None = None,
  indexes: list[IndexDefinition] | None = None,
  events: list[EventDefinition] | None = None,
  permissions: dict[str, str] | None = None,
) -> EdgeDefinition:
  """Create a typed edge with specific from and to table constraints.

  Convenience function for creating edges between specific tables.

  Args:
    name: Edge table name
    from_table: Source table name
    to_table: Target table name
    fields: List of field definitions for edge properties
    indexes: List of index definitions
    events: List of event definitions
    permissions: Dict of permission rules

  Returns:
    EdgeDefinition with both from_table and to_table constraints

  Examples:
    >>> edge = typed_edge(
    ...   'authored',
    ...   from_table='user',
    ...   to_table='post',
    ...   fields=[
    ...     datetime_field('published_at', default='time::now()'),
    ...   ]
    ... )
  """
  return edge_schema(
    name,
    from_table=from_table,
    to_table=to_table,
    fields=fields,
    indexes=indexes,
    events=events,
    permissions=permissions,
  )
