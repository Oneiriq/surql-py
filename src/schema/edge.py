"""Edge/relationship schema definition functions.

This module provides functions for defining edge schemas for graph relationships
in SurrealDB.
"""

from typing import Optional
from pydantic import BaseModel

from src.schema.fields import FieldDefinition
from src.schema.table import IndexDefinition, EventDefinition


class EdgeDefinition(BaseModel):
  """Immutable edge/relationship schema definition.
  
  Represents a graph edge between tables with optional constraints and fields.
  
  Examples:
    >>> edge = EdgeDefinition(
    ...   name='likes',
    ...   from_table='user',
    ...   to_table='post',
    ...   fields=[
    ...     datetime_field('created_at', default='time::now()'),
    ...   ]
    ... )
  """
  
  name: str
  from_table: Optional[str] = None  # If None, allows edges from any table
  to_table: Optional[str] = None    # If None, allows edges to any table
  fields: list[FieldDefinition] = []
  indexes: list[IndexDefinition] = []
  events: list[EventDefinition] = []
  permissions: Optional[dict[str, str]] = None
  
  class Config:
    """Pydantic configuration."""
    frozen = True


# Edge builder functions

def edge_schema(
  name: str,
  *,
  from_table: Optional[str] = None,
  to_table: Optional[str] = None,
  fields: Optional[list[FieldDefinition]] = None,
  indexes: Optional[list[IndexDefinition]] = None,
  events: Optional[list[EventDefinition]] = None,
  permissions: Optional[dict[str, str]] = None,
) -> EdgeDefinition:
  """Create an edge schema definition.
  
  Pure function to create an immutable edge definition for graph relationships.
  
  Args:
    name: Edge table name
    from_table: Optional constraint on source table (None allows any table)
    to_table: Optional constraint on target table (None allows any table)
    fields: List of field definitions for edge properties
    indexes: List of index definitions
    events: List of event definitions
    permissions: Dict of permission rules (select, create, update, delete)
    
  Returns:
    Immutable EdgeDefinition instance
    
  Examples:
    Basic edge without constraints:
    >>> edge = edge_schema('likes')
    
    Edge with table constraints:
    >>> edge = edge_schema(
    ...   'follows',
    ...   from_table='user',
    ...   to_table='user',
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
    from_table=from_table,
    to_table=to_table,
    fields=fields or [],
    indexes=indexes or [],
    events=events or [],
    permissions=permissions,
  )


# Functional composition helpers

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
  return edge.model_copy(
    update={'fields': [*edge.fields, *fields]}
  )


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
  return edge.model_copy(
    update={'indexes': [*edge.indexes, *indexes]}
  )


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
  return edge.model_copy(
    update={'events': [*edge.events, *events]}
  )


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
  fields: Optional[list[FieldDefinition]] = None,
  indexes: Optional[list[IndexDefinition]] = None,
  events: Optional[list[EventDefinition]] = None,
  permissions: Optional[dict[str, str]] = None,
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
  fields: Optional[list[FieldDefinition]] = None,
  indexes: Optional[list[IndexDefinition]] = None,
  events: Optional[list[EventDefinition]] = None,
  permissions: Optional[dict[str, str]] = None,
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
