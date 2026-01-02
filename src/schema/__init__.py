"""Schema definition layer for ethereal ORM.

This package provides functions and types for defining table and edge schemas
in a functional, composable way.
"""

from src.schema.fields import (
  FieldType,
  FieldDefinition,
  field,
  string_field,
  int_field,
  float_field,
  bool_field,
  datetime_field,
  record_field,
  array_field,
  object_field,
  computed_field,
)

from src.schema.table import (
  TableMode,
  IndexType,
  IndexDefinition,
  EventDefinition,
  TableDefinition,
  table_schema,
  index,
  unique_index,
  search_index,
  event,
  with_fields,
  with_indexes,
  with_events,
  with_permissions,
  set_mode,
)

from src.schema.edge import (
  EdgeDefinition,
  edge_schema,
  with_from_table,
  with_to_table,
  with_edge_fields,
  with_edge_indexes,
  with_edge_events,
  with_edge_permissions,
  bidirectional_edge,
  typed_edge,
)

__all__ = [
  # Field types and definitions
  'FieldType',
  'FieldDefinition',
  'field',
  'string_field',
  'int_field',
  'float_field',
  'bool_field',
  'datetime_field',
  'record_field',
  'array_field',
  'object_field',
  'computed_field',
  # Table schema
  'TableMode',
  'IndexType',
  'IndexDefinition',
  'EventDefinition',
  'TableDefinition',
  'table_schema',
  'index',
  'unique_index',
  'search_index',
  'event',
  'with_fields',
  'with_indexes',
  'with_events',
  'with_permissions',
  'set_mode',
  # Edge schema
  'EdgeDefinition',
  'edge_schema',
  'with_from_table',
  'with_to_table',
  'with_edge_fields',
  'with_edge_indexes',
  'with_edge_events',
  'with_edge_permissions',
  'bidirectional_edge',
  'typed_edge',
]
