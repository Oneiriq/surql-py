"""Schema diffing utilities for migration generation.

This module provides functions for comparing schema definitions and generating
SQL statements for schema changes.
"""

from typing import Optional

import structlog

from src.schema.edge import EdgeDefinition
from src.schema.fields import FieldDefinition, FieldType
from src.schema.table import (
  EventDefinition,
  IndexDefinition,
  TableDefinition,
  TableMode,
)
from src.migration.models import DiffOperation, SchemaDiff

logger = structlog.get_logger(__name__)


def diff_tables(
  old_table: Optional[TableDefinition],
  new_table: Optional[TableDefinition],
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
  
  Args:
    old_table: Previous table definition
    new_table: New table definition
    
  Returns:
    List of permission-related SchemaDiff operations
  """
  diffs: list[SchemaDiff] = []
  
  if old_table.permissions != new_table.permissions:
    diffs.append(_generate_modify_permissions_diff(old_table.name, new_table.permissions))
  
  return diffs


def diff_edges(
  old_edge: Optional[EdgeDefinition],
  new_edge: Optional[EdgeDefinition],
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
  
  # Both exist - edges are treated similarly to tables
  # We can leverage the same field/index/event diffing logic
  
  return diffs


# Helper functions to generate specific diff types

def _generate_add_table_diffs(table: TableDefinition) -> list[SchemaDiff]:
  """Generate diffs for adding a new table."""
  diffs: list[SchemaDiff] = []
  
  # Main table definition
  forward_sql = f'DEFINE TABLE {table.name} {table.mode.value};'
  backward_sql = f'REMOVE TABLE {table.name};'
  
  diffs.append(SchemaDiff(
    operation=DiffOperation.ADD_TABLE,
    table=table.name,
    description=f'Add table {table.name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
  ))
  
  # Add all fields
  for field in table.fields:
    diffs.append(_generate_add_field_diff(table.name, field))
  
  # Add all indexes
  for index in table.indexes:
    diffs.append(_generate_add_index_diff(table.name, index))
  
  # Add all events
  for event in table.events:
    diffs.append(_generate_add_event_diff(table.name, event))
  
  # Add permissions if defined
  if table.permissions:
    diffs.append(_generate_modify_permissions_diff(table.name, table.permissions))
  
  return diffs


def _generate_drop_table_diffs(table: TableDefinition) -> list[SchemaDiff]:
  """Generate diffs for dropping a table."""
  return [SchemaDiff(
    operation=DiffOperation.DROP_TABLE,
    table=table.name,
    description=f'Drop table {table.name}',
    forward_sql=f'REMOVE TABLE {table.name};',
    backward_sql=f'DEFINE TABLE {table.name} {table.mode.value};',
  )]


def _generate_add_field_diff(table_name: str, field: FieldDefinition) -> SchemaDiff:
  """Generate diff for adding a field."""
  forward_sql = _field_to_sql(table_name, field)
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
  columns_str = ', '.join(index.columns)
  forward_sql = f'DEFINE INDEX {index.name} ON TABLE {table_name} COLUMNS {columns_str}'
  
  if index.type.value != 'INDEX':
    forward_sql += f' {index.type.value}'
  
  forward_sql += ';'
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
  columns_str = ', '.join(index.columns)
  forward_sql = f'REMOVE INDEX {index.name} ON TABLE {table_name};'
  backward_sql = f'DEFINE INDEX {index.name} ON TABLE {table_name} COLUMNS {columns_str};'
  
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


def _generate_modify_permissions_diff(
  table_name: str,
  permissions: Optional[dict[str, str]],
) -> SchemaDiff:
  """Generate diff for modifying permissions."""
  forward_sql_parts = []
  
  if permissions:
    for operation, condition in permissions.items():
      forward_sql_parts.append(
        f'DEFINE FIELD PERMISSIONS FOR {operation.upper()} ON TABLE {table_name} WHERE {condition};'
      )
  
  forward_sql = ' '.join(forward_sql_parts) if forward_sql_parts else ''
  backward_sql = ''  # Permissions rollback is complex, left as TODO
  
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
  
  # Edge table definition
  forward_sql = f'DEFINE TABLE {edge.name} TYPE RELATION'
  
  if edge.from_table:
    forward_sql += f' FROM {edge.from_table}'
  
  if edge.to_table:
    forward_sql += f' TO {edge.to_table}'
  
  forward_sql += ';'
  backward_sql = f'REMOVE TABLE {edge.name};'
  
  diffs.append(SchemaDiff(
    operation=DiffOperation.ADD_TABLE,
    table=edge.name,
    description=f'Add edge {edge.name}',
    forward_sql=forward_sql,
    backward_sql=backward_sql,
  ))
  
  # Add edge fields
  for field in edge.fields:
    diffs.append(_generate_add_field_diff(edge.name, field))
  
  # Add edge indexes
  for index in edge.indexes:
    diffs.append(_generate_add_index_diff(edge.name, index))
  
  return diffs


def _generate_drop_edge_diffs(edge: EdgeDefinition) -> list[SchemaDiff]:
  """Generate diffs for dropping an edge."""
  return [SchemaDiff(
    operation=DiffOperation.DROP_TABLE,
    table=edge.name,
    description=f'Drop edge {edge.name}',
    forward_sql=f'REMOVE TABLE {edge.name};',
    backward_sql='',
  )]


def _field_to_sql(table_name: str, field: FieldDefinition) -> str:
  """Convert a field definition to SQL statement.
  
  Args:
    table_name: Name of the table
    field: Field definition
    
  Returns:
    SQL statement string
  """
  sql = f'DEFINE FIELD {field.name} ON TABLE {table_name} TYPE {field.type.value}'
  
  if field.assertion:
    sql += f' ASSERT {field.assertion}'
  
  if field.default:
    sql += f' DEFAULT {field.default}'
  
  if field.value:
    sql += f' VALUE {field.value}'
  
  if field.readonly:
    sql += ' READONLY'
  
  if field.flexible:
    sql += ' FLEXIBLE'
  
  sql += ';'
  
  return sql


def _fields_equal(field1: FieldDefinition, field2: FieldDefinition) -> bool:
  """Check if two field definitions are equal.
  
  Args:
    field1: First field definition
    field2: Second field definition
    
  Returns:
    True if fields are equal, False otherwise
  """
  return (
    field1.name == field2.name
    and field1.type == field2.type
    and field1.assertion == field2.assertion
    and field1.default == field2.default
    and field1.value == field2.value
    and field1.readonly == field2.readonly
    and field1.flexible == field2.flexible
  )
