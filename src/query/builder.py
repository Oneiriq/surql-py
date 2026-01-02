"""Core query builder with immutable fluent API for SurrealDB.

This module provides an immutable Query class that enables composable query building
through method chaining. All methods return new Query instances, ensuring immutability.
"""

from __future__ import annotations

from typing import Any, TypeVar

import structlog
from pydantic import BaseModel, ConfigDict, Field

from src.types.operators import Operator, _quote_value
from src.types.record_id import RecordID

logger = structlog.get_logger(__name__)

T = TypeVar('T', bound=BaseModel)


class Query[T: BaseModel](BaseModel):
  """Immutable query representation with fluent API.

  All methods return new Query instances, maintaining immutability.
  Supports SELECT, INSERT, UPDATE, DELETE, and RELATE operations.

  Examples:
    Basic SELECT query:
    >>> query = Query().select(['name', 'email']).from_table('user')
    >>> query.where('age > 18').order_by('name').limit(10)

    INSERT query:
    >>> query = Query().insert('user', {'name': 'Alice', 'email': 'alice@example.com'})

    UPDATE query:
    >>> query = Query().update('user:alice', {'status': 'active'})

    Graph traversal:
    >>> query = Query().select().from_table('user:alice').traverse('->likes->post')
  """

  operation: str | None = None
  table_name: str | None = None
  fields: list[str] = Field(default_factory=list)
  conditions: list[str] = Field(default_factory=list)
  order_fields: list[tuple[str, str]] = Field(default_factory=list)
  group_fields: list[str] = Field(default_factory=list)
  limit_value: int | None = None
  offset_value: int | None = None
  insert_data: dict[str, Any] | None = None
  update_data: dict[str, Any] | None = None
  relate_from: str | None = None
  relate_to: str | None = None
  relate_data: dict[str, Any] | None = None
  join_clauses: list[str] = Field(default_factory=list)
  graph_traversal: str | None = None

  model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

  def select(self, fields: list[str] | None = None) -> Query[T]:
    """Start a SELECT query.

    Args:
      fields: List of field names to select. Defaults to ['*'] if None.

    Returns:
      New Query instance with SELECT operation

    Examples:
      >>> Query().select(['name', 'email'])
      >>> Query().select()  # SELECT *
    """
    return self.model_copy(
      update={
        'operation': 'SELECT',
        'fields': fields or ['*'],
      }
    )

  def from_table(self, table: str) -> Query[T]:
    """Specify the table to query from.

    Args:
      table: Table name or record ID (e.g., 'user' or 'user:alice')

    Returns:
      New Query instance with table set

    Examples:
      >>> Query().select().from_table('user')
      >>> Query().select().from_table('user:alice')
    """
    return self.model_copy(update={'table_name': table})

  def where(self, condition: str | Operator) -> Query[T]:
    """Add WHERE condition to query.

    Args:
      condition: Condition string or Operator instance

    Returns:
      New Query instance with condition added

    Examples:
      >>> Query().select().from_table('user').where('age > 18')
      >>> from src.types.operators import gt
      >>> Query().select().from_table('user').where(gt('age', 18))
    """
    condition_str = condition.to_surql() if isinstance(condition, Operator) else condition
    return self.model_copy(update={'conditions': [*self.conditions, condition_str]})

  def order_by(self, field: str, direction: str = 'ASC') -> Query[T]:
    """Add ORDER BY clause.

    Args:
      field: Field name to order by
      direction: Sort direction ('ASC' or 'DESC')

    Returns:
      New Query instance with ordering added

    Examples:
      >>> Query().select().from_table('user').order_by('name')
      >>> Query().select().from_table('user').order_by('created_at', 'DESC')
    """
    if direction.upper() not in ('ASC', 'DESC'):
      raise ValueError(f'Invalid direction: {direction}. Must be ASC or DESC')

    return self.model_copy(
      update={'order_fields': [*self.order_fields, (field, direction.upper())]}
    )

  def group_by(self, *fields: str) -> Query[T]:
    """Add GROUP BY clause.

    Args:
      fields: Field names to group by

    Returns:
      New Query instance with grouping added

    Examples:
      >>> Query().select(['status', 'COUNT(*)']).from_table('user').group_by('status')
    """
    return self.model_copy(update={'group_fields': [*self.group_fields, *fields]})

  def limit(self, n: int) -> Query[T]:
    """Add LIMIT clause.

    Args:
      n: Maximum number of results

    Returns:
      New Query instance with limit set

    Examples:
      >>> Query().select().from_table('user').limit(10)
    """
    if n < 0:
      raise ValueError(f'Limit must be non-negative, got {n}')

    return self.model_copy(update={'limit_value': n})

  def offset(self, n: int) -> Query[T]:
    """Add OFFSET clause for pagination.

    Args:
      n: Number of results to skip

    Returns:
      New Query instance with offset set

    Examples:
      >>> Query().select().from_table('user').limit(10).offset(20)
    """
    if n < 0:
      raise ValueError(f'Offset must be non-negative, got {n}')

    return self.model_copy(update={'offset_value': n})

  def insert(self, table: str, data: dict[str, Any]) -> Query[T]:
    """Create an INSERT query.

    Args:
      table: Table name to insert into
      data: Data to insert

    Returns:
      New Query instance with INSERT operation

    Examples:
      >>> Query().insert('user', {'name': 'Alice', 'email': 'alice@example.com'})
    """
    return self.model_copy(
      update={
        'operation': 'INSERT',
        'table_name': table,
        'insert_data': data,
      }
    )

  def update(self, target: str, data: dict[str, Any]) -> Query[T]:
    """Create an UPDATE query.

    Args:
      target: Table name or record ID to update
      data: Data to update

    Returns:
      New Query instance with UPDATE operation

    Examples:
      >>> Query().update('user:alice', {'status': 'active'})
      >>> Query().update('user', {'status': 'inactive'}).where('last_login < "2024-01-01"')
    """
    return self.model_copy(
      update={
        'operation': 'UPDATE',
        'table_name': target,
        'update_data': data,
      }
    )

  def delete(self, target: str) -> Query[T]:
    """Create a DELETE query.

    Args:
      target: Table name or record ID to delete

    Returns:
      New Query instance with DELETE operation

    Examples:
      >>> Query().delete('user:alice')
      >>> Query().delete('user').where('deleted_at IS NOT NULL')
    """
    return self.model_copy(
      update={
        'operation': 'DELETE',
        'table_name': target,
      }
    )

  def relate(
    self,
    edge_table: str,
    from_record: str | RecordID[Any],
    to_record: str | RecordID[Any],
    data: dict[str, Any] | None = None,
  ) -> Query[T]:
    """Create a RELATE query for graph edges.

    Args:
      edge_table: Edge table name
      from_record: Source record ID
      to_record: Target record ID
      data: Optional edge data

    Returns:
      New Query instance with RELATE operation

    Examples:
      >>> Query().relate('likes', 'user:alice', 'post:123')
      >>> Query().relate('follows', 'user:alice', 'user:bob', {'since': '2024-01-01'})
    """
    from_str = str(from_record) if isinstance(from_record, RecordID) else from_record
    to_str = str(to_record) if isinstance(to_record, RecordID) else to_record

    return self.model_copy(
      update={
        'operation': 'RELATE',
        'table_name': edge_table,
        'relate_from': from_str,
        'relate_to': to_str,
        'relate_data': data,
      }
    )

  def traverse(self, path: str) -> Query[T]:
    """Add graph traversal path to query.

    Args:
      path: Graph traversal path (e.g., '->likes->post' or '<-follows<-user')

    Returns:
      New Query instance with graph traversal

    Examples:
      >>> Query().select().from_table('user:alice').traverse('->likes->post')
      >>> Query().select().from_table('user:alice').traverse('<-follows<-user')
    """
    return self.model_copy(update={'graph_traversal': path})

  def join(self, join_clause: str) -> Query[T]:
    """Add JOIN clause to query.

    Args:
      join_clause: Raw JOIN clause string

    Returns:
      New Query instance with join added

    Examples:
      >>> Query().select().from_table('user').join('JOIN post ON user.id = post.author')
    """
    return self.model_copy(update={'join_clauses': [*self.join_clauses, join_clause]})

  def to_surql(self) -> str:
    """Convert query to SurrealQL string.

    Returns:
      SurrealQL query string

    Raises:
      ValueError: If query is invalid or incomplete

    Examples:
      >>> query = Query().select(['name']).from_table('user').where('age > 18')
      >>> query.to_surql()
      'SELECT name FROM user WHERE age > 18'
    """
    if not self.operation:
      raise ValueError('Query operation not specified')

    if self.operation == 'SELECT':
      return self._build_select()
    elif self.operation == 'INSERT':
      return self._build_insert()
    elif self.operation == 'UPDATE':
      return self._build_update()
    elif self.operation == 'DELETE':
      return self._build_delete()
    elif self.operation == 'RELATE':
      return self._build_relate()
    else:
      raise ValueError(f'Unsupported operation: {self.operation}')

  def _build_select(self) -> str:
    """Build SELECT query string."""
    if not self.table_name:
      raise ValueError('Table name required for SELECT query')

    # Build field list
    fields_str = ', '.join(self.fields) if self.fields else '*'

    # Start with SELECT ... FROM
    parts = [f'SELECT {fields_str} FROM {self.table_name}']

    # Add graph traversal if present
    if self.graph_traversal:
      parts[0] = f'{parts[0]}{self.graph_traversal}'

    # Add JOIN clauses
    for join in self.join_clauses:
      parts.append(join)

    # Add WHERE conditions
    if self.conditions:
      conditions_str = ' AND '.join(f'({c})' for c in self.conditions)
      parts.append(f'WHERE {conditions_str}')

    # Add GROUP BY
    if self.group_fields:
      group_str = ', '.join(self.group_fields)
      parts.append(f'GROUP BY {group_str}')

    # Add ORDER BY
    if self.order_fields:
      order_str = ', '.join(f'{field} {direction}' for field, direction in self.order_fields)
      parts.append(f'ORDER BY {order_str}')

    # Add LIMIT
    if self.limit_value is not None:
      parts.append(f'LIMIT {self.limit_value}')

    # Add OFFSET
    if self.offset_value is not None:
      parts.append(f'START {self.offset_value}')

    return ' '.join(parts)

  def _build_insert(self) -> str:
    """Build INSERT query string."""
    if not self.table_name:
      raise ValueError('Table name required for INSERT query')

    if not self.insert_data:
      raise ValueError('Insert data required for INSERT query')

    # Build data object
    data_parts = []
    for key, value in self.insert_data.items():
      quoted_value = _quote_value(value)
      data_parts.append(f'{key}: {quoted_value}')

    data_str = '{' + ', '.join(data_parts) + '}'

    return f'CREATE {self.table_name} CONTENT {data_str}'

  def _build_update(self) -> str:
    """Build UPDATE query string."""
    if not self.table_name:
      raise ValueError('Table name required for UPDATE query')

    if not self.update_data:
      raise ValueError('Update data required for UPDATE query')

    # Build SET clauses
    set_parts = []
    for key, value in self.update_data.items():
      quoted_value = _quote_value(value)
      set_parts.append(f'{key} = {quoted_value}')

    set_str = ', '.join(set_parts)

    parts = [f'UPDATE {self.table_name} SET {set_str}']

    # Add WHERE conditions
    if self.conditions:
      conditions_str = ' AND '.join(f'({c})' for c in self.conditions)
      parts.append(f'WHERE {conditions_str}')

    return ' '.join(parts)

  def _build_delete(self) -> str:
    """Build DELETE query string."""
    if not self.table_name:
      raise ValueError('Table name required for DELETE query')

    parts = [f'DELETE {self.table_name}']

    # Add WHERE conditions
    if self.conditions:
      conditions_str = ' AND '.join(f'({c})' for c in self.conditions)
      parts.append(f'WHERE {conditions_str}')

    return ' '.join(parts)

  def _build_relate(self) -> str:
    """Build RELATE query string."""
    if not self.table_name:
      raise ValueError('Edge table name required for RELATE query')

    if not self.relate_from or not self.relate_to:
      raise ValueError('From and to records required for RELATE query')

    parts = [f'RELATE {self.relate_from}->{self.table_name}->{self.relate_to}']

    # Add edge data if present
    if self.relate_data:
      data_parts = []
      for key, value in self.relate_data.items():
        quoted_value = _quote_value(value)
        data_parts.append(f'{key}: {quoted_value}')

      data_str = '{' + ', '.join(data_parts) + '}'
      parts.append(f'CONTENT {data_str}')

    return ' '.join(parts)


# Functional query builder helpers


def select(fields: list[str] | None = None) -> Query[Any]:
  """Create a SELECT query.

  Args:
    fields: List of field names. Defaults to ['*'] if None.

  Returns:
    Query instance with SELECT operation

  Examples:
    >>> select(['name', 'email'])
    >>> select()  # SELECT *
  """
  return Query().select(fields)


def from_table[T: BaseModel](query: Query[T], table: str) -> Query[T]:
  """Add table to query.

  Args:
    query: Query instance
    table: Table name

  Returns:
    New Query instance with table set
  """
  return query.from_table(table)


def where[T: BaseModel](query: Query[T], condition: str | Operator) -> Query[T]:
  """Add WHERE condition to query.

  Args:
    query: Query instance
    condition: Condition string or Operator

  Returns:
    New Query instance with condition added
  """
  return query.where(condition)


def order_by[T: BaseModel](query: Query[T], field: str, direction: str = 'ASC') -> Query[T]:
  """Add ORDER BY clause to query.

  Args:
    query: Query instance
    field: Field name
    direction: Sort direction

  Returns:
    New Query instance with ordering added
  """
  return query.order_by(field, direction)


def limit[T: BaseModel](query: Query[T], n: int) -> Query[T]:
  """Add LIMIT clause to query.

  Args:
    query: Query instance
    n: Maximum number of results

  Returns:
    New Query instance with limit set
  """
  return query.limit(n)


def offset[T: BaseModel](query: Query[T], n: int) -> Query[T]:
  """Add OFFSET clause to query.

  Args:
    query: Query instance
    n: Number of results to skip

  Returns:
    New Query instance with offset set
  """
  return query.offset(n)


def insert(table: str, data: dict[str, Any]) -> Query[Any]:
  """Create an INSERT query.

  Args:
    table: Table name
    data: Data to insert

  Returns:
    Query instance with INSERT operation
  """
  return Query().insert(table, data)


def update(target: str, data: dict[str, Any]) -> Query[Any]:
  """Create an UPDATE query.

  Args:
    target: Table name or record ID
    data: Data to update

  Returns:
    Query instance with UPDATE operation
  """
  return Query().update(target, data)


def delete(target: str) -> Query[Any]:
  """Create a DELETE query.

  Args:
    target: Table name or record ID

  Returns:
    Query instance with DELETE operation
  """
  return Query().delete(target)


def relate(
  edge_table: str,
  from_record: str | RecordID[Any],
  to_record: str | RecordID[Any],
  data: dict[str, Any] | None = None,
) -> Query[Any]:
  """Create a RELATE query.

  Args:
    edge_table: Edge table name
    from_record: Source record ID
    to_record: Target record ID
    data: Optional edge data

  Returns:
    Query instance with RELATE operation
  """
  return Query().relate(edge_table, from_record, to_record, data)
