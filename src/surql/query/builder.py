"""Core query builder with immutable fluent API for SurrealDB.

This module provides an immutable Query class that enables composable query building
through method chaining. All methods return new Query instances, ensuring immutability.

Standalone helper functions and shared types are defined in surql.query.helpers
and re-exported here for backward compatibility.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeVar

import structlog
from pydantic import BaseModel, ConfigDict, Field

from surql.query.helpers import (
  ReturnFormat,
  VectorDistanceType,
  delete,
  from_table,
  insert,
  limit,
  offset,
  order_by,
  relate,
  select,
  update,
  upsert,
  vector_search_query,
  where,
)
from surql.types.operators import Operator, _quote_value, _validate_identifier
from surql.types.record_id import RecordID

if TYPE_CHECKING:
  from surql.query.hints import QueryHint

logger = structlog.get_logger(__name__)

T = TypeVar('T', bound=BaseModel)

# Re-export helpers and types for backward compatibility
__all__ = [
  'Query',
  'ReturnFormat',
  'VectorDistanceType',
  'delete',
  'from_table',
  'insert',
  'limit',
  'offset',
  'order_by',
  'relate',
  'select',
  'update',
  'upsert',
  'vector_search_query',
  'where',
]


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
  return_format: ReturnFormat | None = None
  # Vector search fields
  vector_field: str | None = None
  vector_value: list[float] = Field(default_factory=list)
  vector_k: int | None = None
  vector_distance: VectorDistanceType | None = None
  vector_threshold: float | None = None
  # Query optimization hints
  hints: list[Any] = Field(default_factory=list)

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

    Raises:
      ValueError: If table name contains invalid characters

    Examples:
      >>> Query().select().from_table('user')
      >>> Query().select().from_table('user:alice')
    """
    # Validate table name (extract table part from record ID if present)
    table_part = table.split(':')[0] if ':' in table else table
    _validate_identifier(table_part, 'table name')
    return self.model_copy(update={'table_name': table})

  def where(self, condition: str | Operator) -> Query[T]:
    """Add WHERE condition to query.

    Args:
      condition: Condition string or Operator instance

    Returns:
      New Query instance with condition added

    Examples:
      >>> Query().select().from_table('user').where('age > 18')
      >>> from surql.types.operators import gt
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

    Raises:
      ValueError: If table name or field names contain invalid characters

    Examples:
      >>> Query().insert('user', {'name': 'Alice', 'email': 'alice@example.com'})
    """
    # Validate table name and field names to prevent SQL injection
    _validate_identifier(table, 'table name')
    for field_name in data:
      _validate_identifier(field_name, 'field name')

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

    Raises:
      ValueError: If table name or field names contain invalid characters

    Examples:
      >>> Query().update('user:alice', {'status': 'active'})
      >>> Query().update('user', {'status': 'inactive'}).where('last_login < "2024-01-01"')
    """
    # Validate table name (extract from record ID if present) and field names
    table_part = target.split(':')[0] if ':' in target else target
    _validate_identifier(table_part, 'table name')
    for field_name in data:
      _validate_identifier(field_name, 'field name')

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

    Raises:
      ValueError: If table name contains invalid characters

    Examples:
      >>> Query().delete('user:alice')
      >>> Query().delete('user').where('deleted_at IS NOT NULL')
    """
    # Validate table name (extract from record ID if present)
    table_part = target.split(':')[0] if ':' in target else target
    _validate_identifier(table_part, 'table name')

    return self.model_copy(
      update={
        'operation': 'DELETE',
        'table_name': target,
      }
    )

  def upsert(self, target: str, data: dict[str, Any]) -> Query[T]:
    """Create an UPSERT query.

    Inserts a record if it does not exist, or updates it if it does.

    Args:
      target: Table name or record ID to upsert
      data: Data to upsert

    Returns:
      New Query instance with UPSERT operation

    Raises:
      ValueError: If table name or field names contain invalid characters

    Examples:
      >>> Query().upsert('user:alice', {'name': 'Alice', 'status': 'active'})
      >>> Query().upsert('user', {'name': 'Bob'}).where('email = "bob@example.com"')
    """
    table_part = target.split(':')[0] if ':' in target else target
    _validate_identifier(table_part, 'table name')
    for field_name in data:
      _validate_identifier(field_name, 'field name')

    return self.model_copy(
      update={
        'operation': 'UPSERT',
        'table_name': target,
        'update_data': data,
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

    Raises:
      ValueError: If table names or field names contain invalid characters

    Examples:
      >>> Query().relate('likes', 'user:alice', 'post:123')
      >>> Query().relate('follows', 'user:alice', 'user:bob', {'since': '2024-01-01'})
    """
    # Validate edge table name
    _validate_identifier(edge_table, 'edge table name')

    # Validate from/to record IDs
    from_str = str(from_record) if isinstance(from_record, RecordID) else from_record
    to_str = str(to_record) if isinstance(to_record, RecordID) else to_record

    # Validate table parts of record IDs
    from_table_name = from_str.split(':')[0] if ':' in from_str else from_str
    to_table = to_str.split(':')[0] if ':' in to_str else to_str
    _validate_identifier(from_table_name, 'from table name')
    _validate_identifier(to_table, 'to table name')

    # Validate edge data field names if provided
    if data:
      for field_name in data:
        _validate_identifier(field_name, 'field name')

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

  def vector_search(
    self,
    field: str,
    vector: list[float],
    k: int = 10,
    distance: VectorDistanceType = 'COSINE',
    threshold: float | None = None,
  ) -> Query[T]:
    """Add vector similarity search clause using MTREE operator.

    Performs K-nearest neighbor search using the specified distance metric.
    Requires an MTREE index on the target field.

    Args:
      field: The field containing the vector embedding
      vector: The query vector to compare against
      k: Number of nearest neighbors to return (default: 10)
      distance: Distance metric (default: COSINE)
      threshold: Optional similarity threshold for filtering results

    Returns:
      New Query instance with vector search configured

    Raises:
      ValueError: If k is less than 1 or vector is empty

    Examples:
      >>> query = Query().select().from_table('documents').vector_search(
      ...     field='embedding',
      ...     vector=[0.1, 0.2, 0.3],
      ...     k=10,
      ...     distance='COSINE',
      ...     threshold=0.7,
      ... )
      >>> # Generates: SELECT * FROM documents WHERE embedding <|10,COSINE,0.7|> [0.1, 0.2, 0.3]
    """
    if k < 1:
      raise ValueError(f'k must be at least 1, got {k}')

    if not vector:
      raise ValueError('Vector cannot be empty')

    return self.model_copy(
      update={
        'vector_field': field,
        'vector_value': vector,
        'vector_k': k,
        'vector_distance': distance,
        'vector_threshold': threshold,
      }
    )

  def similarity_score(
    self,
    field: str,
    vector: list[float],
    metric: VectorDistanceType = 'COSINE',
    alias: str = 'similarity',
  ) -> Query[T]:
    """Add a vector similarity score calculation to SELECT fields.

    Adds vector::similarity::{metric}(field, vector) AS alias to the query,
    allowing similarity scores to be returned alongside results.

    Args:
      field: The field containing the vector embedding
      vector: The query vector to compare against
      metric: Similarity metric (default: COSINE)
      alias: Column alias for the similarity score (default: 'similarity')

    Returns:
      New Query instance with similarity score field added

    Examples:
      >>> query = Query().select(['id', 'text']).from_table('chunk') \\
      ...   .similarity_score('embedding', [0.1, 0.2], 'COSINE', 'score')
      >>> # Adds: vector::similarity::cosine(embedding, [0.1, 0.2]) AS score
    """
    vector_str = '[' + ', '.join(str(v) for v in vector) + ']'
    score_expr = f'vector::similarity::{metric.lower()}({field}, {vector_str}) AS {alias}'
    return self.model_copy(update={'fields': [*self.fields, score_expr]})

  def return_none(self) -> Query[T]:
    """Set RETURN NONE for the query.

    Returns nothing from the operation. Useful for performance when
    you don't need the result data.

    Returns:
      New Query instance with RETURN NONE set

    Examples:
      >>> Query().delete('user:alice').return_none()
      >>> Query().update('user:bob', {'status': 'active'}).return_none()
    """
    return self.model_copy(update={'return_format': ReturnFormat.NONE})

  def return_diff(self) -> Query[T]:
    """Set RETURN DIFF for the query.

    Returns only the fields that changed. Most useful for UPDATE operations.

    Returns:
      New Query instance with RETURN DIFF set

    Examples:
      >>> Query().update('user:alice', {'age': 30}).return_diff()
    """
    return self.model_copy(update={'return_format': ReturnFormat.DIFF})

  def return_full(self) -> Query[T]:
    """Set RETURN FULL for the query.

    Returns the full record with all fields included.

    Returns:
      New Query instance with RETURN FULL set

    Examples:
      >>> Query().insert('user', {'name': 'Alice'}).return_full()
      >>> Query().update('user:bob', {'status': 'active'}).return_full()
    """
    return self.model_copy(update={'return_format': ReturnFormat.FULL})

  def return_before(self) -> Query[T]:
    """Set RETURN BEFORE for the query.

    Returns the record state before the operation was applied.

    Returns:
      New Query instance with RETURN BEFORE set

    Examples:
      >>> Query().update('user:alice', {'age': 31}).return_before()
      >>> Query().delete('user:bob').return_before()
    """
    return self.model_copy(update={'return_format': ReturnFormat.BEFORE})

  def return_after(self) -> Query[T]:
    """Set RETURN AFTER for the query.

    Returns the record state after the operation was applied.
    This is the default behavior for UPDATE and DELETE.

    Returns:
      New Query instance with RETURN AFTER set

    Examples:
      >>> Query().update('user:alice', {'age': 31}).return_after()
      >>> Query().delete('user:bob').return_after()
    """
    return self.model_copy(update={'return_format': ReturnFormat.AFTER})

  def add_hint(self, hint: QueryHint) -> Query[T]:
    """Add optimization hint to query.

    Args:
      hint: Query optimization hint

    Returns:
      New Query instance with hint added

    Examples:
      >>> from surql.query.hints import IndexHint
      >>> query = (Query()
      ...   .select(['name', 'email'])
      ...   .from_table('user')
      ...   .add_hint(IndexHint(table='user', index='email_idx'))
      ... )
    """
    return self.model_copy(update={'hints': [*self.hints, hint]})

  def with_hints(self, *hints: QueryHint) -> Query[T]:
    """Add multiple optimization hints to query.

    Args:
      hints: Variable number of query hints

    Returns:
      New Query instance with hints added

    Examples:
      >>> from surql.query.hints import TimeoutHint, ParallelHint
      >>> query = (Query()
      ...   .select()
      ...   .from_table('user')
      ...   .with_hints(
      ...     TimeoutHint(seconds=30),
      ...     ParallelHint(enabled=True, max_workers=4)
      ...   )
      ... )
    """
    return self.model_copy(update={'hints': [*self.hints, *hints]})

  def force_index(self, index: str) -> Query[T]:
    """Convenience method to force index usage.

    Args:
      index: Index name to force

    Returns:
      New Query instance with index hint

    Raises:
      ValueError: If table name is not set

    Examples:
      >>> Query().select().from_table('user').force_index('email_idx')
    """
    from surql.query.hints import IndexHint

    if not self.table_name:
      raise ValueError('Table name required for index hint')

    hint = IndexHint(table=self.table_name, index=index, force=True)
    return self.add_hint(hint)

  def use_index(self, index: str) -> Query[T]:
    """Convenience method to suggest index usage.

    Args:
      index: Index name to suggest

    Returns:
      New Query instance with index hint

    Raises:
      ValueError: If table name is not set

    Examples:
      >>> Query().select().from_table('user').use_index('email_idx')
    """
    from surql.query.hints import IndexHint

    if not self.table_name:
      raise ValueError('Table name required for index hint')

    hint = IndexHint(table=self.table_name, index=index, force=False)
    return self.add_hint(hint)

  def with_timeout(self, seconds: float) -> Query[T]:
    """Convenience method to set query timeout.

    Args:
      seconds: Timeout in seconds

    Returns:
      New Query instance with timeout hint

    Examples:
      >>> Query().select().from_table('user').with_timeout(30.0)
    """
    from surql.query.hints import TimeoutHint

    hint = TimeoutHint(seconds=seconds)
    return self.add_hint(hint)

  def parallel(self, max_workers: int | None = None) -> Query[T]:
    """Convenience method to enable parallel execution.

    Args:
      max_workers: Optional maximum parallel workers

    Returns:
      New Query instance with parallel hint

    Examples:
      >>> Query().select().from_table('user').parallel(max_workers=4)
      >>> Query().select().from_table('user').parallel()
    """
    from surql.query.hints import ParallelHint

    hint = ParallelHint(enabled=True, max_workers=max_workers)
    return self.add_hint(hint)

  def with_fetch(
    self, strategy: Literal['eager', 'lazy', 'batch'], batch_size: int | None = None
  ) -> Query[T]:
    """Convenience method to set fetch strategy.

    Args:
      strategy: Fetch strategy ('eager', 'lazy', or 'batch')
      batch_size: Batch size (required for 'batch' strategy)

    Returns:
      New Query instance with fetch hint

    Examples:
      >>> Query().select().from_table('user').with_fetch('eager')
      >>> Query().select().from_table('user').with_fetch('batch', batch_size=100)
    """
    from surql.query.hints import FetchHint

    hint = FetchHint(strategy=strategy, batch_size=batch_size)
    return self.add_hint(hint)

  def explain(self, full: bool = False) -> Query[T]:
    """Convenience method to add EXPLAIN hint.

    Args:
      full: Whether to include full execution plan

    Returns:
      New Query instance with explain hint

    Examples:
      >>> Query().select().from_table('user').explain()
      >>> Query().select().from_table('user').explain(full=True)
    """
    from surql.query.hints import ExplainHint

    hint = ExplainHint(full=full)
    return self.add_hint(hint)

  def to_surql(self) -> str:
    """Convert query to SurrealQL string with hints.

    Returns:
      SurrealQL query string with optimization hints

    Raises:
      ValueError: If query is invalid or incomplete

    Examples:
      >>> query = Query().select(['name']).from_table('user').where('age > 18')
      >>> query.to_surql()
      'SELECT name FROM user WHERE age > 18'

      >>> from surql.query.hints import TimeoutHint
      >>> query = Query().select().from_table('user').with_timeout(30.0)
      >>> sql = query.to_surql()
      >>> assert '/* TIMEOUT 30.0s */' in sql
    """
    if not self.operation:
      raise ValueError('Query operation not specified')

    # Build base query SQL
    if self.operation == 'SELECT':
      base_sql = self._build_select()
    elif self.operation == 'INSERT':
      base_sql = self._build_insert()
    elif self.operation == 'UPDATE':
      base_sql = self._build_update()
    elif self.operation == 'DELETE':
      base_sql = self._build_delete()
    elif self.operation == 'UPSERT':
      base_sql = self._build_upsert()
    elif self.operation == 'RELATE':
      base_sql = self._build_relate()
    else:
      raise ValueError(f'Unsupported operation: {self.operation}')

    # Add hints if present
    if self.hints:
      from surql.query.hints import render_hints

      hint_str = render_hints(self.hints)
      return f'{hint_str}\n{base_sql}'

    return base_sql

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

    # Build all WHERE conditions including vector search
    where_conditions: list[str] = []

    # Add vector search condition if present
    if self.vector_field and self.vector_value and self.vector_k and self.vector_distance:
      vector_str = '[' + ', '.join(str(v) for v in self.vector_value) + ']'
      if self.vector_threshold is not None:
        operator = f'<|{self.vector_k},{self.vector_distance},{self.vector_threshold}|>'
      else:
        operator = f'<|{self.vector_k},{self.vector_distance}|>'
      vector_condition = f'{self.vector_field} {operator} {vector_str}'
      where_conditions.append(vector_condition)

    # Add regular conditions
    for condition in self.conditions:
      where_conditions.append(f'({condition})')

    # Add WHERE clause if there are any conditions
    if where_conditions:
      parts.append(f'WHERE {" AND ".join(where_conditions)}')

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

    parts = [f'CREATE {self.table_name} CONTENT {data_str}']

    # Add RETURN clause if specified
    if self.return_format:
      parts.append(f'RETURN {self.return_format.value}')

    return ' '.join(parts)

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

    # Add RETURN clause if specified
    if self.return_format:
      parts.append(f'RETURN {self.return_format.value}')

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

    # Add RETURN clause if specified
    if self.return_format:
      parts.append(f'RETURN {self.return_format.value}')

    return ' '.join(parts)

  def _build_upsert(self) -> str:
    """Build UPSERT query string."""
    if not self.table_name:
      raise ValueError('Table name required for UPSERT query')

    if not self.update_data:
      raise ValueError('Data required for UPSERT query')

    # Build CONTENT object
    data_parts = []
    for key, value in self.update_data.items():
      quoted_value = _quote_value(value)
      data_parts.append(f'{key}: {quoted_value}')

    data_str = '{' + ', '.join(data_parts) + '}'

    parts = [f'UPSERT {self.table_name} CONTENT {data_str}']

    # Add WHERE conditions
    if self.conditions:
      conditions_str = ' AND '.join(f'({c})' for c in self.conditions)
      parts.append(f'WHERE {conditions_str}')

    # Add RETURN clause if specified
    if self.return_format:
      parts.append(f'RETURN {self.return_format.value}')

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

    # Add RETURN clause if specified
    if self.return_format:
      parts.append(f'RETURN {self.return_format.value}')

    return ' '.join(parts)
