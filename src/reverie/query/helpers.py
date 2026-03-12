"""Functional query builder helpers and shared types for SurrealDB.

This module provides standalone helper functions that create Query instances
for common operations, plus shared types used across the query subsystem.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
  from reverie.query.builder import Query
  from reverie.types.operators import Operator
  from reverie.types.record_id import RecordID

# Supported distance metrics for vector search
VectorDistanceType = Literal[
  'COSINE',
  'EUCLIDEAN',
  'MANHATTAN',
  'CHEBYSHEV',
  'MINKOWSKI',
  'HAMMING',
  'JACCARD',
  'PEARSON',
  'MAHALANOBIS',
]


class ReturnFormat(str, Enum):
  """Return format for CREATE, UPDATE, and DELETE operations.

  Controls what data is returned from mutation operations:
  - NONE: Return nothing (useful for performance when results not needed)
  - DIFF: Return only the fields that changed (UPDATE operations)
  - FULL: Return the full record including all fields
  - BEFORE: Return the record before the changes
  - AFTER: Return the record after the changes (default)

  Examples:
    >>> query = Query().update('user:john', {'age': 30}).return_diff()
    >>> # Generates: UPDATE user:john SET age = 30 RETURN DIFF
  """

  NONE = 'NONE'
  DIFF = 'DIFF'
  FULL = 'FULL'
  BEFORE = 'BEFORE'
  AFTER = 'AFTER'


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
  from reverie.query.builder import Query

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
  from reverie.query.builder import Query

  return Query().insert(table, data)


def update(target: str, data: dict[str, Any]) -> Query[Any]:
  """Create an UPDATE query.

  Args:
    target: Table name or record ID
    data: Data to update

  Returns:
    Query instance with UPDATE operation
  """
  from reverie.query.builder import Query

  return Query().update(target, data)


def delete(target: str) -> Query[Any]:
  """Create a DELETE query.

  Args:
    target: Table name or record ID

  Returns:
    Query instance with DELETE operation
  """
  from reverie.query.builder import Query

  return Query().delete(target)


def upsert(target: str, data: dict[str, Any]) -> Query[Any]:
  """Create an UPSERT query.

  Args:
    target: Table name or record ID
    data: Data to upsert

  Returns:
    Query instance with UPSERT operation
  """
  from reverie.query.builder import Query

  return Query().upsert(target, data)


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
  from reverie.query.builder import Query

  return Query().relate(edge_table, from_record, to_record, data)


def vector_search_query(
  table: str,
  field: str,
  vector: list[float],
  k: int = 10,
  distance: VectorDistanceType = 'COSINE',
  fields: list[str] | None = None,
) -> Query[Any]:
  """Create a vector similarity search query.

  Convenience function for creating a SELECT query with vector search.

  Args:
    table: Table name to search
    field: The field containing the vector embedding
    vector: The query vector to compare against
    k: Number of nearest neighbors to return (default: 10)
    distance: Distance metric (default: COSINE)
    fields: List of fields to select (default: all fields)

  Returns:
    Query instance configured for vector search

  Examples:
    >>> query = vector_search_query(
    ...     table='documents',
    ...     field='embedding',
    ...     vector=[0.1, 0.2, 0.3],
    ...     k=10,
    ...     distance='COSINE'
    ... )
    >>> # Generates: SELECT * FROM documents WHERE embedding <|10,COSINE|> [0.1, 0.2, 0.3]
  """
  from reverie.query.builder import Query

  return Query().select(fields).from_table(table).vector_search(field, vector, k, distance)
