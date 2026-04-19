"""GraphQuery fluent builder for SurrealDB graph traversal.

This module provides the GraphQuery class, a chainable builder for constructing
graph traversal queries in SurrealDB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog
from pydantic import BaseModel

from surql.connection.client import DatabaseClient
from surql.connection.context import get_db
from surql.types.record_id import RecordID

logger = structlog.get_logger(__name__)


def _extract_graph_result(result: Any) -> list[dict[str, Any]]:
  """Extract data from SurrealDB graph query result.

  Args:
    result: Raw result from SurrealDB

  Returns:
    List of result dictionaries
  """
  if result is None:
    return []

  # Handle list of result objects with 'result' key
  if isinstance(result, list) and len(result) > 0:
    if isinstance(result[0], dict) and 'result' in result[0]:
      extracted: list[dict[str, Any]] = []
      for item in result:
        if isinstance(item, dict) and 'result' in item:
          res = item['result']
          if isinstance(res, list):
            extracted.extend(res)
          elif res is not None:
            extracted.append(res)
      return extracted
    return result

  # Handle single result object with 'result' key
  if isinstance(result, dict) and 'result' in result:
    res = result['result']
    if isinstance(res, list):
      return res
    return [res] if res is not None else []

  return [result] if result is not None else []


# =============================================================================
# GraphQuery: Fluent Builder for Graph Traversal
# =============================================================================


@dataclass
class GraphQuery[T: BaseModel]:
  """Fluent builder for graph traversal queries.

  Provides a chainable API for building complex graph traversal queries
  in SurrealDB. All methods return the same instance for method chaining.

  Examples:
    Basic outgoing traversal:
    >>> query = GraphQuery("user:alice").out("follows").build()
    >>> # Generates: SELECT * FROM user:alice->follows

    Multi-hop traversal with filtering:
    >>> users = await (
    ...     GraphQuery("user:alice")
    ...     .out("follows")
    ...     .out("follows")
    ...     .where("id != user:alice")
    ...     .limit(100)
    ...     .fetch(User)
    ... )

    Incoming edges with depth:
    >>> followers = await (
    ...     GraphQuery("user:alice")
    ...     .in_("follows", depth=2)
    ...     .select("id", "name")
    ...     .fetch()
    ... )

    Bidirectional traversal:
    >>> connections = await (
    ...     GraphQuery("user:alice")
    ...     .both("knows")
    ...     .to("user")
    ...     .fetch(User)
    ... )
  """

  _start: str
  _path: list[str] = field(default_factory=list)
  _conditions: list[str] = field(default_factory=list)
  _limit: int | None = None
  _fields: list[str] = field(default_factory=list)
  _target_table: str | None = None

  def __init__(self, start: str | RecordID[Any]) -> None:
    """Initialize graph query from a starting record.

    Args:
      start: Starting record ID (e.g., 'user:alice' or RecordID instance)

    Examples:
      >>> query = GraphQuery("user:alice")
      >>> query = GraphQuery(RecordID("user", "alice"))
    """
    self._start = str(start) if isinstance(start, RecordID) else start
    self._path = []
    self._conditions = []
    self._limit = None
    self._fields = []
    self._target_table = None

  def out(self, edge: str, depth: int | None = None) -> GraphQuery[T]:
    """Traverse outgoing edges.

    Args:
      edge: Edge table name to traverse
      depth: Optional traversal depth (e.g., 2 for exactly 2 hops)

    Returns:
      Self for method chaining

    Examples:
      >>> GraphQuery("user:alice").out("follows")  # Direct follows
      >>> GraphQuery("user:alice").out("follows", depth=2)  # 2 hops
    """
    # SurrealDB v3 rejects the v2 `->edge{depth}` suffix form. When a
    # depth is supplied, emit the grouped `(->edge->?){depth}` form
    # which is v3-valid and v2-compatible. See Oneiriq/surql-py#34.
    self._path.append(
      f'(->{edge}->?){{{depth}}}' if depth is not None else f'->{edge}'
    )
    return self

  def in_(self, edge: str, depth: int | None = None) -> GraphQuery[T]:
    """Traverse incoming edges.

    Args:
      edge: Edge table name to traverse
      depth: Optional traversal depth

    Returns:
      Self for method chaining

    Examples:
      >>> GraphQuery("user:alice").in_("follows")  # Who follows alice
      >>> GraphQuery("user:alice").in_("follows", depth=2)  # 2 hops back
    """
    self._path.append(
      f'(<-{edge}<-?){{{depth}}}' if depth is not None else f'<-{edge}'
    )
    return self

  def both(self, edge: str, depth: int | None = None) -> GraphQuery[T]:
    """Traverse edges in both directions.

    Args:
      edge: Edge table name to traverse
      depth: Optional traversal depth

    Returns:
      Self for method chaining

    Examples:
      >>> GraphQuery("user:alice").both("knows")  # All connections
    """
    self._path.append(
      f'(<->{edge}<->?){{{depth}}}' if depth is not None else f'<->{edge}'
    )
    return self

  def to(self, table: str) -> GraphQuery[T]:
    """Target specific table type at the end of traversal.

    Args:
      table: Target table name

    Returns:
      Self for method chaining

    Examples:
      >>> GraphQuery("user:alice").out("likes").to("post")
    """
    self._target_table = table
    return self

  def where(self, condition: str) -> GraphQuery[T]:
    """Add filter condition to the query.

    Args:
      condition: WHERE condition string

    Returns:
      Self for method chaining

    Examples:
      >>> GraphQuery("user:alice").out("follows").where("age > 18")
      >>> GraphQuery("user:alice").out("follows").where("id != user:alice")
    """
    self._conditions.append(condition)
    return self

  def select(self, *fields: str) -> GraphQuery[T]:
    """Select specific fields from results.

    Args:
      *fields: Field names to select

    Returns:
      Self for method chaining

    Examples:
      >>> GraphQuery("user:alice").out("follows").select("id", "name", "email")
    """
    self._fields.extend(fields)
    return self

  def limit(self, n: int) -> GraphQuery[T]:
    """Limit number of results.

    Args:
      n: Maximum number of results

    Returns:
      Self for method chaining

    Raises:
      ValueError: If n is negative

    Examples:
      >>> GraphQuery("user:alice").out("follows").limit(10)
    """
    if n < 0:
      raise ValueError(f'Limit must be non-negative, got {n}')
    self._limit = n
    return self

  def build(self) -> str:
    """Build the SurrealQL query string.

    Returns:
      Complete SurrealQL query string

    Raises:
      ValueError: If no traversal path is specified

    Examples:
      >>> query = GraphQuery("user:alice").out("follows").build()
      >>> # Returns: "SELECT * FROM user:alice->follows"
    """
    if not self._path:
      raise ValueError('At least one traversal step (out, in_, both) is required')

    # Build field selection
    fields_str = ', '.join(self._fields) if self._fields else '*'

    # Build path
    path_str = ''.join(self._path)

    # Add target table if specified
    if self._target_table:
      path_str = f'{path_str}->{self._target_table}'

    # Build base query
    parts = [f'SELECT {fields_str} FROM {self._start}{path_str}']

    # Add WHERE conditions
    if self._conditions:
      conditions_str = ' AND '.join(f'({c})' for c in self._conditions)
      parts.append(f'WHERE {conditions_str}')

    # Add LIMIT
    if self._limit is not None:
      parts.append(f'LIMIT {self._limit}')

    return ' '.join(parts)

  async def fetch(
    self,
    model: type[T] | None = None,
    client: DatabaseClient | None = None,
  ) -> list[T] | list[dict[str, Any]]:
    """Execute query and return typed results.

    Args:
      model: Optional Pydantic model for result deserialization
      client: Database client. If None, uses context client.

    Returns:
      List of model instances if model provided, else list of dicts

    Raises:
      QueryError: If query execution fails
      ValidationError: If model validation fails

    Examples:
      >>> users = await GraphQuery("user:alice").out("follows").fetch(User)
      >>> data = await GraphQuery("user:alice").out("follows").fetch()
    """
    sql = self.build()
    db = client or get_db()

    logger.info('graph_query_fetch', start=self._start, sql=sql)

    result = await db.execute(sql)
    data = _extract_graph_result(result)

    if model and data:
      return [model.model_validate(item) for item in data]

    return data

  async def count(self, client: DatabaseClient | None = None) -> int:
    """Count matching records.

    Args:
      client: Database client. If None, uses context client.

    Returns:
      Number of matching records

    Examples:
      >>> count = await GraphQuery("user:alice").out("follows").count()
    """
    if not self._path:
      raise ValueError('At least one traversal step (out, in_, both) is required')

    # Build path
    path_str = ''.join(self._path)

    # Add target table if specified
    if self._target_table:
      path_str = f'{path_str}->{self._target_table}'

    # Build count query
    sql = f'SELECT count() FROM {self._start}{path_str}'

    # Add WHERE conditions
    if self._conditions:
      conditions_str = ' AND '.join(f'({c})' for c in self._conditions)
      sql = f'{sql} WHERE {conditions_str}'

    sql = f'{sql} GROUP ALL'

    db = client or get_db()

    logger.info('graph_query_count', start=self._start, sql=sql)

    result = await db.execute(sql)
    data = _extract_graph_result(result)

    if data and isinstance(data, list) and len(data) > 0:
      first = data[0]
      if isinstance(first, dict):
        return first.get('count', 0)  # type: ignore[no-any-return]

    return 0

  async def exists(self, client: DatabaseClient | None = None) -> bool:
    """Check if any records match the query.

    Args:
      client: Database client. If None, uses context client.

    Returns:
      True if at least one record matches, False otherwise

    Examples:
      >>> has_followers = await GraphQuery("user:alice").in_("follows").exists()
    """
    count = await self.count(client)
    return count > 0
