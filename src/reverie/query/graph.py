"""Graph traversal utilities for SurrealDB's graph capabilities.

This module provides functions for navigating relationships, creating edges,
and performing graph traversal operations.

It includes:
- GraphQuery: A fluent builder for graph traversal queries
- Helper functions for common graph patterns (mutual connections, shortest path, etc.)
- Basic traversal and relationship management functions
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import structlog
from pydantic import BaseModel

from reverie.connection.client import DatabaseClient
from reverie.connection.context import get_db
from reverie.query.builder import Query
from reverie.query.executor import fetch_all
from reverie.types.record_id import RecordID

logger = structlog.get_logger(__name__)


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
    depth_str = str(depth) if depth is not None else ''
    self._path.append(f'->{edge}{depth_str}')
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
    depth_str = str(depth) if depth is not None else ''
    self._path.append(f'<-{edge}{depth_str}')
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
    depth_str = str(depth) if depth is not None else ''
    self._path.append(f'<->{edge}{depth_str}')
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
    # Use count with limit 1 for efficiency
    original_limit = self._limit
    self._limit = 1

    try:
      count = await self.count(client)
      return count > 0
    finally:
      self._limit = original_limit


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
# Helper Functions for Common Graph Patterns
# =============================================================================


async def find_mutual_connections[T: BaseModel](
  record: str | RecordID[Any],
  edge: str,
  model: type[T] | None = None,
  client: DatabaseClient | None = None,
) -> list[T] | list[dict[str, Any]]:
  """Find records that have mutual edge connections with the given record.

  A mutual connection exists when record A has an outgoing edge to record B,
  AND record B has an outgoing edge back to record A.

  Args:
    record: Record ID to find mutual connections for
    edge: Edge table name (e.g., 'follows', 'knows')
    model: Optional Pydantic model for result deserialization
    client: Database client. If None, uses context client.

  Returns:
    List of records with mutual connections

  Examples:
    >>> # Find users who both follow and are followed by alice
    >>> mutual = await find_mutual_connections("user:alice", "follows", User)

    >>> # Find mutual friends
    >>> friends = await find_mutual_connections("user:bob", "knows")
  """
  record_str = str(record) if isinstance(record, RecordID) else record

  logger.info('finding_mutual_connections', record=record_str, edge=edge)

  # Query: Find records that I follow AND that follow me back
  # This uses set intersection via SurrealDB's graph traversal
  sql = f"""
    SELECT * FROM {record_str}->{edge}
    WHERE id IN (SELECT VALUE id FROM <-{edge}<-{record_str})
  """

  db = client or get_db()
  result = await db.execute(sql)
  data = _extract_graph_result(result)

  if model and data:
    return [model.model_validate(item) for item in data]

  return data


async def find_shortest_path(
  from_record: str | RecordID[Any],
  to_record: str | RecordID[Any],
  edge: str,
  max_depth: int = 10,
  client: DatabaseClient | None = None,
) -> list[dict[str, Any]]:
  """Find shortest path between two records through an edge type.

  Uses iterative deepening to find the shortest path from source to target
  through the specified edge type.

  Args:
    from_record: Source record ID
    to_record: Target record ID
    edge: Edge table to traverse
    max_depth: Maximum path depth to search (default: 10)
    client: Database client. If None, uses context client.

  Returns:
    List of records in the path (empty if no path found)

  Examples:
    >>> path = await find_shortest_path("user:alice", "user:charlie", "follows")
    >>> # Returns: [user:alice data, user:bob data, user:charlie data]

    >>> path = await find_shortest_path("user:a", "user:z", "knows", max_depth=5)
  """
  from_str = str(from_record) if isinstance(from_record, RecordID) else from_record
  to_str = str(to_record) if isinstance(to_record, RecordID) else to_record

  logger.info(
    'finding_shortest_path',
    from_record=from_str,
    to_record=to_str,
    edge=edge,
    max_depth=max_depth,
  )

  db = client or get_db()

  # Check if source and target are the same
  if from_str == to_str:
    sql = f'SELECT * FROM {from_str}'
    result = await db.execute(sql)
    return _extract_graph_result(result)

  # Iterative deepening search
  for depth in range(1, max_depth + 1):
    # Build path query for current depth
    # Use depth notation: ->edge{depth}->
    sql = f"""
      SELECT * FROM {from_str}->{edge}{depth}->
      WHERE id = {to_str}
      LIMIT 1
    """

    result = await db.execute(sql)
    data = _extract_graph_result(result)

    if data:
      # Found a path - now reconstruct it
      logger.info('path_found', depth=depth)

      # Get the full path by traversing step by step
      path_records = await _reconstruct_path(from_str, to_str, edge, depth, db)
      return path_records

  logger.info('no_path_found', max_depth=max_depth)
  return []


async def _reconstruct_path(
  from_str: str,
  to_str: str,
  edge: str,
  depth: int,
  db: DatabaseClient,
) -> list[dict[str, Any]]:
  """Reconstruct the path between two records.

  Args:
    from_str: Source record ID string
    to_str: Target record ID string
    edge: Edge table name
    depth: Known path depth
    db: Database client

  Returns:
    List of records in the path
  """
  # Get source record
  source_sql = f'SELECT * FROM {from_str}'
  source_result = await db.execute(source_sql)
  source_data = _extract_graph_result(source_result)

  if depth == 1:
    # Direct connection
    target_sql = f'SELECT * FROM {to_str}'
    target_result = await db.execute(target_sql)
    target_data = _extract_graph_result(target_result)
    return source_data + target_data

  # For longer paths, we need to find intermediate nodes
  # This is a simplified reconstruction - for complex graphs,
  # you might want to use a BFS approach
  path: list[dict[str, Any]] = source_data.copy()

  current = from_str
  for _ in range(depth):
    # Find next node in path towards target
    sql = f"""
      SELECT * FROM {current}->{edge}
      WHERE id = {to_str} OR id IN (
        SELECT VALUE id FROM {current}->{edge}
        WHERE id IN (SELECT VALUE id FROM <-{edge}{depth}<-{to_str})
      )
      LIMIT 1
    """
    result = await db.execute(sql)
    data = _extract_graph_result(result)

    if data:
      next_record = data[0]
      path.append(next_record)
      next_id = next_record.get('id', '')
      if next_id == to_str or str(next_id) == to_str:
        break
      current = str(next_id)
    else:
      break

  return path


async def get_neighbors(
  record: str | RecordID[Any],
  edge: str,
  depth: int = 1,
  direction: Literal['in', 'out', 'both'] = 'both',
  client: DatabaseClient | None = None,
) -> list[dict[str, Any]]:
  """Get all neighbors within N hops.

  Retrieves all records connected to the given record through the specified
  edge type, up to the given depth.

  Args:
    record: Starting record ID
    edge: Edge table name to traverse
    depth: Number of hops (default: 1)
    direction: Traversal direction - 'in', 'out', or 'both' (default: 'both')
    client: Database client. If None, uses context client.

  Returns:
    List of neighbor records (with potential duplicates removed)

  Examples:
    >>> # Get direct followers and followees
    >>> neighbors = await get_neighbors("user:alice", "follows", depth=1, direction="both")

    >>> # Get all users within 2 hops via 'knows' edge
    >>> extended = await get_neighbors("user:alice", "knows", depth=2)
  """
  record_str = str(record) if isinstance(record, RecordID) else record

  logger.info(
    'getting_neighbors',
    record=record_str,
    edge=edge,
    depth=depth,
    direction=direction,
  )

  # Build direction arrow
  if direction == 'out':
    arrow = '->'
  elif direction == 'in':
    arrow = '<-'
  else:  # both
    arrow = '<->'

  # Build depth range for all depths up to specified
  # Collect from depth 1 to specified depth
  all_neighbors: list[dict[str, Any]] = []
  seen_ids: set[str] = set()

  db = client or get_db()

  for d in range(1, depth + 1):
    sql = f'SELECT * FROM {record_str}{arrow}{edge}{d}'
    result = await db.execute(sql)
    data = _extract_graph_result(result)

    # Deduplicate by ID
    for item in data:
      item_id = str(item.get('id', ''))
      if item_id and item_id not in seen_ids and item_id != record_str:
        seen_ids.add(item_id)
        all_neighbors.append(item)

  return all_neighbors


async def compute_degree(
  record: str | RecordID[Any],
  edge: str,
  client: DatabaseClient | None = None,
) -> dict[str, int]:
  """Compute in-degree, out-degree, and total degree for a record.

  The degree of a node in a graph is the number of edges connected to it:
  - In-degree: Number of incoming edges
  - Out-degree: Number of outgoing edges
  - Total: Sum of in-degree and out-degree

  Args:
    record: Record ID to compute degree for
    edge: Edge table name
    client: Database client. If None, uses context client.

  Returns:
    Dictionary with 'in_degree', 'out_degree', and 'total' counts

  Examples:
    >>> degree = await compute_degree("user:alice", "follows")
    >>> print(f"In: {degree['in_degree']}, Out: {degree['out_degree']}")
    >>> # Output: In: 5, Out: 3

    >>> degree = await compute_degree("post:123", "likes")
    >>> print(f"Total likes: {degree['in_degree']}")  # Posts receive likes
  """
  record_str = str(record) if isinstance(record, RecordID) else record

  logger.info('computing_degree', record=record_str, edge=edge)

  db = client or get_db()

  # Count outgoing edges
  out_sql = f'SELECT count() FROM {record_str}->{edge} GROUP ALL'
  out_result = await db.execute(out_sql)
  out_data = _extract_graph_result(out_result)
  out_degree = 0
  if out_data and isinstance(out_data[0], dict):
    out_degree = out_data[0].get('count', 0)

  # Count incoming edges
  in_sql = f'SELECT count() FROM <-{edge}<-{record_str} GROUP ALL'
  in_result = await db.execute(in_sql)
  in_data = _extract_graph_result(in_result)
  in_degree = 0
  if in_data and isinstance(in_data[0], dict):
    in_degree = in_data[0].get('count', 0)

  return {
    'in_degree': in_degree,
    'out_degree': out_degree,
    'total': in_degree + out_degree,
  }


# =============================================================================
# Original Graph Functions (preserved for backward compatibility)
# =============================================================================


async def traverse[T: BaseModel](
  start: str | RecordID[Any],
  path: str,
  model: type[T],
  client: DatabaseClient | None = None,
) -> list[T]:
  """Navigate relationships using graph traversal.

  Args:
    start: Starting record ID
    path: Traversal path (e.g., '->likes->post', '<-follows<-user')
    model: Pydantic model class for deserialization
    client: Database client. If None, uses context client.

  Returns:
    List of records reached through traversal

  Raises:
    QueryError: If traversal fails

  Examples:
    >>> # Find all posts liked by user
    >>> posts = await traverse('user:alice', '->likes->post', Post)

    >>> # Find all followers of user
    >>> followers = await traverse('user:alice', '<-follows<-user', User)

    >>> # Multi-hop: followers of followers
    >>> fof = await traverse('user:alice', '<-follows<-user<-follows<-user', User)
  """
  start_str = str(start) if isinstance(start, RecordID) else start

  logger.info('traversing_graph', start=start_str, path=path)

  query = Query[T]().select().from_table(start_str).traverse(path)

  return await fetch_all(query, model, client)


async def traverse_with_depth[T: BaseModel](
  start: str | RecordID[Any],
  edge_table: str,
  target_table: str,
  direction: str = 'out',
  depth: int | None = None,
  model: type[T] | None = None,
  client: DatabaseClient | None = None,
) -> list[T] | list[dict[str, Any]]:
  """Navigate relationships with optional depth limit.

  Args:
    start: Starting record ID
    edge_table: Edge table name
    target_table: Target table name
    direction: Traversal direction ('out', 'in', 'both')
    depth: Maximum depth (None for unlimited)
    model: Optional Pydantic model for deserialization
    client: Database client. If None, uses context client.

  Returns:
    List of records or dicts

  Raises:
    QueryError: If traversal fails

  Examples:
    >>> # Find posts liked by user (depth 1)
    >>> posts = await traverse_with_depth('user:alice', 'likes', 'post', 'out', 1, Post)

    >>> # Find all connected users (any depth)
    >>> users = await traverse_with_depth('user:alice', 'follows', 'user', 'both', None, User)
  """
  start_str = str(start) if isinstance(start, RecordID) else start

  # Build traversal path
  if direction == 'out':
    arrow = '->'
  elif direction == 'in':
    arrow = '<-'
  elif direction == 'both':
    arrow = '<->'
  else:
    raise ValueError(f'Invalid direction: {direction}. Must be "out", "in", or "both"')

  depth_str = str(depth) if depth is not None else ''
  path = f'{arrow}{edge_table}{depth_str}{arrow}{target_table}'

  logger.info('traversing_with_depth', start=start_str, path=path, depth=depth)

  if model:
    return await traverse(start_str, path, model, client)
  else:
    # Return raw results
    query: Query[Any] = Query().select().from_table(start_str).traverse(path)
    db = client or get_db()
    result = await db.execute(query.to_surql())

    # Extract data from result
    if (
      isinstance(result, list)
      and len(result) > 0
      and isinstance(result[0], dict)
      and 'result' in result[0]
    ):
      return result[0]['result']  # type: ignore[no-any-return]

    return result if isinstance(result, list) else [result] if result else []


async def relate(
  edge_table: str,
  from_record: str | RecordID[Any],
  to_record: str | RecordID[Any],
  data: dict[str, Any] | None = None,
  client: DatabaseClient | None = None,
) -> dict[str, Any]:
  """Create an edge relationship between two records.

  Args:
    edge_table: Edge table name
    from_record: Source record ID
    to_record: Target record ID
    data: Optional edge properties
    client: Database client. If None, uses context client.

  Returns:
    Created edge record

  Raises:
    QueryError: If relation creation fails

  Examples:
    >>> # Create simple edge
    >>> edge = await relate('likes', 'user:alice', 'post:123')

    >>> # Create edge with properties
    >>> edge = await relate(
    ...     'follows',
    ...     'user:alice',
    ...     'user:bob',
    ...     {'since': '2024-01-01', 'weight': 1}
    ... )
  """
  from_str = str(from_record) if isinstance(from_record, RecordID) else from_record
  to_str = str(to_record) if isinstance(to_record, RecordID) else to_record

  logger.info('creating_relation', edge_table=edge_table, from_record=from_str, to_record=to_str)

  query: Query[Any] = Query().relate(edge_table, from_str, to_str, data)

  db = client or get_db()
  result = await db.execute(query.to_surql())

  logger.info('relation_created', edge_table=edge_table)

  # Extract result data
  if (
    isinstance(result, list)
    and len(result) > 0
    and isinstance(result[0], dict)
    and 'result' in result[0]
  ):
    return result[0]['result']  # type: ignore[no-any-return]

  return result  # type: ignore[no-any-return]


async def unrelate(
  edge_table: str,
  from_record: str | RecordID[Any],
  to_record: str | RecordID[Any],
  client: DatabaseClient | None = None,
) -> None:
  """Remove an edge relationship between two records.

  Args:
    edge_table: Edge table name
    from_record: Source record ID
    to_record: Target record ID
    client: Database client. If None, uses context client.

  Raises:
    QueryError: If relation removal fails

  Examples:
    >>> await unrelate('likes', 'user:alice', 'post:123')
  """
  from_str = str(from_record) if isinstance(from_record, RecordID) else from_record
  to_str = str(to_record) if isinstance(to_record, RecordID) else to_record

  logger.info('removing_relation', edge_table=edge_table, from_record=from_str, to_record=to_str)

  # Delete the specific edge
  sql = f'DELETE {from_str}->{edge_table}->{to_str}'

  db = client or get_db()
  await db.execute(sql)

  logger.info('relation_removed', edge_table=edge_table)


async def get_outgoing_edges[T: BaseModel](
  record: str | RecordID[Any],
  edge_table: str,
  model: type[T] | None = None,
  client: DatabaseClient | None = None,
) -> list[T] | list[dict[str, Any]]:
  """Get all outgoing edges from a record.

  Args:
    record: Source record ID
    edge_table: Edge table name
    model: Optional Pydantic model for deserialization
    client: Database client. If None, uses context client.

  Returns:
    List of edge records

  Examples:
    >>> # Get all likes from user
    >>> likes = await get_outgoing_edges('user:alice', 'likes')
  """
  record_str = str(record) if isinstance(record, RecordID) else record

  logger.info('fetching_outgoing_edges', record=record_str, edge_table=edge_table)

  sql = f'SELECT * FROM {record_str}->{edge_table}'

  db = client or get_db()
  result = await db.execute(sql)

  # Extract data
  data: list[Any] = []
  if isinstance(result, list) and len(result) > 0:
    data = result[0]['result'] if isinstance(result[0], dict) and 'result' in result[0] else result

  if model and data:
    return [model.model_validate(item) for item in data]

  return data


async def get_incoming_edges[T: BaseModel](
  record: str | RecordID[Any],
  edge_table: str,
  model: type[T] | None = None,
  client: DatabaseClient | None = None,
) -> list[T] | list[dict[str, Any]]:
  """Get all incoming edges to a record.

  Args:
    record: Target record ID
    edge_table: Edge table name
    model: Optional Pydantic model for deserialization
    client: Database client. If None, uses context client.

  Returns:
    List of edge records

  Examples:
    >>> # Get all follows to user
    >>> follows = await get_incoming_edges('user:alice', 'follows')
  """
  record_str = str(record) if isinstance(record, RecordID) else record

  logger.info('fetching_incoming_edges', record=record_str, edge_table=edge_table)

  sql = f'SELECT * FROM <-{edge_table}<-{record_str}'

  db = client or get_db()
  result = await db.execute(sql)

  # Extract data
  data: list[Any] = []
  if isinstance(result, list) and len(result) > 0:
    data = result[0]['result'] if isinstance(result[0], dict) and 'result' in result[0] else result

  if model and data:
    return [model.model_validate(item) for item in data]

  return data


async def get_related_records[T: BaseModel](
  record: str | RecordID[Any],
  edge_table: str,
  target_table: str,
  direction: str = 'out',
  model: type[T] | None = None,
  client: DatabaseClient | None = None,
) -> list[T] | list[dict[str, Any]]:
  """Get records related through an edge.

  Args:
    record: Source or target record ID
    edge_table: Edge table name
    target_table: Related records table name
    direction: Relationship direction ('out' or 'in')
    model: Optional Pydantic model for deserialization
    client: Database client. If None, uses context client.

  Returns:
    List of related records

  Examples:
    >>> # Get all posts liked by user
    >>> posts = await get_related_records('user:alice', 'likes', 'post', 'out', Post)

    >>> # Get all users who liked a post
    >>> users = await get_related_records('post:123', 'likes', 'user', 'in', User)
  """
  record_str = str(record) if isinstance(record, RecordID) else record

  logger.info(
    'fetching_related_records', record=record_str, edge_table=edge_table, target_table=target_table
  )

  if direction == 'out':
    path = f'->{edge_table}->{target_table}'
  elif direction == 'in':
    path = f'<-{edge_table}<-{target_table}'
  else:
    raise ValueError(f'Invalid direction: {direction}. Must be "out" or "in"')

  if model:
    return await traverse(record_str, path, model, client)
  else:
    sql = f'SELECT * FROM {record_str}{path}'
    db = client or get_db()
    result = await db.execute(sql)

    # Extract data
    if (
      isinstance(result, list)
      and len(result) > 0
      and isinstance(result[0], dict)
      and 'result' in result[0]
    ):
      return result[0]['result']  # type: ignore[no-any-return]

    return result if isinstance(result, list) else [result] if result else []


async def count_related(
  record: str | RecordID[Any],
  edge_table: str,
  direction: str = 'out',
  client: DatabaseClient | None = None,
) -> int:
  """Count related records through an edge.

  Args:
    record: Source or target record ID
    edge_table: Edge table name
    direction: Relationship direction ('out' or 'in')
    client: Database client. If None, uses context client.

  Returns:
    Number of related records

  Examples:
    >>> # Count posts liked by user
    >>> count = await count_related('user:alice', 'likes', 'out')

    >>> # Count followers of user
    >>> count = await count_related('user:alice', 'follows', 'in')
  """
  record_str = str(record) if isinstance(record, RecordID) else record

  logger.info('counting_related', record=record_str, edge_table=edge_table, direction=direction)

  if direction == 'out':
    sql = f'SELECT count() FROM {record_str}->{edge_table}'
  elif direction == 'in':
    sql = f'SELECT count() FROM <-{edge_table}<-{record_str}'
  else:
    raise ValueError(f'Invalid direction: {direction}. Must be "out" or "in"')

  db = client or get_db()
  result = await db.execute(sql)

  # Extract count
  if (
    isinstance(result, list)
    and len(result) > 0
    and isinstance(result[0], dict)
    and 'result' in result[0]
  ):
    data = result[0]['result']
    if isinstance(data, list) and len(data) > 0:
      return data[0].get('count', 0)  # type: ignore[no-any-return]

  return 0


async def shortest_path(
  from_record: str | RecordID[Any],
  to_record: str | RecordID[Any],
  edge_table: str,
  max_depth: int = 10,
  client: DatabaseClient | None = None,
) -> list[dict[str, Any]]:
  """Find shortest path between two records.

  Note: This is a simple implementation. For complex graph algorithms,
  consider using specialized graph databases or algorithms.

  Args:
    from_record: Source record ID
    to_record: Target record ID
    edge_table: Edge table to traverse
    max_depth: Maximum path depth to search
    client: Database client. If None, uses context client.

  Returns:
    List of records in the path

  Examples:
    >>> path = await shortest_path('user:alice', 'user:charlie', 'follows', max_depth=5)
  """
  from_str = str(from_record) if isinstance(from_record, RecordID) else from_record
  to_str = str(to_record) if isinstance(to_record, RecordID) else to_record

  logger.info('finding_shortest_path', from_record=from_str, to_record=to_str, max_depth=max_depth)

  # Use iterative depth-first search
  # This is a simplified implementation
  for depth in range(1, max_depth + 1):
    sql = f"""
    SELECT * FROM {from_str}
    ->{edge_table}{depth}->
    WHERE id = {to_str}
    """

    db = client or get_db()
    result = await db.execute(sql)

    if result:
      # Found path at this depth
      logger.info('path_found', depth=depth)
      return result  # type: ignore[no-any-return]

  logger.info('no_path_found', max_depth=max_depth)
  return []
