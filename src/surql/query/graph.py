"""Graph traversal utilities for SurrealDB's graph capabilities.

This module provides functions for navigating relationships, creating edges,
and performing graph traversal operations.

It includes:
- Helper functions for common graph patterns (mutual connections, shortest path, etc.)
- Basic traversal and relationship management functions
- Re-exports GraphQuery from graph_query for backward compatibility
"""

from __future__ import annotations

from typing import Any, Literal

import structlog
from pydantic import BaseModel

from surql.connection.client import DatabaseClient
from surql.connection.context import get_db
from surql.query.builder import Query
from surql.query.executor import fetch_all
from surql.query.graph_query import GraphQuery, _extract_graph_result
from surql.types.record_id import RecordID

# Re-export GraphQuery for backward compatibility
__all__ = ['GraphQuery']

logger = structlog.get_logger(__name__)


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
