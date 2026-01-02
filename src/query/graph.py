"""Graph traversal utilities for SurrealDB's graph capabilities.

This module provides functions for navigating relationships, creating edges,
and performing graph traversal operations.
"""

from __future__ import annotations

from typing import Any, TypeVar

import structlog
from pydantic import BaseModel

from src.connection.client import DatabaseClient
from src.connection.context import get_db
from src.query.builder import Query
from src.query.executor import fetch_all
from src.types.record_id import RecordID

logger = structlog.get_logger(__name__)

T = TypeVar('T', bound=BaseModel)


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
