"""High-level CRUD operations for database records.

This module provides convenient async functions for common database operations
with type safety through Pydantic models.
"""

from typing import Any, TypeVar

import structlog
from pydantic import BaseModel

from src.connection.client import DatabaseClient
from src.connection.context import get_db
from src.query.builder import Query
from src.query.executor import fetch_all, fetch_one
from src.query.results import RecordResult, ListResult, record, records
from src.types.operators import Operator
from src.types.record_id import RecordID

logger = structlog.get_logger(__name__)

T = TypeVar('T', bound=BaseModel)


async def create_record(
  table: str,
  data: T | dict[str, Any],
  client: DatabaseClient | None = None,
) -> dict[str, Any]:
  """Create a single record in the database.
  
  Args:
    table: Table name to insert into
    data: Record data (Pydantic model or dict)
    client: Database client. If None, uses context client.
    
  Returns:
    Created record with ID
    
  Raises:
    QueryError: If creation fails
    
  Examples:
    >>> user = User(name='Alice', email='alice@example.com')
    >>> created = await create_record('user', user)
    >>> print(created['id'])
  """
  db = client or get_db()
  
  # Convert Pydantic model to dict if needed
  record_data = data.model_dump() if isinstance(data, BaseModel) else data
  
  logger.info('creating_record', table=table)
  logger.debug('record_data', data=record_data)
  
  result = await db.create(table, record_data)
  
  logger.info('record_created', table=table, record_id=result.get('id') if isinstance(result, dict) else None)
  
  return result  # type: ignore[no-any-return]


async def create_records(
  table: str,
  data: list[T] | list[dict[str, Any]],
  client: DatabaseClient | None = None,
) -> list[dict[str, Any]]:
  """Create multiple records in the database.
  
  Args:
    table: Table name to insert into
    data: List of record data (Pydantic models or dicts)
    client: Database client. If None, uses context client.
    
  Returns:
    List of created records with IDs
    
  Raises:
    QueryError: If creation fails
    
  Examples:
    >>> users = [User(name='Alice'), User(name='Bob')]
    >>> created = await create_records('user', users)
  """
  db = client or get_db()
  
  logger.info('creating_multiple_records', table=table, count=len(data))
  
  results = []
  for item in data:
    record_data = item.model_dump() if isinstance(item, BaseModel) else item
    result = await db.create(table, record_data)
    results.append(result)
  
  logger.info('records_created', table=table, count=len(results))
  
  return results


async def get_record(
  table: str,
  record_id: str | 'RecordID[Any]',
  model: type[T],
  client: DatabaseClient | None = None,
) -> T | None:
  """Fetch a single record by ID.
  
  Args:
    table: Table name
    record_id: Record ID (string or RecordID instance)
    model: Pydantic model class for deserialization
    client: Database client. If None, uses context client.
    
  Returns:
    Model instance or None if not found
    
  Raises:
    QueryError: If fetch fails
    
  Examples:
    >>> user = await get_record('user', 'alice', User)
    >>> if user:
    ...     print(user.name)
  """
  db = client or get_db()
  
  # Build record target
  if isinstance(record_id, RecordID):
    target = str(record_id)
  else:
    target = f'{table}:{record_id}'
  
  logger.info('fetching_record', target=target)
  
  result = await db.select(target)
  
  if not result:
    logger.debug('record_not_found', target=target)
    return None
  
  # Handle list response from SurrealDB
  if isinstance(result, list):
    result = result[0] if result else None
  
  if not result:
    return None
  
  return model.model_validate(result)


async def update_record(
  table: str,
  record_id: str | 'RecordID[Any]',
  data: T | dict[str, Any],
  client: DatabaseClient | None = None,
) -> dict[str, Any]:
  """Update an existing record.
  
  Replaces the entire record with new data.
  
  Args:
    table: Table name
    record_id: Record ID
    data: New record data (Pydantic model or dict)
    client: Database client. If None, uses context client.
    
  Returns:
    Updated record
    
  Raises:
    QueryError: If update fails
    
  Examples:
    >>> user = User(name='Alice Updated', email='alice@example.com')
    >>> updated = await update_record('user', 'alice', user)
  """
  db = client or get_db()
  
  # Build record target
  if isinstance(record_id, RecordID):
    target = str(record_id)
  else:
    target = f'{table}:{record_id}'
  
  # Convert Pydantic model to dict if needed
  record_data = data.model_dump() if isinstance(data, BaseModel) else data
  
  logger.info('updating_record', target=target)
  logger.debug('update_data', data=record_data)
  
  result = await db.update(target, record_data)
  
  logger.info('record_updated', target=target)
  
  return result  # type: ignore[no-any-return]


async def merge_record(
  table: str,
  record_id: str | 'RecordID[Any]',
  data: dict[str, Any],
  client: DatabaseClient | None = None,
) -> dict[str, Any]:
  """Merge data into an existing record.
  
  Updates only the specified fields, keeping others unchanged.
  
  Args:
    table: Table name
    record_id: Record ID
    data: Partial data to merge
    client: Database client. If None, uses context client.
    
  Returns:
    Updated record
    
  Raises:
    QueryError: If merge fails
    
  Examples:
    >>> updated = await merge_record('user', 'alice', {'status': 'active'})
  """
  db = client or get_db()
  
  # Build record target
  if isinstance(record_id, RecordID):
    target = str(record_id)
  else:
    target = f'{table}:{record_id}'
  
  logger.info('merging_record', target=target)
  logger.debug('merge_data', data=data)
  
  result = await db.merge(target, data)
  
  logger.info('record_merged', target=target)
  
  return result  # type: ignore[no-any-return]


async def delete_record(
  table: str,
  record_id: str | 'RecordID[Any]',
  client: DatabaseClient | None = None,
) -> None:
  """Delete a record from the database.
  
  Args:
    table: Table name
    record_id: Record ID
    client: Database client. If None, uses context client.
    
  Raises:
    QueryError: If deletion fails
    
  Examples:
    >>> await delete_record('user', 'alice')
  """
  db = client or get_db()
  
  # Build record target
  if isinstance(record_id, RecordID):
    target = str(record_id)
  else:
    target = f'{table}:{record_id}'
  
  logger.info('deleting_record', target=target)
  
  await db.delete(target)
  
  logger.info('record_deleted', target=target)


async def delete_records(
  table: str,
  condition: str | Operator | None = None,
  client: DatabaseClient | None = None,
) -> None:
  """Delete multiple records matching condition.
  
  Args:
    table: Table name
    condition: Optional WHERE condition
    client: Database client. If None, uses context client.
    
  Raises:
    QueryError: If deletion fails
    
  Examples:
    >>> await delete_records('user', 'status = "inactive"')
    >>> from src.types.operators import eq
    >>> await delete_records('user', eq('status', 'inactive'))
  """
  db = client or get_db()
  
  query: Query[Any] = Query().delete(table)
  
  if condition:
    query = query.where(condition)
  
  logger.info('deleting_records', table=table, has_condition=condition is not None)
  
  await db.execute(query.to_surql())
  
  logger.info('records_deleted', table=table)


async def query_records(
  table: str,
  model: type[T],
  conditions: list[str | Operator] | None = None,
  order_by: tuple[str, str] | None = None,
  limit: int | None = None,
  offset: int | None = None,
  client: DatabaseClient | None = None,
) -> list[T]:
  """Query records with filters and pagination.
  
  Args:
    table: Table name
    model: Pydantic model class for deserialization
    conditions: List of WHERE conditions
    order_by: Tuple of (field, direction)
    limit: Maximum number of results
    offset: Number of results to skip
    client: Database client. If None, uses context client.
    
  Returns:
    List of model instances
    
  Raises:
    QueryError: If query fails
    
  Examples:
    >>> users = await query_records(
    ...     'user',
    ...     User,
    ...     conditions=['age > 18', 'status = "active"'],
    ...     order_by=('created_at', 'DESC'),
    ...     limit=10
    ... )
  """
  query = Query[T]().select().from_table(table)
  
  if conditions:
    for condition in conditions:
      query = query.where(condition)
  
  if order_by:
    field, direction = order_by
    query = query.order_by(field, direction)
  
  if limit is not None:
    query = query.limit(limit)
  
  if offset is not None:
    query = query.offset(offset)
  
  return await fetch_all(query, model, client)


async def query_records_wrapped(
  table: str,
  model: type[T],
  conditions: list[str | Operator] | None = None,
  order_by: tuple[str, str] | None = None,
  limit: int | None = None,
  offset: int | None = None,
  client: DatabaseClient | None = None,
) -> ListResult[T]:
  """Query records and return wrapped result.
  
  Args:
    table: Table name
    model: Pydantic model class for deserialization
    conditions: List of WHERE conditions
    order_by: Tuple of (field, direction)
    limit: Maximum number of results
    offset: Number of results to skip
    client: Database client. If None, uses context client.
    
  Returns:
    ListResult wrapper containing records
    
  Examples:
    >>> result = await query_records_wrapped('user', User, limit=10)
    >>> for user in result:
    ...     print(user.name)
  """
  items = await query_records(
    table=table,
    model=model,
    conditions=conditions,
    order_by=order_by,
    limit=limit,
    offset=offset,
    client=client,
  )
  
  return records(items=items, limit=limit, offset=offset)


async def count_records(
  table: str,
  condition: str | Operator | None = None,
  client: DatabaseClient | None = None,
) -> int:
  """Count records in a table.
  
  Args:
    table: Table name
    condition: Optional WHERE condition
    client: Database client. If None, uses context client.
    
  Returns:
    Number of records
    
  Raises:
    QueryError: If query fails
    
  Examples:
    >>> total = await count_records('user')
    >>> active = await count_records('user', 'status = "active"')
  """
  db = client or get_db()
  
  query: Query[Any] = Query().select(['count()']).from_table(table)
  
  if condition:
    query = query.where(condition)
  
  logger.info('counting_records', table=table)
  
  result = await db.execute(query.to_surql())
  
  # Extract count from result
  if isinstance(result, list) and len(result) > 0:
    if isinstance(result[0], dict):
      if 'result' in result[0]:
        data = result[0]['result']
        if isinstance(data, list) and len(data) > 0:
          return data[0].get('count', 0)  # type: ignore[no-any-return]
  
  return 0


async def exists(
  table: str,
  record_id: str | 'RecordID[Any]',
  client: DatabaseClient | None = None,
) -> bool:
  """Check if a record exists.
  
  Args:
    table: Table name
    record_id: Record ID
    client: Database client. If None, uses context client.
    
  Returns:
    True if record exists, False otherwise
    
  Examples:
    >>> if await exists('user', 'alice'):
    ...     print('User exists')
  """
  result = await get_record(table, record_id, BaseModel, client)
  return result is not None


async def first(
  table: str,
  model: type[T],
  condition: str | Operator | None = None,
  order_by: tuple[str, str] | None = None,
  client: DatabaseClient | None = None,
) -> T | None:
  """Get the first record matching conditions.
  
  Args:
    table: Table name
    model: Pydantic model class for deserialization
    condition: Optional WHERE condition
    order_by: Optional tuple of (field, direction)
    client: Database client. If None, uses context client.
    
  Returns:
    First matching record or None
    
  Examples:
    >>> user = await first('user', User, 'age > 18', ('created_at', 'DESC'))
  """
  results = await query_records(
    table=table,
    model=model,
    conditions=[condition] if condition else None,
    order_by=order_by,
    limit=1,
    client=client,
  )
  
  return results[0] if results else None


async def last(
  table: str,
  model: type[T],
  condition: str | Operator | None = None,
  order_by: tuple[str, str] | None = None,
  client: DatabaseClient | None = None,
) -> T | None:
  """Get the last record matching conditions.
  
  Args:
    table: Table name
    model: Pydantic model class for deserialization
    condition: Optional WHERE condition
    order_by: Optional tuple of (field, direction) - will be reversed
    client: Database client. If None, uses context client.
    
  Returns:
    Last matching record or None
    
  Examples:
    >>> user = await last('user', User, 'age > 18', ('created_at', 'ASC'))
  """
  # Reverse order direction to get last
  if order_by:
    field, direction = order_by
    reversed_direction = 'DESC' if direction.upper() == 'ASC' else 'ASC'
    order_by = (field, reversed_direction)
  
  results = await query_records(
    table=table,
    model=model,
    conditions=[condition] if condition else None,
    order_by=order_by,
    limit=1,
    client=client,
  )
  
  return results[0] if results else None
