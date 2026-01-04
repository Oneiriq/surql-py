"""Query execution with result deserialization.

This module provides async functions for executing queries and deserializing
results using Pydantic models.
"""

from collections.abc import AsyncIterator
from typing import Any, TypeVar

import structlog
from pydantic import BaseModel, ValidationError

from reverie.connection.client import DatabaseClient, QueryError
from reverie.connection.context import get_db
from reverie.query.builder import Query
from reverie.query.results import ListResult, RecordResult, record, records

logger = structlog.get_logger(__name__)

T = TypeVar('T', bound=BaseModel)


async def execute_query[T: BaseModel](
  query: Query[T],
  client: DatabaseClient | None = None,
) -> Any:
  """Execute query and return raw results.

  Args:
    query: Query instance to execute
    client: Database client. If None, uses context client.

  Returns:
    Raw query results (or execution plan if EXPLAIN hint is present)

  Raises:
    QueryError: If query execution fails

  Examples:
    >>> query = Query().select().from_table('user')
    >>> results = await execute_query(query)

    >>> # With EXPLAIN hint, returns execution plan
    >>> query = Query().select().from_table('user').explain()
    >>> plan = await execute_query(query)
  """
  db = client or get_db()

  surql = query.to_surql()

  # Check for EXPLAIN hint
  has_explain = _has_explain_hint(query)

  logger.info(
    'executing_query',
    operation=query.operation,
    table=query.table_name,
    has_explain=has_explain,
  )
  logger.debug('query_sql', sql=surql)

  try:
    result = await db.execute(surql)
    logger.debug('query_executed', result_type=type(result).__name__)
    return result
  except Exception as e:
    logger.error('query_execution_failed', error=str(e), sql=surql)
    raise QueryError(f'Query execution failed: {e}') from e


async def fetch_one(
  query: Query[T],
  model: type[T],
  client: DatabaseClient | None = None,
) -> T | None:
  """Execute query and return single result.

  Args:
    query: Query instance to execute
    model: Pydantic model class for deserialization
    client: Database client. If None, uses context client.

  Returns:
    Single model instance or None if no results

  Raises:
    QueryError: If query execution fails
    ValidationError: If result validation fails

  Examples:
    >>> query = Query().select().from_table('user:alice')
    >>> user = await fetch_one(query, User)
  """
  result = await execute_query(query, client)

  # Handle different result formats from SurrealDB
  if result is None:
    return None

  # Extract actual data from SurrealDB response
  data = _extract_result_data(result)

  if not data:
    return None

  # Get first record if list
  if isinstance(data, list):
    if not data:
      return None
    data = data[0]

  try:
    return model.model_validate(data)
  except ValidationError as e:
    logger.error('validation_failed', error=str(e), data=data)
    raise


async def fetch_all(
  query: Query[T],
  model: type[T],
  client: DatabaseClient | None = None,
) -> list[T]:
  """Execute query and return all results.

  Args:
    query: Query instance to execute
    model: Pydantic model class for deserialization
    client: Database client. If None, uses context client.

  Returns:
    List of model instances

  Raises:
    QueryError: If query execution fails
    ValidationError: If result validation fails

  Examples:
    >>> query = Query().select().from_table('user').where('age > 18')
    >>> users = await fetch_all(query, User)
  """
  result = await execute_query(query, client)

  # Extract actual data from SurrealDB response
  data = _extract_result_data(result)

  if not data:
    return []

  # Ensure data is a list
  if not isinstance(data, list):
    data = [data]

  try:
    return [model.model_validate(item) for item in data]
  except ValidationError as e:
    logger.error('validation_failed', error=str(e), data=data)
    raise


async def fetch_many(
  query: Query[T],
  model: type[T],
  client: DatabaseClient | None = None,
  _batch_size: int = 100,
) -> AsyncIterator[T]:
  """Execute query and stream results as async iterator.

  Useful for large result sets to avoid loading all data into memory.

  Args:
    query: Query instance to execute
    model: Pydantic model class for deserialization
    client: Database client. If None, uses context client.
    batch_size: Number of records to fetch per batch

  Yields:
    Model instances one at a time

  Raises:
    QueryError: If query execution fails
    ValidationError: If result validation fails

  Examples:
    >>> query = Query().select().from_table('user')
    >>> async for user in fetch_many(query, User):
    ...     print(user.name)
  """
  # For now, fetch all and yield (SurrealDB client doesn't support streaming)
  # TODO: Implement true streaming when SurrealDB client supports it
  results = await fetch_all(query, model, client)

  for item in results:
    yield item


async def fetch_record(
  query: Query[T],
  model: type[T],
  client: DatabaseClient | None = None,
) -> RecordResult[T]:
  """Execute query and return single record result.

  Args:
    query: Query instance to execute
    model: Pydantic model class for deserialization
    client: Database client. If None, uses context client.

  Returns:
    RecordResult wrapper containing record or None

  Examples:
    >>> query = Query().select().from_table('user:alice')
    >>> result = await fetch_record(query, User)
    >>> if result.exists:
    ...     user = result.unwrap()
  """
  rec = await fetch_one(query, model, client)
  return record(rec, exists=rec is not None)


async def fetch_records(
  query: Query[T],
  model: type[T],
  client: DatabaseClient | None = None,
) -> ListResult[T]:
  """Execute query and return list result.

  Args:
    query: Query instance to execute
    model: Pydantic model class for deserialization
    client: Database client. If None, uses context client.

  Returns:
    ListResult wrapper containing records

  Examples:
    >>> query = Query().select().from_table('user').limit(10)
    >>> result = await fetch_records(query, User)
    >>> for user in result:
    ...     print(user.name)
  """
  items = await fetch_all(query, model, client)

  return records(
    items=items,
    limit=query.limit_value,
    offset=query.offset_value,
  )


async def execute_raw(
  sql: str,
  params: dict[str, Any] | None = None,
  client: DatabaseClient | None = None,
) -> Any:
  """Execute raw SurrealQL query.

  Args:
    sql: Raw SurrealQL query string
    params: Optional query parameters
    client: Database client. If None, uses context client.

  Returns:
    Raw query results

  Raises:
    QueryError: If query execution fails

  Examples:
    >>> results = await execute_raw('SELECT * FROM user WHERE age > $age', {'age': 18})
  """
  db = client or get_db()

  logger.info('executing_raw_query')
  logger.debug('raw_query_sql', sql=sql, params=params)

  try:
    result = await db.execute(sql, params)
    logger.debug('raw_query_executed', result_type=type(result).__name__)
    return result
  except Exception as e:
    logger.error('raw_query_failed', error=str(e), sql=sql)
    raise QueryError(f'Raw query execution failed: {e}') from e


async def execute_raw_typed[T: BaseModel](
  sql: str,
  model: type[T],
  params: dict[str, Any] | None = None,
  client: DatabaseClient | None = None,
) -> list[T]:
  """Execute raw SurrealQL query with type validation.

  Args:
    sql: Raw SurrealQL query string
    model: Pydantic model class for deserialization
    params: Optional query parameters
    client: Database client. If None, uses context client.

  Returns:
    List of validated model instances

  Raises:
    QueryError: If query execution fails
    ValidationError: If result validation fails

  Examples:
    >>> users = await execute_raw_typed(
    ...     'SELECT * FROM user WHERE age > $age',
    ...     User,
    ...     {'age': 18}
    ... )
  """
  result = await execute_raw(sql, params, client)

  # Extract actual data from SurrealDB response
  data = _extract_result_data(result)

  if not data:
    return []

  # Ensure data is a list
  if not isinstance(data, list):
    data = [data]

  try:
    return [model.model_validate(item) for item in data]
  except ValidationError as e:
    logger.error('validation_failed', error=str(e), data=data)
    raise


# Helper functions


def _extract_result_data(result: Any) -> Any:
  """Extract actual data from SurrealDB response.

  SurrealDB client may wrap results in various formats.
  This function normalizes the response.

  Args:
    result: Raw result from SurrealDB client

  Returns:
    Extracted data
  """
  if result is None:
    return None

  # Handle list of result objects with 'result' key
  if isinstance(result, list) and len(result) > 0:
    # Check if it's a list of result wrappers
    if isinstance(result[0], dict) and 'result' in result[0]:
      # Extract all results
      extracted = []
      for item in result:
        if isinstance(item, dict) and 'result' in item:
          res = item['result']
          if isinstance(res, list):
            extracted.extend(res)
          elif res is not None:
            extracted.append(res)
      return extracted if extracted else None
    else:
      return result

  # Handle single result object with 'result' key
  if isinstance(result, dict) and 'result' in result:
    return result['result']

  return result


def _has_explain_hint(query: Query[Any]) -> bool:
  """Check if query has an EXPLAIN hint.

  Args:
    query: Query instance to check

  Returns:
    True if query has EXPLAIN hint
  """
  from reverie.query.hints import ExplainHint

  return any(isinstance(hint, ExplainHint) for hint in query.hints)
