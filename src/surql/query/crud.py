"""High-level CRUD operations for database records.

This module provides convenient async functions for common database operations
with type safety through Pydantic models.
"""

from __future__ import annotations

from typing import Any, TypeVar

import structlog
from pydantic import BaseModel

from surql.connection.client import DatabaseClient
from surql.connection.context import get_db
from surql.query.builder import Query
from surql.query.executor import fetch_all
from surql.query.results import ListResult, extract_result, records
from surql.types.operators import Operator, _validate_identifier
from surql.types.record_id import RecordID

logger = structlog.get_logger(__name__)

T = TypeVar('T', bound=BaseModel)


async def create_record[T: BaseModel](
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

  logger.info(
    'record_created', table=table, record_id=result.get('id') if isinstance(result, dict) else None
  )

  return result  # type: ignore[no-any-return]


async def create_records[T: BaseModel](
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


async def get_record[T: BaseModel](
  table: str,
  record_id: str | RecordID[Any],
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
  target = str(record_id) if isinstance(record_id, RecordID) else f'{table}:{record_id}'

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


async def update_record[T: BaseModel](
  table: str,
  record_id: str | RecordID[Any],
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
  target = str(record_id) if isinstance(record_id, RecordID) else f'{table}:{record_id}'

  # Convert Pydantic model to dict if needed
  record_data = data.model_dump() if isinstance(data, BaseModel) else data

  logger.info('updating_record', target=target)
  logger.debug('update_data', data=record_data)

  result = await db.update(target, record_data)

  logger.info('record_updated', target=target)

  return result  # type: ignore[no-any-return]


async def merge_record(
  table: str,
  record_id: str | RecordID[Any],
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
  target = str(record_id) if isinstance(record_id, RecordID) else f'{table}:{record_id}'

  logger.info('merging_record', target=target)
  logger.debug('merge_data', data=data)

  result = await db.merge(target, data)

  logger.info('record_merged', target=target)

  return result  # type: ignore[no-any-return]


async def delete_record(
  table: str,
  record_id: str | RecordID[Any],
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
  target = str(record_id) if isinstance(record_id, RecordID) else f'{table}:{record_id}'

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
    >>> from surql.types.operators import eq
    >>> await delete_records('user', eq('status', 'inactive'))
  """
  db = client or get_db()

  query: Query[Any] = Query().delete(table)

  if condition:
    query = query.where(condition)

  logger.info('deleting_records', table=table, has_condition=condition is not None)

  await db.execute(query.to_surql())

  logger.info('records_deleted', table=table)


async def query_records[T: BaseModel](
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


async def query_records_wrapped[T: BaseModel](
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

  # SurrealDB v3 returns one result row per matched record for a bare
  # `SELECT count() FROM <table>`, e.g. 42 records -> 42 rows of
  # `{count: 1}`. Our downstream extractor reads only `data[0]`, which
  # would silently collapse the count to `1`. Append `GROUP ALL` so the
  # server returns a single aggregated row `[{count: N}]`.
  query: Query[Any] = Query().select(['count()']).from_table(table).group_all()

  if condition:
    query = query.where(condition)

  logger.info('counting_records', table=table)

  result = await db.execute(query.to_surql())

  # The SDK's ``query`` method returns two possible shapes depending on
  # the server version / statement count:
  #
  #   - ``[{'status': 'OK', 'result': [{'count': N}], ...}]`` — classic
  #     response envelope.
  #   - ``[{'count': N}]`` — SDK 2.x unwraps single-statement queries.
  #
  # Accept both so an SDK upgrade does not silently collapse the count
  # to zero. (Prior to this, bug #30, the bare-list shape fell through
  # to ``return 0`` on v3.)
  if not isinstance(result, list) or not result:
    return 0

  first = result[0]
  rows = first['result'] if isinstance(first, dict) and 'result' in first else result

  if isinstance(rows, list) and rows and isinstance(rows[0], dict):
    return int(rows[0].get('count', 0))

  return 0


async def upsert_record(
  table: str,
  record_id: str | RecordID[Any],
  data: dict[str, Any] | BaseModel,
  client: DatabaseClient | None = None,
) -> dict[str, Any]:
  """Insert or update a record.

  Creates the record if it does not exist, or updates it if it does.

  Args:
    table: Table name
    record_id: Record ID
    data: Record data (Pydantic model or dict)
    client: Database client. If None, uses context client.

  Returns:
    Upserted record

  Raises:
    QueryError: If upsert fails

  Examples:
    >>> result = await upsert_record('user', 'alice', {'name': 'Alice', 'status': 'active'})
  """
  db = client or get_db()

  target = str(record_id) if isinstance(record_id, RecordID) else f'{table}:{record_id}'
  record_data = data.model_dump() if isinstance(data, BaseModel) else data

  logger.info('upserting_record', target=target)

  query: Query[Any] = Query().upsert(target, record_data)
  result = await db.execute(query.to_surql())

  if isinstance(result, list) and len(result) > 0:
    if isinstance(result[0], dict) and 'result' in result[0]:
      data_list = result[0]['result']
      if isinstance(data_list, list) and len(data_list) > 0:
        return data_list[0]  # type: ignore[no-any-return]
    elif isinstance(result[0], dict):
      return result[0]

  return result  # type: ignore[no-any-return]


async def exists(
  table: str,
  record_id: str | RecordID[Any],
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


async def first[T: BaseModel](
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


async def last[T: BaseModel](
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


# ---------------------------------------------------------------------------
# Aggregation (issue #47 / #1)
# ---------------------------------------------------------------------------


def _render_aggregate_value(value: Any) -> str:
  """Render an ``aggregate_records`` projection value as SurrealQL."""
  if hasattr(value, 'to_surql'):
    return value.to_surql()  # type: ignore[no-any-return]
  if isinstance(value, str):
    return value
  return str(value)


def _build_aggregate_query(
  table: str,
  select: dict[str, Any],
  group_by: list[str] | None,
  group_all: bool,
  where: str | Operator | None = None,
) -> str:
  """Build the SurrealQL for :func:`aggregate_records`.

  Kept separate from the async entry point so it is trivially unit-testable
  without a live database.

  Args:
    table: Source table name.
    select: Mapping of output alias -> SurrealQL expression (or factory /
      ``FunctionExpression`` / ``SurrealFn``).
    group_by: Optional list of group-by fields.
    group_all: When ``True`` aggregates the entire table into a single row.
    where: Optional WHERE clause (string or ``Operator``).

  Returns:
    Fully-rendered SurrealQL query string.

  Raises:
    ValueError: If ``select`` is empty or neither ``group_by`` nor
      ``group_all`` is provided.
  """
  if not select:
    raise ValueError('aggregate_records requires a non-empty select mapping')
  if not group_all and not group_by:
    raise ValueError('aggregate_records requires either group_all=True or group_by=[...].')

  _validate_identifier(table, 'table name')

  for alias in select:
    _validate_identifier(alias, 'select alias')

  if group_by:
    for group_field in group_by:
      # Allow dotted field paths by validating only the head identifier.
      head = group_field.split('.')[0]
      _validate_identifier(head, 'group by field')

  projection_parts: list[str] = []
  if group_by:
    projection_parts.extend(group_by)
  for alias, expr in select.items():
    rendered = _render_aggregate_value(expr)
    projection_parts.append(f'{rendered} AS {alias}')

  projection = ', '.join(projection_parts)
  sql_parts = [f'SELECT {projection} FROM {table}']

  if where is not None:
    condition_str = where.to_surql() if isinstance(where, Operator) else where
    sql_parts.append(f'WHERE ({condition_str})')

  if group_all:
    sql_parts.append('GROUP ALL')
  elif group_by:
    sql_parts.append(f'GROUP BY {", ".join(group_by)}')

  return ' '.join(sql_parts)


async def aggregate_records(
  table: str,
  select: dict[str, Any],
  group_by: list[str] | None = None,
  group_all: bool = False,
  where: str | Operator | None = None,
  client: DatabaseClient | None = None,
) -> list[dict[str, Any]]:
  """Run a GROUP BY / GROUP ALL aggregation against ``table``.

  Produces a SurrealQL query of the form::

    SELECT <group_by>, <expr1> AS <alias1>, <expr2> AS <alias2>
    FROM <table>
    [WHERE ...]
    (GROUP BY ... | GROUP ALL)

  and returns the resulting rows as dictionaries via
  :func:`~surql.query.results.extract_result`, so callers don't have to
  manually unwrap the SurrealDB response envelope.

  Args:
    table: Source table name.
    select: Mapping of output alias -> projection expression. Values may be
      ``SurrealFn`` instances (e.g. ``math_sum_fn('strength')``),
      ``FunctionExpression`` objects, or raw SurrealQL strings.
    group_by: List of fields to group by. When supplied, ``group_all`` must
      be ``False``.
    group_all: If ``True``, aggregate the entire table into a single row.
      Mutually exclusive with ``group_by``.
    where: Optional WHERE condition (string or :class:`Operator`).
    client: Database client. When ``None``, the context client is used.

  Returns:
    List of aggregated rows, one dict per group (or a single dict when
    ``group_all=True``).

  Raises:
    ValueError: If ``select`` is empty or neither ``group_by`` nor
      ``group_all`` is provided.

  Examples:
    >>> from surql.query.functions import count_if, math_sum_fn
    >>> counts = await aggregate_records(
    ...   table='memory_entry',
    ...   select={'count': count_if(), 'total_strength': math_sum_fn('strength')},
    ...   group_by=['network'],
    ... )
  """
  if group_all and group_by:
    raise ValueError('aggregate_records: pass group_all=True OR group_by=[...], not both.')

  db = client or get_db()
  sql = _build_aggregate_query(
    table=table,
    select=select,
    group_by=group_by,
    group_all=group_all,
    where=where,
  )

  logger.info(
    'aggregating_records',
    table=table,
    group_by=group_by,
    group_all=group_all,
  )
  logger.debug('aggregate_sql', sql=sql)

  result = await db.execute(sql)
  rows = extract_result(result)
  logger.info('aggregate_completed', table=table, row_count=len(rows))
  return rows
