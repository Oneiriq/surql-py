"""Batch operation helpers for efficient multi-record operations.

This module provides convenient async functions for batch database operations
including batch upserts, batch relationships, and batch inserts/deletes.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel

from surql.connection.context import get_db
from surql.connection.transaction import Transaction
from surql.types.operators import _quote_value, _validate_identifier

if TYPE_CHECKING:
  from surql.connection.client import DatabaseClient

logger = structlog.get_logger(__name__)


def _format_item_for_surql(item: dict[str, Any]) -> str:
  """Format a dictionary item for SurrealQL array syntax.

  Args:
    item: Dictionary to format

  Returns:
    SurrealQL object string representation

  Raises:
    ValueError: If any field name contains invalid characters
  """
  parts = []
  for key, value in item.items():
    # Validate field name to prevent SQL injection
    _validate_identifier(key, 'field name')
    # Handle nested dicts/lists with JSON, others with quote_value
    if isinstance(value, (dict, list)):
      parts.append(f'{key}: {json.dumps(value)}')
    else:
      parts.append(f'{key}: {_quote_value(value)}')
  return '{ ' + ', '.join(parts) + ' }'


def _format_items_array(items: list[dict[str, Any]]) -> str:
  """Format a list of items as a SurrealQL array.

  Args:
    items: List of dictionaries to format

  Returns:
    SurrealQL array string representation
  """
  item_strs = [_format_item_for_surql(item) for item in items]
  return '[\n  ' + ',\n  '.join(item_strs) + '\n]'


def _build_upsert_target(_table: str, raw: str) -> str:
  """Return an UPSERT target clause valid on SurrealDB v3.

  Callers pass either a table name or a record id like ``user:alice``.
  Table names are validated as identifiers; record ids are emitted
  verbatim (SurrealDB parses ``table:id`` natively as a record target).
  The ``_table`` arg is reserved for callers that might want a fallback
  to a table-level UPSERT in a future revision — today the function
  returns ``raw`` verbatim when it contains a colon, and validates it
  otherwise.
  """
  if ':' in raw:
    return raw
  _validate_identifier(raw, 'table name')
  return raw


async def upsert_many(
  client: DatabaseClient | Transaction | None,
  table: str,
  items: list[dict[str, Any] | BaseModel],
  conflict_fields: list[str] | None = None,
) -> list[dict[str, Any]]:
  """Batch upsert multiple records.

  Inserts records if they don't exist, or updates them if they do.
  Uses SurrealDB's UPSERT statement for efficient batch operations.

  Atomicity (1.7.0)
  =================

  ``client`` may be either a connected
  :class:`~surql.connection.client.DatabaseClient` or an active
  :class:`~surql.connection.transaction.Transaction`. The two modes
  behave differently:

  - **DatabaseClient (or ``None`` → context client) — autocommit.**
    The batch is sent as a single multi-statement query. If one
    record fails schema validation mid-batch, *earlier records may
    have already committed* (SurrealDB v3 autocommits per statement
    unless wrapped in ``BEGIN … COMMIT``). This is the legacy
    behaviour and is preserved for backwards compatibility.

  - **Transaction — atomic.** The same per-record ``UPSERT`` statements
    are queued on the supplied transaction via ``trx.execute``. They
    inherit the surrounding ``BEGIN TRANSACTION``/``COMMIT TRANSACTION``
    framing, so a single bad record rolls back the *entire* batch on
    commit (no half-seeded tables). Results are not available at call
    time — ``Transaction.execute`` buffers statements — so this mode
    returns ``[]``. Callers who need the per-row results should
    inspect the value returned by :meth:`Transaction.commit`.

  Use the transaction-bound form when seeding multiple tables in one
  atomic step or when partial-success would leave the database in a
  shape downstream code can't recover from. The mode is auto-detected
  from ``client`` so no API call-site rewrite is needed beyond passing
  the transaction handle.

  Args:
    client: SurrealDB client instance, active transaction, or ``None``
      (in which case the context client is used in autocommit mode).
    table: Target table name
    items: List of records to upsert (Pydantic models or dicts)
    conflict_fields: Optional fields to check for conflicts (used in WHERE clause)

  Returns:
    List of upserted records (autocommit mode), or ``[]`` when called
    inside a transaction (results land in ``Transaction.commit`` instead).

  Raises:
    ValueError: If items list is empty or table name is invalid
    TransactionError: If a ``Transaction`` is passed but is not in the
      ``ACTIVE`` state (e.g. already committed or cancelled).
    QueryError: If the database operation fails

  Examples:
    Basic autocommit upsert (legacy behaviour):
    >>> results = await upsert_many(client, "users", [
    ...     {"id": "user:1", "name": "Alice", "age": 30},
    ...     {"id": "user:2", "name": "Bob", "age": 25}
    ... ])

    Atomic batch — rolls back if any record fails validation:
    >>> async with surql.transaction(db) as trx:
    ...     await upsert_many(trx, "users", users_batch)
    ...     await upsert_many(trx, "posts", posts_batch)
    ...     # Commits both batches atomically on context exit; cancels
    ...     # both if either upsert_many raises (or if the surrounding
    ...     # block raises before exit).

    With conflict handling:
    >>> results = await upsert_many(
    ...     client, "users", items,
    ...     conflict_fields=["email"]
    ... )
  """
  if not items:
    logger.debug('upsert_many_empty_list', table=table)
    return []

  if not table:
    raise ValueError('Table name is required')

  # Validate table name to prevent SQL injection
  _validate_identifier(table, 'table name')

  # Validate conflict fields if provided
  if conflict_fields:
    for field in conflict_fields:
      _validate_identifier(field, 'conflict field name')

  # Resolve the executor exactly once. Transaction handles bypass the
  # context-client lookup — passing a transaction signals that the
  # caller wants atomicity, so we must not silently downgrade to the
  # autocommit context client even if ``client`` was ``None`` after
  # some unrelated check elsewhere.
  txn: Transaction | None = client if isinstance(client, Transaction) else None
  executor: DatabaseClient | Transaction = (
    txn if txn is not None else (client if client is not None else get_db())
  )

  logger.info(
    'upsert_many_start',
    table=table,
    count=len(items),
    mode='transaction' if txn is not None else 'autocommit',
  )

  # Convert Pydantic models to dicts
  item_dicts: list[dict[str, Any]] = []
  for item in items:
    if isinstance(item, BaseModel):
      item_dicts.append(item.model_dump())
    else:
      item_dicts.append(item)

  # SurrealDB v3 rejects `UPSERT INTO <table> [ {...}, {...} ]` with a
  # parse error. Emit one `UPSERT <target> CONTENT $data` statement per
  # record and batch them into a single multi-statement query. Matches
  # the surql-rs / surql-go ports. See Oneiriq/surql-py#32.
  statements: list[str] = []
  params: dict[str, Any] = {}
  for idx, data in enumerate(item_dicts):
    payload = {k: v for k, v in data.items() if k != 'id'}
    target = data.get('id') or table
    target_clean = _build_upsert_target(table, target)
    bind = f'item_{idx}'
    params[bind] = payload
    if conflict_fields:
      conditions = ' AND '.join(f'{field} = ${bind}.{field}' for field in conflict_fields)
      statements.append(f'UPSERT {target_clean} CONTENT ${bind} WHERE {conditions};')
    else:
      statements.append(f'UPSERT {target_clean} CONTENT ${bind};')

  # In transaction mode, queue each statement individually so the
  # transaction's per-statement param bookkeeping (duplicate-key
  # detection in ``Transaction.execute``) works correctly. The full
  # batch then flushes as a single ``BEGIN … COMMIT`` RPC on
  # ``Transaction.commit``, inheriting v3's rollback-on-any-error
  # semantics.
  if txn is not None:
    for stmt, bind in zip(statements, params, strict=True):
      await txn.execute(stmt, {bind: params[bind]})
    logger.info(
      'upsert_many_queued_on_transaction',
      table=table,
      queued=len(statements),
    )
    # Results aren't available until the transaction commits — return
    # an empty list and let callers inspect ``Transaction.commit``'s
    # return value if they need per-row data.
    return []

  query = '\n'.join(statements)
  logger.debug('upsert_many_query', query=query)

  result = await executor.execute(query, params)

  # Extract results from execute response
  records: list[dict[str, Any]] = []
  if isinstance(result, list) and len(result) > 0:
    first_result = result[0]
    if isinstance(first_result, dict) and 'result' in first_result:
      data = first_result['result']
      if isinstance(data, list):
        records = data

  logger.info('upsert_many_complete', table=table, upserted=len(records))

  return records


async def relate_many(
  client: DatabaseClient | None,
  from_table: str,
  edge: str,
  to_table: str,
  relations: list[tuple[str, str, dict[str, Any] | None]],
) -> list[dict[str, Any]]:
  """Create multiple graph relationships.

  Uses SurrealDB's RELATE statement to create edges between records.
  Supports optional edge data for each relationship.

  Args:
    client: SurrealDB client instance. If None, uses context client.
    from_table: Source node table name (used for documentation, not in query)
    edge: Edge table name
    to_table: Target node table name (used for documentation, not in query)
    relations: List of (from_id, to_id, optional_data) tuples.
               from_id and to_id should be full record IDs (e.g., "person:alice")

  Returns:
    List of created edge records

  Raises:
    ValueError: If relations list is empty or edge name is invalid
    QueryError: If the database operation fails

  Examples:
    Basic relationships:
    >>> results = await relate_many(client, "person", "knows", "person", [
    ...     ("person:alice", "person:bob", {"since": "2024-01-01"}),
    ...     ("person:alice", "person:charlie", None),
    ...     ("person:bob", "person:charlie", {"strength": 0.8})
    ... ])
  """
  db = client or get_db()

  if not relations:
    logger.debug('relate_many_empty_list', edge=edge, from_table=from_table, to_table=to_table)
    return []

  if not edge:
    raise ValueError('Edge table name is required')

  # Validate table names to prevent SQL injection
  _validate_identifier(from_table, 'from table name')
  _validate_identifier(edge, 'edge table name')
  _validate_identifier(to_table, 'to table name')

  logger.info(
    'relate_many_start',
    from_table=from_table,
    edge=edge,
    to_table=to_table,
    count=len(relations),
  )

  # Build individual RELATE statements and execute them together
  statements: list[str] = []
  for from_id, to_id, data in relations:
    # Validate record ID table parts
    from_id_table = from_id.split(':')[0] if ':' in from_id else from_id
    to_id_table = to_id.split(':')[0] if ':' in to_id else to_id
    _validate_identifier(from_id_table, 'from record table')
    _validate_identifier(to_id_table, 'to record table')

    # Build the RELATE statement
    relate_stmt = f'RELATE {from_id}->{edge}->{to_id}'

    # Add SET clause if data is provided
    if data:
      set_parts = []
      for key, value in data.items():
        # Validate field name
        _validate_identifier(key, 'field name')
        if isinstance(value, (dict, list)):
          set_parts.append(f'{key} = {json.dumps(value)}')
        else:
          set_parts.append(f'{key} = {_quote_value(value)}')
      relate_stmt += ' SET ' + ', '.join(set_parts)

    statements.append(relate_stmt + ';')

  # Execute all statements as a single query
  query = '\n'.join(statements)
  logger.debug('relate_many_query', query=query)

  result = await db.execute(query)

  # Extract all results from multi-statement response
  records: list[dict[str, Any]] = []
  if isinstance(result, list):
    for res in result:
      if isinstance(res, dict) and 'result' in res:
        data = res['result']
        if isinstance(data, list):
          records.extend(data)
        elif isinstance(data, dict):
          records.append(data)

  logger.info('relate_many_complete', edge=edge, created=len(records))

  return records


async def insert_many(
  client: DatabaseClient | None,
  table: str,
  items: list[dict[str, Any] | BaseModel],
) -> list[dict[str, Any]]:
  """Insert multiple records (fails if exists).

  Uses SurrealDB's INSERT statement for efficient batch inserts.
  Unlike upsert_many, this will fail if any record already exists.

  Args:
    client: SurrealDB client instance. If None, uses context client.
    table: Target table name
    items: List of records to insert (Pydantic models or dicts)

  Returns:
    List of inserted records

  Raises:
    ValueError: If items list is empty or table name is invalid
    QueryError: If the database operation fails or records already exist

  Examples:
    >>> results = await insert_many(client, "users", [
    ...     {"name": "Alice", "email": "alice@example.com"},
    ...     {"name": "Bob", "email": "bob@example.com"}
    ... ])
  """
  db = client or get_db()

  if not items:
    logger.debug('insert_many_empty_list', table=table)
    return []

  if not table:
    raise ValueError('Table name is required')

  # Validate table name to prevent SQL injection
  _validate_identifier(table, 'table name')

  logger.info('insert_many_start', table=table, count=len(items))

  # Convert Pydantic models to dicts
  item_dicts: list[dict[str, Any]] = []
  for item in items:
    if isinstance(item, BaseModel):
      item_dicts.append(item.model_dump())
    else:
      item_dicts.append(item)

  # Build INSERT statement (field names validated in _format_items_array)
  items_array = _format_items_array(item_dicts)
  query = f'INSERT INTO {table} {items_array};'

  logger.debug('insert_many_query', query=query)

  result = await db.execute(query)

  # Extract results from execute response
  records: list[dict[str, Any]] = []
  if isinstance(result, list) and len(result) > 0:
    first_result = result[0]
    if isinstance(first_result, dict) and 'result' in first_result:
      data = first_result['result']
      if isinstance(data, list):
        records = data

  logger.info('insert_many_complete', table=table, inserted=len(records))

  return records


async def delete_many(
  client: DatabaseClient | None,
  table: str,
  ids: list[str],
) -> list[dict[str, Any]]:
  """Delete multiple records by ID.

  Deletes records matching the provided IDs and returns the deleted records.

  Args:
    client: SurrealDB client instance. If None, uses context client.
    table: Target table name
    ids: List of record IDs to delete. Can be either:
         - Simple IDs: ["1", "2", "alice"]
         - Full record IDs: ["user:1", "user:2", "user:alice"]

  Returns:
    List of deleted records (may be empty if records didn't exist)

  Raises:
    ValueError: If ids list is empty or table name is invalid
    QueryError: If the database operation fails

  Examples:
    With simple IDs:
    >>> results = await delete_many(client, "users", ["1", "2", "3"])

    With full record IDs:
    >>> results = await delete_many(client, "users", [
    ...     "user:alice", "user:bob"
    ... ])
  """
  db = client or get_db()

  if not ids:
    logger.debug('delete_many_empty_list', table=table)
    return []

  if not table:
    raise ValueError('Table name is required')

  # Validate table name to prevent SQL injection
  _validate_identifier(table, 'table name')

  logger.info('delete_many_start', table=table, count=len(ids))

  # Build DELETE statements for each ID
  # Normalize IDs to ensure they have the table prefix
  records: list[dict[str, Any]] = []

  for record_id in ids:
    # Validate record ID table part if it includes a table prefix
    if ':' in record_id:
      id_table = record_id.split(':')[0]
      _validate_identifier(id_table, 'record ID table')

    # Check if ID already has table prefix
    target = record_id if ':' in record_id else f'{table}:{record_id}'

    query = f'DELETE {target} RETURN BEFORE;'
    logger.debug('delete_many_query', query=query)

    result = await db.execute(query)

    # Extract result from execute response
    if isinstance(result, list) and len(result) > 0:
      first_result = result[0]
      if isinstance(first_result, dict) and 'result' in first_result:
        data = first_result['result']
        if isinstance(data, list) and len(data) > 0:
          records.extend(data)
        elif isinstance(data, dict):
          records.append(data)

  logger.info('delete_many_complete', table=table, deleted=len(records))

  return records


# Functional helper for generating batch upsert SQL
def build_upsert_query(
  table: str,
  items: list[dict[str, Any]],
  conflict_fields: list[str] | None = None,
) -> str:
  """Build a SurrealQL UPSERT query string without executing it.

  Useful for previewing or logging the query before execution.

  Args:
    table: Target table name
    items: List of records to upsert
    conflict_fields: Optional fields to check for conflicts

  Returns:
    SurrealQL UPSERT query string

  Examples:
    >>> query = build_upsert_query("users", [
    ...     {"id": "user:1", "name": "Alice"},
    ...     {"id": "user:2", "name": "Bob"}
    ... ])
    >>> print(query)
    UPSERT INTO users [
      { id: 'user:1', name: 'Alice' },
      { id: 'user:2', name: 'Bob' }
    ];
  """
  if not items:
    return ''

  # Validate table name and conflict fields to prevent SQL injection
  _validate_identifier(table, 'table name')
  if conflict_fields:
    for field in conflict_fields:
      _validate_identifier(field, 'conflict field name')

  # SurrealDB v3 rejects `UPSERT INTO <table> [ ... ]`. Emit per-record
  # statements matching the surql-rs / surql-go ports. Non-id items are
  # targeted by table (UPSERT will pick the first match or insert).
  statements: list[str] = []
  for data in items:
    payload = {k: v for k, v in data.items() if k != 'id'}
    target = data.get('id') or table
    target_clean = _build_upsert_target(table, target)
    payload_literal = _format_items_array([payload]).strip('[] \n\t,')
    if conflict_fields:
      conditions = ' AND '.join(f'{field} = $item.{field}' for field in conflict_fields)
      statements.append(f'UPSERT {target_clean} CONTENT {payload_literal} WHERE {conditions};')
    else:
      statements.append(f'UPSERT {target_clean} CONTENT {payload_literal};')

  return '\n'.join(statements)


# Functional helper for generating batch relate SQL
def build_relate_query(
  from_id: str,
  edge: str,
  to_id: str,
  data: dict[str, Any] | None = None,
) -> str:
  """Build a SurrealQL RELATE query string without executing it.

  Useful for previewing or logging the query before execution.

  Args:
    from_id: Source record ID (e.g., "person:alice")
    edge: Edge table name
    to_id: Target record ID (e.g., "person:bob")
    data: Optional edge data

  Returns:
    SurrealQL RELATE query string

  Examples:
    >>> query = build_relate_query(
    ...     "person:alice", "knows", "person:bob",
    ...     {"since": "2024-01-01"}
    ... )
    >>> print(query)
    RELATE person:alice->knows->person:bob SET since = '2024-01-01';
  """
  # Validate edge table name to prevent SQL injection
  _validate_identifier(edge, 'edge table name')

  # Validate record ID table parts
  from_table = from_id.split(':')[0] if ':' in from_id else from_id
  to_table = to_id.split(':')[0] if ':' in to_id else to_id
  _validate_identifier(from_table, 'from record table')
  _validate_identifier(to_table, 'to record table')

  relate_stmt = f'RELATE {from_id}->{edge}->{to_id}'

  if data:
    set_parts = []
    for key, value in data.items():
      # Validate field name
      _validate_identifier(key, 'field name')
      if isinstance(value, (dict, list)):
        set_parts.append(f'{key} = {json.dumps(value)}')
      else:
        set_parts.append(f'{key} = {_quote_value(value)}')
    relate_stmt += ' SET ' + ', '.join(set_parts)

  return relate_stmt + ';'
