"""Async SurrealDB client wrapper with connection pooling and retry logic."""

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import structlog
from surrealdb import AsyncSurreal
from surrealdb import RecordID as SdkRecordID
from tenacity import (
  AsyncRetrying,
  RetryError,
  retry_if_exception_type,
  stop_after_attempt,
  wait_exponential,
)

from surql.connection.config import ConnectionConfig

logger = structlog.get_logger(__name__)

# Pattern matching SurrealDB record ID targets: "table:id" or "table:<complex_id>"
_RECORD_ID_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*:.+$')


def _is_record_id_target(target: str) -> bool:
  """Check whether a target string looks like a single record ID (table:id).

  Args:
    target: The select target string

  Returns:
    True if target matches record ID format, False for table-only targets
  """
  return bool(_RECORD_ID_PATTERN.match(target))


def _denormalize_params(value: Any) -> Any:
  """Recursively convert record ID strings back to SDK RecordID objects.

  When consumers receive normalized responses (RecordID -> string), they may
  pass those strings back as field values in subsequent create/update calls.
  SurrealDB 3.x rejects plain strings where it expects record types, so this
  function detects strings matching the ``table:id`` pattern and converts them
  back to ``surrealdb.RecordID`` objects before sending to the SDK.

  Args:
    value: Any value from user-provided data (dicts, lists, scalars)

  Returns:
    The value with record ID strings replaced by SDK RecordID objects
  """
  if isinstance(value, str) and _is_record_id_target(value):
    table, id_part = value.split(':', 1)
    return SdkRecordID(table, id_part)
  if isinstance(value, dict):
    return {k: _denormalize_params(v) for k, v in value.items()}
  if isinstance(value, list):
    return [_denormalize_params(item) for item in value]
  return value


def _normalize_sdk_value(value: Any) -> Any:
  """Recursively convert SurrealDB SDK types to plain Python types.

  Converts SDK RecordID objects to their string representation so consumers
  receive plain strings they can pass back to subsequent operations without
  SurrealDB 3.x rejecting them with type coercion errors.

  Args:
    value: Any value returned by the SurrealDB SDK

  Returns:
    The value with SDK types replaced by plain Python equivalents
  """
  if isinstance(value, SdkRecordID):
    return str(value)
  if isinstance(value, dict):
    return {k: _normalize_sdk_value(v) for k, v in value.items()}
  if isinstance(value, list):
    return [_normalize_sdk_value(item) for item in value]
  return value


class DatabaseError(Exception):
  """Base exception for database operations."""

  pass


class ConnectionError(DatabaseError):
  """Raised when connection to database fails."""

  pass


class QueryError(DatabaseError):
  """Raised when query execution fails."""

  pass


class DatabaseClient:
  """Async wrapper around SurrealDB client with connection pooling and retry logic."""

  def __init__(self, config: ConnectionConfig) -> None:
    """Initialize database client with configuration.

    Args:
      config: Connection configuration
    """
    self._config = config
    self._client: Any = None
    self._connected = False
    self._semaphore = asyncio.Semaphore(config.max_connections)
    self._log = logger.bind(
      namespace=config.namespace,
      database=config.database,
      url=config.url,
    )

    # Import here to avoid circular imports
    from surql.connection.auth import AuthManager
    from surql.connection.streaming import StreamingManager

    self._auth = AuthManager()
    self._streaming: StreamingManager | None = None

  @property
  def is_connected(self) -> bool:
    """Check if client is currently connected."""
    return self._connected and self._client is not None

  async def connect(self) -> None:
    """Establish connection to the database with retry logic.

    Raises:
      ConnectionError: If connection fails after all retries
    """
    if self._connected:
      self._log.info('reconnecting_client')
      await self.disconnect()

    try:
      async for attempt in AsyncRetrying(
        retry=retry_if_exception_type((Exception,)),
        stop=stop_after_attempt(self._config.retry_max_attempts),
        wait=wait_exponential(
          multiplier=self._config.retry_multiplier,
          min=self._config.retry_min_wait,
          max=self._config.retry_max_wait,
        ),
        reraise=True,
      ):
        with attempt:
          self._log.info('connecting_to_database', attempt=attempt.retry_state.attempt_number)

          self._client = AsyncSurreal(self._config.url)
          await self._client.connect(self._config.url)

          if self._config.username and self._config.password:
            await self._client.signin(
              {
                'username': self._config.username,
                'password': self._config.password,
              }
            )
            self._log.debug('authenticated_successfully')

          await self._client.use(self._config.namespace, self._config.database)
          self._connected = True

          # Initialize streaming if WebSocket and enabled
          if self._config.enable_live_queries:
            from surql.connection.streaming import StreamingManager

            self._streaming = StreamingManager(self._client)

          self._log.info('connected_to_database')

    except RetryError as e:
      self._log.error('connection_failed_after_retries', error=str(e))
      raise ConnectionError(
        f'Failed to connect after {self._config.retry_max_attempts} attempts'
      ) from e
    except Exception as e:
      self._log.error('unexpected_connection_error', error=str(e), error_type=type(e).__name__)
      raise ConnectionError(f'Connection failed: {e}') from e

  async def disconnect(self) -> None:
    """Close the database connection.

    Raises:
      DatabaseError: If disconnect operation fails
    """
    if not self._connected:
      self._log.debug('client_not_connected')
      return

    try:
      if self._client:
        await self._client.close()
        self._log.info('disconnected_from_database')
    except Exception as e:
      self._log.error('disconnect_failed', error=str(e))
      raise DatabaseError(f'Failed to disconnect: {e}') from e
    finally:
      self._client = None
      self._connected = False

  async def execute(
    self,
    query: str,
    params: dict[str, Any] | None = None,
  ) -> Any:
    """Execute raw SurrealQL query with retry logic.

    Args:
      query: SurrealQL query string
      params: Optional query parameters

    Returns:
      Query results

    Raises:
      ConnectionError: If client is not connected
      QueryError: If query execution fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_query', query=query, params=params)

        resolved_params = _denormalize_params(params) if params else {}
        result = await self._client.query(query, resolved_params)

        self._log.debug('query_executed_successfully', result_type=type(result).__name__)
        return _normalize_sdk_value(result)

      except Exception as e:
        self._log.error(
          'query_execution_failed',
          error=str(e),
          error_type=type(e).__name__,
          query=query,
        )
        raise QueryError(f'Query execution failed: {e}') from e

  async def select(self, target: str) -> Any:
    """Execute SELECT operation.

    When the target is a single record ID (e.g. ``user:alice``), the SDK may
    return a list.  This method detects that case and unwraps the first
    element so callers always receive a dict (or ``None``) for single-record
    selects, and a list for table-level selects.

    Args:
      target: Target table or record ID

    Returns:
      Selected record dict (single ID) or list of records (table)

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_select', target=target)
        result = await self._client.select(target)
        result = _normalize_sdk_value(result)

        # Unwrap single-record selects: SDK returns a list even for record IDs
        if _is_record_id_target(target) and isinstance(result, list):
          return result[0] if result else None

        return result
      except Exception as e:
        self._log.error('select_failed', error=str(e), target=target)
        raise QueryError(f'SELECT operation failed: {e}') from e

  async def create(self, table: str, data: dict[str, Any]) -> Any:
    """Execute CREATE operation.

    Normalizes SDK-specific types (e.g. ``RecordID`` objects) in the
    response so that consumers receive plain Python types they can safely
    pass back to subsequent operations.

    Args:
      table: Target table name
      data: Record data

    Returns:
      Created record with SDK types normalized to plain Python types

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_create', table=table, data=data)
        result = await self._client.create(table, _denormalize_params(data))
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('create_failed', error=str(e), table=table)
        raise QueryError(f'CREATE operation failed: {e}') from e

  async def update(self, target: str, data: dict[str, Any]) -> Any:
    """Execute UPDATE operation.

    Args:
      target: Target table or record ID
      data: Update data

    Returns:
      Updated record with SDK types normalized

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_update', target=target, data=data)
        result = await self._client.update(target, _denormalize_params(data))
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('update_failed', error=str(e), target=target)
        raise QueryError(f'UPDATE operation failed: {e}') from e

  async def merge(self, target: str, data: dict[str, Any]) -> Any:
    """Execute MERGE operation.

    Args:
      target: Target table or record ID
      data: Data to merge

    Returns:
      Merged record with SDK types normalized

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_merge', target=target, data=data)
        result = await self._client.merge(target, _denormalize_params(data))
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('merge_failed', error=str(e), target=target)
        raise QueryError(f'MERGE operation failed: {e}') from e

  async def delete(self, target: str) -> Any:
    """Execute DELETE operation.

    Args:
      target: Target table or record ID

    Returns:
      Deletion result with SDK types normalized

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_delete', target=target)
        result = await self._client.delete(target)
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('delete_failed', error=str(e), target=target)
        raise QueryError(f'DELETE operation failed: {e}') from e

  async def insert_relation(self, table: str, data: dict[str, Any]) -> Any:
    """Execute INSERT RELATION operation for edges.

    Args:
      table: Edge table name
      data: Relation data with 'in' and 'out' fields

    Returns:
      Created relation with SDK types normalized

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_insert_relation', table=table, data=data)
        result = await self._client.insert_relation(table, _denormalize_params(data))
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('insert_relation_failed', error=str(e), table=table)
        raise QueryError(f'INSERT RELATION operation failed: {e}') from e

  async def __aenter__(self) -> 'DatabaseClient':
    """Async context manager entry."""
    await self.connect()
    return self

  async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    """Async context manager exit."""
    await self.disconnect()


@asynccontextmanager
async def get_client(config: ConnectionConfig) -> AsyncIterator[DatabaseClient]:
  """Context manager for database client lifecycle.

  Args:
    config: Connection configuration

  Yields:
    Connected database client

  Example:
    ```python
    async with get_client(config) as client:
      results = await client.execute('SELECT * FROM user')
    ```
  """
  client = DatabaseClient(config)
  await client.connect()
  try:
    yield client
  finally:
    await client.disconnect()
