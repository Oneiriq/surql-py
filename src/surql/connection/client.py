"""Async SurrealDB client wrapper with connection pooling and retry logic."""

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
  from surql.connection.streaming import (
    EmbeddedPollingStreamingManager,
    LiveQuery,
    StreamingManager,
  )
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
#
# The negative lookahead ``(?!//)`` after the colon excludes URL schemes
# (``http://``, ``https://``, ``ws://``, ``wss://``, ``file://``, ...) which
# share the ``<word>:<rest>`` shape with record-id literals but must NOT be
# coerced to ``RecordID`` objects. Without this guard, any caller passing a
# URL parameter (e.g. ``base_url='http://10.0.0.51:11434'``) would have it
# silently rewritten to ``RecordID('http', '//10.0.0.51:11434')``, which
# SurrealDB returns a coerce error for in the result text of an otherwise
# OK-status query response -- the kind of bug that's hard to spot because
# the Python wrapper sees ``status: 'OK'`` and reports success.
_RECORD_ID_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*:(?!//).+$')


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


def _extract_select_rows(value: Any) -> list[Any]:
  """Flatten a SurrealDB query response to a list of result rows.

  The SDK's ``query`` method may return any of these shapes depending
  on the server version and statement count:

  - ``[{'result': [rows...], 'status': 'OK', ...}, ...]`` -- classic
    response envelope, one entry per statement in the batch.
  - ``[rows...]`` -- a bare list of rows (SDK 2.x unwraps single-
    statement queries).
  - ``rows...`` -- a scalar or dict (single-record selects on some
    paths).

  This helper flattens all of those into a list of row dicts.

  Args:
    value: Raw SDK response

  Returns:
    List of row dicts (possibly empty)
  """
  if value is None:
    return []
  if isinstance(value, dict):
    if 'result' in value:
      inner = value['result']
      return inner if isinstance(inner, list) else [inner] if inner is not None else []
    return [value]
  if isinstance(value, list):
    if len(value) == 0:
      return []
    if isinstance(value[0], dict) and 'result' in value[0] and 'status' in value[0]:
      # Statement envelope form; flatten the first statement's rows.
      inner = value[0].get('result')
      return inner if isinstance(inner, list) else [inner] if inner is not None else []
    return list(value)
  return [value]


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
    from surql.connection.streaming import (
      EmbeddedPollingStreamingManager,
      StreamingManager,
    )

    self._auth = AuthManager()
    self._streaming: StreamingManager | EmbeddedPollingStreamingManager | None = None

  @property
  def is_connected(self) -> bool:
    """Check if client is currently connected."""
    return self._connected and self._client is not None

  @property
  def streaming(self) -> 'StreamingManager | EmbeddedPollingStreamingManager':
    """Public accessor for the live-query streaming manager.

    Returns:
      The connection's :class:`StreamingManager` (for ``ws://`` / ``wss://``
      URLs) or :class:`EmbeddedPollingStreamingManager` (for embedded engine
      URLs such as ``surrealkv://``, ``mem://``, ``file://``). Both expose
      the same surface (``live`` / ``subscribe`` / ``kill`` / ``kill_all`` /
      ``get_active_queries``); the polling variant emits CREATE-only
      notifications at ``ConnectionConfig.live_poll_interval_s`` cadence and
      cannot observe UPDATE / DELETE -- it exists because the upstream
      ``surrealdb`` Python SDK ships an embedded connection that is missing
      the live-notification dispatcher.

    Raises:
      ConnectionError: If the client is not connected.
      StreamingError: If live queries are disabled on this connection
        (set ``enable_live_queries=True`` on :class:`ConnectionConfig`,
        which requires a WebSocket or embedded engine URL).

    Example:
      ```python
      query = await client.streaming.live('reading')
      async for notification in client.streaming.subscribe(query):
        ...
      ```
    """
    if not self.is_connected:
      raise ConnectionError('Client is not connected to database')
    if self._streaming is None:
      from surql.connection.streaming import StreamingError

      raise StreamingError(
        'Live queries are disabled. Set enable_live_queries=True on '
        'ConnectionConfig and use a WebSocket or embedded engine URL.'
      )
    return self._streaming

  async def live(self, table: str, diff: bool = False) -> 'LiveQuery':
    """Start a LIVE SELECT on a table.

    Convenience wrapper around ``client.streaming.live(table, diff=diff)``
    so callers can subscribe to live changes without reaching into the
    streaming manager directly.

    Args:
      table: Table name to watch.
      diff: If True, notifications carry JSON-Patch diffs instead of full
        records.

    Returns:
      A :class:`LiveQuery` handle. Pass it to ``client.streaming.subscribe``
      to consume notifications, or ``client.streaming.kill`` to stop it.

    Raises:
      ConnectionError: If the client is not connected.
      StreamingError: If live queries are disabled or the underlying
        ``LIVE`` call fails.

    Example:
      ```python
      query = await client.live('reading')
      async for notification in client.streaming.subscribe(query):
        ...
      ```
    """
    return await self.streaming.live(table, diff=diff)

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

          # Initialize streaming if enabled. WebSocket URLs use the native
          # notification dispatcher; embedded URLs (surrealkv://, mem://,
          # file://) use a polling fallback because the upstream SDK's
          # ``AsyncEmbeddedSurrealConnection`` does not implement live-query
          # notifications.
          if self._config.enable_live_queries:
            from surql.connection.streaming import (
              EmbeddedPollingStreamingManager,
              StreamingManager,
              is_embedded_url,
            )

            if is_embedded_url(self._config.url):
              self._streaming = EmbeddedPollingStreamingManager(
                self,
                interval_s=self._config.live_poll_interval_s,
                max_seen_ids=self._config.live_poll_max_seen_ids,
              )
              self._log.info(
                'embedded_polling_streaming_enabled',
                interval_s=self._config.live_poll_interval_s,
                max_seen_ids=self._config.live_poll_max_seen_ids,
                note='LIVE notifications via polling fallback (CREATE only)',
              )
            else:
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

    On SurrealDB v3, passing ``'table:id'`` as a bare string to
    ``db.select`` is interpreted as a table name containing a colon
    (and silently returns nothing). When the target matches the
    record-id pattern we dispatch via raw SurrealQL
    ``SELECT * FROM type::record($table, $id)`` so the server treats it
    as a specific record. Mirrors the TS / rs / go ports.

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

        if _is_record_id_target(target):
          table, id_part = target.split(':', 1)
          raw = await self._client.query(
            'SELECT * FROM type::record($table, $id)',
            {'table': table, 'id': id_part},
          )
          rows = _extract_select_rows(_normalize_sdk_value(raw))
          return rows[0] if rows else None

        result = await self._client.select(target)
        return _normalize_sdk_value(result)
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
