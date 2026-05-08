"""Async SurrealDB client wrapper with connection pooling and retry logic."""

import asyncio
import re
from collections.abc import AsyncIterator, Awaitable, Callable
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
# Two guards keep this from over-matching:
#
# 1. Negative lookahead ``(?!//)`` after the colon excludes URL schemes
#    (``http://``, ``https://``, ``ws://``, ``wss://``, ``file://``, ...) which
#    share the ``<word>:<rest>`` shape with record-id literals. Without this
#    guard, ``base_url='http://10.0.0.51:11434'`` was silently rewritten to
#    ``RecordID('http', '//10.0.0.51:11434')`` and SurrealDB returned a coerce
#    error in the result text of an otherwise OK-status query response.
#
# 2. ``\S+$`` (non-whitespace, end-anchored) rejects prose. Real record IDs
#    never contain whitespace; English content fields starting with a word
#    plus colon (``Pattern: when ...``, ``TODO: implement``, ``Note: see``)
#    were being coerced to ``RecordID('Pattern', ' when ...')`` and rejected
#    at the schema layer with "Couldn't coerce value for field `content`...
#    Expected `string` but found `Pattern:`...". Pattern-matching string
#    values to detect record IDs is best-effort -- callers can always pass
#    ``RecordID(table, id)`` explicitly when the target really is a record
#    that happens to contain unusual characters.
# ``\Z`` (absolute end-of-string) is used instead of ``$`` so a trailing
# newline doesn't slip past the anchor -- ``$`` matches at end-of-string OR
# just before a final ``\n`` by default, which would let ``"user:alice\n"``
# coerce.
_RECORD_ID_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*:(?!//)\S+\Z')


# Substrings that indicate the underlying WebSocket / transport went away
# mid-flight. When the upstream SurrealDB process is recreated (docker
# `compose up -d surrealdb`, k8s pod restart, etc.) every queued query
# raises one of these messages instead of triggering a clean reconnect,
# which permanently breaks the long-running client until process restart.
# Detecting them lets us reconnect once and retry the call, so consumers
# survive a DB recycle without manual intervention.
_DISCONNECT_ERROR_SUBSTRINGS: tuple[str, ...] = (
  'no close frame received or sent',
  'connection is closed',
  'received 1011',
  'received 1012',
  'received 1013',
  'WebSocket is not connected',
  'cannot send while sending',
  'connection lost',
  'connection reset',
  'broken pipe',
)


def _is_disconnect_error(exc: BaseException, _depth: int = 0) -> bool:
  """Return True when the exception indicates a transport-level disconnect.

  Walks the cause/context chain so wrapper exceptions (e.g. SDK's own
  ``SurrealDBError`` wrapping a ``websockets.ConnectionClosed``) are still
  classified correctly.
  """
  if _depth > 5:
    return False
  if 'ConnectionClosed' in type(exc).__name__:
    return True
  msg = str(exc)
  if any(s in msg for s in _DISCONNECT_ERROR_SUBSTRINGS):
    return True
  for nested in (getattr(exc, '__cause__', None), getattr(exc, '__context__', None)):
    if nested is not None and nested is not exc and _is_disconnect_error(nested, _depth + 1):
      return True
  return False


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
    # Coalesces concurrent reconnect attempts: if N queries all observe
    # a dead WebSocket simultaneously, only the first reconnects and the
    # rest wait + retry on its newly-established connection.
    self._reconnect_lock = asyncio.Lock()
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

  async def _ensure_reconnected(self) -> None:
    """Ensure the client has a live connection, reconnecting if needed.

    Multiple concurrent callers coalesce on ``_reconnect_lock``: the first
    one observes the dead socket and reconnects, the rest find
    ``is_connected`` true on entry and return immediately. ``connect()``
    is the single source of truth for the dial / signin / use sequence.
    """
    async with self._reconnect_lock:
      if self.is_connected:
        return
      # Force the disconnect path inside connect() to be a no-op since
      # the underlying socket is already dead.
      self._connected = False
      self._client = None
      await self.connect()

  async def _invoke(
    self,
    op: str,
    fn: Callable[[Any], Awaitable[Any]],
    *,
    log_kwargs: dict[str, Any] | None = None,
    error_message: str | None = None,
  ) -> Any:
    """Run an SDK call with auto-reconnect on transport-level disconnects.

    The first call follows the original fast path: acquire the semaphore,
    invoke ``fn(self._client)``, normalize the result. If the SDK raises
    something matching ``_is_disconnect_error`` (a recreated SurrealDB
    container is the canonical case — every queued query raises
    ``"no close frame received or sent"`` until the client redials), this
    method releases the semaphore, reconnects under the reconnect lock,
    then retries the call exactly once on the fresh connection. Any other
    exception, or a second failure post-reconnect, is wrapped in
    :class:`QueryError` with the original chained as ``__cause__`` so
    callers keep the same surface they had before.
    """
    log_kwargs = log_kwargs or {}
    label = error_message or f'{op.upper()} operation failed'

    if self._client is None:
      # Caller never invoked connect(). Preserve the historical surface
      # — telling them to redial once is a behavior change we don't want
      # since it would mask "forgot to connect" bugs.
      raise ConnectionError('Client is not connected to database')

    if not self._connected:
      # A previous call's reconnect failed (typical when SurrealDB is
      # mid-recreate and not accepting connections yet). Retry the
      # reconnect now so transient outages self-heal as soon as the
      # server is back, rather than leaving the client permanently
      # broken until the host process restarts.
      try:
        await self._ensure_reconnected()
      except Exception as err:
        raise ConnectionError(f'Client is not connected to database: {err}') from err

    async def _run_once() -> Any:
      async with self._semaphore:
        return _normalize_sdk_value(await fn(self._client))

    try:
      return await _run_once()
    except Exception as first_err:
      if not _is_disconnect_error(first_err):
        self._log.error(f'{op}_failed', error=str(first_err), **log_kwargs)
        raise QueryError(f'{label}: {first_err}') from first_err

      self._log.warning(
        f'{op}_disconnect_detected_reconnecting',
        error=str(first_err),
        error_type=type(first_err).__name__,
        **log_kwargs,
      )
      # Mark the local flag dead before grabbing the lock. Without this,
      # _ensure_reconnected's short-circuit (`if self.is_connected:`)
      # would see the still-True flag and skip the actual redial. The
      # only signal we've had that the socket is dead is the exception
      # — flip the flag so the lock-holder follows the reconnect path.
      self._connected = False
      try:
        await self._ensure_reconnected()
      except Exception as reconnect_err:
        self._log.error(
          f'{op}_reconnect_failed',
          error=str(reconnect_err),
          error_type=type(reconnect_err).__name__,
          **log_kwargs,
        )
        raise QueryError(
          f'{label}: reconnect failed after disconnect: {reconnect_err}'
        ) from reconnect_err

      try:
        result = await _run_once()
      except Exception as retry_err:
        self._log.error(
          f'{op}_failed_post_reconnect',
          error=str(retry_err),
          error_type=type(retry_err).__name__,
          **log_kwargs,
        )
        raise QueryError(f'{label}: {retry_err}') from retry_err

      self._log.info(f'{op}_recovered_after_reconnect', **log_kwargs)
      return result

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
    self._log.debug('executing_query', query=query, params=params)
    resolved_params = _denormalize_params(params) if params else {}

    async def _do(client: Any) -> Any:
      return await client.query(query, resolved_params)

    return await self._invoke(
      'query',
      _do,
      log_kwargs={'query': query},
      error_message='Query execution failed',
    )

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
    ``SELECT * FROM type::thing($table, $id)`` so the server treats it
    as a specific record. Mirrors the TS / rs / go ports.

    Note: this used to call ``type::record($table, $id)``, but in v3 the
    two-arg form of ``type::record(value, type)`` is a *type coercion*
    (cast ``value`` into ``record<type>``), NOT a table+id constructor;
    the constructor is ``type::thing(table, id)``.

    Args:
      target: Target table or record ID

    Returns:
      Selected record dict (single ID) or list of records (table)

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    self._log.debug('executing_select', target=target)

    if _is_record_id_target(target):
      table, id_part = target.split(':', 1)

      async def _do_record(client: Any) -> Any:
        raw = await client.query(
          'SELECT * FROM type::thing($table, $id)',
          {'table': table, 'id': id_part},
        )
        rows = _extract_select_rows(_normalize_sdk_value(raw))
        return rows[0] if rows else None

      # `_do_record` already returns plain Python (None or dict); skip
      # the outer normalize by short-circuiting through _invoke's fn.
      return await self._invoke(
        'select',
        _do_record,
        log_kwargs={'target': target},
        error_message='SELECT operation failed',
      )

    async def _do_table(client: Any) -> Any:
      return await client.select(target)

    return await self._invoke(
      'select',
      _do_table,
      log_kwargs={'target': target},
      error_message='SELECT operation failed',
    )

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
    self._log.debug('executing_create', table=table, data=data)
    payload = _denormalize_params(data)

    async def _do(client: Any) -> Any:
      return await client.create(table, payload)

    return await self._invoke(
      'create',
      _do,
      log_kwargs={'table': table},
      error_message='CREATE operation failed',
    )

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
    self._log.debug('executing_update', target=target, data=data)
    payload = _denormalize_params(data)

    async def _do(client: Any) -> Any:
      return await client.update(target, payload)

    return await self._invoke(
      'update',
      _do,
      log_kwargs={'target': target},
      error_message='UPDATE operation failed',
    )

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
    self._log.debug('executing_merge', target=target, data=data)
    payload = _denormalize_params(data)

    async def _do(client: Any) -> Any:
      return await client.merge(target, payload)

    return await self._invoke(
      'merge',
      _do,
      log_kwargs={'target': target},
      error_message='MERGE operation failed',
    )

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
    self._log.debug('executing_delete', target=target)

    async def _do(client: Any) -> Any:
      return await client.delete(target)

    return await self._invoke(
      'delete',
      _do,
      log_kwargs={'target': target},
      error_message='DELETE operation failed',
    )

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
    self._log.debug('executing_insert_relation', table=table, data=data)
    payload = _denormalize_params(data)

    async def _do(client: Any) -> Any:
      return await client.insert_relation(table, payload)

    return await self._invoke(
      'insert_relation',
      _do,
      log_kwargs={'table': table},
      error_message='INSERT RELATION operation failed',
    )

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
