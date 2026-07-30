"""Multiplexed SurrealDB session wrapper.

A :class:`Session` wraps a single SDK session (``surrealdb`` ``new_session()``)
multiplexed over a :class:`~surql.connection.client.DatabaseClient`'s
connection. It mirrors the client's query surface (execute / select / create /
update / merge / delete) plus session lifecycle (use / signin / invalidate /
close), reusing the same value normalize/denormalize helpers and the parent
client's concurrency semaphore.

Sessions are a WebSocket-only feature in the installed ``surrealdb`` SDK; the
HTTP and embedded engines raise on ``new_session()`` (see
:meth:`DatabaseClient.new_session`). Obtain a session via that method rather
than constructing this class directly.
"""

from typing import TYPE_CHECKING, Any

import structlog

from surql.connection.client import (
  ConnectionError,
  QueryError,
  _denormalize_params,
  _extract_select_rows,
  _is_record_id_target,
  _normalize_sdk_value,
  _normalize_target,
  _strip_record_id_brackets,
)

if TYPE_CHECKING:
  from surql.connection.client import DatabaseClient

logger = structlog.get_logger(__name__)


class Session:
  """An isolated SurrealDB session multiplexed over a client's connection.

  Mirrors the :class:`~surql.connection.client.DatabaseClient` query surface and
  reuses its value normalisation + concurrency controls. Each session carries
  its own ``USE`` namespace/database and auth state while sharing the underlying
  socket.

  Use it as an async context manager so the session is detached on exit:

    ```python
    async with await client.new_session() as session:
      await session.use('ns', 'db')
      await session.signin({'username': 'root', 'password': 'root'})
      rows = await session.execute('SELECT * FROM user')
    ```
  """

  def __init__(self, client: 'DatabaseClient', sdk_session: Any) -> None:
    """Initialise a session wrapper.

    Args:
      client: The owning database client (provides the shared semaphore and
        logging context).
      sdk_session: The underlying SDK session object returned by the SDK's
        ``new_session()``.
    """
    self._client = client
    self._session = sdk_session
    self._closed = False
    self._log = logger.bind(component='session')

  def _ensure_open(self) -> None:
    """Raise if the session has been closed."""
    if self._closed:
      raise ConnectionError('Session is closed')

  async def execute(self, query: str, params: dict[str, Any] | None = None) -> Any:
    """Execute a raw SurrealQL query within this session.

    Args:
      query: SurrealQL query string.
      params: Optional query parameters (record-id strings / surql RecordIDs /
        FileRefs are denormalised to SDK values before sending).

    Returns:
      Normalised query results.

    Raises:
      ConnectionError: If the session is closed.
      QueryError: If query execution fails.
    """
    self._ensure_open()
    async with self._client._semaphore:
      try:
        self._log.debug('session_executing_query', query=query, params=params)
        resolved = _denormalize_params(params) if params else {}
        result = await self._session.query(query, resolved)
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('session_query_failed', error=str(e), query=query)
        raise QueryError(f'Query execution failed: {e}') from e

  async def select(self, target: str) -> Any:
    """Select a record or table within this session.

    Mirrors :meth:`DatabaseClient.select`: a record-id target is dispatched via
    ``SELECT * FROM type::record($table, $id)`` (bound params) so SurrealDB v3
    treats it as a specific record; a table target uses the SDK ``select``.

    Args:
      target: Target table or record ID.

    Returns:
      A single record dict (record-id target) or a list (table target).

    Raises:
      ConnectionError: If the session is closed.
      QueryError: If the operation fails.
    """
    self._ensure_open()
    async with self._client._semaphore:
      try:
        self._log.debug('session_executing_select', target=target)
        if _is_record_id_target(target):
          table, id_part = target.split(':', 1)
          id_part = _strip_record_id_brackets(id_part)
          raw = await self._session.query(
            'SELECT * FROM type::record($table, $id)',
            {'table': table, 'id': id_part},
          )
          rows = _extract_select_rows(_normalize_sdk_value(raw))
          return rows[0] if rows else None
        result = await self._session.select(target)
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('session_select_failed', error=str(e), target=target)
        raise QueryError(f'SELECT operation failed: {e}') from e

  async def create(self, table: str, data: dict[str, Any]) -> Any:
    """Create a record within this session.

    Args:
      table: Target table name.
      data: Record data.

    Returns:
      The created record with SDK types normalised.

    Raises:
      ConnectionError: If the session is closed.
      QueryError: If the operation fails.
    """
    self._ensure_open()
    async with self._client._semaphore:
      try:
        self._log.debug('session_executing_create', table=table)
        result = await self._session.create(table, _denormalize_params(data))
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('session_create_failed', error=str(e), table=table)
        raise QueryError(f'CREATE operation failed: {e}') from e

  async def update(self, target: str, data: dict[str, Any]) -> Any:
    """Update a record or table within this session.

    Args:
      target: Target table or record ID.
      data: Update data.

    Returns:
      The updated record with SDK types normalised.

    Raises:
      ConnectionError: If the session is closed.
      QueryError: If the operation fails.
    """
    self._ensure_open()
    async with self._client._semaphore:
      try:
        self._log.debug('session_executing_update', target=target)
        result = await self._session.update(_normalize_target(target), _denormalize_params(data))
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('session_update_failed', error=str(e), target=target)
        raise QueryError(f'UPDATE operation failed: {e}') from e

  async def merge(self, target: str, data: dict[str, Any]) -> Any:
    """Merge data into a record or table within this session.

    Args:
      target: Target table or record ID.
      data: Data to merge.

    Returns:
      The merged record with SDK types normalised.

    Raises:
      ConnectionError: If the session is closed.
      QueryError: If the operation fails.
    """
    self._ensure_open()
    async with self._client._semaphore:
      try:
        self._log.debug('session_executing_merge', target=target)
        result = await self._session.merge(_normalize_target(target), _denormalize_params(data))
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('session_merge_failed', error=str(e), target=target)
        raise QueryError(f'MERGE operation failed: {e}') from e

  async def delete(self, target: str) -> Any:
    """Delete a record or table within this session.

    Args:
      target: Target table or record ID.

    Returns:
      The deletion result with SDK types normalised.

    Raises:
      ConnectionError: If the session is closed.
      QueryError: If the operation fails.
    """
    self._ensure_open()
    async with self._client._semaphore:
      try:
        self._log.debug('session_executing_delete', target=target)
        result = await self._session.delete(_normalize_target(target))
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('session_delete_failed', error=str(e), target=target)
        raise QueryError(f'DELETE operation failed: {e}') from e

  async def use(self, namespace: str, database: str) -> None:
    """Select the namespace and database for this session.

    Args:
      namespace: Namespace name.
      database: Database name.

    Raises:
      ConnectionError: If the session is closed.
    """
    self._ensure_open()
    async with self._client._semaphore:
      await self._session.use(namespace, database)

  async def signin(self, credentials: dict[str, Any]) -> Any:
    """Sign in within this session.

    Args:
      credentials: Sign-in payload (e.g. ``{'username': ..., 'password': ...}``).

    Returns:
      The SDK sign-in result (typically auth tokens), normalised.

    Raises:
      ConnectionError: If the session is closed.
      QueryError: If sign-in fails.
    """
    self._ensure_open()
    async with self._client._semaphore:
      try:
        result = await self._session.signin(credentials)
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('session_signin_failed', error=str(e))
        raise QueryError(f'Sign-in failed: {e}') from e

  async def invalidate(self) -> None:
    """Invalidate the current authentication for this session.

    Raises:
      ConnectionError: If the session is closed.
    """
    self._ensure_open()
    async with self._client._semaphore:
      await self._session.invalidate()

  async def live(self, table: str, diff: bool = False) -> Any:
    """Start a ``LIVE SELECT`` on a table within this session.

    Thin passthrough to the SDK session's ``live`` — returns the live-query
    UUID. Consuming notifications is handled by the SDK / streaming layer.

    Args:
      table: Table name to watch.
      diff: If True, notifications carry JSON-Patch diffs.

    Returns:
      The live-query identifier (normalised).

    Raises:
      ConnectionError: If the session is closed.
      QueryError: If the LIVE call fails.
    """
    self._ensure_open()
    async with self._client._semaphore:
      try:
        result = await self._session.live(table, diff)
        return _normalize_sdk_value(result)
      except Exception as e:
        self._log.error('session_live_failed', error=str(e), table=table)
        raise QueryError(f'LIVE operation failed: {e}') from e

  async def close_session(self) -> None:
    """Detach (close) this session, releasing its server-side state.

    Idempotent — closing an already-closed session is a no-op. The shared
    connection itself is not closed (it is owned by the parent client).
    """
    if self._closed:
      return
    try:
      await self._session.close_session()
    except Exception as e:  # pragma: no cover - defensive cleanup
      self._log.warning('session_close_failed', error=str(e))
    finally:
      self._closed = True

  async def aclose(self) -> None:
    """Alias for :meth:`close_session` (mirrors the async-resource convention)."""
    await self.close_session()

  async def __aenter__(self) -> 'Session':
    """Async context manager entry — returns the session."""
    return self

  async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    """Async context manager exit — detaches the session."""
    await self.close_session()
