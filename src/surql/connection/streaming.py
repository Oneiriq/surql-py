"""Live query and real-time streaming support."""

import inspect
from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import UUID, uuid4

import anyio
import structlog

from surql.connection._embedded_live_poll import EmbeddedLivePoller

logger = structlog.get_logger(__name__)

# URL prefixes that indicate the SurrealDB Python SDK will route through the
# embedded engine (``AsyncEmbeddedSurrealConnection``). The embedded engine's
# Rust extension does not expose a notification stream, so live queries fall
# back to the polling implementation in ``_embedded_live_poll``.
EMBEDDED_URL_PREFIXES: tuple[str, ...] = (
  'mem://',
  'memory://',
  'file://',
  'surrealkv://',
  'rocksdb://',
  'tikv://',
)


def is_embedded_url(url: str) -> bool:
  """Return True if ``url`` will route through the embedded engine."""
  return any(url.startswith(p) for p in EMBEDDED_URL_PREFIXES)


class StreamingError(Exception):
  """Raised when streaming operations fail."""

  pass


class LiveQuery:
  """Live query subscription wrapper."""

  def __init__(
    self,
    query_uuid: UUID,
    table: str,
    diff: bool = False,
  ) -> None:
    """Initialize live query.

    Args:
      query_uuid: Query UUID from SurrealDB
      table: Table name being watched
      diff: Whether diff mode is enabled
    """
    self.query_uuid = query_uuid
    self.table = table
    self.diff = diff
    self._active = True
    self._callbacks: list[Callable[[dict[str, Any]], None]] = []
    logger.info(
      'live_query_created',
      query_uuid=str(query_uuid),
      table=table,
      diff=diff,
    )

  def add_callback(self, callback: Callable[[dict[str, Any]], None]) -> None:
    """Add a callback for live query notifications.

    Args:
      callback: Function to call on each notification
    """
    self._callbacks.append(callback)

  def remove_callback(self, callback: Callable[[dict[str, Any]], None]) -> None:
    """Remove a callback.

    Args:
      callback: Callback function to remove
    """
    if callback in self._callbacks:
      self._callbacks.remove(callback)

  async def notify(self, notification: dict[str, Any]) -> None:
    """Notify all callbacks of a new event.

    Args:
      notification: Notification data from SurrealDB
    """
    for callback in self._callbacks:
      try:
        if inspect.iscoroutinefunction(callback):
          await callback(notification)
        else:
          callback(notification)
      except Exception as e:
        logger.error(
          'callback_error',
          error=str(e),
          query_uuid=str(self.query_uuid),
        )

  def deactivate(self) -> None:
    """Mark query as inactive."""
    self._active = False
    logger.info('live_query_deactivated', query_uuid=str(self.query_uuid))

  @property
  def is_active(self) -> bool:
    """Check if query is active."""
    return self._active


class StreamingManager:
  """Manager for live queries and real-time streaming."""

  def __init__(self, client: Any) -> None:
    """Initialize streaming manager.

    Args:
      client: Database client with live query support
    """
    self._client = client
    self._queries: dict[UUID, LiveQuery] = {}
    self._subscription_scopes: dict[UUID, anyio.CancelScope] = {}
    logger.info('streaming_manager_initialized')

  async def live(
    self,
    table: str,
    diff: bool = False,
  ) -> LiveQuery:
    """Start a live query on a table.

    Args:
      table: Table name to watch
      diff: Return JSON Patch diffs instead of full records

    Returns:
      Live query wrapper

    Raises:
      StreamingError: If live query fails to start

    Example:
      ```python
      query = await streaming.live('person')
      async for notification in streaming.subscribe(query):
        print(f"Change: {notification}")
      ```
    """
    try:
      query_uuid = await self._client.live(table, diff=diff)

      live_query = LiveQuery(
        query_uuid=query_uuid,
        table=table,
        diff=diff,
      )
      self._queries[query_uuid] = live_query

      logger.info(
        'live_query_started',
        query_uuid=str(query_uuid),
        table=table,
      )

      return live_query

    except Exception as e:
      logger.error('live_query_failed', error=str(e), table=table)
      raise StreamingError(f'Failed to start live query: {e}') from e

  async def subscribe(
    self,
    query: LiveQuery,
  ) -> AsyncIterator[dict[str, Any]]:
    """Subscribe to live query notifications as async iterator.

    Args:
      query: Live query to subscribe to

    Yields:
      Notification dictionaries

    Example:
      ```python
      query = await streaming.live('person')
      async for notification in streaming.subscribe(query):
        action = notification.get('action')
        data = notification.get('result')
        print(f"{action}: {data}")
      ```
    """
    try:
      async for notification in self._client.subscribe_live(query.query_uuid):
        # Check for close action
        if notification.get('action') == 'CLOSE':
          query.deactivate()
          break

        # Notify callbacks
        await query.notify(notification)

        # Yield to iterator consumer
        yield notification

    except Exception as e:
      logger.error(
        'subscription_error',
        error=str(e),
        query_uuid=str(query.query_uuid),
      )
      query.deactivate()
      raise StreamingError(f'Subscription failed: {e}') from e

  async def subscribe_with_callback(
    self,
    query: LiveQuery,
    callback: Callable[[dict[str, Any]], None],
  ) -> None:
    """Subscribe to live query with callback function.

    Consumes all notifications from the live query, invoking the callback
    for each one. Returns when the subscription closes (CLOSE action
    received or stream ends).

    Args:
      query: Live query to subscribe to
      callback: Function to call on each notification

    Example:
      ```python
      def on_change(notification):
        print(f"Change detected: {notification}")

      query = await streaming.live('person')
      await streaming.subscribe_with_callback(query, on_change)
      ```
    """
    query.add_callback(callback)
    async for _notification in self.subscribe(query):
      pass  # Callbacks are handled in subscribe()

  async def kill(self, query: LiveQuery) -> None:
    """Kill a live query.

    Args:
      query: Live query to kill

    Example:
      ```python
      await streaming.kill(query)
      ```
    """
    try:
      await self._client.kill(query.query_uuid)
      query.deactivate()

      # Cancel subscription scope if exists
      if query.query_uuid in self._subscription_scopes:
        self._subscription_scopes[query.query_uuid].cancel()
        del self._subscription_scopes[query.query_uuid]

      if query.query_uuid in self._queries:
        del self._queries[query.query_uuid]

      logger.info('live_query_killed', query_uuid=str(query.query_uuid))

    except Exception as e:
      logger.error(
        'kill_query_failed',
        error=str(e),
        query_uuid=str(query.query_uuid),
      )
      raise StreamingError(f'Failed to kill query: {e}') from e

  async def kill_all(self) -> None:
    """Kill all active live queries."""
    queries = list(self._queries.values())
    for query in queries:
      if query.is_active:
        await self.kill(query)

    logger.info('all_queries_killed', count=len(queries))

  def get_active_queries(self) -> list[LiveQuery]:
    """Get all active live queries.

    Returns:
      List of active queries
    """
    return [q for q in self._queries.values() if q.is_active]


class EmbeddedPollingStreamingManager:
  """Polling-based streaming manager for the embedded SurrealDB engine.

  The official ``surrealdb`` Python SDK's ``AsyncEmbeddedSurrealConnection``
  does not implement live-query notification dispatch -- the underlying Rust
  extension only exposes a one-shot ``execute(cbor) -> bytes`` call, and the
  inherited ``live`` method references a ``live_queues`` attribute that is
  never initialized. This manager emulates LIVE behavior by polling the
  watched table for newly-inserted rows.

  This is a **degraded fallback**: only CREATE-style notifications are
  emitted, latency is bounded by the poll interval, and updates / deletes are
  not observed. It exists so that downstream code which subscribes to LIVE
  events can keep working when SurrealDB is run in-process via
  ``surrealkv://`` / ``mem://`` / ``file://`` URLs without a sidecar
  ``ws://`` SurrealDB.

  The public API matches :class:`StreamingManager` so callers can hold a
  ``StreamingManager | EmbeddedPollingStreamingManager`` union without
  branching on engine type at every call site.

  Args:
    client: A connected :class:`surql.connection.DatabaseClient` instance
      that can issue raw queries via ``client.execute(...)``. The polling
      manager does not access the underlying SDK client directly.
    interval_s: Default poll interval used when ``live()`` is called without
      an explicit override. Defaults to 0.25 seconds (4 Hz).
    max_seen_ids: Default LRU cap on per-table seen-id sets. Defaults to
      10_000.
  """

  def __init__(
    self,
    client: Any,
    *,
    interval_s: float = 0.25,
    max_seen_ids: int = 10_000,
  ) -> None:
    self._client = client
    self._interval_s = interval_s
    self._max_seen = max_seen_ids
    self._queries: dict[UUID, LiveQuery] = {}
    self._pollers: dict[UUID, EmbeddedLivePoller] = {}
    logger.info(
      'embedded_polling_streaming_manager_initialized',
      interval_s=interval_s,
      max_seen_ids=max_seen_ids,
    )

  async def live(
    self,
    table: str,
    diff: bool = False,
    *,
    interval_s: float | None = None,
    max_seen_ids: int | None = None,
  ) -> LiveQuery:
    """Register a polling subscription for ``table``.

    Args:
      table: Table to watch.
      diff: Accepted for API compatibility with :class:`StreamingManager`.
        The polling fallback does not produce diffs and ignores this flag;
        a warning is logged when ``diff=True`` is requested so callers can
        spot the degradation.
      interval_s: Optional per-query override for the poll interval.
      max_seen_ids: Optional per-query override for the seen-id LRU cap.

    Returns:
      A :class:`LiveQuery` handle whose ``query_uuid`` is locally generated
      (the embedded engine never issues one).
    """
    if diff:
      logger.warning(
        'embedded_polling_diff_not_supported',
        table=table,
        note='diff mode ignored; emitting CREATE-only notifications',
      )

    query_uuid = uuid4()
    poller = EmbeddedLivePoller(
      self._client,
      table,
      interval_s=interval_s if interval_s is not None else self._interval_s,
      max_seen_ids=max_seen_ids if max_seen_ids is not None else self._max_seen,
    )
    live_query = LiveQuery(query_uuid=query_uuid, table=table, diff=False)
    self._queries[query_uuid] = live_query
    self._pollers[query_uuid] = poller

    logger.info(
      'embedded_live_query_started',
      query_uuid=str(query_uuid),
      table=table,
      interval_s=poller._interval_s,
    )
    return live_query

  async def subscribe(
    self,
    query: LiveQuery,
  ) -> AsyncIterator[dict[str, Any]]:
    """Yield CREATE-shaped notifications from the table poller.

    Args:
      query: A :class:`LiveQuery` previously returned by :meth:`live`.

    Yields:
      Dicts shaped ``{'action': 'CREATE', 'result': <row>}``.

    Raises:
      StreamingError: If ``query`` is unknown to this manager (i.e. wasn't
        produced by ``EmbeddedPollingStreamingManager.live``).
    """
    poller = self._pollers.get(query.query_uuid)
    if poller is None:
      raise StreamingError(
        f'unknown query uuid {query.query_uuid}; was it created by this manager?'
      )
    try:
      async for notification in poller.stream():
        await query.notify(notification)
        yield notification
    except Exception as exc:
      logger.error(
        'embedded_subscription_error',
        error=str(exc),
        query_uuid=str(query.query_uuid),
      )
      query.deactivate()
      raise StreamingError(f'Subscription failed: {exc}') from exc
    finally:
      query.deactivate()

  async def subscribe_with_callback(
    self,
    query: LiveQuery,
    callback: Callable[[dict[str, Any]], None],
  ) -> None:
    """Subscribe and invoke ``callback`` for each notification.

    Mirrors :meth:`StreamingManager.subscribe_with_callback`.
    """
    query.add_callback(callback)
    async for _notification in self.subscribe(query):
      pass

  async def kill(self, query: LiveQuery) -> None:
    """Stop a polling subscription and forget the query.

    Args:
      query: The :class:`LiveQuery` to terminate.
    """
    poller = self._pollers.pop(query.query_uuid, None)
    if poller is not None:
      poller.stop()
    self._queries.pop(query.query_uuid, None)
    query.deactivate()
    logger.info('embedded_live_query_killed', query_uuid=str(query.query_uuid))

  async def kill_all(self) -> None:
    """Stop every active polling subscription managed here."""
    for query in list(self._queries.values()):
      if query.is_active:
        await self.kill(query)
    logger.info('embedded_all_queries_killed')

  def get_active_queries(self) -> list[LiveQuery]:
    """Return the list of currently-active queries."""
    return [q for q in self._queries.values() if q.is_active]
