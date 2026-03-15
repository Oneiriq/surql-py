"""Live query and real-time streaming support."""

import inspect
from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import UUID

import anyio
import structlog

logger = structlog.get_logger(__name__)


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
