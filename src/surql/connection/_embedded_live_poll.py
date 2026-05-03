"""Poll-based fallback for LIVE SELECT against the embedded SurrealDB engine.

The official ``surrealdb`` Python SDK's :class:`AsyncEmbeddedSurrealConnection`
does not implement a notification dispatcher: its Rust extension
(``_surrealdb_ext.AsyncEmbeddedDB``) only exposes a one-shot
``execute(cbor_request) -> bytes`` surface, so the inherited ``live`` /
``subscribe_live`` methods either error (``live_queues`` attribute missing) or
silently never deliver notifications.

This module provides a degraded-but-functional alternative: instead of pushing
notifications from the engine, it periodically polls a table and emits
``{'action': 'CREATE', 'result': <row>}`` notifications for rows whose record
ids haven't been seen yet. It is designed for append-mostly workloads (sensor
telemetry, event logs, audit streams) where consumers care about new rows
arriving and can tolerate the polling cadence as a latency floor.

This is **not** a drop-in equivalent of native LIVE: there is no UPDATE or
DELETE event, the cadence is bounded by ``interval_s``, and a long-running
subscription accumulates seen ids in memory (capped via ``max_seen_ids``).
The intended consumer is :class:`surql.connection.streaming.StreamingManager`
when the underlying client is detected to be embedded.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import OrderedDict
from collections.abc import AsyncIterator
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class EmbeddedLivePoller:
  """Poll a table for newly-inserted rows and yield CREATE-style notifications.

  Each tick, this issues ``SELECT * FROM <table>`` (using the wrapped
  ``DatabaseClient.execute`` path so it goes through the SDK's CBOR pipeline)
  and emits one notification per row whose stringified ``id`` was not present
  in a bounded LRU cache of previously-seen ids. On the first tick the cache
  is primed (no notifications emitted) so consumers don't see a flood of
  historical rows on subscription.

  Args:
    client: A connected :class:`surql.connection.DatabaseClient` instance whose
      underlying SDK client is the embedded engine.
    table: The table to watch.
    interval_s: Seconds between polls. Defaults to 0.25 (4 Hz).
    max_seen_ids: Maximum number of record ids to remember (FIFO eviction).
      Older ids beyond this cap will be re-emitted as CREATE if they reappear
      in subsequent polls; tune to your retention window. Defaults to 10_000.
  """

  def __init__(
    self,
    client: Any,
    table: str,
    *,
    interval_s: float = 0.25,
    max_seen_ids: int = 10_000,
  ) -> None:
    if interval_s <= 0:
      raise ValueError('interval_s must be positive')
    if max_seen_ids <= 0:
      raise ValueError('max_seen_ids must be positive')
    self._client = client
    self._table = table
    self._interval_s = interval_s
    self._max_seen = max_seen_ids
    self._seen: OrderedDict[str, None] = OrderedDict()
    self._stopped = asyncio.Event()
    self._primed = False

  def stop(self) -> None:
    """Signal the poll loop to terminate after the current tick."""
    self._stopped.set()

  @property
  def is_stopped(self) -> bool:
    """Whether stop() has been requested."""
    return self._stopped.is_set()

  def _remember(self, record_id: str) -> bool:
    """Record a new id, evicting the oldest if at capacity.

    Returns:
      True when the id was newly seen, False if it was already known.
    """
    if record_id in self._seen:
      # Refresh recency so active ids stay in the LRU.
      self._seen.move_to_end(record_id)
      return False
    self._seen[record_id] = None
    if len(self._seen) > self._max_seen:
      self._seen.popitem(last=False)
    return True

  async def _fetch_rows(self) -> list[dict[str, Any]]:
    """Run one SELECT and return the row list (empty on transient error)."""
    try:
      result = await self._client.execute(f'SELECT * FROM {self._table}')
    except Exception as exc:
      logger.warning(
        'embedded_live_poll.fetch_failed',
        table=self._table,
        error=str(exc),
      )
      return []
    rows = self._extract_rows(result)
    return rows

  @staticmethod
  def _extract_rows(value: Any) -> list[dict[str, Any]]:
    """Flatten a query response to a list of row dicts.

    The wrapped client may return either a list of rows directly (post-
    normalization) or a statement-envelope list -- accept both.
    """
    if value is None:
      return []
    if isinstance(value, dict):
      if 'result' in value and isinstance(value['result'], list):
        return [r for r in value['result'] if isinstance(r, dict)]
      return [value]
    if isinstance(value, list):
      if not value:
        return []
      first = value[0]
      if isinstance(first, dict) and 'result' in first and 'status' in first:
        inner = first.get('result')
        if isinstance(inner, list):
          return [r for r in inner if isinstance(r, dict)]
        return []
      return [r for r in value if isinstance(r, dict)]
    return []

  async def stream(self) -> AsyncIterator[dict[str, Any]]:
    """Yield CREATE-shaped notifications for newly-observed rows.

    Yields:
      Dicts shaped ``{'action': 'CREATE', 'result': <row>}``. The shape mirrors
      the SurrealDB WS LIVE notification envelope so downstream consumers
      (e.g. :class:`StreamingManager.subscribe`) treat poll events the same as
      native ones.
    """
    logger.info(
      'embedded_live_poll.starting',
      table=self._table,
      interval_s=self._interval_s,
    )
    try:
      while not self._stopped.is_set():
        rows = await self._fetch_rows()
        if not self._primed:
          # First tick primes the seen-set; do not emit historical rows.
          for row in rows:
            rid = row.get('id')
            if rid is not None:
              self._remember(str(rid))
          self._primed = True
        else:
          for row in rows:
            rid = row.get('id')
            if rid is None:
              continue
            if self._remember(str(rid)):
              yield {'action': 'CREATE', 'result': row}
        with contextlib.suppress(TimeoutError):
          await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_s)
    finally:
      logger.info('embedded_live_poll.stopped', table=self._table)


__all__ = ['EmbeddedLivePoller']
