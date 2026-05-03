"""Tests for the embedded-engine LIVE polling fallback.

These tests rely on real ``asyncio`` primitives (``create_task``, ``wait_for``,
``sleep``) so they pin the anyio backend to ``asyncio`` -- the trio backend
would error trying to use a missing running asyncio loop. The streaming
manager itself is event-loop-agnostic; only the polling driver uses asyncio
loops directly because it needs cooperative cancellation.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from surql.connection._embedded_live_poll import EmbeddedLivePoller
from surql.connection.client import DatabaseClient
from surql.connection.config import ConnectionConfig
from surql.connection.streaming import (
  EmbeddedPollingStreamingManager,
  LiveQuery,
  StreamingError,
  is_embedded_url,
)


@pytest.fixture
def anyio_backend() -> str:
  """Restrict every test in this module to the asyncio backend."""
  return 'asyncio'


class _FakeClient:
  """Minimal stand-in for :class:`DatabaseClient` used by the poller.

  Only ``execute(query)`` is consumed by :class:`EmbeddedLivePoller`. Each
  call pops the next response from ``responses`` (or raises the next entry
  if it's an Exception).
  """

  def __init__(self, responses: list[Any]) -> None:
    self._responses = list(responses)
    self.calls: list[str] = []

  async def execute(
    self,
    query: str,
    params: dict[str, Any] | None = None,  # noqa: ARG002
  ) -> Any:
    self.calls.append(query)
    if not self._responses:
      return []
    nxt = self._responses.pop(0)
    if isinstance(nxt, Exception):
      raise nxt
    return nxt


class TestIsEmbeddedUrl:
  """Smoke tests for the engine-detection helper."""

  @pytest.mark.parametrize(
    'url',
    [
      'mem://',
      'memory://',
      'file:///tmp/db',
      'surrealkv:///data/foo.db',
      'rocksdb:///tmp/foo.db',
      'tikv://127.0.0.1:2379',
    ],
  )
  def test_embedded_urls(self, url: str) -> None:
    assert is_embedded_url(url) is True

  @pytest.mark.parametrize(
    'url',
    [
      'ws://localhost:8000/rpc',
      'wss://example.com/rpc',
      'http://localhost:8000',
      'https://example.com',
    ],
  )
  def test_remote_urls(self, url: str) -> None:
    assert is_embedded_url(url) is False


class TestEmbeddedLivePoller:
  """Behavior of the per-table poller."""

  @pytest.mark.anyio
  async def test_first_tick_primes_without_emitting(self) -> None:
    """Existing rows present at subscription time are NOT emitted."""
    client = _FakeClient(
      responses=[
        # Tick 1 (priming): two existing rows
        [{'id': 'reading:a', 'soil': 30}, {'id': 'reading:b', 'soil': 31}],
        # Tick 2: same rows, no new ones
        [{'id': 'reading:a', 'soil': 30}, {'id': 'reading:b', 'soil': 31}],
      ]
    )
    poller = EmbeddedLivePoller(client, 'reading', interval_s=0.01)

    received: list[dict[str, Any]] = []

    async def consume() -> None:
      async for note in poller.stream():
        received.append(note)

    task = asyncio.create_task(consume())
    # Let two ticks complete then stop.
    await asyncio.sleep(0.05)
    poller.stop()
    await task

    assert received == []

  @pytest.mark.anyio
  async def test_emits_create_for_new_rows(self) -> None:
    """Rows whose id is new are emitted with action=CREATE."""
    client = _FakeClient(
      responses=[
        # Tick 1 (priming): one existing row
        [{'id': 'reading:a', 'soil': 30}],
        # Tick 2: new row appears
        [{'id': 'reading:a', 'soil': 30}, {'id': 'reading:b', 'soil': 31}],
        # Tick 3: another new row
        [
          {'id': 'reading:a', 'soil': 30},
          {'id': 'reading:b', 'soil': 31},
          {'id': 'reading:c', 'soil': 32},
        ],
      ]
    )
    poller = EmbeddedLivePoller(client, 'reading', interval_s=0.01)
    received: list[dict[str, Any]] = []

    async def consume() -> None:
      async for note in poller.stream():
        received.append(note)
        if len(received) >= 2:
          poller.stop()

    task = asyncio.create_task(consume())
    await asyncio.wait_for(task, timeout=2.0)

    assert len(received) == 2
    assert all(n['action'] == 'CREATE' for n in received)
    new_ids = {n['result']['id'] for n in received}
    assert new_ids == {'reading:b', 'reading:c'}

  @pytest.mark.anyio
  async def test_seen_lru_cap_evicts_oldest(self) -> None:
    """Seen-id LRU evicts oldest ids when over capacity."""
    poller = EmbeddedLivePoller(_FakeClient([]), 'reading', interval_s=0.01, max_seen_ids=2)
    assert poller._remember('a') is True
    assert poller._remember('b') is True
    assert poller._remember('c') is True  # evicts 'a'
    # 'a' should be re-emit-able now
    assert poller._remember('a') is True
    # 'c' should still be remembered
    assert poller._remember('c') is False

  @pytest.mark.anyio
  async def test_fetch_failure_does_not_crash_stream(self) -> None:
    """A transient query error skips the tick but keeps the loop alive."""
    client = _FakeClient(
      responses=[
        # Tick 1 (priming): empty
        [],
        # Tick 2: raises
        RuntimeError('transient'),
        # Tick 3: a new row
        [{'id': 'reading:x'}],
      ]
    )
    poller = EmbeddedLivePoller(client, 'reading', interval_s=0.01)
    received: list[dict[str, Any]] = []

    async def consume() -> None:
      async for note in poller.stream():
        received.append(note)
        poller.stop()

    task = asyncio.create_task(consume())
    await asyncio.wait_for(task, timeout=2.0)
    assert received[0]['action'] == 'CREATE'
    assert received[0]['result']['id'] == 'reading:x'

  @pytest.mark.anyio
  async def test_extract_rows_handles_envelope_shapes(self) -> None:
    """``_extract_rows`` accepts both bare lists and statement envelopes."""
    extract = EmbeddedLivePoller._extract_rows
    # Bare list of rows (post-normalization)
    assert extract([{'id': 'a'}, {'id': 'b'}]) == [{'id': 'a'}, {'id': 'b'}]
    # Statement-envelope form
    assert extract([{'status': 'OK', 'result': [{'id': 'a'}]}]) == [{'id': 'a'}]
    # None / empty
    assert extract(None) == []
    assert extract([]) == []
    # Single dict
    assert extract({'result': [{'id': 'a'}]}) == [{'id': 'a'}]

  @pytest.mark.anyio
  async def test_invalid_init_args(self) -> None:
    """Constructor rejects nonsensical arguments."""
    with pytest.raises(ValueError):
      EmbeddedLivePoller(_FakeClient([]), 'reading', interval_s=0)
    with pytest.raises(ValueError):
      EmbeddedLivePoller(_FakeClient([]), 'reading', max_seen_ids=0)


class TestEmbeddedPollingStreamingManager:
  """Behavior of the polling streaming manager."""

  @pytest.mark.anyio
  async def test_live_returns_query_with_local_uuid(self) -> None:
    """``live`` produces a LiveQuery with a locally-generated UUID."""
    manager = EmbeddedPollingStreamingManager(_FakeClient([]), interval_s=0.01)
    query = await manager.live('reading')

    assert isinstance(query, LiveQuery)
    assert query.table == 'reading'
    assert query.diff is False
    assert isinstance(query.query_uuid, UUID)
    assert query in manager.get_active_queries()

  @pytest.mark.anyio
  async def test_diff_flag_logged_but_ignored(self) -> None:
    """Requesting diff mode warns; the manager still returns a query."""
    manager = EmbeddedPollingStreamingManager(_FakeClient([]))
    query = await manager.live('reading', diff=True)
    # diff mode is degraded to False on the polling fallback
    assert query.diff is False

  @pytest.mark.anyio
  async def test_subscribe_yields_create_notifications(self) -> None:
    """End-to-end: subscribe sees CREATE for newly-inserted rows."""
    client = _FakeClient(
      responses=[
        [],  # priming tick: no rows
        [{'id': 'reading:a'}],  # one new row
      ]
    )
    manager = EmbeddedPollingStreamingManager(client, interval_s=0.01)
    query = await manager.live('reading')

    received: list[dict[str, Any]] = []

    async def consume() -> None:
      async for note in manager.subscribe(query):
        received.append(note)
        await manager.kill(query)

    await asyncio.wait_for(consume(), timeout=2.0)
    assert received == [{'action': 'CREATE', 'result': {'id': 'reading:a'}}]
    assert query.is_active is False

  @pytest.mark.anyio
  async def test_subscribe_invokes_callbacks(self) -> None:
    """``subscribe_with_callback`` fans notifications through the callback."""
    client = _FakeClient(
      responses=[
        [],  # priming
        [{'id': 'reading:a'}],
      ]
    )
    manager = EmbeddedPollingStreamingManager(client, interval_s=0.01)
    query = await manager.live('reading')
    seen: list[dict[str, Any]] = []

    def cb(note: dict[str, Any]) -> None:
      seen.append(note)
      manager.get_active_queries()  # just exercise the API
      # Stop the poller after the first notification arrives
      asyncio.get_running_loop().call_soon(lambda: manager._pollers[query.query_uuid].stop())

    await asyncio.wait_for(
      manager.subscribe_with_callback(query, cb),
      timeout=2.0,
    )
    assert len(seen) == 1
    assert seen[0]['action'] == 'CREATE'

  @pytest.mark.anyio
  async def test_subscribe_unknown_query_raises(self) -> None:
    """``subscribe`` rejects a query not produced by this manager."""
    manager = EmbeddedPollingStreamingManager(_FakeClient([]))
    foreign = LiveQuery(query_uuid=UUID('11111111-1111-1111-1111-111111111111'), table='reading')

    with pytest.raises(StreamingError, match='unknown query uuid'):
      async for _ in manager.subscribe(foreign):
        pass

  @pytest.mark.anyio
  async def test_kill_all_stops_every_poller(self) -> None:
    """``kill_all`` deactivates every active query."""
    manager = EmbeddedPollingStreamingManager(_FakeClient([]))
    q1 = await manager.live('reading')
    q2 = await manager.live('detection')

    assert {q.query_uuid for q in manager.get_active_queries()} == {q1.query_uuid, q2.query_uuid}

    await manager.kill_all()

    assert manager.get_active_queries() == []
    assert q1.is_active is False
    assert q2.is_active is False


class TestDatabaseClientEmbeddedSelection:
  """``DatabaseClient.connect`` picks the right manager per URL scheme."""

  def _make_config(self, url: str) -> ConnectionConfig:
    return ConnectionConfig(
      _env_file=None,
      url=url,
      namespace='test',
      database='test',
      enable_live_queries=True,
    )

  @pytest.mark.anyio
  async def test_embedded_url_installs_polling_manager(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """A ``surrealkv://`` URL routes to the polling manager."""
    config = self._make_config('surrealkv:///tmp/x.db')
    client = DatabaseClient(config)

    fake_sdk = MagicMock()
    fake_sdk.connect = AsyncMock()
    fake_sdk.use = AsyncMock()
    monkeypatch.setattr('surql.connection.client.AsyncSurreal', lambda _url: fake_sdk)

    await client.connect()
    try:
      assert isinstance(client.streaming, EmbeddedPollingStreamingManager)
    finally:
      client._connected = False  # short-circuit disconnect

  @pytest.mark.anyio
  async def test_websocket_url_installs_native_manager(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """A ``ws://`` URL routes to the native streaming manager."""
    from surql.connection.streaming import StreamingManager

    config = self._make_config('ws://localhost:8000/rpc')
    client = DatabaseClient(config)

    fake_sdk = MagicMock()
    fake_sdk.connect = AsyncMock()
    fake_sdk.use = AsyncMock()
    monkeypatch.setattr('surql.connection.client.AsyncSurreal', lambda _url: fake_sdk)

    await client.connect()
    try:
      assert isinstance(client.streaming, StreamingManager)
    finally:
      client._connected = False
