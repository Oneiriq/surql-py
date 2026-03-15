"""Tests for streaming module."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from surql.connection.streaming import LiveQuery, StreamingError, StreamingManager


class TestLiveQuery:
  """Tests for LiveQuery."""

  def test_create_live_query(self):
    """Test creating a live query."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person', diff=False)

    assert query.query_uuid == query_uuid
    assert query.table == 'person'
    assert query.diff is False
    assert query.is_active is True

  def test_live_query_with_diff(self):
    """Test creating a live query with diff mode."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person', diff=True)

    assert query.diff is True

  def test_add_callback(self):
    """Test adding a callback."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person')

    def callback(notification):
      pass

    query.add_callback(callback)
    assert callback in query._callbacks

  def test_remove_callback(self):
    """Test removing a callback."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person')

    def callback(notification):
      pass

    query.add_callback(callback)
    query.remove_callback(callback)
    assert callback not in query._callbacks

  @pytest.mark.anyio
  async def test_notify_sync_callback(self):
    """Test notifying with synchronous callback."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person')

    callback_called = []

    def callback(notification):
      callback_called.append(notification)

    query.add_callback(callback)
    notification = {'action': 'CREATE', 'result': {'id': 'person:1'}}
    await query.notify(notification)

    assert len(callback_called) == 1
    assert callback_called[0] == notification

  @pytest.mark.anyio
  async def test_notify_async_callback(self):
    """Test notifying with asynchronous callback."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person')

    callback_called = []

    async def callback(notification):
      callback_called.append(notification)

    query.add_callback(callback)
    notification = {'action': 'UPDATE', 'result': {'id': 'person:1'}}
    await query.notify(notification)

    assert len(callback_called) == 1
    assert callback_called[0] == notification

  @pytest.mark.anyio
  async def test_notify_multiple_callbacks(self):
    """Test notifying multiple callbacks."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person')

    callback1_called = []
    callback2_called = []

    def callback1(notification):
      callback1_called.append(notification)

    def callback2(notification):
      callback2_called.append(notification)

    query.add_callback(callback1)
    query.add_callback(callback2)
    notification = {'action': 'DELETE', 'result': {'id': 'person:1'}}
    await query.notify(notification)

    assert len(callback1_called) == 1
    assert len(callback2_called) == 1

  @pytest.mark.anyio
  async def test_notify_callback_error_handling(self):
    """Test that callback errors don't break notification."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person')

    def failing_callback(_notification):
      raise ValueError('Test error')

    callback_called = []

    def working_callback(notification):
      callback_called.append(notification)

    query.add_callback(failing_callback)
    query.add_callback(working_callback)
    notification = {'action': 'CREATE', 'result': {'id': 'person:1'}}

    # Should not raise error
    await query.notify(notification)

    # Working callback should still be called
    assert len(callback_called) == 1

  def test_deactivate(self):
    """Test deactivating a query."""
    query_uuid = uuid4()
    query = LiveQuery(query_uuid=query_uuid, table='person')

    assert query.is_active is True
    query.deactivate()
    assert query.is_active is False


class TestStreamingManager:
  """Tests for StreamingManager."""

  @pytest.fixture
  def mock_client(self):
    """Create mock database client."""
    client = MagicMock()
    client.live = AsyncMock()
    client.subscribe_live = AsyncMock()
    client.kill = AsyncMock()
    return client

  @pytest.fixture
  def streaming_manager(self, mock_client):
    """Create streaming manager instance."""
    return StreamingManager(mock_client)

  @pytest.mark.anyio
  async def test_live_query_creation(self, streaming_manager, mock_client):
    """Test creating a live query."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid

    query = await streaming_manager.live('person')

    assert isinstance(query, LiveQuery)
    assert query.query_uuid == query_uuid
    assert query.table == 'person'
    assert query.diff is False
    mock_client.live.assert_called_once_with('person', diff=False)

  @pytest.mark.anyio
  async def test_live_query_with_diff(self, streaming_manager, mock_client):
    """Test creating a live query with diff mode."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid

    query = await streaming_manager.live('person', diff=True)

    assert query.diff is True
    mock_client.live.assert_called_once_with('person', diff=True)

  @pytest.mark.anyio
  async def test_live_query_failure(self, streaming_manager, mock_client):
    """Test live query creation failure."""
    mock_client.live.side_effect = Exception('Connection error')

    with pytest.raises(StreamingError, match='Failed to start live query'):
      await streaming_manager.live('person')

  @pytest.mark.anyio
  async def test_subscribe_iterator(self, streaming_manager, mock_client):
    """Test subscribing to live query with iterator."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid

    notifications = [
      {'action': 'CREATE', 'result': {'id': 'person:1'}},
      {'action': 'UPDATE', 'result': {'id': 'person:1'}},
      {'action': 'CLOSE'},
    ]

    async def mock_subscribe(_uuid):
      for notif in notifications:
        yield notif

    mock_client.subscribe_live = mock_subscribe

    query = await streaming_manager.live('person')
    received = []

    async for notification in streaming_manager.subscribe(query):
      received.append(notification)

    assert len(received) == 2  # CLOSE should not be yielded
    assert received[0]['action'] == 'CREATE'
    assert received[1]['action'] == 'UPDATE'
    assert query.is_active is False

  @pytest.mark.anyio
  async def test_subscribe_close_action(self, streaming_manager, mock_client):
    """Test that CLOSE action deactivates query."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid

    async def mock_subscribe(_uuid):
      yield {'action': 'CLOSE'}

    mock_client.subscribe_live = mock_subscribe

    query = await streaming_manager.live('person')

    async for _notification in streaming_manager.subscribe(query):
      pass

    assert query.is_active is False

  @pytest.mark.anyio
  async def test_subscribe_error_handling(self, streaming_manager, mock_client):
    """Test subscription error handling."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid

    async def mock_subscribe(_uuid):
      yield {'action': 'CREATE', 'result': {'id': 'person:1'}}
      raise Exception('Connection lost')

    mock_client.subscribe_live = mock_subscribe

    query = await streaming_manager.live('person')

    with pytest.raises(StreamingError, match='Subscription failed'):
      async for _notification in streaming_manager.subscribe(query):
        pass

    assert query.is_active is False

  @pytest.mark.anyio
  async def test_subscribe_with_callback(self, streaming_manager, mock_client):
    """Test subscribing with callback function."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid

    notifications = [
      {'action': 'CREATE', 'result': {'id': 'person:1'}},
      {'action': 'CLOSE'},
    ]

    async def mock_subscribe(_uuid):
      for notif in notifications:
        yield notif

    mock_client.subscribe_live = mock_subscribe

    query = await streaming_manager.live('person')
    callback_received = []

    def callback(notification):
      callback_received.append(notification)

    # Subscribe with callback (consumes all notifications inline)
    await streaming_manager.subscribe_with_callback(query, callback)

    # Callback should have been called for CREATE (not CLOSE)
    assert len(callback_received) == 1
    assert callback_received[0]['action'] == 'CREATE'

  @pytest.mark.anyio
  async def test_kill_query(self, streaming_manager, mock_client):
    """Test killing a live query."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid

    query = await streaming_manager.live('person')
    await streaming_manager.kill(query)

    assert query.is_active is False
    mock_client.kill.assert_called_once_with(query_uuid)

  @pytest.mark.anyio
  async def test_kill_query_cleans_up_state(self, streaming_manager, mock_client):
    """Test killing a query cleans up internal state."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid

    query = await streaming_manager.live('person')
    assert query.query_uuid in streaming_manager._queries

    await streaming_manager.kill(query)

    assert query.is_active is False
    assert query.query_uuid not in streaming_manager._queries

  @pytest.mark.anyio
  async def test_kill_query_failure(self, streaming_manager, mock_client):
    """Test kill query failure."""
    query_uuid = uuid4()
    mock_client.live.return_value = query_uuid
    mock_client.kill.side_effect = Exception('Kill failed')

    query = await streaming_manager.live('person')

    with pytest.raises(StreamingError, match='Failed to kill query'):
      await streaming_manager.kill(query)

  @pytest.mark.anyio
  async def test_kill_all(self, streaming_manager, mock_client):
    """Test killing all active queries."""
    query_uuid1 = uuid4()
    query_uuid2 = uuid4()
    mock_client.live.side_effect = [query_uuid1, query_uuid2]

    query1 = await streaming_manager.live('person')
    query2 = await streaming_manager.live('company')

    await streaming_manager.kill_all()

    assert query1.is_active is False
    assert query2.is_active is False
    assert len(streaming_manager.get_active_queries()) == 0

  @pytest.mark.anyio
  async def test_kill_all_with_inactive_queries(self, streaming_manager, mock_client):
    """Test kill_all only kills active queries."""
    query_uuid1 = uuid4()
    query_uuid2 = uuid4()
    mock_client.live.side_effect = [query_uuid1, query_uuid2]

    query1 = await streaming_manager.live('person')
    await streaming_manager.live('company')

    # Deactivate query1
    query1.deactivate()

    await streaming_manager.kill_all()

    # kill should be called once for query2 only
    assert mock_client.kill.call_count == 1

  @pytest.mark.anyio
  async def test_get_active_queries(self, streaming_manager, mock_client):
    """Test getting active queries."""
    query_uuid1 = uuid4()
    query_uuid2 = uuid4()
    query_uuid3 = uuid4()
    mock_client.live.side_effect = [query_uuid1, query_uuid2, query_uuid3]

    query1 = await streaming_manager.live('person')
    query2 = await streaming_manager.live('company')
    query3 = await streaming_manager.live('product')

    # Deactivate one query
    query2.deactivate()

    active = streaming_manager.get_active_queries()

    assert len(active) == 2
    assert query1 in active
    assert query2 not in active
    assert query3 in active

  @pytest.mark.anyio
  async def test_multiple_queries_on_same_table(self, streaming_manager, mock_client):
    """Test creating multiple queries on the same table."""
    query_uuid1 = uuid4()
    query_uuid2 = uuid4()
    mock_client.live.side_effect = [query_uuid1, query_uuid2]

    query1 = await streaming_manager.live('person')
    query2 = await streaming_manager.live('person', diff=True)

    assert query1.query_uuid != query2.query_uuid
    assert query1.diff is False
    assert query2.diff is True
    assert len(streaming_manager.get_active_queries()) == 2
