"""Tests for connection registry module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from surql.connection.config import ConnectionConfig
from surql.connection.registry import ConnectionRegistry, RegistryError, get_registry


@pytest.fixture
async def clean_registry():
  """Clean registry before and after each test.

  Note: We directly reset internal state rather than calling clear()
  to avoid issues with mock clients that may not have proper async methods.
  """
  registry = get_registry()
  # Directly reset registry state without calling disconnect
  registry._connections.clear()
  registry._configs.clear()
  registry._default_name = None
  yield registry
  # Same for cleanup
  registry._connections.clear()
  registry._configs.clear()
  registry._default_name = None


@pytest.fixture
def mock_config():
  """Create mock connection configuration."""
  return ConnectionConfig(
    _env_file=None,
    db_url='ws://localhost:8000/rpc',
    db_user='test',
    db_pass='test',
    db_ns='test',
    db='test',
  )


@pytest.fixture
def mock_client():
  """Create mock database client."""
  client = MagicMock()
  client.connect = AsyncMock()
  client.disconnect = AsyncMock()
  client.is_connected = True
  return client


class TestConnectionRegistry:
  """Tests for ConnectionRegistry."""

  def test_singleton_pattern(self):
    """Test registry uses singleton pattern."""
    registry1 = ConnectionRegistry()
    registry2 = ConnectionRegistry()
    assert registry1 is registry2

  def test_get_registry(self):
    """Test get_registry function."""
    registry = get_registry()
    assert isinstance(registry, ConnectionRegistry)
    assert registry is get_registry()

  @pytest.mark.anyio
  async def test_register_connection(self, clean_registry, mock_config):
    """Test connection registration."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      client = await clean_registry.register('test', mock_config, connect=True)

      assert client is not None
      assert 'test' in clean_registry.list_connections()
      assert clean_registry.default_name == 'test'
      mock_client.connect.assert_called_once()

  @pytest.mark.anyio
  async def test_register_without_connect(self, clean_registry, mock_config):
    """Test registration without immediate connection."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = False
      MockClient.return_value = mock_client

      client = await clean_registry.register('test', mock_config, connect=False)

      assert client is not None
      mock_client.connect.assert_not_called()

  @pytest.mark.anyio
  async def test_register_duplicate_name(self, clean_registry, mock_config):
    """Test registration with duplicate name raises error."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      await clean_registry.register('test', mock_config)

      with pytest.raises(RegistryError, match='already registered'):
        await clean_registry.register('test', mock_config)

  @pytest.mark.anyio
  async def test_register_multiple_connections(self, clean_registry):
    """Test registering multiple connections."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      config1 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test1', db='test1'
      )
      config2 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8001/rpc', db_ns='test2', db='test2'
      )

      await clean_registry.register('conn1', config1)
      await clean_registry.register('conn2', config2)

      assert len(clean_registry.list_connections()) == 2
      assert 'conn1' in clean_registry.list_connections()
      assert 'conn2' in clean_registry.list_connections()

  @pytest.mark.anyio
  async def test_unregister_connection(self, clean_registry, mock_config):
    """Test connection unregistration."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.disconnect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      await clean_registry.register('test', mock_config)
      await clean_registry.unregister('test')

      assert 'test' not in clean_registry.list_connections()
      mock_client.disconnect.assert_called_once()

  @pytest.mark.anyio
  async def test_unregister_nonexistent(self, clean_registry):
    """Test unregistering non-existent connection raises error."""
    with pytest.raises(RegistryError, match='not found'):
      await clean_registry.unregister('nonexistent')

  @pytest.mark.anyio
  async def test_unregister_without_disconnect(self, clean_registry, mock_config):
    """Test unregistration without disconnecting."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.disconnect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      await clean_registry.register('test', mock_config)
      await clean_registry.unregister('test', disconnect=False)

      mock_client.disconnect.assert_not_called()

  @pytest.mark.anyio
  async def test_get_connection(self, clean_registry):
    """Test getting a registered connection."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      config = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test', db='test'
      )
      await clean_registry.register('test', config)
      client = clean_registry.get('test')
      assert client is mock_client

  @pytest.mark.anyio
  async def test_get_default_connection(self, clean_registry):
    """Test getting default connection."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      config = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test', db='test'
      )
      await clean_registry.register('test', config)
      client = clean_registry.get()
      assert client is mock_client

  @pytest.mark.anyio
  async def test_get_nonexistent_connection(self, clean_registry):
    """Test getting non-existent connection raises error."""
    with pytest.raises(RegistryError, match='not found'):
      clean_registry.get('nonexistent')

  @pytest.mark.anyio
  async def test_get_no_default(self, clean_registry):
    """Test getting connection when no default set raises error."""
    with pytest.raises(RegistryError, match='No default connection'):
      clean_registry.get()

  @pytest.mark.anyio
  async def test_get_config(self, clean_registry):
    """Test getting connection configuration."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      config = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test', db='test'
      )
      await clean_registry.register('test', config)
      retrieved_config = clean_registry.get_config('test')
      assert retrieved_config.db_ns == 'test'

  @pytest.mark.anyio
  async def test_set_default(self, clean_registry):
    """Test setting default connection."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      config1 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test1', db='test1'
      )
      config2 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8001/rpc', db_ns='test2', db='test2'
      )

      await clean_registry.register('conn1', config1)
      await clean_registry.register('conn2', config2)

      clean_registry.set_default('conn2')
      assert clean_registry.default_name == 'conn2'

  @pytest.mark.anyio
  async def test_set_default_nonexistent(self, clean_registry):
    """Test setting non-existent connection as default raises error."""
    with pytest.raises(RegistryError, match='not found'):
      clean_registry.set_default('nonexistent')

  @pytest.mark.anyio
  async def test_list_connections(self, clean_registry):
    """Test listing all connections."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      config1 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test1', db='test1'
      )
      config2 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8001/rpc', db_ns='test2', db='test2'
      )

      await clean_registry.register('conn1', config1)
      await clean_registry.register('conn2', config2)

      connections = clean_registry.list_connections()
      assert len(connections) == 2
      assert 'conn1' in connections
      assert 'conn2' in connections

  @pytest.mark.anyio
  async def test_disconnect_all(self, clean_registry):
    """Test disconnecting all connections."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client1 = MagicMock()
      mock_client1.connect = AsyncMock()
      mock_client1.disconnect = AsyncMock()
      mock_client1.is_connected = True

      mock_client2 = MagicMock()
      mock_client2.connect = AsyncMock()
      mock_client2.disconnect = AsyncMock()
      mock_client2.is_connected = True

      MockClient.side_effect = [mock_client1, mock_client2]

      config1 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test1', db='test1'
      )
      config2 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8001/rpc', db_ns='test2', db='test2'
      )

      await clean_registry.register('conn1', config1)
      await clean_registry.register('conn2', config2)
      await clean_registry.disconnect_all()

      mock_client1.disconnect.assert_called_once()
      mock_client2.disconnect.assert_called_once()

  @pytest.mark.anyio
  async def test_clear(self, clean_registry):
    """Test clearing all connections."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.disconnect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      config = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test', db='test'
      )
      await clean_registry.register('test', config)
      await clean_registry.clear()

      assert len(clean_registry.list_connections()) == 0
      assert clean_registry.default_name is None
      mock_client.disconnect.assert_called_once()

  @pytest.mark.anyio
  async def test_register_set_default_flag(self, clean_registry):
    """Test registering with set_default flag."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client = MagicMock()
      mock_client.connect = AsyncMock()
      mock_client.is_connected = True
      MockClient.return_value = mock_client

      config1 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test1', db='test1'
      )
      config2 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8001/rpc', db_ns='test2', db='test2'
      )

      await clean_registry.register('conn1', config1)
      await clean_registry.register('conn2', config2, set_default=True)

      assert clean_registry.default_name == 'conn2'

  @pytest.mark.anyio
  async def test_unregister_updates_default(self, clean_registry):
    """Test unregistering default connection updates to next available."""
    with patch('surql.connection.registry.DatabaseClient') as MockClient:
      mock_client1 = MagicMock()
      mock_client1.connect = AsyncMock()
      mock_client1.disconnect = AsyncMock()
      mock_client1.is_connected = True

      mock_client2 = MagicMock()
      mock_client2.connect = AsyncMock()
      mock_client2.disconnect = AsyncMock()
      mock_client2.is_connected = True

      MockClient.side_effect = [mock_client1, mock_client2]

      config1 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8000/rpc', db_ns='test1', db='test1'
      )
      config2 = ConnectionConfig(
        _env_file=None, db_url='ws://localhost:8001/rpc', db_ns='test2', db='test2'
      )

      await clean_registry.register('conn1', config1)
      await clean_registry.register('conn2', config2)

      assert clean_registry.default_name == 'conn1'

      await clean_registry.unregister('conn1')

      # Should update to remaining connection
      assert clean_registry.default_name == 'conn2'
