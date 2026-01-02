"""Tests for the connection module (config, client, and transaction)."""

import os
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import ValidationError

from src.connection.client import (
  ConnectionError,
  DatabaseClient,
  DatabaseError,
  QueryError,
  get_client,
)
from src.connection.config import ConnectionConfig
from src.connection.transaction import (
  Transaction,
  TransactionError,
  TransactionState,
  transaction,
)


class TestConnectionConfig:
  """Test suite for ConnectionConfig class."""

  def test_connection_config_with_defaults(self) -> None:
    """Test connection config with default values."""
    # Isolate test from .env file and environment variables
    # Clear all DB_ prefixed environment variables to test true defaults
    env_vars = {k: v for k, v in os.environ.items() if not k.startswith('DB_')}

    # Patch os.environ and prevent .env file loading
    with patch.dict('os.environ', env_vars, clear=True):
      # pydantic-settings allows overriding _env_file to disable .env loading
      config = ConnectionConfig(_env_file=None)

      assert config.url == 'ws://localhost:8000/rpc'
      assert config.namespace == 'development'
      assert config.database == 'main'
      assert config.username is None
      assert config.password is None
      assert config.timeout == 30.0
      assert config.max_connections == 10
      assert config.retry_max_attempts == 3
      assert config.retry_min_wait == 1.0
      assert config.retry_max_wait == 10.0
      assert config.retry_multiplier == 2.0

  def test_connection_config_with_custom_values(self) -> None:
    """Test connection config with custom values."""
    config = ConnectionConfig(
      url='wss://db.example.com/rpc',
      namespace='production',
      database='app_db',
      username='admin',
      password='secret',
      timeout=60.0,
      max_connections=20,
    )

    assert config.url == 'wss://db.example.com/rpc'
    assert config.namespace == 'production'
    assert config.database == 'app_db'
    assert config.username == 'admin'
    assert config.password == 'secret'
    assert config.timeout == 60.0
    assert config.max_connections == 20

  def test_connection_config_from_env(self) -> None:
    """Test connection config loading from environment variables."""
    env_vars = {
      'DB_URL': 'ws://test-db:8000/rpc',
      'DB_NAMESPACE': 'test_ns',
      'DB_DATABASE': 'test_db',
      'DB_USERNAME': 'test_user',
      'DB_PASSWORD': 'test_pass',
    }

    with patch.dict('os.environ', env_vars, clear=False):
      config = ConnectionConfig()

      assert config.url == 'ws://test-db:8000/rpc'
      assert config.namespace == 'test_ns'
      assert config.database == 'test_db'
      assert config.username == 'test_user'
      assert config.password == 'test_pass'

  def test_validate_url_valid_ws(self) -> None:
    """Test URL validation with valid ws:// protocol."""
    config = ConnectionConfig(url='ws://localhost:8000/rpc')
    assert config.url == 'ws://localhost:8000/rpc'

  def test_validate_url_valid_wss(self) -> None:
    """Test URL validation with valid wss:// protocol."""
    config = ConnectionConfig(url='wss://example.com/rpc')
    assert config.url == 'wss://example.com/rpc'

  def test_validate_url_valid_http(self) -> None:
    """Test URL validation with valid http:// protocol."""
    config = ConnectionConfig(url='http://localhost:8000/rpc')
    assert config.url == 'http://localhost:8000/rpc'

  def test_validate_url_valid_https(self) -> None:
    """Test URL validation with valid https:// protocol."""
    config = ConnectionConfig(url='https://example.com/rpc')
    assert config.url == 'https://example.com/rpc'

  def test_validate_url_invalid_protocol(self) -> None:
    """Test URL validation with invalid protocol."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(url='tcp://localhost:8000')

    assert 'URL must use' in str(exc_info.value)

  def test_validate_url_empty(self) -> None:
    """Test URL validation with empty string."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(url='')

    assert 'URL cannot be empty' in str(exc_info.value)

  def test_validate_namespace_valid(self) -> None:
    """Test namespace validation with valid name."""
    config = ConnectionConfig(namespace='my_namespace')
    assert config.namespace == 'my_namespace'

  def test_validate_namespace_empty(self) -> None:
    """Test namespace validation with empty string."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(namespace='')

    assert 'Identifier cannot be empty' in str(exc_info.value)

  def test_validate_namespace_invalid_chars(self) -> None:
    """Test namespace validation with invalid characters."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(namespace='my namespace')

    assert 'alphanumeric' in str(exc_info.value)

  def test_validate_database_valid(self) -> None:
    """Test database validation with valid name."""
    config = ConnectionConfig(database='my-database')
    assert config.database == 'my-database'

  def test_validate_database_empty(self) -> None:
    """Test database validation with empty string."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(database='')

    assert 'Identifier cannot be empty' in str(exc_info.value)

  def test_timeout_minimum_value(self) -> None:
    """Test timeout has minimum value constraint."""
    with pytest.raises(ValidationError):
      ConnectionConfig(timeout=0.5)

  def test_max_connections_minimum(self) -> None:
    """Test max_connections has minimum value."""
    with pytest.raises(ValidationError):
      ConnectionConfig(max_connections=0)

  def test_max_connections_maximum(self) -> None:
    """Test max_connections has maximum value."""
    with pytest.raises(ValidationError):
      ConnectionConfig(max_connections=101)

  def test_retry_max_wait_validation(self) -> None:
    """Test that retry_max_wait must be greater than retry_min_wait."""
    with pytest.raises(ValidationError):
      ConnectionConfig(retry_min_wait=5.0, retry_max_wait=3.0)


class TestDatabaseClient:
  """Test suite for DatabaseClient class."""

  def test_database_client_initialization(self, db_config: ConnectionConfig) -> None:
    """Test DatabaseClient initialization."""
    client = DatabaseClient(db_config)

    assert client._config == db_config
    assert client._client is None
    assert client._connected is False
    assert client.is_connected is False

  @pytest.mark.anyio
  async def test_connect_success(
    self, db_config: ConnectionConfig, mock_surreal_client: Mock
  ) -> None:
    """Test successful database connection."""
    client = DatabaseClient(db_config)

    with patch('src.connection.client.AsyncSurreal', return_value=mock_surreal_client):
      await client.connect()

      assert client.is_connected is True
      mock_surreal_client.connect.assert_called_once()
      mock_surreal_client.signin.assert_called_once()
      mock_surreal_client.use.assert_called_once_with('test', 'test_db')

  @pytest.mark.anyio
  async def test_connect_without_auth(self) -> None:
    """Test connection without username/password."""
    config = ConnectionConfig(username=None, password=None)
    client = DatabaseClient(config)
    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.use = AsyncMock()

    with patch('src.connection.client.AsyncSurreal', return_value=mock_surreal):
      await client.connect()

      assert client.is_connected is True
      mock_surreal.signin.assert_not_called()

  @pytest.mark.anyio
  async def test_connect_already_connected(self, mock_db_client: DatabaseClient) -> None:
    """Test connecting when already connected."""
    # Client is already connected from fixture
    await mock_db_client.connect()

    # Should still be connected (warning logged but no error)
    assert mock_db_client.is_connected is True

  @pytest.mark.anyio
  async def test_connect_failure(self, db_config: ConnectionConfig) -> None:
    """Test connection failure after retries."""
    client = DatabaseClient(db_config)
    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock(side_effect=Exception('Connection failed'))

    with patch('src.connection.client.AsyncSurreal', return_value=mock_surreal):
      with pytest.raises(ConnectionError) as exc_info:
        await client.connect()

      assert 'Connection failed' in str(exc_info.value)
      assert client.is_connected is False

  @pytest.mark.anyio
  async def test_disconnect_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful disconnection."""
    # Save reference to mock client before disconnect sets it to None
    mock_client = mock_db_client._client
    await mock_db_client.disconnect()

    mock_client.close.assert_called_once()
    assert mock_db_client.is_connected is False

  @pytest.mark.anyio
  async def test_disconnect_not_connected(self, db_config: ConnectionConfig) -> None:
    """Test disconnecting when not connected."""
    client = DatabaseClient(db_config)

    # Should not raise error
    await client.disconnect()
    assert client.is_connected is False

  @pytest.mark.anyio
  async def test_disconnect_failure(self, mock_db_client: DatabaseClient) -> None:
    """Test disconnection failure."""
    mock_db_client._client.close = AsyncMock(side_effect=Exception('Close failed'))

    with pytest.raises(DatabaseError):
      await mock_db_client.disconnect()

    # Client should still be marked as disconnected
    assert mock_db_client.is_connected is False

  @pytest.mark.anyio
  async def test_execute_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful query execution."""
    query = 'SELECT * FROM user'
    params = {'limit': 10}

    result = await mock_db_client.execute(query, params)

    mock_db_client._client.query.assert_called_once_with(query, params)
    assert result is not None

  @pytest.mark.anyio
  async def test_execute_not_connected(self, db_config: ConnectionConfig) -> None:
    """Test execute when not connected."""
    client = DatabaseClient(db_config)

    with pytest.raises(ConnectionError) as exc_info:
      await client.execute('SELECT * FROM user')

    assert 'not connected' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_execute_query_error(self, mock_db_client: DatabaseClient) -> None:
    """Test execute with query error."""
    mock_db_client._client.query = AsyncMock(side_effect=Exception('Query failed'))

    with pytest.raises(QueryError) as exc_info:
      await mock_db_client.execute('INVALID QUERY')

    assert 'Query execution failed' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_select_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful SELECT operation."""
    await mock_db_client.select('user')

    mock_db_client._client.select.assert_called_once_with('user')

  @pytest.mark.anyio
  async def test_select_not_connected(self, db_config: ConnectionConfig) -> None:
    """Test SELECT when not connected."""
    client = DatabaseClient(db_config)

    with pytest.raises(ConnectionError):
      await client.select('user')

  @pytest.mark.anyio
  async def test_create_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful CREATE operation."""
    data = {'name': 'Alice', 'email': 'alice@example.com'}
    result = await mock_db_client.create('user', data)

    mock_db_client._client.create.assert_called_once_with('user', data)
    assert result['id'] == 'user:123'

  @pytest.mark.anyio
  async def test_update_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful UPDATE operation."""
    data = {'status': 'active'}
    await mock_db_client.update('user:alice', data)

    mock_db_client._client.update.assert_called_once_with('user:alice', data)

  @pytest.mark.anyio
  async def test_merge_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful MERGE operation."""
    data = {'status': 'active'}
    await mock_db_client.merge('user:alice', data)

    mock_db_client._client.merge.assert_called_once_with('user:alice', data)

  @pytest.mark.anyio
  async def test_delete_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful DELETE operation."""
    await mock_db_client.delete('user:alice')

    mock_db_client._client.delete.assert_called_once_with('user:alice')

  @pytest.mark.anyio
  async def test_insert_relation_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful INSERT RELATION operation."""
    data = {'in': 'user:alice', 'out': 'post:123'}
    await mock_db_client.insert_relation('likes', data)

    mock_db_client._client.insert_relation.assert_called_once_with('likes', data)

  @pytest.mark.anyio
  async def test_context_manager(self, db_config: ConnectionConfig) -> None:
    """Test DatabaseClient as async context manager."""
    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.signin = AsyncMock()
    mock_surreal.use = AsyncMock()
    mock_surreal.close = AsyncMock()

    with patch('src.connection.client.AsyncSurreal', return_value=mock_surreal):
      async with DatabaseClient(db_config) as client:
        assert client.is_connected is True

      mock_surreal.close.assert_called_once()

  @pytest.mark.anyio
  async def test_get_client_context_manager(self, db_config: ConnectionConfig) -> None:
    """Test get_client context manager."""
    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.signin = AsyncMock()
    mock_surreal.use = AsyncMock()
    mock_surreal.close = AsyncMock()

    with patch('src.connection.client.AsyncSurreal', return_value=mock_surreal):
      async with get_client(db_config) as client:
        assert client.is_connected is True

      # Should disconnect after exiting context
      assert client._connected is False


class TestTransaction:
  """Test suite for Transaction class."""

  def test_transaction_initialization(self, mock_db_client: DatabaseClient) -> None:
    """Test Transaction initialization."""
    txn = Transaction(mock_db_client)

    assert txn._client == mock_db_client
    assert txn.state == TransactionState.PENDING
    assert txn.is_active is False

  @pytest.mark.anyio
  async def test_begin_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful transaction begin."""
    txn = Transaction(mock_db_client)

    await txn.begin()

    assert txn.state == TransactionState.ACTIVE
    assert txn.is_active is True
    mock_db_client._client.query.assert_called()

  @pytest.mark.anyio
  async def test_begin_already_active(self, mock_db_client: DatabaseClient) -> None:
    """Test beginning transaction that is already active."""
    txn = Transaction(mock_db_client)
    await txn.begin()

    with pytest.raises(TransactionError) as exc_info:
      await txn.begin()

    assert 'Cannot begin transaction' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_commit_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful transaction commit."""
    txn = Transaction(mock_db_client)
    await txn.begin()

    await txn.commit()

    assert txn.state == TransactionState.COMMITTED
    assert txn.is_active is False

  @pytest.mark.anyio
  async def test_commit_not_active(self, mock_db_client: DatabaseClient) -> None:
    """Test committing transaction that is not active."""
    txn = Transaction(mock_db_client)

    with pytest.raises(TransactionError) as exc_info:
      await txn.commit()

    assert 'Cannot commit transaction' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_commit_failure(self, mock_db_client: DatabaseClient) -> None:
    """Test transaction commit failure."""
    txn = Transaction(mock_db_client)
    await txn.begin()

    mock_db_client._client.query = AsyncMock(side_effect=Exception('Commit failed'))

    with pytest.raises(TransactionError):
      await txn.commit()

    assert txn.state == TransactionState.CANCELLED

  @pytest.mark.anyio
  async def test_cancel_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful transaction cancel."""
    txn = Transaction(mock_db_client)
    await txn.begin()

    await txn.cancel()

    assert txn.state == TransactionState.CANCELLED
    assert txn.is_active is False

  @pytest.mark.anyio
  async def test_cancel_pending(self, mock_db_client: DatabaseClient) -> None:
    """Test canceling pending transaction."""
    txn = Transaction(mock_db_client)

    await txn.cancel()

    assert txn.state == TransactionState.CANCELLED

  @pytest.mark.anyio
  async def test_cancel_already_committed(self, mock_db_client: DatabaseClient) -> None:
    """Test canceling already committed transaction."""
    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.commit()

    # Should not raise error, just log warning
    await txn.cancel()
    assert txn.state == TransactionState.COMMITTED

  @pytest.mark.anyio
  async def test_execute_in_transaction(self, mock_db_client: DatabaseClient) -> None:
    """Test executing query in transaction context."""
    txn = Transaction(mock_db_client)
    await txn.begin()

    result = await txn.execute('CREATE user:alice SET name = "Alice"')

    assert result is not None

  @pytest.mark.anyio
  async def test_execute_not_active(self, mock_db_client: DatabaseClient) -> None:
    """Test executing query when transaction is not active."""
    txn = Transaction(mock_db_client)

    with pytest.raises(TransactionError) as exc_info:
      await txn.execute('SELECT * FROM user')

    assert 'Cannot execute query' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_transaction_context_manager_success(self, mock_db_client: DatabaseClient) -> None:
    """Test Transaction as async context manager with success."""
    async with Transaction(mock_db_client) as txn:
      assert txn.is_active is True
      await txn.execute('CREATE user:alice')

    # Should auto-commit on successful exit
    assert txn.state == TransactionState.COMMITTED

  @pytest.mark.anyio
  async def test_transaction_context_manager_exception(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Test Transaction context manager with exception."""
    txn = Transaction(mock_db_client)

    try:
      async with txn:
        assert txn.is_active is True
        raise ValueError('Test error')
    except ValueError:
      pass

    # Should auto-cancel on exception
    assert txn.state == TransactionState.CANCELLED

  @pytest.mark.anyio
  async def test_transaction_helper_function(self, mock_db_client: DatabaseClient) -> None:
    """Test transaction helper context manager function."""
    async with transaction(mock_db_client) as txn:
      assert txn.is_active is True


class TestTransactionState:
  """Test suite for TransactionState enum."""

  def test_transaction_state_values(self) -> None:
    """Test TransactionState enum values."""
    assert TransactionState.PENDING.value == 'pending'
    assert TransactionState.ACTIVE.value == 'active'
    assert TransactionState.COMMITTED.value == 'committed'
    assert TransactionState.CANCELLED.value == 'cancelled'


class TestConnectionErrors:
  """Test suite for connection error classes."""

  def test_database_error(self) -> None:
    """Test DatabaseError exception."""
    error = DatabaseError('Test error')
    assert str(error) == 'Test error'
    assert isinstance(error, Exception)

  def test_connection_error(self) -> None:
    """Test ConnectionError exception."""
    error = ConnectionError('Connection failed')
    assert str(error) == 'Connection failed'
    assert isinstance(error, DatabaseError)

  def test_query_error(self) -> None:
    """Test QueryError exception."""
    error = QueryError('Query failed')
    assert str(error) == 'Query failed'
    assert isinstance(error, DatabaseError)

  def test_transaction_error(self) -> None:
    """Test TransactionError exception."""
    error = TransactionError('Transaction failed')
    assert str(error) == 'Transaction failed'
    assert isinstance(error, Exception)
