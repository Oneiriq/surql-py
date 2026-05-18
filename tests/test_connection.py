"""Tests for the connection module (config, client, and transaction)."""

import os
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import ValidationError

from surql.connection.client import (
  ConnectionError,
  DatabaseClient,
  DatabaseError,
  QueryError,
  get_client,
)
from surql.connection.config import ConnectionConfig
from surql.connection.transaction import (
  Transaction,
  TransactionError,
  TransactionState,
  transaction,
)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch):
  """Clear all SURQL_ environment variables for test isolation."""
  for key in list(os.environ.keys()):
    if key.startswith('SURQL_'):
      monkeypatch.delenv(key, raising=False)
  yield


class TestConnectionConfig:
  """Test suite for ConnectionConfig class."""

  def test_connection_config_with_defaults(self, clean_env) -> None:  # noqa: ARG002
    """Test connection config with default values."""
    # Create config with _env_file=None to disable .env loading
    # and explicitly pass only the required defaults to test
    config = ConnectionConfig(
      _env_file=None,
      db_url='ws://localhost:8000/rpc',
      db_ns='development',
      db='main',
    )

    assert config.url == 'ws://localhost:8000/rpc'
    assert config.namespace == 'development'
    assert config.database == 'main'
    # Note: username/password may come from env, just check url/ns/db defaults
    assert config.timeout == 30.0
    assert config.max_connections == 10
    assert config.retry_max_attempts == 3
    assert config.retry_min_wait == 1.0
    assert config.retry_max_wait == 10.0
    assert config.retry_multiplier == 2.0
    assert config.enable_live_queries is True

  def test_connection_config_with_custom_values(self, clean_env) -> None:  # noqa: ARG002
    """Test connection config with custom values."""
    config = ConnectionConfig(
      _env_file=None,
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

  def test_connection_config_from_env(self, clean_env, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test connection config loading from environment variables."""
    # Set the test environment variables (clean_env already cleared SURQL_* vars)
    # Also clear Windows USERNAME env var that might interfere via the 'username' alias
    _ = clean_env  # Used for side effect
    monkeypatch.delenv('USERNAME', raising=False)
    monkeypatch.delenv('USER', raising=False)

    monkeypatch.setenv('SURQL_DB_URL', 'ws://test-db:8000/rpc')
    monkeypatch.setenv('SURQL_DB_NS', 'test_ns')
    monkeypatch.setenv('SURQL_DB', 'test_db')
    monkeypatch.setenv('SURQL_DB_USER', 'test_user')
    monkeypatch.setenv('SURQL_DB_PASS', 'test_pass')

    config = ConnectionConfig(_env_file=None)

    assert config.url == 'ws://test-db:8000/rpc'
    assert config.namespace == 'test_ns'
    assert config.database == 'test_db'
    assert config.username == 'test_user'
    assert config.password == 'test_pass'

  def test_validate_url_valid_ws(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with valid ws:// protocol."""
    config = ConnectionConfig(_env_file=None, url='ws://localhost:8000/rpc')
    assert config.url == 'ws://localhost:8000/rpc'

  def test_validate_url_valid_wss(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with valid wss:// protocol."""
    config = ConnectionConfig(_env_file=None, url='wss://example.com/rpc')
    assert config.url == 'wss://example.com/rpc'

  def test_validate_url_valid_http(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with valid http:// protocol."""
    # HTTP URLs require live queries disabled since live queries need WebSocket
    config = ConnectionConfig(
      _env_file=None, url='http://localhost:8000/rpc', enable_live_queries=False
    )
    assert config.url == 'http://localhost:8000/rpc'

  def test_validate_url_valid_https(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with valid https:// protocol."""
    # HTTPS URLs require live queries disabled since live queries need WebSocket
    config = ConnectionConfig(
      _env_file=None, url='https://example.com/rpc', enable_live_queries=False
    )
    assert config.url == 'https://example.com/rpc'

  def test_validate_url_invalid_protocol(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with invalid protocol."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(_env_file=None, url='tcp://localhost:8000')

    assert 'URL must use' in str(exc_info.value)

  def test_validate_url_valid_memory(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with valid memory:// embedded protocol."""
    config = ConnectionConfig(_env_file=None, url='memory://')
    assert config.url == 'memory://'

  def test_validate_url_valid_mem(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with valid mem:// (short form) embedded protocol."""
    config = ConnectionConfig(_env_file=None, url='mem://')
    assert config.url == 'mem://'

  def test_validate_url_valid_file(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with valid file:// embedded protocol."""
    config = ConnectionConfig(_env_file=None, url='file:///var/lib/app.db')
    assert config.url == 'file:///var/lib/app.db'

  def test_validate_url_valid_surrealkv(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with valid surrealkv:// embedded protocol."""
    config = ConnectionConfig(_env_file=None, url='surrealkv:///var/lib/app.db')
    assert config.url == 'surrealkv:///var/lib/app.db'

  def test_validate_live_queries_allowed_with_embedded(self, clean_env) -> None:  # noqa: ARG002
    """Live queries must be allowed with embedded engines (they run in-process)."""
    config = ConnectionConfig(
      _env_file=None,
      url='surrealkv:///tmp/app.db',
      enable_live_queries=True,
    )
    assert config.enable_live_queries is True

  def test_validate_live_queries_allowed_with_memory(self, clean_env) -> None:  # noqa: ARG002
    """Live queries must be allowed with the in-memory embedded engine."""
    config = ConnectionConfig(
      _env_file=None,
      url='memory://',
      enable_live_queries=True,
    )
    assert config.enable_live_queries is True

  def test_validate_url_error_message_lists_embedded(self, clean_env) -> None:  # noqa: ARG002
    """Error message for invalid URLs should list embedded schemes so users discover them."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(_env_file=None, url='unknown://foo')
    message = str(exc_info.value)
    assert 'surrealkv://' in message
    assert 'memory://' in message

  def test_validate_url_empty(self, clean_env) -> None:  # noqa: ARG002
    """Test URL validation with empty string."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(_env_file=None, url='')

    assert 'URL cannot be empty' in str(exc_info.value)

  def test_validate_namespace_valid(self, clean_env) -> None:  # noqa: ARG002
    """Test namespace validation with valid name."""
    config = ConnectionConfig(_env_file=None, namespace='my_namespace')
    assert config.namespace == 'my_namespace'

  def test_validate_namespace_empty(self, clean_env) -> None:  # noqa: ARG002
    """Test namespace validation with empty string."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(_env_file=None, namespace='')

    assert 'Identifier cannot be empty' in str(exc_info.value)

  def test_validate_namespace_invalid_chars(self, clean_env) -> None:  # noqa: ARG002
    """Test namespace validation with invalid characters."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(_env_file=None, namespace='my namespace')

    assert 'alphanumeric' in str(exc_info.value)

  def test_validate_database_valid(self, clean_env) -> None:  # noqa: ARG002
    """Test database validation with valid name."""
    config = ConnectionConfig(_env_file=None, database='my-database')
    assert config.database == 'my-database'

  def test_validate_database_empty(self, clean_env) -> None:  # noqa: ARG002
    """Test database validation with empty string."""
    with pytest.raises(ValidationError) as exc_info:
      ConnectionConfig(_env_file=None, database='')

    assert 'Identifier cannot be empty' in str(exc_info.value)

  def test_timeout_minimum_value(self, clean_env) -> None:  # noqa: ARG002
    """Test timeout has minimum value constraint."""
    with pytest.raises(ValidationError):
      ConnectionConfig(_env_file=None, timeout=0.5)

  def test_max_connections_minimum(self, clean_env) -> None:  # noqa: ARG002
    """Test max_connections has minimum value."""
    with pytest.raises(ValidationError):
      ConnectionConfig(_env_file=None, max_connections=0)

  def test_max_connections_maximum(self, clean_env) -> None:  # noqa: ARG002
    """Test max_connections has maximum value."""
    with pytest.raises(ValidationError):
      ConnectionConfig(_env_file=None, max_connections=101)

  def test_retry_max_wait_validation(self, clean_env) -> None:  # noqa: ARG002
    """Test that retry_max_wait must be greater than retry_min_wait."""
    with pytest.raises(ValidationError):
      ConnectionConfig(_env_file=None, retry_min_wait=5.0, retry_max_wait=3.0)


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

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal_client):
      await client.connect()

      assert client.is_connected is True
      mock_surreal_client.connect.assert_called_once()
      mock_surreal_client.signin.assert_called_once()
      mock_surreal_client.use.assert_called_once_with('test', 'test_db')

  @pytest.mark.anyio
  async def test_connect_without_auth(self, clean_env) -> None:  # noqa: ARG002
    """Test connection without username/password."""
    config = ConnectionConfig(_env_file=None, username=None, password=None)
    client = DatabaseClient(config)
    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.use = AsyncMock()

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
      await client.connect()

      assert client.is_connected is True
      mock_surreal.signin.assert_not_called()

  @pytest.mark.anyio
  async def test_connect_already_connected(self, mock_db_client: DatabaseClient) -> None:
    """Test reconnecting when already connected disconnects first."""
    # Save reference to the original mock client
    original_client = mock_db_client._client

    # Mock AsyncSurreal so reconnect doesn't hit a real server
    new_mock = Mock()
    new_mock.connect = AsyncMock()
    new_mock.signin = AsyncMock()
    new_mock.use = AsyncMock()

    with patch('surql.connection.client.AsyncSurreal', return_value=new_mock):
      await mock_db_client.connect()

    # Original client should have been closed
    original_client.close.assert_called_once()
    # Should still be connected after reconnect
    assert mock_db_client.is_connected is True

  @pytest.mark.anyio
  async def test_connect_failure(self, db_config: ConnectionConfig) -> None:
    """Test connection failure after retries."""
    client = DatabaseClient(db_config)
    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock(side_effect=Exception('Connection failed'))

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
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
    """Test successful SELECT operation on a bare table target."""
    await mock_db_client.select('user')

    mock_db_client._client.select.assert_called_once_with('user')

  @pytest.mark.anyio
  async def test_select_record_id_uses_type_thing_query(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Regression (bug #15): `"table:id"` targets must route through
    `SELECT * FROM type::record($table, $id)`.

    On SurrealDB v3, passing a bare ``"user:alice"`` string to
    ``db.select`` is interpreted as a table name containing a colon
    and returns nothing. The client must detect record-id-shaped
    targets and dispatch via raw SurrealQL so the server treats them
    as record ids. TS / rs / go ports all do this.

    Must use ``type::record(table, id)`` (not ``type::record(table, id)``):
    in v3 the two-arg form of ``type::record`` is a type coercion, not a
    constructor.
    """
    mock_db_client._client.query = AsyncMock(return_value=[{'id': 'user:alice', 'name': 'Alice'}])

    result = await mock_db_client.select('user:alice')

    # Must NOT go through the SDK's bare string `select` path.
    mock_db_client._client.select.assert_not_called()

    # Must hit `query` with `type::record($table, $id)` and bound params.
    mock_db_client._client.query.assert_called_once()
    sql, params = mock_db_client._client.query.call_args.args
    assert 'type::record($table, $id)' in sql
    assert params == {'table': 'user', 'id': 'alice'}

    # Single-record unwrap still yields a dict (not a list).
    assert isinstance(result, dict)
    assert result['id'] == 'user:alice'

  @pytest.mark.anyio
  async def test_select_record_id_returns_none_when_missing(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Missing record yields `None`, not an empty list."""
    mock_db_client._client.query = AsyncMock(return_value=[])

    result = await mock_db_client.select('user:ghost')

    assert result is None

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
    """Test successful UPDATE operation.

    Targets shaped like record IDs (`'user:alice'`) are normalized to SDK
    `RecordID(table, id)` objects before reaching the underlying client so the
    SDK doesn't have to re-parse the string (and so bracketed unicode forms
    like `'user:⟨a-b⟩'` work — #91).
    """
    from surrealdb import RecordID as SdkRecordID

    data = {'status': 'active'}
    await mock_db_client.update('user:alice', data)

    mock_db_client._client.update.assert_called_once()
    call_args = mock_db_client._client.update.call_args
    assert isinstance(call_args[0][0], SdkRecordID)
    assert call_args[0][0].table_name == 'user'
    assert call_args[0][0].id == 'alice'
    assert call_args[0][1] == data

  @pytest.mark.anyio
  async def test_merge_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful MERGE operation. Target normalized to SDK RecordID — see #91."""
    from surrealdb import RecordID as SdkRecordID

    data = {'status': 'active'}
    await mock_db_client.merge('user:alice', data)

    mock_db_client._client.merge.assert_called_once()
    call_args = mock_db_client._client.merge.call_args
    assert isinstance(call_args[0][0], SdkRecordID)
    assert call_args[0][0].table_name == 'user'
    assert call_args[0][0].id == 'alice'
    assert call_args[0][1] == data

  @pytest.mark.anyio
  async def test_delete_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful DELETE operation. Target normalized to SDK RecordID — see #91."""
    from surrealdb import RecordID as SdkRecordID

    await mock_db_client.delete('user:alice')

    mock_db_client._client.delete.assert_called_once()
    call_args = mock_db_client._client.delete.call_args
    assert isinstance(call_args[0][0], SdkRecordID)
    assert call_args[0][0].table_name == 'user'
    assert call_args[0][0].id == 'alice'

  @pytest.mark.anyio
  async def test_insert_relation_success(self, mock_db_client: DatabaseClient) -> None:
    """Test successful INSERT RELATION operation."""
    data = {'in': 'user:alice', 'out': 'post:123'}
    await mock_db_client.insert_relation('likes', data)

    mock_db_client._client.insert_relation.assert_called_once()
    call_args = mock_db_client._client.insert_relation.call_args
    assert call_args[0][0] == 'likes'
    sent_data = call_args[0][1]
    # Data is denormalized: record ID strings become SDK RecordID objects
    assert sent_data['in'].table_name == 'user'
    assert sent_data['in'].id == 'alice'
    assert sent_data['out'].table_name == 'post'
    assert sent_data['out'].id == '123'

  @pytest.mark.anyio
  async def test_context_manager(self, db_config: ConnectionConfig) -> None:
    """Test DatabaseClient as async context manager."""
    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.signin = AsyncMock()
    mock_surreal.use = AsyncMock()
    mock_surreal.close = AsyncMock()

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
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

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
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
    """Test successful transaction begin.

    With the bug #13 buffer strategy, ``begin()`` no longer contacts
    the server -- it just flips the state machine so statements can
    be queued.
    """
    txn = Transaction(mock_db_client)

    await txn.begin()

    assert txn.state == TransactionState.ACTIVE
    assert txn.is_active is True
    # Crucially, `begin` does NOT send a bare "BEGIN TRANSACTION"
    # RPC. v3 would accept it, but then the matching bare "COMMIT"
    # would land in a fresh request and be rejected.
    mock_db_client._client.query.assert_not_called()

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
    """Test successful transaction commit flushes buffered statements."""
    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.execute('CREATE user:alice SET name = "Alice"')
    await txn.execute('CREATE user:bob SET name = "Bob"')

    await txn.commit()

    assert txn.state == TransactionState.COMMITTED
    assert txn.is_active is False

    # Buffered statements must be flushed as a single batched query
    # wrapped in BEGIN TRANSACTION ... COMMIT TRANSACTION. As of 1.6.0
    # the commit RPC is routed via the SDK's ``query_raw`` method
    # (preserves the per-statement envelope required for sentinel
    # inspection — see ``transaction.py`` module docstring).
    mock_db_client._client.query_raw.assert_called_once()
    batched = mock_db_client._client.query_raw.call_args.args[0]
    assert batched.startswith('BEGIN TRANSACTION;')
    assert batched.rstrip().endswith('COMMIT TRANSACTION;')
    assert 'CREATE user:alice' in batched
    assert 'CREATE user:bob' in batched
    # Sentinel marker is injected immediately before COMMIT.
    assert "RETURN '__txn_ok__';" in batched

  @pytest.mark.anyio
  async def test_commit_not_active(self, mock_db_client: DatabaseClient) -> None:
    """Test committing transaction that is not active."""
    txn = Transaction(mock_db_client)

    with pytest.raises(TransactionError) as exc_info:
      await txn.commit()

    assert 'Cannot commit transaction' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_commit_failure(self, mock_db_client: DatabaseClient) -> None:
    """Test transaction commit failure when the RPC itself raises."""
    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.execute('CREATE user:alice')

    # The 1.6.0 commit path routes through ``query_raw``; patch that
    # mock to simulate a network/RPC failure.
    mock_db_client._client.query_raw = AsyncMock(side_effect=Exception('Commit failed'))

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
  async def test_execute_queues_without_server_call(self, mock_db_client: DatabaseClient) -> None:
    """Regression (bug #13): ``execute`` buffers; no RPC until commit.

    Pre-patch, each ``Transaction.execute`` forwarded live to
    ``client.execute``. SurrealDB v3 treats each RPC as standalone so
    the matching bare ``COMMIT`` would later be rejected. The new
    design queues statements and flushes them atomically at commit.
    """
    txn = Transaction(mock_db_client)
    await txn.begin()
    mock_db_client._client.query.reset_mock()

    result = await txn.execute('CREATE user:alice SET name = "Alice"')

    # No server RPC yet -- purely a buffer append.
    assert result is None
    mock_db_client._client.query.assert_not_called()

  @pytest.mark.anyio
  async def test_execute_not_active(self, mock_db_client: DatabaseClient) -> None:
    """Test executing query when transaction is not active."""
    txn = Transaction(mock_db_client)

    with pytest.raises(TransactionError) as exc_info:
      await txn.execute('SELECT * FROM user')

    assert 'Cannot execute query' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_commit_sends_single_batched_rpc(self, mock_db_client: DatabaseClient) -> None:
    """Regression (bug #13): commit sends exactly one RPC.

    Pre-patch ``begin``/``commit`` each issued their own ``execute``
    call. On v3, the standalone ``COMMIT`` in the second RPC fails
    with 'no transaction is currently open'. The batched
    ``BEGIN ...; stmts; COMMIT;`` must all land in a single query.
    """
    txn = Transaction(mock_db_client)
    async with txn:
      await txn.execute('UPDATE user:alice SET x = 1')
      await txn.execute('UPDATE user:bob   SET x = 2')

    # Exactly one RPC on the wire, wrapped in BEGIN/COMMIT. The
    # commit path uses ``query_raw`` (1.6.0+) instead of ``query`` so
    # the SDK does not collapse the per-statement envelope.
    assert mock_db_client._client.query_raw.call_count == 1
    batched = mock_db_client._client.query_raw.call_args.args[0]
    assert batched.count('BEGIN TRANSACTION') == 1
    assert batched.count('COMMIT TRANSACTION') == 1

  @pytest.mark.anyio
  async def test_commit_merges_params_across_statements(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Queued statement params are merged into a single vars dict."""
    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.execute('UPDATE user:alice SET name = $n1', {'n1': 'Alice'})
    await txn.execute('UPDATE user:bob   SET name = $n2', {'n2': 'Bob'})
    await txn.commit()

    params = mock_db_client._client.query_raw.call_args.args[1]
    assert params == {'n1': 'Alice', 'n2': 'Bob'}

  @pytest.mark.anyio
  async def test_commit_returns_user_statement_results(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """1.6.0: commit returns per-statement user results, framing stripped.

    The SDK envelope is shaped
    ``{'result': [BEGIN-ack, stmt1, stmt2, sentinel, COMMIT-ack]}``.
    Callers should see only ``[stmt1, stmt2]`` — the BEGIN, sentinel,
    and COMMIT entries are framing.
    """
    txn = Transaction(mock_db_client)
    mock_db_client._client.query_raw = AsyncMock(
      return_value={
        'id': 'req-1',
        'result': [
          {'result': None, 'status': 'OK'},  # BEGIN ack
          {'result': [{'id': 'user:alice'}], 'status': 'OK'},
          {'result': [{'id': 'user:bob'}], 'status': 'OK'},
          {'result': '__txn_ok__', 'status': 'OK'},  # sentinel
          {'result': None, 'status': 'OK'},  # COMMIT ack
        ],
      }
    )

    await txn.begin()
    await txn.execute('CREATE user:alice')
    await txn.execute('CREATE user:bob')
    result = await txn.commit()

    assert txn.state == TransactionState.COMMITTED
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]['result'] == [{'id': 'user:alice'}]
    assert result[1]['result'] == [{'id': 'user:bob'}]

  @pytest.mark.anyio
  async def test_commit_raises_on_mid_batch_failure(self, mock_db_client: DatabaseClient) -> None:
    """1.6.0: mid-batch errors no longer silently succeed.

    Pre-1.6.0, the SDK's ``query`` method collapsed both success and
    failure to ``None`` for batched ``BEGIN ... COMMIT`` requests, so
    a single bad statement that triggered a server-side rollback was
    indistinguishable from a clean commit. ``commit`` now inspects
    the per-statement envelope returned by ``query_raw`` and raises
    ``TransactionError`` when the rollback marker (``status == 'ERR'``)
    appears.
    """
    txn = Transaction(mock_db_client)
    # Simulate the v3.0.5 envelope for a batch that failed because
    # statement 2 violated a SCHEMAFULL type constraint.
    mock_db_client._client.query_raw = AsyncMock(
      return_value={
        'id': 'req-2',
        'result': [
          {'result': None, 'status': 'OK'},  # BEGIN ack
          {
            'details': {'kind': 'NotExecuted'},
            'kind': 'Query',
            'result': 'The query was not executed due to a failed transaction',
            'status': 'ERR',
          },
          {
            'kind': 'Internal',
            'result': (
              "Couldn't coerce value for field `age` of `bar:2`: "
              "Expected `int` but found `'not_an_int'`"
            ),
            'status': 'ERR',
          },
          {
            'details': {'kind': 'Cancelled'},
            'kind': 'Query',
            'result': 'The query was not executed due to a cancelled transaction',
            'status': 'ERR',
          },
        ],
      }
    )

    await txn.begin()
    await txn.execute('CREATE bar:1 SET age = 10')
    await txn.execute("CREATE bar:2 SET age = 'not_an_int'")

    with pytest.raises(TransactionError) as exc_info:
      await txn.commit()

    # The error must mention the server's per-statement explanation
    # so operators can act on it directly.
    msg = str(exc_info.value)
    assert 'SurrealDB rolled back the batch' in msg
    assert 'Couldn' in msg and 'coerce value' in msg
    # State machine transitions to CANCELLED (not COMMITTED) on rollback.
    assert txn.state == TransactionState.CANCELLED

  @pytest.mark.anyio
  async def test_commit_raises_when_sentinel_absent(self, mock_db_client: DatabaseClient) -> None:
    """Sentinel-absent envelope is treated as a failure even with no ERR rows.

    Defensive: if a future SurrealDB version emits a degenerate
    envelope (no ERR rows AND no sentinel), the commit must not be
    treated as a silent success. The error message names the missing
    marker so operators know what to inspect.
    """
    txn = Transaction(mock_db_client)
    mock_db_client._client.query_raw = AsyncMock(
      return_value={
        'id': 'req-3',
        'result': [
          {'result': None, 'status': 'OK'},
          {'result': [{'id': 'user:x'}], 'status': 'OK'},
          # No sentinel, no ERR — should still be flagged.
          {'result': None, 'status': 'OK'},
        ],
      }
    )

    await txn.begin()
    await txn.execute('CREATE user:x')

    with pytest.raises(TransactionError) as exc_info:
      await txn.commit()

    assert 'sentinel marker absent' in str(exc_info.value)
    assert txn.state == TransactionState.CANCELLED

  @pytest.mark.anyio
  async def test_commit_falls_back_when_query_raw_unavailable(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Older SDKs lacking ``query_raw`` fall through to ``execute``.

    The commit path checks for ``query_raw`` on the SDK client; when
    absent it routes through ``DatabaseClient.execute`` (the
    pre-1.6.0 path). In that mode sentinel inspection is skipped —
    the prior silent-swallow behaviour is preserved rather than
    breaking the commit. This guards against pinning surql-py to an
    SDK upgrade that lags.
    """
    # Strip ``query_raw`` from the SDK mock to simulate an SDK that
    # hasn't shipped the method yet.
    del mock_db_client._client.query_raw

    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.execute('CREATE user:alice')
    # Should not raise; falls through to execute and returns its
    # collapsed result (the default mock returns ``[{'result': []}]``).
    await txn.commit()

    assert txn.state == TransactionState.COMMITTED

  @pytest.mark.anyio
  async def test_commit_normalizes_surql_record_id_params(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """1.6.1: Transaction.commit normalises ``surql.RecordID`` bound params.

    ``DatabaseClient.execute`` runs incoming params through
    ``_denormalize_params`` so ``surql.types.record_id.RecordID``
    (surql-py's Pydantic wrapper) is converted to ``surrealdb.RecordID``
    (the SDK's native CBOR-encodable class) before the SDK's encoder
    sees them. Pre-1.6.1, ``Transaction.commit`` skipped that
    normalisation and passed the params dict through unchanged to
    ``query_raw``, so callers that queued ``surql.RecordID`` values via
    bound params (``txn.execute(sql, {'parent': RecordID(...)})``)
    crashed at commit with
    ``no encoder for type <class 'surql.types.record_id.RecordID'>``.

    The fix routes the Transaction param dict through the same
    ``_denormalize_params`` helper inside ``Transaction._raw_query``.
    This asserts the params handed to the SDK are
    ``surrealdb.RecordID`` instances, NOT surql-py's wrapper.
    """
    from surrealdb import RecordID as SdkRecordID

    from surql.types.record_id import RecordID as SurqlRecordID

    captured: dict[str, dict[str, object]] = {}

    async def capture_query_raw(_sql: str, params: dict[str, object]) -> dict[str, object]:
      captured['params'] = params
      return {'result': [{'result': '__txn_ok__', 'status': 'OK'}]}

    mock_db_client._client.query_raw = AsyncMock(side_effect=capture_query_raw)

    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.execute(
      'UPDATE foo:bar SET parent = $parent',
      {'parent': SurqlRecordID(table='foo', id='baz')},
    )
    # The commit must not raise: pre-1.6.1 the SDK's CBOR encoder
    # would reject the surql-py wrapper.
    await txn.commit()

    assert txn.state == TransactionState.COMMITTED
    sdk_params = captured['params']
    assert 'parent' in sdk_params
    # Critical: the SDK must receive the native ``surrealdb.RecordID``,
    # not surql-py's Pydantic wrapper. If this assertion regresses, the
    # ``no encoder`` crash will return for real callers.
    assert isinstance(sdk_params['parent'], SdkRecordID)
    assert not isinstance(sdk_params['parent'], SurqlRecordID)
    assert sdk_params['parent'].table_name == 'foo'
    assert sdk_params['parent'].id == 'baz'

  @pytest.mark.anyio
  async def test_commit_normalizes_mixed_param_types(self, mock_db_client: DatabaseClient) -> None:
    """1.6.1: param normalisation preserves non-RecordID values.

    ``_denormalize_params`` recurses into dicts/lists; primitives and
    other values pass through untouched. This guards against a future
    over-eager conversion damaging strings, ints, bools, dicts, or
    lists that share a params dict with a ``surql.RecordID``.
    """
    from surrealdb import RecordID as SdkRecordID

    from surql.types.record_id import RecordID as SurqlRecordID

    captured: dict[str, dict[str, object]] = {}

    async def capture_query_raw(_sql: str, params: dict[str, object]) -> dict[str, object]:
      captured['params'] = params
      return {'result': [{'result': '__txn_ok__', 'status': 'OK'}]}

    mock_db_client._client.query_raw = AsyncMock(side_effect=capture_query_raw)

    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.execute(
      'UPDATE foo:bar SET parent = $parent, name = $name, count = $count, '
      'tags = $tags, meta = $meta',
      {
        'parent': SurqlRecordID(table='foo', id='baz'),
        'name': 'Alice',
        'count': 42,
        'tags': ['a', 'b', 'c'],
        'meta': {'active': True, 'score': 3.14},
      },
    )
    await txn.commit()

    assert txn.state == TransactionState.COMMITTED
    sdk_params = captured['params']
    # RecordID converted to SDK type.
    assert isinstance(sdk_params['parent'], SdkRecordID)
    # Primitives untouched.
    assert sdk_params['name'] == 'Alice'
    assert sdk_params['count'] == 42
    assert sdk_params['tags'] == ['a', 'b', 'c']
    assert sdk_params['meta'] == {'active': True, 'score': 3.14}

  @pytest.mark.anyio
  async def test_duplicate_param_keys_rejected(self, mock_db_client: DatabaseClient) -> None:
    """Duplicate param names across queued statements raise early."""
    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.execute('UPDATE user:alice SET x = $v', {'v': 1})

    with pytest.raises(TransactionError) as exc_info:
      await txn.execute('UPDATE user:bob SET x = $v', {'v': 2})

    assert 'Duplicate transaction parameter names' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_cancel_does_not_contact_server(self, mock_db_client: DatabaseClient) -> None:
    """``cancel`` drops the buffer; no CANCEL RPC is issued."""
    txn = Transaction(mock_db_client)
    await txn.begin()
    await txn.execute('CREATE user:alice')
    mock_db_client._client.query.reset_mock()
    mock_db_client._client.query_raw.reset_mock()

    await txn.cancel()

    # Nothing was sent to the server, and the buffer is empty.
    mock_db_client._client.query.assert_not_called()
    mock_db_client._client.query_raw.assert_not_called()
    assert txn._statements == []
    assert txn.state == TransactionState.CANCELLED

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
