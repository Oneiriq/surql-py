"""Tests for the connection context module."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from surql.connection.client import DatabaseClient
from surql.connection.config import ConnectionConfig
from surql.connection.context import (
  ContextError,
  clear_db,
  connection_override,
  connection_scope,
  get_db,
  has_db,
  set_db,
)


class TestContextBasicOperations:
  """Test suite for basic context operations (get_db, set_db, clear_db, has_db)."""

  @pytest.mark.anyio
  async def test_set_and_get_db(self, mock_db_client: DatabaseClient) -> None:
    """Test setting and retrieving database client from context."""
    clear_db()

    set_db(mock_db_client)
    retrieved = get_db()

    assert retrieved is mock_db_client
    clear_db()

  @pytest.mark.anyio
  async def test_has_db_returns_true_when_set(self, mock_db_client: DatabaseClient) -> None:
    """Test has_db returns True when client is set."""
    clear_db()

    set_db(mock_db_client)

    assert has_db() is True
    clear_db()

  @pytest.mark.anyio
  async def test_has_db_returns_false_when_not_set(self) -> None:
    """Test has_db returns False when no client is set."""
    clear_db()

    assert has_db() is False

  @pytest.mark.anyio
  async def test_clear_db_removes_client(self, mock_db_client: DatabaseClient) -> None:
    """Test clear_db removes client from context."""
    set_db(mock_db_client)
    assert has_db() is True

    clear_db()

    assert has_db() is False

  @pytest.mark.anyio
  async def test_clear_db_idempotent(self) -> None:
    """Test clear_db can be called multiple times safely."""
    clear_db()
    clear_db()

    assert has_db() is False

  @pytest.mark.anyio
  async def test_get_db_raises_error_when_no_context(self) -> None:
    """Test get_db raises ContextError when no context is set."""
    clear_db()

    with pytest.raises(ContextError) as exc_info:
      get_db()

    assert 'No active database connection' in str(exc_info.value)
    assert 'Use connection_scope() or set_db() first' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_set_db_overwrites_previous_client(
    self, mock_db_client: DatabaseClient, db_config: ConnectionConfig
  ) -> None:
    """Test set_db overwrites any previously set client."""
    clear_db()

    # Set first client
    set_db(mock_db_client)
    assert get_db() is mock_db_client

    # Set second client
    second_client = DatabaseClient(db_config)
    set_db(second_client)

    # Should retrieve the second client
    assert get_db() is second_client
    assert get_db() is not mock_db_client
    clear_db()


class TestConnectionScope:
  """Test suite for connection_scope context manager."""

  @pytest.mark.anyio
  async def test_connection_scope_basic(self, db_config: ConnectionConfig) -> None:
    """Test connection_scope creates and cleans up context."""
    clear_db()

    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.signin = AsyncMock()
    mock_surreal.use = AsyncMock()
    mock_surreal.close = AsyncMock()

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
      async with connection_scope(db_config) as client:
        # Context should be set during scope
        assert has_db()
        assert get_db() is client
        assert client.is_connected

      # Context should be cleared after scope
      assert not has_db()

  @pytest.mark.anyio
  async def test_connection_scope_clears_context_on_exit(self, db_config: ConnectionConfig) -> None:
    """Test connection_scope properly clears context on exit."""
    clear_db()

    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.signin = AsyncMock()
    mock_surreal.use = AsyncMock()
    mock_surreal.close = AsyncMock()

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
      async with connection_scope(db_config):
        pass

    # Verify context is cleared
    assert not has_db()
    with pytest.raises(ContextError):
      get_db()

  @pytest.mark.anyio
  async def test_connection_scope_clears_context_on_exception(
    self, db_config: ConnectionConfig
  ) -> None:
    """Test connection_scope clears context even when exception is raised."""
    clear_db()

    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.signin = AsyncMock()
    mock_surreal.use = AsyncMock()
    mock_surreal.close = AsyncMock()

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
      try:
        async with connection_scope(db_config):
          assert has_db()
          raise ValueError('Test error')
      except ValueError:
        pass

    # Context should still be cleared despite exception
    assert not has_db()

  @pytest.mark.anyio
  async def test_connection_scope_yields_connected_client(
    self, db_config: ConnectionConfig
  ) -> None:
    """Test connection_scope yields a connected database client."""
    clear_db()

    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.signin = AsyncMock()
    mock_surreal.use = AsyncMock()
    mock_surreal.close = AsyncMock()

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
      async with connection_scope(db_config) as client:
        assert isinstance(client, DatabaseClient)
        assert client.is_connected


class TestConnectionOverride:
  """Test suite for connection_override context manager."""

  @pytest.mark.anyio
  async def test_connection_override_basic(self, db_config: ConnectionConfig) -> None:
    """Test connection_override temporarily overrides current context."""
    clear_db()

    override_client = DatabaseClient(db_config)

    async with connection_override(override_client):
      assert has_db()
      assert get_db() is override_client

    # Context should be cleared after override (since there was no previous)
    assert not has_db()

  @pytest.mark.anyio
  async def test_connection_override_restores_previous_client(
    self, mock_db_client: DatabaseClient, db_config: ConnectionConfig
  ) -> None:
    """Test connection_override properly restores previous context."""
    clear_db()

    # Set initial client
    set_db(mock_db_client)
    assert get_db() is mock_db_client

    # Override with different client
    override_client = DatabaseClient(db_config)
    async with connection_override(override_client):
      assert get_db() is override_client
      assert get_db() is not mock_db_client

    # Should restore original context
    assert has_db()
    assert get_db() is mock_db_client
    clear_db()

  @pytest.mark.anyio
  async def test_connection_override_restores_none_context(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Test connection_override restores None context if no previous client."""
    clear_db()

    async with connection_override(mock_db_client):
      assert has_db()
      assert get_db() is mock_db_client

    # Should restore None context
    assert not has_db()

  @pytest.mark.anyio
  async def test_connection_override_restores_on_exception(
    self, mock_db_client: DatabaseClient, db_config: ConnectionConfig
  ) -> None:
    """Test connection_override restores previous context even on exception."""
    clear_db()

    # Set initial client
    original_client = DatabaseClient(db_config)
    set_db(original_client)

    try:
      async with connection_override(mock_db_client):
        assert get_db() is mock_db_client
        raise ValueError('Test error')
    except ValueError:
      pass

    # Should restore original client despite exception
    assert has_db()
    assert get_db() is original_client
    clear_db()

  @pytest.mark.anyio
  async def test_connection_override_yields_override_client(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Test connection_override yields the override client."""
    clear_db()

    async with connection_override(mock_db_client) as client:
      assert client is mock_db_client


class TestNestedContexts:
  """Test suite for nested context operations."""

  @pytest.mark.anyio
  async def test_nested_connection_scopes(self, db_config: ConnectionConfig) -> None:
    """Test nested connection_scope context managers.

    Note: connection_scope clears context on exit, so nested scopes
    don't preserve the outer context. Use connection_override for that.
    """
    clear_db()

    mock_surreal1 = Mock()
    mock_surreal1.connect = AsyncMock()
    mock_surreal1.signin = AsyncMock()
    mock_surreal1.use = AsyncMock()
    mock_surreal1.close = AsyncMock()

    mock_surreal2 = Mock()
    mock_surreal2.connect = AsyncMock()
    mock_surreal2.signin = AsyncMock()
    mock_surreal2.use = AsyncMock()
    mock_surreal2.close = AsyncMock()

    call_count = [0]

    def create_mock(*_args, **_kwargs):
      call_count[0] += 1
      return mock_surreal1 if call_count[0] == 1 else mock_surreal2

    with patch('surql.connection.client.AsyncSurreal', side_effect=create_mock):
      async with connection_scope(db_config) as client1:
        assert get_db() is client1

        async with connection_scope(db_config) as client2:
          # Inner scope should override outer
          assert get_db() is client2
          assert get_db() is not client1

        # After inner scope exits, context is cleared (not restored)
        # This is expected behavior - connection_scope clears on exit
        assert not has_db()

    # All contexts should be cleared
    assert not has_db()

  @pytest.mark.anyio
  async def test_nested_connection_overrides(
    self, mock_db_client: DatabaseClient, db_config: ConnectionConfig
  ) -> None:
    """Test nested connection_override context managers."""
    clear_db()
    set_db(mock_db_client)

    override_client1 = DatabaseClient(db_config)
    override_client2 = DatabaseClient(db_config)

    async with connection_override(override_client1):
      assert get_db() is override_client1

      async with connection_override(override_client2):
        # Inner override should be active
        assert get_db() is override_client2

      # Should restore to first override
      assert get_db() is override_client1

    # Should restore to original
    assert get_db() is mock_db_client
    clear_db()

  @pytest.mark.anyio
  async def test_override_inside_scope(self, db_config: ConnectionConfig) -> None:
    """Test connection_override inside connection_scope."""
    clear_db()

    mock_surreal = Mock()
    mock_surreal.connect = AsyncMock()
    mock_surreal.signin = AsyncMock()
    mock_surreal.use = AsyncMock()
    mock_surreal.close = AsyncMock()

    override_client = DatabaseClient(db_config)

    with patch('surql.connection.client.AsyncSurreal', return_value=mock_surreal):
      async with connection_scope(db_config) as scope_client:
        assert get_db() is scope_client

        async with connection_override(override_client):
          # Override should be active
          assert get_db() is override_client

        # Should restore to scope client
        assert get_db() is scope_client

    # All contexts should be cleared
    assert not has_db()


class TestContextIsolation:
  """Test suite for context isolation and cleanup."""

  @pytest.mark.anyio
  async def test_context_isolation_between_tests(self) -> None:
    """Test that context is properly isolated between test cases."""
    clear_db()

    # Verify clean state
    assert not has_db()

  @pytest.mark.anyio
  async def test_multiple_sequential_operations(
    self, mock_db_client: DatabaseClient, db_config: ConnectionConfig
  ) -> None:
    """Test multiple sequential context operations."""
    clear_db()

    # First operation
    set_db(mock_db_client)
    assert get_db() is mock_db_client
    clear_db()

    # Second operation
    second_client = DatabaseClient(db_config)
    set_db(second_client)
    assert get_db() is second_client
    clear_db()

    # Third operation
    assert not has_db()
    with pytest.raises(ContextError):
      get_db()


class TestContextError:
  """Test suite for ContextError exception."""

  def test_context_error_is_exception(self) -> None:
    """Test that ContextError is an Exception."""
    error = ContextError('Test error')
    assert isinstance(error, Exception)
    assert str(error) == 'Test error'

  @pytest.mark.anyio
  async def test_context_error_message_from_get_db(self) -> None:
    """Test ContextError message from get_db includes helpful info."""
    clear_db()

    with pytest.raises(ContextError) as exc_info:
      get_db()

    error_msg = str(exc_info.value)
    assert 'No active database connection' in error_msg
    assert 'connection_scope()' in error_msg
    assert 'set_db()' in error_msg
