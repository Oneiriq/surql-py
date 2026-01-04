"""Tests for authentication module."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from reverie.connection.auth import (
  AuthManager,
  AuthType,
  DatabaseCredentials,
  NamespaceCredentials,
  RootCredentials,
  ScopeCredentials,
  TokenAuth,
)


class TestCredentialModels:
  """Tests for credential models."""

  def test_root_credentials(self):
    """Test root credentials model."""
    creds = RootCredentials(username='root', password='root')
    assert creds.username == 'root'
    assert creds.password == 'root'
    assert creds.to_dict() == {'username': 'root', 'password': 'root'}

  def test_namespace_credentials(self):
    """Test namespace credentials model."""
    creds = NamespaceCredentials(namespace='test', username='ns_user', password='ns_pass')
    assert creds.namespace == 'test'
    assert creds.username == 'ns_user'
    assert creds.password == 'ns_pass'
    assert creds.to_dict() == {
      'namespace': 'test',
      'username': 'ns_user',
      'password': 'ns_pass',
    }

  def test_database_credentials(self):
    """Test database credentials model."""
    creds = DatabaseCredentials(
      namespace='test',
      database='main',
      username='db_user',
      password='db_pass',
    )
    assert creds.namespace == 'test'
    assert creds.database == 'main'
    assert creds.username == 'db_user'
    assert creds.password == 'db_pass'
    assert creds.to_dict() == {
      'namespace': 'test',
      'database': 'main',
      'username': 'db_user',
      'password': 'db_pass',
    }

  def test_scope_credentials(self):
    """Test scope credentials model."""
    creds = ScopeCredentials(
      namespace='test',
      database='main',
      access='user',
      variables={'email': 'test@example.com', 'password': 'pass123'},
    )
    assert creds.namespace == 'test'
    assert creds.database == 'main'
    assert creds.access == 'user'
    assert creds.variables == {'email': 'test@example.com', 'password': 'pass123'}
    result = creds.to_dict()
    assert result['namespace'] == 'test'
    assert result['database'] == 'main'
    assert result['access'] == 'user'
    assert result['email'] == 'test@example.com'
    assert result['password'] == 'pass123'

  def test_scope_credentials_default_variables(self):
    """Test scope credentials with default variables."""
    creds = ScopeCredentials(
      namespace='test',
      database='main',
      access='user',
    )
    assert creds.variables == {}

  def test_token_auth(self):
    """Test token auth model."""
    token_auth = TokenAuth(token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9')
    assert token_auth.token == 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9'


class TestAuthManager:
  """Tests for AuthManager."""

  @pytest.fixture
  def auth_manager(self):
    """Create auth manager instance."""
    return AuthManager()

  @pytest.fixture
  def mock_client(self):
    """Create mock database client."""
    client = MagicMock()
    client.signup = AsyncMock(return_value='signup_token')
    client.signin = AsyncMock(return_value='signin_token')
    client.authenticate = AsyncMock()
    client.invalidate = AsyncMock()
    return client

  @pytest.mark.anyio
  async def test_signup(self, auth_manager, mock_client):
    """Test user signup."""
    creds = ScopeCredentials(
      namespace='test',
      database='main',
      access='user',
      variables={'email': 'user@example.com', 'password': 'pass123'},
    )

    token = await auth_manager.signup(mock_client, creds)

    assert token == 'signup_token'
    assert auth_manager.current_token == 'signup_token'
    assert auth_manager.auth_type == AuthType.SCOPE
    assert auth_manager.is_authenticated is True
    mock_client.signup.assert_called_once()

  @pytest.mark.anyio
  async def test_signin_root(self, auth_manager, mock_client):
    """Test root signin."""
    creds = RootCredentials(username='root', password='root')

    token = await auth_manager.signin(mock_client, creds)

    assert token == 'signin_token'
    assert auth_manager.current_token == 'signin_token'
    assert auth_manager.auth_type == AuthType.ROOT
    assert auth_manager.is_authenticated is True
    mock_client.signin.assert_called_once_with({'username': 'root', 'password': 'root'})

  @pytest.mark.anyio
  async def test_signin_namespace(self, auth_manager, mock_client):
    """Test namespace signin."""
    creds = NamespaceCredentials(namespace='test', username='ns_user', password='ns_pass')

    token = await auth_manager.signin(mock_client, creds)

    assert token == 'signin_token'
    assert auth_manager.auth_type == AuthType.NAMESPACE
    assert auth_manager.is_authenticated is True

  @pytest.mark.anyio
  async def test_signin_database(self, auth_manager, mock_client):
    """Test database signin."""
    creds = DatabaseCredentials(
      namespace='test',
      database='main',
      username='db_user',
      password='db_pass',
    )

    token = await auth_manager.signin(mock_client, creds)

    assert token == 'signin_token'
    assert auth_manager.auth_type == AuthType.DATABASE
    assert auth_manager.is_authenticated is True

  @pytest.mark.anyio
  async def test_signin_scope(self, auth_manager, mock_client):
    """Test scope signin."""
    creds = ScopeCredentials(
      namespace='test',
      database='main',
      access='user',
      variables={'email': 'user@example.com', 'password': 'pass123'},
    )

    token = await auth_manager.signin(mock_client, creds)

    assert token == 'signin_token'
    assert auth_manager.auth_type == AuthType.SCOPE
    assert auth_manager.is_authenticated is True

  @pytest.mark.anyio
  async def test_authenticate(self, auth_manager, mock_client):
    """Test JWT token authentication."""
    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9'

    await auth_manager.authenticate(mock_client, token)

    assert auth_manager.current_token == token
    assert auth_manager.is_authenticated is True
    mock_client.authenticate.assert_called_once_with(token)

  @pytest.mark.anyio
  async def test_invalidate(self, auth_manager, mock_client):
    """Test session invalidation."""
    # First authenticate
    auth_manager._current_token = 'some_token'
    auth_manager._auth_type = AuthType.ROOT

    await auth_manager.invalidate(mock_client)

    assert auth_manager.current_token is None
    assert auth_manager.auth_type is None
    assert auth_manager.is_authenticated is False
    mock_client.invalidate.assert_called_once()

  def test_initial_state(self, auth_manager):
    """Test auth manager initial state."""
    assert auth_manager.current_token is None
    assert auth_manager.auth_type is None
    assert auth_manager.is_authenticated is False

  def test_auth_type_enum(self):
    """Test AuthType enum values."""
    assert AuthType.ROOT == 'root'
    assert AuthType.NAMESPACE == 'namespace'
    assert AuthType.DATABASE == 'database'
    assert AuthType.SCOPE == 'scope'
