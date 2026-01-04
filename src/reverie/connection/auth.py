"""Authentication module for SurrealDB connections."""

from enum import Enum
from typing import Any, cast

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class AuthType(str, Enum):
  """Authentication type levels."""

  ROOT = 'root'
  NAMESPACE = 'namespace'
  DATABASE = 'database'
  SCOPE = 'scope'


class RootCredentials(BaseModel):
  """Root-level authentication credentials."""

  username: str = Field(description='Root username')
  password: str = Field(description='Root password')

  def to_dict(self) -> dict[str, str]:
    """Convert to dictionary for SDK."""
    return {
      'username': self.username,
      'password': self.password,
    }


class NamespaceCredentials(BaseModel):
  """Namespace-level authentication credentials."""

  namespace: str = Field(description='Namespace name')
  username: str = Field(description='Namespace username')
  password: str = Field(description='Namespace password')

  def to_dict(self) -> dict[str, str]:
    """Convert to dictionary for SDK."""
    return {
      'namespace': self.namespace,
      'username': self.username,
      'password': self.password,
    }


class DatabaseCredentials(BaseModel):
  """Database-level authentication credentials."""

  namespace: str = Field(description='Namespace name')
  database: str = Field(description='Database name')
  username: str = Field(description='Database username')
  password: str = Field(description='Database password')

  def to_dict(self) -> dict[str, str]:
    """Convert to dictionary for SDK."""
    return {
      'namespace': self.namespace,
      'database': self.database,
      'username': self.username,
      'password': self.password,
    }


class ScopeCredentials(BaseModel):
  """Scope-level authentication credentials."""

  namespace: str = Field(description='Namespace name')
  database: str = Field(description='Database name')
  access: str = Field(description='Access/scope name')
  variables: dict[str, Any] = Field(
    default_factory=dict,
    description='Scope variables (e.g., email, password)',
  )

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for SDK."""
    return {
      'namespace': self.namespace,
      'database': self.database,
      'access': self.access,
      **self.variables,
    }


class TokenAuth(BaseModel):
  """JWT token authentication."""

  token: str = Field(description='JWT authentication token')


class AuthManager:
  """Authentication manager for database clients."""

  def __init__(self) -> None:
    """Initialize auth manager."""
    self._current_token: str | None = None
    self._auth_type: AuthType | None = None

  async def signup(
    self,
    client: Any,
    credentials: ScopeCredentials,
  ) -> str:
    """Sign up a new user with scope credentials.

    Args:
      client: Database client
      credentials: Scope credentials for signup

    Returns:
      JWT token

    Example:
      ```python
      creds = ScopeCredentials(
        namespace='prod',
        database='app',
        access='user',
        variables={'email': 'user@example.com', 'password': 'pass123'}
      )
      token = await auth_manager.signup(client, creds)
      ```
    """
    logger.info('user_signup', access=credentials.access)
    token = await client.signup(credentials.to_dict())
    self._current_token = cast(str, token)
    self._auth_type = AuthType.SCOPE
    return cast(str, token)

  async def signin(
    self,
    client: Any,
    credentials: RootCredentials | NamespaceCredentials | DatabaseCredentials | ScopeCredentials,
  ) -> str:
    """Sign in with provided credentials.

    Args:
      client: Database client
      credentials: Authentication credentials

    Returns:
      JWT token

    Example:
      ```python
      # Root signin
      root_creds = RootCredentials(username='root', password='root')
      token = await auth_manager.signin(client, root_creds)

      # Scope signin
      scope_creds = ScopeCredentials(
        namespace='prod',
        database='app',
        access='user',
        variables={'email': 'user@example.com', 'password': 'pass123'}
      )
      token = await auth_manager.signin(client, scope_creds)
      ```
    """
    if isinstance(credentials, RootCredentials):
      auth_type = AuthType.ROOT
    elif isinstance(credentials, NamespaceCredentials):
      auth_type = AuthType.NAMESPACE
    elif isinstance(credentials, DatabaseCredentials):
      auth_type = AuthType.DATABASE
    else:
      auth_type = AuthType.SCOPE

    logger.info('user_signin', auth_type=auth_type)
    token = await client.signin(credentials.to_dict())
    self._current_token = cast(str, token)
    self._auth_type = auth_type
    return cast(str, token)

  async def authenticate(
    self,
    client: Any,
    token: str,
  ) -> None:
    """Authenticate with an existing JWT token.

    Args:
      client: Database client
      token: JWT authentication token

    Example:
      ```python
      await auth_manager.authenticate(client, saved_token)
      ```
    """
    logger.info('token_authentication')
    await client.authenticate(token)
    self._current_token = token

  async def invalidate(self, client: Any) -> None:
    """Invalidate the current session.

    Args:
      client: Database client

    Example:
      ```python
      await auth_manager.invalidate(client)
      ```
    """
    logger.info('session_invalidate')
    await client.invalidate()
    self._current_token = None
    self._auth_type = None

  @property
  def current_token(self) -> str | None:
    """Get the current authentication token."""
    return self._current_token

  @property
  def auth_type(self) -> AuthType | None:
    """Get the current authentication type."""
    return self._auth_type

  @property
  def is_authenticated(self) -> bool:
    """Check if currently authenticated."""
    return self._current_token is not None
