"""Database connection configuration."""

from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConnectionConfig(BaseSettings):
  """Database connection configuration with environment variable support."""

  model_config = SettingsConfigDict(
    env_prefix='SURQL_',
    env_file='.env',
    env_file_encoding='utf-8',
    case_sensitive=False,
    extra='ignore',
    populate_by_name=True,
  )

  # Connection details
  db_url: str = Field(
    default='ws://localhost:8000/rpc',
    description='SurrealDB connection URL',
    alias='url',
  )
  db_ns: str = Field(
    default='development',
    description='Database namespace',
    alias='namespace',
  )
  db: str = Field(
    default='main',
    description='Database name',
    alias='database',
  )
  db_user: str | None = Field(
    default=None,
    description='Authentication username',
    alias='username',
  )
  db_pass: str | None = Field(
    default=None,
    description='Authentication password',
    alias='password',
    repr=False,
  )

  # Connection pool settings
  db_timeout: float = Field(
    default=30.0,
    ge=1.0,
    description='Connection timeout in seconds',
    alias='timeout',
  )
  db_max_connections: int = Field(
    default=10,
    ge=1,
    le=100,
    description='Maximum number of concurrent connections',
    alias='max_connections',
  )

  # Retry settings
  db_retry_max_attempts: int = Field(
    default=3,
    ge=1,
    le=10,
    description='Maximum number of retry attempts',
    alias='retry_max_attempts',
  )
  db_retry_min_wait: float = Field(
    default=1.0,
    ge=0.1,
    description='Minimum wait time between retries',
    alias='retry_min_wait',
  )
  db_retry_max_wait: float = Field(
    default=10.0,
    ge=1.0,
    description='Maximum wait time between retries',
    alias='retry_max_wait',
  )
  db_retry_multiplier: float = Field(
    default=2.0,
    ge=1.0,
    description='Multiplier for exponential backoff',
    alias='retry_multiplier',
  )

  # Live query support
  enable_live_queries: bool = Field(
    default=True,
    description='Enable live query support (requires WebSocket)',
  )

  @field_validator('db_ns', 'db')
  @classmethod
  def validate_identifier(cls, v: str) -> str:
    """Validate namespace and database identifiers."""
    if not v:
      raise ValueError('Identifier cannot be empty')
    if not v.replace('_', '').replace('-', '').isalnum():
      raise ValueError('Identifier must be alphanumeric with optional underscores/hyphens')
    return v

  @field_validator('db_url')
  @classmethod
  def validate_url(cls, v: str) -> str:
    """Validate connection URL format."""
    if not v:
      raise ValueError('URL cannot be empty')
    if not any(v.startswith(proto) for proto in ['ws://', 'wss://', 'http://', 'https://']):
      raise ValueError('URL must use ws://, wss://, http://, or https:// protocol')
    return v

  @field_validator('enable_live_queries')
  @classmethod
  def validate_live_queries(cls, v: bool, info: Any) -> bool:
    """Validate live query support based on connection protocol."""
    data = info.data if hasattr(info, 'data') else {}
    url = data.get('db_url', '')
    if v and (url.startswith('http://') or url.startswith('https://')):
      raise ValueError('Live queries require WebSocket connection (ws:// or wss://)')
    return v

  @field_validator('db_retry_max_wait')
  @classmethod
  def validate_retry_wait(cls, v: float, info: Any) -> float:
    """Ensure max wait is greater than min wait."""
    data = info.data if hasattr(info, 'data') else {}
    min_wait = data.get('db_retry_min_wait')
    if min_wait is not None and v <= min_wait:
      raise ValueError('db_retry_max_wait must be greater than db_retry_min_wait')
    return v

  @property
  def url(self) -> str:
    """Alias for db_url."""
    return self.db_url

  @property
  def namespace(self) -> str:
    """Alias for db_ns."""
    return self.db_ns

  @property
  def database(self) -> str:
    """Alias for db."""
    return self.db

  @property
  def username(self) -> str | None:
    """Alias for db_user."""
    return self.db_user

  @property
  def password(self) -> str | None:
    """Alias for db_pass."""
    return self.db_pass

  @property
  def timeout(self) -> float:
    """Alias for db_timeout."""
    return self.db_timeout

  @property
  def max_connections(self) -> int:
    """Alias for db_max_connections."""
    return self.db_max_connections

  @property
  def retry_max_attempts(self) -> int:
    """Alias for db_retry_max_attempts."""
    return self.db_retry_max_attempts

  @property
  def retry_min_wait(self) -> float:
    """Alias for db_retry_min_wait."""
    return self.db_retry_min_wait

  @property
  def retry_max_wait(self) -> float:
    """Alias for db_retry_max_wait."""
    return self.db_retry_max_wait

  @property
  def retry_multiplier(self) -> float:
    """Alias for db_retry_multiplier."""
    return self.db_retry_multiplier


class NamedConnectionConfig(BaseSettings):
  """Named connection configuration for multi-connection setup."""

  model_config = SettingsConfigDict(
    env_file='.env',
    env_file_encoding='utf-8',
    case_sensitive=False,
    extra='ignore',
  )

  name: str = Field(description='Connection name')
  config: ConnectionConfig = Field(description='Connection configuration')

  @classmethod
  def from_env(cls, name: str) -> 'NamedConnectionConfig':
    """Load named connection from environment variables.

    Args:
      name: Connection name (e.g., 'PRIMARY', 'REPLICA')

    Returns:
      Named connection configuration
    """
    prefix = f'SURQL_{name.upper()}_'

    # Create a dynamic subclass with the named prefix
    class NamedConfig(ConnectionConfig):
      model_config = SettingsConfigDict(
        env_prefix=prefix,
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore',
      )

    config = NamedConfig()
    return cls(name=name.lower(), config=config)
