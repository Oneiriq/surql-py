"""Database connection configuration."""

from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConnectionConfig(BaseSettings):
  """Database connection configuration with environment variable support."""

  model_config = SettingsConfigDict(
    env_prefix='DB_',
    env_file='.env',
    env_file_encoding='utf-8',
    case_sensitive=False,
    extra='ignore',
  )

  url: str = Field(
    default='ws://localhost:8000/rpc',
    description='SurrealDB connection URL',
  )
  namespace: str = Field(
    default='development',
    description='Database namespace',
  )
  database: str = Field(
    default='main',
    description='Database name',
  )
  username: str | None = Field(
    default=None,
    description='Authentication username',
  )
  password: str | None = Field(
    default=None,
    description='Authentication password',
  )
  timeout: float = Field(
    default=30.0,
    ge=1.0,
    description='Connection timeout in seconds',
  )
  max_connections: int = Field(
    default=10,
    ge=1,
    le=100,
    description='Maximum number of concurrent connections',
  )
  retry_max_attempts: int = Field(
    default=3,
    ge=1,
    le=10,
    description='Maximum number of retry attempts for failed operations',
  )
  retry_min_wait: float = Field(
    default=1.0,
    ge=0.1,
    description='Minimum wait time between retries in seconds',
  )
  retry_max_wait: float = Field(
    default=10.0,
    ge=1.0,
    description='Maximum wait time between retries in seconds',
  )
  retry_multiplier: float = Field(
    default=2.0,
    ge=1.0,
    description='Multiplier for exponential backoff',
  )

  @field_validator('namespace', 'database')
  @classmethod
  def validate_identifier(cls, v: str) -> str:
    """Validate namespace and database identifiers."""
    if not v:
      raise ValueError('Identifier cannot be empty')
    if not v.replace('_', '').replace('-', '').isalnum():
      raise ValueError('Identifier must be alphanumeric with optional underscores/hyphens')
    return v

  @field_validator('url')
  @classmethod
  def validate_url(cls, v: str) -> str:
    """Validate connection URL format."""
    if not v:
      raise ValueError('URL cannot be empty')
    if not any(v.startswith(proto) for proto in ['ws://', 'wss://', 'http://', 'https://']):
      raise ValueError('URL must use ws://, wss://, http://, or https:// protocol')
    return v

  @field_validator('retry_max_wait')
  @classmethod
  def validate_retry_wait(cls, v: float, info: Any) -> float:
    """Ensure max wait is greater than min wait."""
    data = info.data if hasattr(info, 'data') else {}
    min_wait = data.get('retry_min_wait')
    if min_wait is not None and v <= min_wait:
      raise ValueError('retry_max_wait must be greater than retry_min_wait')
    return v
