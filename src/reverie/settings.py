"""Application settings using Pydantic BaseSettings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from reverie.connection.config import ConnectionConfig


class Settings(BaseSettings):
  """Application settings loaded from environment variables."""

  model_config = SettingsConfigDict(
    env_prefix='REVERIE_',
    env_file='.env',
    env_file_encoding='utf-8',
    case_sensitive=False,
    extra='ignore',
  )

  environment: Literal['development', 'staging', 'production'] = Field(
    default='development',
    description='Application environment',
  )
  debug: bool = Field(
    default=True,
    description='Debug mode enabled',
  )
  log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'] = Field(
    default='INFO',
    description='Logging level',
  )
  app_name: str = Field(
    default='reverie',
    description='Application name',
  )
  version: str = Field(
    default='0.1.0',
    description='Application version',
  )

  database: ConnectionConfig = Field(
    default_factory=ConnectionConfig,
    description='Database connection configuration',
  )


@lru_cache
def get_settings() -> Settings:
  """Get cached settings instance."""
  return Settings()


@lru_cache
def get_db_config() -> ConnectionConfig:
  """Get cached database configuration instance.

  Returns:
    Database connection configuration

  Example:
    ```python
    config = get_db_config()
    async with get_client(config) as client:
      await client.execute('SELECT * FROM user')
    ```
  """
  return get_settings().database
