"""Application settings using Pydantic BaseSettings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from reverie.connection.config import ConnectionConfig


class Settings(BaseSettings):
  """Application settings loaded from environment variables."""

  model_config = SettingsConfigDict(
    env_file='.env',
    env_file_encoding='utf-8',
    case_sensitive=False,
    extra='ignore',
  )

  environment: Literal['development', 'staging', 'production'] = 'development'
  debug: bool = True
  log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'] = 'INFO'
  app_name: str = 'reverie'
  version: str = '0.1.0'

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
