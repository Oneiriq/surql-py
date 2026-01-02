"""Application settings using Pydantic BaseSettings."""

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8', case_sensitive=False
    )

    environment: Literal['development', 'staging', 'production'] = 'development'
    debug: bool = True
    log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'] = 'INFO'
    app_name: str = 'python-uv-simple'
    version: str = '0.1.0'


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
