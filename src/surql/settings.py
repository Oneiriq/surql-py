"""Application settings using Pydantic BaseSettings.

This module provides configuration management for surql with support for
multiple configuration sources in priority order:

1. Environment variables (SURQL_* prefix)
2. .env file
3. pyproject.toml [tool.surql] section
4. Default values

Example pyproject.toml configuration:
    [tool.surql]
    migration_path = "db/migrations"
    environment = "production"
"""

import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from surql.connection.config import ConnectionConfig


class PyProjectTomlSettingsSource(PydanticBaseSettingsSource):
  """Custom settings source that reads from pyproject.toml [tool.surql] section."""

  def __init__(self, settings_cls: type[BaseSettings]) -> None:
    super().__init__(settings_cls)
    self._toml_data: dict[str, Any] = {}
    self._load_toml()

  def _load_toml(self) -> None:
    """Load TOML configuration from pyproject.toml."""
    pyproject_path = Path.cwd() / 'pyproject.toml'
    if pyproject_path.exists():
      try:
        with open(pyproject_path, 'rb') as f:
          data = tomllib.load(f)
        self._toml_data = data.get('tool', {}).get('surql', {})
      except (tomllib.TOMLDecodeError, OSError):
        self._toml_data = {}

  def get_field_value(
    self,
    _field: Any,
    field_name: str,
  ) -> tuple[Any, str, bool]:
    """Get field value from TOML configuration.

    Args:
      _field: The field definition (unused, required by base class)
      field_name: Name of the field

    Returns:
      Tuple of (value, field_name, is_complex)
    """
    value = self._toml_data.get(field_name)
    return value, field_name, False

  def __call__(self) -> dict[str, Any]:
    """Return all TOML settings as a dictionary."""
    return self._toml_data


class Settings(BaseSettings):
  """Application settings loaded from environment variables and pyproject.toml.

  Configuration is loaded from multiple sources in priority order:
  1. Environment variables (highest priority)
  2. .env file
  3. pyproject.toml [tool.surql] section
  4. Default values (lowest priority)

  Example:
    Environment variable: SURQL_MIGRATION_PATH=/custom/path
    .env file: SURQL_MIGRATION_PATH=/custom/path
    pyproject.toml: [tool.surql] migration_path = "db/migrations"
  """

  model_config = SettingsConfigDict(
    env_prefix='SURQL_',
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
    default='surql',
    description='Application name',
  )
  version: str = Field(
    default='0.6.0',
    description='Application version',
  )

  migration_path: Path = Field(
    default=Path('migrations'),
    description='Path to migrations directory (relative to project root)',
  )

  database: ConnectionConfig = Field(
    default_factory=ConnectionConfig,
    description='Database connection configuration',
  )

  @classmethod
  def settings_customise_sources(
    cls,
    settings_cls: type[BaseSettings],
    init_settings: PydanticBaseSettingsSource,
    env_settings: PydanticBaseSettingsSource,
    dotenv_settings: PydanticBaseSettingsSource,
    file_secret_settings: PydanticBaseSettingsSource,
  ) -> tuple[PydanticBaseSettingsSource, ...]:
    """Customize settings sources to include pyproject.toml.

    Priority order (first wins):
    1. init_settings - programmatic settings
    2. env_settings - environment variables
    3. dotenv_settings - .env file
    4. pyproject_toml - pyproject.toml [tool.surql]
    5. file_secret_settings - secret files

    Returns:
      Tuple of settings sources in priority order
    """
    return (
      init_settings,
      env_settings,
      dotenv_settings,
      PyProjectTomlSettingsSource(settings_cls),
      file_secret_settings,
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


def get_migration_path() -> Path:
  """Get the configured migration path.

  Returns the migration path from configuration sources in priority order:
  1. SURQL_MIGRATION_PATH environment variable
  2. .env file
  3. pyproject.toml [tool.surql] migration_path
  4. Default: ./migrations

  Returns:
    Path to migrations directory (may be relative or absolute)

  Example:
    ```python
    from surql.settings import get_migration_path

    migrations_dir = get_migration_path()
    print(f'Migrations directory: {migrations_dir}')
    ```
  """
  return get_settings().migration_path
