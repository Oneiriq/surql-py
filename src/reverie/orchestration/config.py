"""Environment configuration for multi-database orchestration.

This module provides environment and instance configuration management
for deploying migrations across multiple database instances.
"""

from pathlib import Path

import structlog
from pydantic import BaseModel, ConfigDict, Field, field_validator

from reverie.connection.config import ConnectionConfig

logger = structlog.get_logger(__name__)


class EnvironmentConfig(BaseModel):
  """Configuration for a database environment with connection details.

  Examples:
    >>> config = EnvironmentConfig(
    ...   name='production',
    ...   connection=ConnectionConfig(
    ...     db_url='ws://prod.example.com:8000/rpc',
    ...     db_ns='production',
    ...     db='main',
    ...   ),
    ...   priority=1,
    ...   tags={'prod', 'critical'},
    ... )
  """

  name: str = Field(..., description='Environment name (e.g., production, staging)')
  connection: ConnectionConfig = Field(..., description='Database connection configuration')
  priority: int = Field(
    default=100,
    ge=0,
    description='Deployment priority (lower = higher priority)',
  )
  tags: set[str] = Field(
    default_factory=set,
    description='Tags for environment categorization',
  )
  require_approval: bool = Field(
    default=False,
    description='Require manual approval before deployment',
  )
  allow_destructive: bool = Field(
    default=True,
    description='Allow destructive migrations (DROP, etc.)',
  )

  model_config = ConfigDict(frozen=True)

  @field_validator('name')
  @classmethod
  def validate_name(cls, v: str) -> str:
    """Validate environment name."""
    if not v:
      raise ValueError('Environment name cannot be empty')
    if not v.replace('_', '').replace('-', '').isalnum():
      raise ValueError('Environment name must be alphanumeric with optional underscores/hyphens')
    return v


class EnvironmentRegistry:
  """Registry for managing multiple database environments.

  Examples:
    >>> registry = EnvironmentRegistry()
    >>> registry.register_environment(
    ...   name='production',
    ...   connection=prod_config,
    ...   priority=1,
    ... )
    >>> env = registry.get_environment('production')
  """

  def __init__(self) -> None:
    """Initialize empty registry."""
    self._environments: dict[str, EnvironmentConfig] = {}

  def register_environment(
    self,
    name: str,
    connection: ConnectionConfig,
    priority: int = 100,
    tags: set[str] | None = None,
    require_approval: bool = False,
    allow_destructive: bool = True,
  ) -> None:
    """Register a new environment.

    Args:
      name: Environment name
      connection: Connection configuration
      priority: Deployment priority (lower = higher priority)
      tags: Optional tags for categorization
      require_approval: Require manual approval
      allow_destructive: Allow destructive migrations
    """
    env_config = EnvironmentConfig(
      name=name,
      connection=connection,
      priority=priority,
      tags=tags or set(),
      require_approval=require_approval,
      allow_destructive=allow_destructive,
    )
    self._environments[name] = env_config
    logger.info('environment_registered', name=name, priority=priority)

  def unregister_environment(self, name: str) -> None:
    """Unregister an environment.

    Args:
      name: Environment name
    """
    if name in self._environments:
      del self._environments[name]
      logger.info('environment_unregistered', name=name)

  def get_environment(self, name: str) -> EnvironmentConfig | None:
    """Get environment configuration by name.

    Args:
      name: Environment name

    Returns:
      Environment configuration or None if not found
    """
    return self._environments.get(name)

  def list_environments(self) -> list[str]:
    """List all registered environment names.

    Returns:
      List of environment names sorted by priority
    """
    envs = sorted(self._environments.values(), key=lambda e: e.priority)
    return [env.name for env in envs]

  def get_environments_by_tag(self, tag: str) -> list[EnvironmentConfig]:
    """Get all environments with a specific tag.

    Args:
      tag: Tag to filter by

    Returns:
      List of matching environment configurations
    """
    return [env for env in self._environments.values() if tag in env.tags]

  @classmethod
  def from_config_file(cls, path: Path) -> 'EnvironmentRegistry':
    """Load registry from JSON configuration file.

    Args:
      path: Path to configuration file

    Returns:
      Populated EnvironmentRegistry

    Examples:
      >>> registry = EnvironmentRegistry.from_config_file(Path('environments.json'))
    """
    import json

    registry = cls()

    if not path.exists():
      logger.warning('config_file_not_found', path=str(path))
      return registry

    # Load configuration
    config_data = json.loads(path.read_text())

    # Parse and register each environment
    for env_data in config_data.get('environments', []):
      # Extract connection config
      conn_data = env_data.get('connection', {})
      connection = ConnectionConfig(**conn_data)

      # Register environment
      registry.register_environment(
        name=env_data['name'],
        connection=connection,
        priority=env_data.get('priority', 100),
        tags=set(env_data.get('tags', [])),
        require_approval=env_data.get('require_approval', False),
        allow_destructive=env_data.get('allow_destructive', True),
      )

    logger.info('registry_loaded_from_file', path=str(path), count=len(registry._environments))
    return registry


# Global registry instance
_global_registry: EnvironmentRegistry | None = None


def get_registry() -> EnvironmentRegistry:
  """Get global environment registry.

  Returns:
    Global EnvironmentRegistry instance
  """
  global _global_registry
  if _global_registry is None:
    _global_registry = EnvironmentRegistry()
  return _global_registry


def set_registry(registry: EnvironmentRegistry) -> None:
  """Set global environment registry.

  Args:
    registry: Registry instance to set as global
  """
  global _global_registry
  _global_registry = registry


def configure_environments(config_path: Path) -> None:
  """Configure global registry from file.

  Args:
    config_path: Path to configuration file
  """
  global _global_registry
  _global_registry = EnvironmentRegistry.from_config_file(config_path)


def register_environment(
  name: str,
  connection: ConnectionConfig,
  priority: int = 100,
  tags: set[str] | None = None,
  require_approval: bool = False,
  allow_destructive: bool = True,
) -> None:
  """Register an environment in the global registry.

  Args:
    name: Environment name
    connection: Connection configuration
    priority: Deployment priority (lower = higher priority)
    tags: Optional tags for categorization
    require_approval: Require manual approval
    allow_destructive: Allow destructive migrations
  """
  registry = get_registry()
  registry.register_environment(
    name=name,
    connection=connection,
    priority=priority,
    tags=tags,
    require_approval=require_approval,
    allow_destructive=allow_destructive,
  )
