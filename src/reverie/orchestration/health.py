"""Health checking for database instances.

This module provides health check functionality for verifying database
connectivity and migration state.
"""

import structlog
from pydantic import BaseModel, ConfigDict, Field

from reverie.connection.client import ConnectionError as ClientConnectionError
from reverie.connection.client import QueryError, get_client
from reverie.orchestration.config import EnvironmentConfig

logger = structlog.get_logger(__name__)


class HealthStatus(BaseModel):
  """Health status for a database instance.

  Examples:
    >>> status = HealthStatus(
    ...   environment='production',
    ...   is_healthy=True,
    ...   can_connect=True,
    ...   migration_table_exists=True,
    ... )
  """

  environment: str = Field(..., description='Environment name')
  is_healthy: bool = Field(..., description='Overall health status')
  can_connect: bool = Field(..., description='Can connect to database')
  migration_table_exists: bool = Field(default=False, description='Migration table exists')
  error: str | None = Field(None, description='Error message if unhealthy')

  model_config = ConfigDict(frozen=True)


class HealthCheck:
  """Health check operations for database environments.

  Examples:
    >>> health = HealthCheck()
    >>> status = await health.check_environment(env_config)
    >>> if status.is_healthy:
    ...   print('Environment is healthy')
  """

  async def check_environment(self, env: EnvironmentConfig) -> HealthStatus:
    """Check health of a single environment.

    Args:
      env: Environment configuration

    Returns:
      Health status

    Examples:
      >>> status = await health.check_environment(prod_config)
      >>> print(f'Healthy: {status.is_healthy}')
    """
    can_connect = await self.check_connectivity(env)

    if not can_connect:
      return HealthStatus(
        environment=env.name,
        is_healthy=False,
        can_connect=False,
        migration_table_exists=False,
        error='Cannot connect to database',
      )

    migration_table_exists = await self.check_migration_table(env)

    return HealthStatus(
      environment=env.name,
      is_healthy=True,
      can_connect=True,
      migration_table_exists=migration_table_exists,
      error=None,
    )

  async def check_connectivity(self, env: EnvironmentConfig) -> bool:
    """Check if database is reachable.

    Args:
      env: Environment configuration

    Returns:
      True if database is reachable, False otherwise

    Examples:
      >>> can_connect = await health.check_connectivity(env_config)
    """
    try:
      async with get_client(env.connection) as client:
        await client.execute('RETURN 1')
        logger.debug('connectivity_check_passed', environment=env.name)
        return True
    except (ClientConnectionError, QueryError) as e:
      logger.warning('connectivity_check_failed', environment=env.name, error=str(e))
      return False
    except Exception as e:
      logger.error('unexpected_connectivity_error', environment=env.name, error=str(e))
      return False

  async def check_migration_table(self, env: EnvironmentConfig) -> bool:
    """Check if migration history table exists.

    Args:
      env: Environment configuration

    Returns:
      True if migration table exists, False otherwise

    Examples:
      >>> table_exists = await health.check_migration_table(env_config)
    """
    try:
      async with get_client(env.connection) as client:
        # Try to query migration history table
        await client.execute('SELECT * FROM _migration_history LIMIT 1')
        logger.debug('migration_table_exists', environment=env.name)
        return True
    except QueryError:
      logger.debug('migration_table_not_found', environment=env.name)
      return False
    except Exception as e:
      logger.error('migration_table_check_error', environment=env.name, error=str(e))
      return False

  async def check_schema_integrity(self, env: EnvironmentConfig) -> dict[str, bool]:
    """Perform comprehensive schema integrity check.

    Args:
      env: Environment configuration

    Returns:
      Dictionary of check results

    Examples:
      >>> integrity = await health.check_schema_integrity(env_config)
      >>> if not integrity['migration_table']:
      ...   print('Migration table missing')
    """
    checks: dict[str, bool] = {
      'connectivity': False,
      'migration_table': False,
    }

    # Check connectivity
    checks['connectivity'] = await self.check_connectivity(env)

    if not checks['connectivity']:
      return checks

    # Check migration table
    checks['migration_table'] = await self.check_migration_table(env)

    return checks

  async def verify_all_environments(
    self,
    environments: list[EnvironmentConfig],
  ) -> dict[str, HealthStatus]:
    """Verify health of multiple environments.

    Args:
      environments: List of environment configurations

    Returns:
      Dictionary mapping environment name to health status

    Examples:
      >>> statuses = await health.verify_all_environments([env1, env2])
      >>> for name, status in statuses.items():
      ...   print(f'{name}: {status.is_healthy}')
    """
    results: dict[str, HealthStatus] = {}

    for env in environments:
      status = await self.check_environment(env)
      results[env.name] = status
      logger.info(
        'environment_health_checked',
        environment=env.name,
        is_healthy=status.is_healthy,
      )

    return results


async def check_environment_health(env: EnvironmentConfig) -> HealthStatus:
  """Convenience function to check environment health.

  Args:
    env: Environment configuration

  Returns:
    Health status

  Examples:
    >>> status = await check_environment_health(prod_config)
    >>> assert status.is_healthy
  """
  health = HealthCheck()
  return await health.check_environment(env)


async def verify_connectivity(env: EnvironmentConfig) -> bool:
  """Convenience function to verify connectivity.

  Args:
    env: Environment configuration

  Returns:
    True if can connect, False otherwise

  Examples:
    >>> can_connect = await verify_connectivity(env_config)
  """
  health = HealthCheck()
  return await health.check_connectivity(env)
