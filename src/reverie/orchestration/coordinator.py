"""Migration deployment coordinator.

This module coordinates migration deployment across multiple database
environments using various deployment strategies.
"""

from typing import Any

import structlog
from pydantic import BaseModel, ConfigDict, Field

from reverie.connection.client import get_client
from reverie.migration.executor import MigrationExecutionError, execute_migration
from reverie.migration.models import Migration, MigrationDirection
from reverie.orchestration.config import EnvironmentConfig, EnvironmentRegistry
from reverie.orchestration.health import HealthCheck
from reverie.orchestration.strategy import (
  CanaryStrategy,
  DeploymentResult,
  DeploymentStatus,
  DeploymentStrategy,
  ParallelStrategy,
  RollingStrategy,
  SequentialStrategy,
)

logger = structlog.get_logger(__name__)


class OrchestrationError(Exception):
  """Raised when orchestration fails."""

  pass


class DeploymentPlan(BaseModel):
  """Plan for deploying migrations across environments.

  Examples:
    >>> plan = DeploymentPlan(
    ...   environments=['staging', 'production'],
    ...   migrations=[migration1, migration2],
    ...   strategy='sequential',
    ... )
  """

  environments: list[str] = Field(..., description='Target environment names')
  migrations: list[Migration] = Field(..., description='Migrations to deploy')
  strategy: str = Field(default='sequential', description='Deployment strategy')
  batch_size: int = Field(default=1, ge=1, description='Batch size for rolling strategy')
  canary_percentage: float = Field(
    default=10.0,
    ge=1.0,
    le=50.0,
    description='Canary percentage for canary strategy',
  )
  max_concurrent: int = Field(default=5, ge=1, description='Max concurrent for parallel strategy')
  verify_health: bool = Field(default=True, description='Verify health before deployment')
  auto_rollback: bool = Field(default=True, description='Auto rollback on failure')
  dry_run: bool = Field(default=False, description='Simulate without executing')

  model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class MigrationCoordinator:
  """Coordinates migration deployment across multiple environments.

  Examples:
    >>> coordinator = MigrationCoordinator(registry)
    >>> result = await coordinator.deploy_to_environments(
    ...   environments=['staging', 'production'],
    ...   migrations=[migration1, migration2],
    ...   strategy='rolling',
    ... )
  """

  def __init__(self, registry: EnvironmentRegistry) -> None:
    """Initialize coordinator.

    Args:
      registry: Environment registry
    """
    self.registry = registry
    self.health_check = HealthCheck()

  async def deploy_to_environments(
    self,
    environments: list[str],
    migrations: list[Migration],
    strategy: str = 'sequential',
    batch_size: int = 1,
    canary_percentage: float = 10.0,
    max_concurrent: int = 5,
    verify_health: bool = True,
    auto_rollback: bool = True,
    dry_run: bool = False,
  ) -> dict[str, DeploymentResult]:
    """Deploy migrations to multiple environments.

    Args:
      environments: List of environment names
      migrations: Migrations to deploy
      strategy: Deployment strategy (sequential, parallel, rolling, canary)
      batch_size: Batch size for rolling deployments
      canary_percentage: Percentage for canary deployments
      max_concurrent: Maximum concurrent deployments for parallel strategy
      verify_health: Verify health before deployment
      auto_rollback: Auto rollback on failure
      dry_run: Simulate deployment without executing

    Returns:
      Dictionary mapping environment name to deployment result

    Raises:
      OrchestrationError: If deployment fails critically
    """
    logger.info(
      'orchestration_started',
      environments=environments,
      migrations=len(migrations),
      strategy=strategy,
      dry_run=dry_run,
    )

    # Get environment configurations
    env_configs: list[EnvironmentConfig] = []
    for env_name in environments:
      config = self.registry.get_environment(env_name)
      if not config:
        raise OrchestrationError(f'Environment not found: {env_name}')
      env_configs.append(config)

    # Verify health if requested
    if verify_health and not dry_run:
      logger.info('verifying_environment_health')
      health_statuses = await self.health_check.verify_all_environments(env_configs)

      unhealthy = [name for name, status in health_statuses.items() if not status.is_healthy]
      if unhealthy:
        raise OrchestrationError(f'Unhealthy environments: {", ".join(unhealthy)}')

    # Create deployment strategy
    deployment_strategy = self._create_strategy(
      strategy=strategy,
      batch_size=batch_size,
      canary_percentage=canary_percentage,
      max_concurrent=max_concurrent,
      dry_run=dry_run,
    )

    # Execute deployment
    try:
      results = await deployment_strategy.deploy(env_configs, migrations)
    except Exception as e:
      logger.error('deployment_execution_failed', error=str(e))
      raise OrchestrationError(f'Deployment failed: {e}') from e

    # Convert to dictionary
    result_dict = {result.environment: result for result in results}

    # Check for failures
    failures = [r for r in results if r.status == DeploymentStatus.FAILED]

    if failures and auto_rollback and not dry_run:
      logger.warning('initiating_auto_rollback', failures=len(failures))
      await self._rollback_deployments(env_configs, migrations, results)

    logger.info(
      'orchestration_completed',
      total=len(results),
      successful=sum(1 for r in results if r.status == DeploymentStatus.SUCCESS),
      failed=len(failures),
    )

    return result_dict

  def _create_strategy(
    self,
    strategy: str,
    batch_size: int,
    canary_percentage: float,
    max_concurrent: int,
    dry_run: bool,
  ) -> DeploymentStrategy:
    """Create deployment strategy instance.

    Args:
      strategy: Strategy name
      batch_size: Batch size for rolling
      canary_percentage: Canary percentage
      max_concurrent: Max concurrent for parallel
      dry_run: Dry run mode

    Returns:
      DeploymentStrategy instance
    """
    strategy_lower = strategy.lower()

    if strategy_lower == 'sequential':
      return SequentialStrategy(dry_run=dry_run)
    elif strategy_lower == 'parallel':
      return ParallelStrategy(max_concurrent=max_concurrent, dry_run=dry_run)
    elif strategy_lower == 'rolling':
      return RollingStrategy(batch_size=batch_size, dry_run=dry_run)
    elif strategy_lower == 'canary':
      return CanaryStrategy(canary_percentage=canary_percentage, dry_run=dry_run)
    else:
      raise ValueError(
        f'Unknown strategy: {strategy}. Must be one of: sequential, parallel, rolling, canary'
      )

  async def _rollback_deployments(
    self,
    env_configs: list[EnvironmentConfig],
    migrations: list[Migration],
    results: list[DeploymentResult],
  ) -> None:
    """Rollback successful deployments.

    Args:
      env_configs: Environment configurations
      migrations: Migrations that were deployed
      results: Deployment results
    """
    logger.warning('rolling_back_deployments')

    # Find successful deployments
    successful = [r for r in results if r.status == DeploymentStatus.SUCCESS]

    for result in successful:
      # Find environment config
      env = next((e for e in env_configs if e.name == result.environment), None)
      if not env:
        continue

      try:
        logger.info('rolling_back_environment', environment=result.environment)

        async with get_client(env.connection) as client:
          # Rollback in reverse order
          for migration in reversed(migrations):
            try:
              await execute_migration(client, migration, MigrationDirection.DOWN)
            except MigrationExecutionError as e:
              logger.error(
                'rollback_migration_failed',
                environment=result.environment,
                migration=migration.version,
                error=str(e),
              )

        logger.info('environment_rolled_back', environment=result.environment)

      except Exception as e:
        logger.error(
          'rollback_failed',
          environment=result.environment,
          error=str(e),
        )

  async def get_deployment_status(
    self,
    environments: list[str],
  ) -> dict[str, bool]:
    """Get current deployment status for environments.

    Args:
      environments: Environment names to check

    Returns:
      Dictionary mapping environment to healthy status
    """
    env_configs: list[EnvironmentConfig] = []
    for env_name in environments:
      config = self.registry.get_environment(env_name)
      if config:
        env_configs.append(config)

    health_statuses = await self.health_check.verify_all_environments(env_configs)
    return {name: status.is_healthy for name, status in health_statuses.items()}


async def deploy_to_environments(
  registry: EnvironmentRegistry,
  environments: list[str],
  migrations: list[Migration],
  strategy: str = 'sequential',
  **kwargs: Any,
) -> dict[str, DeploymentResult]:
  """Convenience function for deploying to environments.

  Args:
    registry: Environment registry
    environments: Target environment names
    migrations: Migrations to deploy
    strategy: Deployment strategy
    **kwargs: Additional deployment options

  Returns:
    Dictionary mapping environment name to deployment result

  Examples:
    >>> results = await deploy_to_environments(
    ...   registry,
    ...   ['staging', 'production'],
    ...   migrations,
    ...   strategy='rolling',
    ...   batch_size=2,
    ... )
  """
  coordinator = MigrationCoordinator(registry)
  return await coordinator.deploy_to_environments(
    environments=environments,
    migrations=migrations,
    strategy=strategy,
    **kwargs,
  )
