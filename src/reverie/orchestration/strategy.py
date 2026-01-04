"""Deployment strategies for multi-database orchestration.

This module implements various deployment strategies for rolling out
migrations across multiple database instances.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from enum import Enum

import structlog
from pydantic import BaseModel, ConfigDict, Field

from reverie.connection.client import get_client
from reverie.migration.executor import MigrationExecutionError, execute_migration
from reverie.migration.models import Migration, MigrationDirection
from reverie.orchestration.config import EnvironmentConfig

logger = structlog.get_logger(__name__)


class DeploymentStatus(Enum):
  """Status of a deployment operation."""

  PENDING = 'pending'
  IN_PROGRESS = 'in_progress'
  SUCCESS = 'success'
  FAILED = 'failed'
  ROLLED_BACK = 'rolled_back'


class DeploymentResult(BaseModel):
  """Result of deploying to a single environment.

  Examples:
    >>> result = DeploymentResult(
    ...   environment='production',
    ...   status=DeploymentStatus.SUCCESS,
    ...   started_at=datetime.now(UTC),
    ...   completed_at=datetime.now(UTC),
    ... )
  """

  environment: str = Field(..., description='Environment name')
  status: DeploymentStatus = Field(..., description='Deployment status')
  started_at: datetime = Field(..., description='Start time')
  completed_at: datetime | None = Field(None, description='Completion time')
  error: str | None = Field(None, description='Error message if failed')
  execution_time_ms: int | None = Field(None, description='Execution time in milliseconds')
  migrations_applied: int = Field(default=0, description='Number of migrations applied')

  model_config = ConfigDict(frozen=True)

  @property
  def duration_seconds(self) -> float | None:
    """Get deployment duration in seconds."""
    if self.completed_at:
      return (self.completed_at - self.started_at).total_seconds()
    return None


class DeploymentStrategy(ABC):
  """Base class for deployment strategies."""

  def __init__(self, dry_run: bool = False) -> None:
    """Initialize strategy.

    Args:
      dry_run: If True, simulate deployment without executing
    """
    self.dry_run = dry_run

  @abstractmethod
  async def deploy(
    self,
    environments: list[EnvironmentConfig],
    migrations: list[Migration],
  ) -> list[DeploymentResult]:
    """Deploy migrations to environments.

    Args:
      environments: List of target environments
      migrations: Migrations to deploy

    Returns:
      List of deployment results
    """
    pass

  async def _deploy_to_environment(
    self,
    env: EnvironmentConfig,
    migrations: list[Migration],
  ) -> DeploymentResult:
    """Deploy migrations to a single environment.

    Args:
      env: Environment configuration
      migrations: Migrations to deploy

    Returns:
      Deployment result
    """
    started_at = datetime.now(UTC)

    if self.dry_run:
      # Simulate deployment
      await asyncio.sleep(0.1)
      return DeploymentResult(
        environment=env.name,
        status=DeploymentStatus.SUCCESS,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        error=None,
        execution_time_ms=100,
        migrations_applied=len(migrations),
      )

    try:
      logger.info('deploying_to_environment', environment=env.name, migrations=len(migrations))

      async with get_client(env.connection) as client:
        for migration in migrations:
          await execute_migration(client, migration, MigrationDirection.UP)

      completed_at = datetime.now(UTC)
      execution_time_ms = int((completed_at - started_at).total_seconds() * 1000)

      logger.info(
        'deployment_successful',
        environment=env.name,
        execution_time_ms=execution_time_ms,
      )

      return DeploymentResult(
        environment=env.name,
        status=DeploymentStatus.SUCCESS,
        started_at=started_at,
        completed_at=completed_at,
        error=None,
        execution_time_ms=execution_time_ms,
        migrations_applied=len(migrations),
      )

    except MigrationExecutionError as e:
      logger.error('deployment_failed', environment=env.name, error=str(e))
      return DeploymentResult(
        environment=env.name,
        status=DeploymentStatus.FAILED,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        error=str(e),
        execution_time_ms=None,
        migrations_applied=0,
      )

    except Exception as e:
      logger.error('unexpected_deployment_error', environment=env.name, error=str(e))
      return DeploymentResult(
        environment=env.name,
        status=DeploymentStatus.FAILED,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        error=f'Unexpected error: {e}',
        execution_time_ms=None,
        migrations_applied=0,
      )


class SequentialStrategy(DeploymentStrategy):
  """Deploy to environments one at a time in order.

  Examples:
    >>> strategy = SequentialStrategy()
    >>> results = await strategy.deploy([env1, env2], migrations)
  """

  async def deploy(
    self,
    environments: list[EnvironmentConfig],
    migrations: list[Migration],
  ) -> list[DeploymentResult]:
    """Deploy sequentially to each environment.

    Args:
      environments: List of target environments
      migrations: Migrations to deploy

    Returns:
      List of deployment results
    """
    logger.info('sequential_deployment_started', environments=len(environments))
    results: list[DeploymentResult] = []

    for env in environments:
      result = await self._deploy_to_environment(env, migrations)
      results.append(result)

      # Stop on first failure
      if result.status == DeploymentStatus.FAILED:
        logger.warning('sequential_deployment_stopped_on_failure', environment=env.name)
        break

    return results


class ParallelStrategy(DeploymentStrategy):
  """Deploy to all environments in parallel.

  Examples:
    >>> strategy = ParallelStrategy(max_concurrent=3)
    >>> results = await strategy.deploy([env1, env2, env3], migrations)
  """

  def __init__(self, max_concurrent: int = 5, dry_run: bool = False) -> None:
    """Initialize parallel strategy.

    Args:
      max_concurrent: Maximum concurrent deployments
      dry_run: If True, simulate deployment
    """
    super().__init__(dry_run)
    self.max_concurrent = max_concurrent

  async def deploy(
    self,
    environments: list[EnvironmentConfig],
    migrations: list[Migration],
  ) -> list[DeploymentResult]:
    """Deploy in parallel to all environments.

    Args:
      environments: List of target environments
      migrations: Migrations to deploy

    Returns:
      List of deployment results
    """
    logger.info(
      'parallel_deployment_started',
      environments=len(environments),
      max_concurrent=self.max_concurrent,
    )

    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(self.max_concurrent)

    async def deploy_with_semaphore(env: EnvironmentConfig) -> DeploymentResult:
      async with semaphore:
        return await self._deploy_to_environment(env, migrations)

    # Execute all deployments in parallel
    tasks = [deploy_with_semaphore(env) for env in environments]
    results = await asyncio.gather(*tasks)

    return list(results)


class RollingStrategy(DeploymentStrategy):
  """Deploy in batches with verification between batches.

  Examples:
    >>> strategy = RollingStrategy(batch_size=2)
    >>> results = await strategy.deploy([env1, env2, env3, env4], migrations)
  """

  def __init__(self, batch_size: int = 1, dry_run: bool = False) -> None:
    """Initialize rolling strategy.

    Args:
      batch_size: Number of environments to deploy per batch
      dry_run: If True, simulate deployment
    """
    super().__init__(dry_run)
    self.batch_size = batch_size

  async def deploy(
    self,
    environments: list[EnvironmentConfig],
    migrations: list[Migration],
  ) -> list[DeploymentResult]:
    """Deploy in rolling batches.

    Args:
      environments: List of target environments
      migrations: Migrations to deploy

    Returns:
      List of deployment results
    """
    logger.info(
      'rolling_deployment_started',
      environments=len(environments),
      batch_size=self.batch_size,
    )

    results: list[DeploymentResult] = []

    # Deploy in batches
    for i in range(0, len(environments), self.batch_size):
      batch = environments[i : i + self.batch_size]

      logger.info(
        'deploying_batch',
        batch_num=i // self.batch_size + 1,
        batch_size=len(batch),
      )

      # Deploy batch in parallel
      tasks = [self._deploy_to_environment(env, migrations) for env in batch]
      batch_results = await asyncio.gather(*tasks)
      results.extend(batch_results)

      # Check for failures in batch
      batch_failed = any(r.status == DeploymentStatus.FAILED for r in batch_results)
      if batch_failed:
        logger.error('batch_failed_stopping')
        break

      # Add delay between batches for stability
      if i + self.batch_size < len(environments):
        await asyncio.sleep(1.0)

    return results


class CanaryStrategy(DeploymentStrategy):
  """Deploy to canary environments first, then remaining.

  Examples:
    >>> strategy = CanaryStrategy(canary_percentage=20.0)
    >>> results = await strategy.deploy([env1, env2, env3, env4, env5], migrations)
  """

  def __init__(self, canary_percentage: float = 10.0, dry_run: bool = False) -> None:
    """Initialize canary strategy.

    Args:
      canary_percentage: Percentage of environments for canary (1-50)
      dry_run: If True, simulate deployment
    """
    super().__init__(dry_run)
    if not 1.0 <= canary_percentage <= 50.0:
      raise ValueError('canary_percentage must be between 1.0 and 50.0')
    self.canary_percentage = canary_percentage

  async def deploy(
    self,
    environments: list[EnvironmentConfig],
    migrations: list[Migration],
  ) -> list[DeploymentResult]:
    """Deploy to canary environments first, then remainder.

    Args:
      environments: List of target environments
      migrations: Migrations to deploy

    Returns:
      List of deployment results
    """
    logger.info(
      'canary_deployment_started',
      environments=len(environments),
      canary_percentage=self.canary_percentage,
    )

    # Calculate canary count
    canary_count = max(1, int(len(environments) * self.canary_percentage / 100))
    canary_envs = environments[:canary_count]
    remaining_envs = environments[canary_count:]

    logger.info('deploying_to_canary', canary_count=canary_count)

    # Deploy to canary environments
    canary_tasks = [self._deploy_to_environment(env, migrations) for env in canary_envs]
    canary_results = await asyncio.gather(*canary_tasks)

    # Check canary success
    canary_failed = any(r.status == DeploymentStatus.FAILED for r in canary_results)

    if canary_failed:
      logger.error('canary_deployment_failed')
      return canary_results

    logger.info('canary_successful_proceeding', remaining=len(remaining_envs))

    # Deploy to remaining environments
    remaining_tasks = [self._deploy_to_environment(env, migrations) for env in remaining_envs]
    remaining_results = await asyncio.gather(*remaining_tasks)

    return canary_results + remaining_results
