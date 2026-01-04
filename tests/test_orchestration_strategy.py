"""Tests for orchestration deployment strategies."""

import pytest

from reverie.connection.config import ConnectionConfig
from reverie.migration.models import Migration
from reverie.orchestration.config import EnvironmentConfig
from reverie.orchestration.strategy import (
  CanaryStrategy,
  DeploymentResult,
  DeploymentStatus,
  ParallelStrategy,
  RollingStrategy,
  SequentialStrategy,
)


@pytest.fixture
def sample_environments() -> list[EnvironmentConfig]:
  """Create sample environment configurations."""
  environments = []
  for i in range(4):
    conn = ConnectionConfig(
      db_url='ws://localhost:8000/rpc',
      db_ns=f'test{i}',
      db='main',
    )
    env = EnvironmentConfig(name=f'env{i}', connection=conn)
    environments.append(env)
  return environments


@pytest.fixture
def sample_migrations() -> list[Migration]:
  """Create sample migrations."""
  from pathlib import Path

  def up() -> list[str]:
    return ['CREATE TABLE test SCHEMAFULL;']

  def down() -> list[str]:
    return ['REMOVE TABLE test;']

  migration = Migration(
    version='20260109_120000',
    description='Test migration',
    path=Path('test.py'),
    up=up,
    down=down,
    checksum='test123',
  )
  return [migration]


class TestDeploymentResult:
  """Tests for DeploymentResult."""

  def test_create_deployment_result(self) -> None:
    """Test creating deployment result."""
    from datetime import UTC, datetime

    started = datetime.now(UTC)
    completed = datetime.now(UTC)

    result = DeploymentResult(
      environment='production',
      status=DeploymentStatus.SUCCESS,
      started_at=started,
      completed_at=completed,
      migrations_applied=2,
    )

    assert result.environment == 'production'
    assert result.status == DeploymentStatus.SUCCESS
    assert result.migrations_applied == 2
    assert result.duration_seconds is not None
    assert result.duration_seconds >= 0

  def test_deployment_result_immutable(self) -> None:
    """Test that deployment result is immutable."""
    from datetime import UTC, datetime

    from pydantic import ValidationError

    result = DeploymentResult(
      environment='test',
      status=DeploymentStatus.SUCCESS,
      started_at=datetime.now(UTC),
    )

    with pytest.raises(ValidationError):
      result.status = DeploymentStatus.FAILED  # type: ignore


class TestSequentialStrategy:
  """Tests for SequentialStrategy."""

  @pytest.mark.asyncio
  async def test_sequential_dry_run(
    self,
    sample_environments: list[EnvironmentConfig],
    sample_migrations: list[Migration],
  ) -> None:
    """Test sequential strategy in dry run mode."""
    strategy = SequentialStrategy(dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)

    assert len(results) == len(sample_environments)
    assert all(r.status == DeploymentStatus.SUCCESS for r in results)
    assert all(r.migrations_applied == len(sample_migrations) for r in results)

  @pytest.mark.asyncio
  async def test_sequential_deployment_order(
    self,
    sample_environments: list[EnvironmentConfig],
    sample_migrations: list[Migration],
  ) -> None:
    """Test that sequential strategy deploys in order."""
    strategy = SequentialStrategy(dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)

    # Results should be in same order as environments
    for i, result in enumerate(results):
      assert result.environment == f'env{i}'


class TestParallelStrategy:
  """Tests for ParallelStrategy."""

  @pytest.mark.asyncio
  async def test_parallel_dry_run(
    self,
    sample_environments: list[EnvironmentConfig],
    sample_migrations: list[Migration],
  ) -> None:
    """Test parallel strategy in dry run mode."""
    strategy = ParallelStrategy(max_concurrent=2, dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)

    assert len(results) == len(sample_environments)
    assert all(r.status == DeploymentStatus.SUCCESS for r in results)

  @pytest.mark.asyncio
  async def test_parallel_max_concurrent(
    self,
    sample_environments: list[EnvironmentConfig],
    sample_migrations: list[Migration],
  ) -> None:
    """Test parallel strategy respects max_concurrent."""
    strategy = ParallelStrategy(max_concurrent=2, dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)

    # All should complete successfully
    assert len(results) == len(sample_environments)
    assert all(r.status == DeploymentStatus.SUCCESS for r in results)


class TestRollingStrategy:
  """Tests for RollingStrategy."""

  @pytest.mark.asyncio
  async def test_rolling_dry_run(
    self,
    sample_environments: list[EnvironmentConfig],
    sample_migrations: list[Migration],
  ) -> None:
    """Test rolling strategy in dry run mode."""
    strategy = RollingStrategy(batch_size=2, dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)

    assert len(results) == len(sample_environments)
    assert all(r.status == DeploymentStatus.SUCCESS for r in results)

  @pytest.mark.asyncio
  async def test_rolling_batch_size(
    self,
    sample_environments: list[EnvironmentConfig],
    sample_migrations: list[Migration],
  ) -> None:
    """Test rolling strategy with different batch sizes."""
    # Test batch size of 1
    strategy = RollingStrategy(batch_size=1, dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)
    assert len(results) == 4

    # Test batch size of 2
    strategy = RollingStrategy(batch_size=2, dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)
    assert len(results) == 4

    # Test batch size larger than environments
    strategy = RollingStrategy(batch_size=10, dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)
    assert len(results) == 4


class TestCanaryStrategy:
  """Tests for CanaryStrategy."""

  @pytest.mark.asyncio
  async def test_canary_dry_run(
    self,
    sample_environments: list[EnvironmentConfig],
    sample_migrations: list[Migration],
  ) -> None:
    """Test canary strategy in dry run mode."""
    strategy = CanaryStrategy(canary_percentage=25.0, dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)

    assert len(results) == len(sample_environments)
    assert all(r.status == DeploymentStatus.SUCCESS for r in results)

  @pytest.mark.asyncio
  async def test_canary_percentage_calculation(
    self,
    sample_environments: list[EnvironmentConfig],
    sample_migrations: list[Migration],
  ) -> None:
    """Test canary percentage calculation."""
    # With 4 environments and 25%, should deploy to 1 first (max(1, 4 * 0.25))
    strategy = CanaryStrategy(canary_percentage=25.0, dry_run=True)
    results = await strategy.deploy(sample_environments, sample_migrations)

    # Should deploy to all 4 eventually
    assert len(results) == 4

  def test_canary_percentage_validation(self) -> None:
    """Test canary percentage validation."""
    # Test valid percentages
    CanaryStrategy(canary_percentage=10.0)
    CanaryStrategy(canary_percentage=50.0)

    # Test invalid percentages
    with pytest.raises(ValueError):
      CanaryStrategy(canary_percentage=0.5)

    with pytest.raises(ValueError):
      CanaryStrategy(canary_percentage=51.0)

  @pytest.mark.asyncio
  async def test_canary_minimum_count(
    self,
    sample_migrations: list[Migration],
  ) -> None:
    """Test that canary always deploys to at least one environment."""
    # Create single environment
    conn = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    env = EnvironmentConfig(name='single', connection=conn)

    strategy = CanaryStrategy(canary_percentage=10.0, dry_run=True)
    results = await strategy.deploy([env], sample_migrations)

    # Should deploy to the one environment
    assert len(results) == 1
    assert results[0].status == DeploymentStatus.SUCCESS


class TestStrategyEdgeCases:
  """Tests for edge cases in deployment strategies."""

  @pytest.mark.asyncio
  async def test_empty_environments(self, sample_migrations: list[Migration]) -> None:
    """Test deployment with no environments."""
    strategy = SequentialStrategy(dry_run=True)
    results = await strategy.deploy([], sample_migrations)
    assert len(results) == 0

  @pytest.mark.asyncio
  async def test_empty_migrations(self, sample_environments: list[EnvironmentConfig]) -> None:
    """Test deployment with no migrations."""
    strategy = SequentialStrategy(dry_run=True)
    results = await strategy.deploy(sample_environments, [])

    # Should still return results but with 0 migrations applied
    assert len(results) == len(sample_environments)
    assert all(r.migrations_applied == 0 for r in results)

  @pytest.mark.asyncio
  async def test_single_environment(self, sample_migrations: list[Migration]) -> None:
    """Test deployment to single environment."""
    conn = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    env = EnvironmentConfig(name='single', connection=conn)

    for strategy_cls in [SequentialStrategy, ParallelStrategy, RollingStrategy, CanaryStrategy]:
      if strategy_cls == CanaryStrategy:
        strategy = strategy_cls(canary_percentage=10.0, dry_run=True)
      else:
        strategy = strategy_cls(dry_run=True)

      results = await strategy.deploy([env], sample_migrations)
      assert len(results) == 1
      assert results[0].status == DeploymentStatus.SUCCESS
