"""Tests for orchestration coordinator and health checking."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from reverie.connection.config import ConnectionConfig
from reverie.migration.models import Migration
from reverie.orchestration.config import EnvironmentConfig, EnvironmentRegistry
from reverie.orchestration.coordinator import MigrationCoordinator, OrchestrationError
from reverie.orchestration.health import HealthCheck, HealthStatus
from reverie.orchestration.strategy import DeploymentStatus


@pytest.fixture
def test_registry() -> EnvironmentRegistry:
  """Create test registry with sample environments."""
  registry = EnvironmentRegistry()

  for i in range(3):
    conn = ConnectionConfig(
      db_url=f'ws://test{i}.example.com:8000/rpc',
      db_ns=f'test{i}',
      db='main',
    )
    registry.register_environment(
      name=f'env{i}',
      connection=conn,
      priority=i * 10,
    )

  return registry


@pytest.fixture
def sample_migration() -> Migration:
  """Create sample migration."""

  def up() -> list[str]:
    return ['CREATE TABLE test SCHEMAFULL;']

  def down() -> list[str]:
    return ['REMOVE TABLE test;']

  return Migration(
    version='20260109_120000',
    description='Test migration',
    path=Path('test.py'),
    up=up,
    down=down,
    checksum='test123',
  )


class TestHealthCheck:
  """Tests for HealthCheck."""

  @pytest.mark.asyncio
  async def test_check_connectivity_success(self) -> None:
    """Test successful connectivity check."""
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    env = EnvironmentConfig(name='test', connection=connection)

    health = HealthCheck()

    # Mock the client
    with patch('reverie.orchestration.health.get_client') as mock_get_client:
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_client.execute = AsyncMock(return_value=1)

      result = await health.check_connectivity(env)
      assert result is True

  @pytest.mark.asyncio
  async def test_check_connectivity_failure(self) -> None:
    """Test failed connectivity check."""
    from reverie.connection.client import ConnectionError as ClientConnectionError

    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    env = EnvironmentConfig(name='test', connection=connection)

    health = HealthCheck()

    # Mock the client to raise error
    with patch('reverie.orchestration.health.get_client') as mock_get_client:
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_client.execute = AsyncMock(side_effect=ClientConnectionError('Connection failed'))

      result = await health.check_connectivity(env)
      assert result is False

  @pytest.mark.asyncio
  async def test_check_migration_table_exists(self) -> None:
    """Test migration table existence check."""
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    env = EnvironmentConfig(name='test', connection=connection)

    health = HealthCheck()

    with patch('reverie.orchestration.health.get_client') as mock_get_client:
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_client.execute = AsyncMock(return_value=[])

      result = await health.check_migration_table(env)
      assert result is True

  @pytest.mark.asyncio
  async def test_check_migration_table_not_exists(self) -> None:
    """Test migration table not existing."""
    from reverie.connection.client import QueryError

    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    env = EnvironmentConfig(name='test', connection=connection)

    health = HealthCheck()

    with patch('reverie.orchestration.health.get_client') as mock_get_client:
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_client.execute = AsyncMock(side_effect=QueryError('Table not found'))

      result = await health.check_migration_table(env)
      assert result is False

  @pytest.mark.asyncio
  async def test_check_environment_healthy(self) -> None:
    """Test checking environment health."""
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    env = EnvironmentConfig(name='test', connection=connection)

    health = HealthCheck()

    with patch('reverie.orchestration.health.get_client') as mock_get_client:
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_client.execute = AsyncMock(return_value=[])

      status = await health.check_environment(env)
      assert isinstance(status, HealthStatus)
      assert status.environment == 'test'
      assert status.is_healthy is True
      assert status.can_connect is True

  @pytest.mark.asyncio
  async def test_check_environment_unhealthy(self) -> None:
    """Test checking unhealthy environment."""
    from reverie.connection.client import ConnectionError as ClientConnectionError

    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    env = EnvironmentConfig(name='test', connection=connection)

    health = HealthCheck()

    with patch('reverie.orchestration.health.get_client') as mock_get_client:
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_client.execute = AsyncMock(side_effect=ClientConnectionError('Failed'))

      status = await health.check_environment(env)
      assert status.is_healthy is False
      assert status.can_connect is False
      assert status.error is not None

  @pytest.mark.asyncio
  async def test_verify_all_environments(self) -> None:
    """Test verifying multiple environments."""
    env1 = EnvironmentConfig(
      name='env1',
      connection=ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test1', db='main'),
    )
    env2 = EnvironmentConfig(
      name='env2',
      connection=ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test2', db='main'),
    )

    health = HealthCheck()

    with patch('reverie.orchestration.health.get_client') as mock_get_client:
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_client.execute = AsyncMock(return_value=[])

      statuses = await health.verify_all_environments([env1, env2])
      assert len(statuses) == 2
      assert 'env1' in statuses
      assert 'env2' in statuses


class TestMigrationCoordinator:
  """Tests for MigrationCoordinator."""

  @pytest.mark.asyncio
  async def test_deploy_sequential_dry_run(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test sequential deployment in dry run mode."""
    coordinator = MigrationCoordinator(test_registry)

    results = await coordinator.deploy_to_environments(
      environments=['env0', 'env1'],
      migrations=[sample_migration],
      strategy='sequential',
      verify_health=False,
      dry_run=True,
    )

    assert len(results) == 2
    assert 'env0' in results
    assert 'env1' in results
    assert all(r.status == DeploymentStatus.SUCCESS for r in results.values())

  @pytest.mark.asyncio
  async def test_deploy_parallel_dry_run(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test parallel deployment in dry run mode."""
    coordinator = MigrationCoordinator(test_registry)

    results = await coordinator.deploy_to_environments(
      environments=['env0', 'env1', 'env2'],
      migrations=[sample_migration],
      strategy='parallel',
      verify_health=False,
      dry_run=True,
    )

    assert len(results) == 3
    assert all(r.status == DeploymentStatus.SUCCESS for r in results.values())

  @pytest.mark.asyncio
  async def test_deploy_rolling_dry_run(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test rolling deployment in dry run mode."""
    coordinator = MigrationCoordinator(test_registry)

    results = await coordinator.deploy_to_environments(
      environments=['env0', 'env1', 'env2'],
      migrations=[sample_migration],
      strategy='rolling',
      batch_size=2,
      verify_health=False,
      dry_run=True,
    )

    assert len(results) == 3
    assert all(r.status == DeploymentStatus.SUCCESS for r in results.values())

  @pytest.mark.asyncio
  async def test_deploy_canary_dry_run(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test canary deployment in dry run mode."""
    coordinator = MigrationCoordinator(test_registry)

    results = await coordinator.deploy_to_environments(
      environments=['env0', 'env1', 'env2'],
      migrations=[sample_migration],
      strategy='canary',
      canary_percentage=30.0,
      verify_health=False,
      dry_run=True,
    )

    assert len(results) == 3
    assert all(r.status == DeploymentStatus.SUCCESS for r in results.values())

  @pytest.mark.asyncio
  async def test_deploy_environment_not_found(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test deployment to non-existent environment."""
    coordinator = MigrationCoordinator(test_registry)

    with pytest.raises(OrchestrationError, match='Environment not found'):
      await coordinator.deploy_to_environments(
        environments=['nonexistent'],
        migrations=[sample_migration],
        strategy='sequential',
        verify_health=False,
        dry_run=True,
      )

  @pytest.mark.asyncio
  async def test_deploy_invalid_strategy(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test deployment with invalid strategy."""
    coordinator = MigrationCoordinator(test_registry)

    with pytest.raises(ValueError, match='Unknown strategy'):
      await coordinator.deploy_to_environments(
        environments=['env0'],
        migrations=[sample_migration],
        strategy='invalid',
        verify_health=False,
        dry_run=True,
      )

  @pytest.mark.asyncio
  async def test_get_deployment_status(self, test_registry: EnvironmentRegistry) -> None:
    """Test getting deployment status."""
    coordinator = MigrationCoordinator(test_registry)

    with patch.object(coordinator.health_check, 'verify_all_environments') as mock_verify:
      mock_verify.return_value = {
        'env0': HealthStatus(
          environment='env0',
          is_healthy=True,
          can_connect=True,
        ),
        'env1': HealthStatus(
          environment='env1',
          is_healthy=False,
          can_connect=False,
          error='Connection failed',
        ),
      }

      statuses = await coordinator.get_deployment_status(['env0', 'env1'])
      assert statuses['env0'] is True
      assert statuses['env1'] is False

  @pytest.mark.asyncio
  async def test_deploy_with_health_check(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test deployment with health check enabled."""
    coordinator = MigrationCoordinator(test_registry)

    with patch.object(coordinator.health_check, 'verify_all_environments') as mock_verify:
      # Mock healthy environments
      mock_verify.return_value = {
        'env0': HealthStatus(
          environment='env0',
          is_healthy=True,
          can_connect=True,
        ),
      }

      results = await coordinator.deploy_to_environments(
        environments=['env0'],
        migrations=[sample_migration],
        strategy='sequential',
        verify_health=True,
        dry_run=True,
      )

      assert len(results) == 1
      assert results['env0'].status == DeploymentStatus.SUCCESS

  @pytest.mark.asyncio
  async def test_deploy_unhealthy_environment(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test deployment fails with unhealthy environment."""
    coordinator = MigrationCoordinator(test_registry)

    with patch.object(coordinator.health_check, 'verify_all_environments') as mock_verify:
      # Mock unhealthy environment
      mock_verify.return_value = {
        'env0': HealthStatus(
          environment='env0',
          is_healthy=False,
          can_connect=False,
          error='Connection failed',
        ),
      }

      with pytest.raises(OrchestrationError, match='Unhealthy environments'):
        await coordinator.deploy_to_environments(
          environments=['env0'],
          migrations=[sample_migration],
          strategy='sequential',
          verify_health=True,
          dry_run=False,
        )


class TestCoordinatorEdgeCases:
  """Tests for edge cases in coordinator."""

  @pytest.mark.asyncio
  async def test_deploy_no_migrations(
    self,
    test_registry: EnvironmentRegistry,
  ) -> None:
    """Test deployment with no migrations."""
    coordinator = MigrationCoordinator(test_registry)

    results = await coordinator.deploy_to_environments(
      environments=['env0'],
      migrations=[],
      strategy='sequential',
      verify_health=False,
      dry_run=True,
    )

    assert len(results) == 1
    assert results['env0'].migrations_applied == 0

  @pytest.mark.asyncio
  async def test_deploy_no_environments(
    self,
    test_registry: EnvironmentRegistry,
    sample_migration: Migration,
  ) -> None:
    """Test deployment with no environments."""
    coordinator = MigrationCoordinator(test_registry)

    results = await coordinator.deploy_to_environments(
      environments=[],
      migrations=[sample_migration],
      strategy='sequential',
      verify_health=False,
      dry_run=True,
    )

    assert len(results) == 0
