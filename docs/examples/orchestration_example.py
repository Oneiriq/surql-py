"""Multi-Database Migration Orchestration Examples.

This example demonstrates how to use surql's orchestration features to
deploy migrations across multiple database environments.
"""

import asyncio
from pathlib import Path

from surql.connection.config import ConnectionConfig
from surql.migration.discovery import discover_migrations
from surql.orchestration import (
  EnvironmentConfig,
  EnvironmentRegistry,
  MigrationCoordinator,
  configure_environments,
  deploy_to_environments,
  get_registry,
  register_environment,
)


async def example_basic_orchestration() -> None:
  """Example: Basic multi-environment deployment."""
  print('=== Basic Orchestration Example ===\n')

  # Create environment registry
  registry = EnvironmentRegistry()

  # Register staging environment
  staging_conn = ConnectionConfig(
    db_url='ws://staging.example.com:8000/rpc',
    db_ns='staging',
    db='main',
  )
  registry.register_environment(
    name='staging',
    connection=staging_conn,
    priority=50,
    tags={'staging', 'test'},
  )

  # Register production environment
  prod_conn = ConnectionConfig(
    db_url='ws://prod.example.com:8000/rpc',
    db_ns='production',
    db='main',
  )
  registry.register_environment(
    name='production',
    connection=prod_conn,
    priority=1,
    tags={'production', 'critical'},
    require_approval=True,
    allow_destructive=False,
  )

  # Discover migrations
  migrations = discover_migrations(Path('migrations'))

  # Create coordinator
  coordinator = MigrationCoordinator(registry)

  # Deploy sequentially
  results = await coordinator.deploy_to_environments(
    environments=['staging', 'production'],
    migrations=migrations,
    strategy='sequential',
    verify_health=True,
    auto_rollback=True,
    dry_run=True,  # Simulate deployment
  )

  # Display results
  for env_name, result in results.items():
    print(f'{env_name}: {result.status.value} ({result.migrations_applied} migrations)')


async def example_rolling_deployment() -> None:
  """Example: Rolling deployment across multiple replicas."""
  print('\n=== Rolling Deployment Example ===\n')

  registry = EnvironmentRegistry()

  # Register multiple production database instances
  for i in range(4):
    conn = ConnectionConfig(
      db_url=f'ws://prod-db{i}.example.com:8000/rpc',
      db_ns='production',
      db='main',
    )
    registry.register_environment(
      name=f'prod-db{i}',
      connection=conn,
      priority=i,
      tags={'production', 'replica'},
    )

  migrations = discover_migrations(Path('migrations'))

  coordinator = MigrationCoordinator(registry)

  # Deploy in batches of 2
  results = await coordinator.deploy_to_environments(
    environments=['prod-db0', 'prod-db1', 'prod-db2', 'prod-db3'],
    migrations=migrations,
    strategy='rolling',
    batch_size=2,
    verify_health=True,
    dry_run=True,
  )

  print(f'Deployed to {len(results)} instances')
  for env_name, result in results.items():
    duration = result.duration_seconds or 0
    print(f'  {env_name}: {result.status.value} ({duration:.2f}s)')


async def example_canary_deployment() -> None:
  """Example: Canary deployment to test subset first."""
  print('\n=== Canary Deployment Example ===\n')

  registry = EnvironmentRegistry()

  # Register 5 production instances
  for i in range(5):
    conn = ConnectionConfig(
      db_url=f'ws://prod{i}.example.com:8000/rpc',
      db_ns='production',
      db='main',
    )
    registry.register_environment(
      name=f'prod{i}',
      connection=conn,
      tags={'production'},
    )

  migrations = discover_migrations(Path('migrations'))

  coordinator = MigrationCoordinator(registry)

  # Deploy to 20% (1 instance) first as canary
  results = await coordinator.deploy_to_environments(
    environments=['prod0', 'prod1', 'prod2', 'prod3', 'prod4'],
    migrations=migrations,
    strategy='canary',
    canary_percentage=20.0,
    verify_health=True,
    dry_run=True,
  )

  print(f'Canary deployment completed on {len(results)} instances')


async def example_parallel_deployment() -> None:
  """Example: Parallel deployment across environments."""
  print('\n=== Parallel Deployment Example ===\n')

  registry = EnvironmentRegistry()

  # Register multiple independent environments
  environments = ['dev1', 'dev2', 'dev3', 'qa1', 'qa2']
  for env_name in environments:
    conn = ConnectionConfig(
      db_url=f'ws://{env_name}.example.com:8000/rpc',
      db_ns=env_name,
      db='main',
    )
    registry.register_environment(
      name=env_name,
      connection=conn,
      tags={'development'},
    )

  migrations = discover_migrations(Path('migrations'))

  coordinator = MigrationCoordinator(registry)

  # Deploy to all environments in parallel (max 3 concurrent)
  results = await coordinator.deploy_to_environments(
    environments=environments,
    migrations=migrations,
    strategy='parallel',
    max_concurrent=3,
    verify_health=True,
    dry_run=True,
  )

  print(f'Parallel deployment to {len(results)} environments')


async def example_config_file() -> None:
  """Example: Load environments from configuration file."""
  print('\n=== Configuration File Example ===\n')

  # Create sample configuration file
  config_file = Path('environments.json')

  # In a real scenario, this would already exist
  # For this example, we'll show the expected format:
  print('Create environments.json with the following structure:')
  print(
    """
{
  "environments": [
    {
      "name": "development",
      "connection": {
        "db_url": "ws://localhost:8000/rpc",
        "db_ns": "development",
        "db": "main"
      },
      "priority": 100,
      "tags": ["dev", "local"]
    },
    {
      "name": "production",
      "connection": {
        "db_url": "ws://prod.example.com:8000/rpc",
        "db_ns": "production",
        "db": "main"
      },
      "priority": 1,
      "tags": ["prod", "critical"],
      "require_approval": true,
      "allow_destructive": false
    }
  ]
}
"""
  )

  # Load environments from config file
  if config_file.exists():
    configure_environments(config_file)
    registry = get_registry()
    print(f'\nLoaded {len(registry.list_environments())} environments:')
    for env_name in registry.list_environments():
      env = registry.get_environment(env_name)
      if env:
        print(f'  - {env.name} (priority: {env.priority})')


async def example_health_checks() -> None:
  """Example: Health checking before deployment."""
  print('\n=== Health Check Example ===\n')

  from surql.orchestration import HealthCheck

  registry = EnvironmentRegistry()

  # Register environment
  conn = ConnectionConfig(
    db_url='ws://test.example.com:8000/rpc',
    db_ns='test',
    db='main',
  )
  env_config = EnvironmentConfig(name='test', connection=conn)
  registry.register_environment(
    name='test',
    connection=conn,
  )

  # Perform health check
  health = HealthCheck()
  status = await health.check_environment(env_config)

  print(f'Environment: {status.environment}')
  print(f'Healthy: {status.is_healthy}')
  print(f'Can Connect: {status.can_connect}')
  print(f'Migration Table Exists: {status.migration_table_exists}')

  if status.error:
    print(f'Error: {status.error}')


async def example_using_global_registry() -> None:
  """Example: Using global registry for convenience."""
  print('\n=== Global Registry Example ===\n')

  # Register environments in global registry
  dev_conn = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='development',
    db='main',
  )

  register_environment(
    name='development',
    connection=dev_conn,
    priority=100,
    tags={'dev'},
  )

  # Get migrations
  migrations = discover_migrations(Path('migrations'))

  # Deploy using convenience function
  results = await deploy_to_environments(
    registry=get_registry(),
    environments=['development'],
    migrations=migrations,
    strategy='sequential',
    dry_run=True,
  )

  print(f'Deployed to {len(results)} environment(s)')


async def main() -> None:
  """Run all examples."""
  print('surql Multi-Database Orchestration Examples\n')
  print('=' * 60)

  # Note: These examples use dry_run=True so they won't actually
  # connect to databases. In a real scenario, remove dry_run=True
  # and ensure your databases are accessible.

  await example_basic_orchestration()
  await example_rolling_deployment()
  await example_canary_deployment()
  await example_parallel_deployment()
  await example_config_file()
  await example_health_checks()
  await example_using_global_registry()

  print('\n' + '=' * 60)
  print('\nAll examples completed!')
  print(
    '\nTip: Use the CLI for interactive orchestration:'
    '\n  surql orchestrate deploy -e staging,production --strategy rolling --batch-size 2'
  )


if __name__ == '__main__':
  asyncio.run(main())
