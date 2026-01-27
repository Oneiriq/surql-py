"""Multi-database orchestration CLI commands.

This module provides CLI commands for deploying migrations across
multiple database environments.
"""

import asyncio
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from reverie.cli.common import (
  SYMBOL_ERROR,
  SYMBOL_SUCCESS,
  SYMBOL_WARNING,
  console,
  display_error,
  display_info,
  display_success,
  display_warning,
)
from reverie.migration.discovery import discover_migrations
from reverie.orchestration.config import configure_environments, get_registry
from reverie.orchestration.coordinator import MigrationCoordinator
from reverie.orchestration.health import HealthCheck
from reverie.orchestration.strategy import DeploymentResult, DeploymentStatus

app = typer.Typer(name='orchestrate', help='Multi-database orchestration commands')


@app.command('deploy')
def deploy_command(
  environments: Annotated[
    str,
    typer.Option('--environments', '-e', help='Comma-separated environment names'),
  ],
  strategy: Annotated[
    str,
    typer.Option(help='Deployment strategy (sequential, parallel, rolling, canary)'),
  ] = 'sequential',
  batch_size: Annotated[int, typer.Option(help='Batch size for rolling strategy')] = 1,
  canary_percent: Annotated[
    float,
    typer.Option(help='Canary percentage for canary strategy'),
  ] = 10.0,
  max_concurrent: Annotated[
    int,
    typer.Option(help='Max concurrent deployments for parallel strategy'),
  ] = 5,
  dry_run: Annotated[bool, typer.Option(help='Simulate deployment without executing')] = False,
  skip_health_check: Annotated[bool, typer.Option(help='Skip health verification')] = False,
  no_rollback: Annotated[
    bool,
    typer.Option(help='Disable auto-rollback on failure'),
  ] = False,
  config: Annotated[
    Path,
    typer.Option(help='Environment configuration file'),
  ] = Path('environments.json'),
  migrations_dir: Annotated[
    Path,
    typer.Option('--migrations-dir', '-m', help='Migrations directory'),
  ] = Path('migrations'),
) -> None:
  """Deploy migrations across multiple database environments.

  Examples:

    Deploy to staging and production sequentially:
      reverie orchestrate deploy -e staging,production

    Rolling deployment with batches of 2:
      reverie orchestrate deploy -e prod1,prod2,prod3,prod4 --strategy rolling --batch-size 2

    Canary deployment to 20% of instances first:
      reverie orchestrate deploy -e prod1,prod2,prod3,prod4,prod5 --strategy canary --canary-percent 20

    Dry run to see what would happen:
      reverie orchestrate deploy -e production --dry-run
  """
  asyncio.run(
    _deploy_async(
      environments=environments.split(','),
      strategy=strategy,
      batch_size=batch_size,
      canary_percent=canary_percent,
      max_concurrent=max_concurrent,
      dry_run=dry_run,
      skip_health_check=skip_health_check,
      no_rollback=no_rollback,
      config_path=config,
      migrations_dir=migrations_dir,
    )
  )


async def _deploy_async(
  environments: list[str],
  strategy: str,
  batch_size: int,
  canary_percent: float,
  max_concurrent: int,
  dry_run: bool,
  skip_health_check: bool,
  no_rollback: bool,
  config_path: Path,
  migrations_dir: Path,
) -> None:
  """Async deploy implementation."""
  # Load environment configuration
  if config_path.exists():
    configure_environments(config_path)
    display_info(f'Loaded environments from {config_path}')
  else:
    display_error(f'Configuration file not found: {config_path}')
    raise typer.Exit(1)

  # Discover migrations
  if not migrations_dir.exists():
    display_error(f'Migrations directory not found: {migrations_dir}')
    raise typer.Exit(1)

  migrations = discover_migrations(migrations_dir)

  if not migrations:
    display_warning('No migrations found')
    raise typer.Exit(0)

  display_info(f'Found {len(migrations)} migration(s)')

  # Validate strategy
  valid_strategies = ['sequential', 'parallel', 'rolling', 'canary']
  if strategy.lower() not in valid_strategies:
    display_error(f'Invalid strategy: {strategy}. Must be one of: {", ".join(valid_strategies)}')
    raise typer.Exit(1)

  if dry_run:
    display_warning('DRY RUN MODE - No changes will be made')

  # Create coordinator
  registry = get_registry()
  coordinator = MigrationCoordinator(registry)

  try:
    # Execute deployment
    display_info(f'Deploying to environments: {", ".join(environments)}')
    display_info(f'Strategy: {strategy}')

    results = await coordinator.deploy_to_environments(
      environments=environments,
      migrations=migrations,
      strategy=strategy,
      batch_size=batch_size,
      canary_percentage=canary_percent,
      max_concurrent=max_concurrent,
      verify_health=not skip_health_check,
      auto_rollback=not no_rollback,
      dry_run=dry_run,
    )

    # Display results
    _display_deployment_results(results)

    # Check for failures
    failures = [r for r in results.values() if r.status == DeploymentStatus.FAILED]

    if failures:
      display_error(f'Deployment failed on {len(failures)} environment(s)')
      raise typer.Exit(1)

    display_success(f'Successfully deployed to {len(results)} environment(s)')

  except Exception as e:
    display_error(f'Deployment error: {e}')
    raise typer.Exit(1) from e


@app.command('status')
def status_command(
  environments: Annotated[
    str,
    typer.Option('--environments', '-e', help='Comma-separated environment names'),
  ],
  config: Annotated[
    Path,
    typer.Option(help='Environment configuration file'),
  ] = Path('environments.json'),
) -> None:
  """Check deployment status of environments.

  Examples:

    Check status of all production environments:
      reverie orchestrate status -e prod1,prod2,prod3
  """
  asyncio.run(
    _status_async(
      environments=environments.split(','),
      config_path=config,
    )
  )


async def _status_async(
  environments: list[str],
  config_path: Path,
) -> None:
  """Async status implementation."""
  # Load environment configuration
  if config_path.exists():
    configure_environments(config_path)
  else:
    display_error(f'Configuration file not found: {config_path}')
    raise typer.Exit(1)

  # Get registry and check status
  registry = get_registry()
  coordinator = MigrationCoordinator(registry)

  try:
    statuses = await coordinator.get_deployment_status(environments)

    # Display status table
    table = Table(title='Environment Deployment Status')
    table.add_column('Environment', style='cyan')
    table.add_column('Status', style='bold')

    for env_name, is_healthy in statuses.items():
      status_str = '[green]Healthy[/green]' if is_healthy else '[red]Unhealthy[/red]'
      table.add_row(env_name, status_str)

    console.print(table)

  except Exception as e:
    display_error(f'Status check error: {e}')
    raise typer.Exit(1) from e


@app.command('validate')
def validate_command(
  config: Annotated[
    Path,
    typer.Option(help='Environment configuration file'),
  ] = Path('environments.json'),
) -> None:
  """Validate environment configuration and connectivity.

  Examples:

    Validate environment configuration:
      reverie orchestrate validate

    Validate specific configuration file:
      reverie orchestrate validate --config prod-environments.json
  """
  asyncio.run(_validate_async(config_path=config))


async def _validate_async(config_path: Path) -> None:
  """Async validate implementation."""
  # Check if config file exists
  if not config_path.exists():
    display_error(f'Configuration file not found: {config_path}')
    raise typer.Exit(1)

  # Load configuration
  try:
    configure_environments(config_path)
    registry = get_registry()
    environments = registry.list_environments()

    if not environments:
      display_warning('No environments configured')
      raise typer.Exit(0)

    display_info(f'Found {len(environments)} environment(s)')

    # Check health of each environment
    health_check = HealthCheck()
    table = Table(title='Environment Validation')
    table.add_column('Environment', style='cyan')
    table.add_column('Connectivity', style='bold')
    table.add_column('Migration Table', style='bold')
    table.add_column('Status', style='bold')

    all_healthy = True

    for env_name in environments:
      env_config = registry.get_environment(env_name)
      if not env_config:
        continue

      status = await health_check.check_environment(env_config)

      conn_status = f'[green]{SYMBOL_SUCCESS}[/green]' if status.can_connect else f'[red]{SYMBOL_ERROR}[/red]'
      table_status = f'[green]{SYMBOL_SUCCESS}[/green]' if status.migration_table_exists else f'[yellow]{SYMBOL_WARNING}[/yellow]'
      overall = '[green]Healthy[/green]' if status.is_healthy else '[red]Unhealthy[/red]'

      if not status.is_healthy:
        all_healthy = False

      table.add_row(env_name, conn_status, table_status, overall)

    console.print(table)

    if all_healthy:
      display_success('All environments validated successfully')
    else:
      display_warning('Some environments failed validation')
      raise typer.Exit(1)

  except Exception as e:
    display_error(f'Validation error: {e}')
    raise typer.Exit(1) from e


def _display_deployment_results(results: dict[str, DeploymentResult]) -> None:
  """Display deployment results table.

  Args:
    results: Dictionary of deployment results
  """
  from reverie.orchestration.strategy import DeploymentResult

  table = Table(title='Deployment Results')
  table.add_column('Environment', style='cyan')
  table.add_column('Status', style='bold')
  table.add_column('Migrations', justify='right')
  table.add_column('Duration (s)', justify='right')
  table.add_column('Error')

  for env_name, result in results.items():
    if not isinstance(result, DeploymentResult):
      continue

    # Format status with color
    if result.status == DeploymentStatus.SUCCESS:
      status_str = '[green]Success[/green]'
    elif result.status == DeploymentStatus.FAILED:
      status_str = '[red]Failed[/red]'
    else:
      status_str = result.status.value

    # Format duration
    duration = f'{result.duration_seconds:.2f}' if result.duration_seconds else 'N/A'

    # Format error
    error = (
      result.error[:50] + '...' if result.error and len(result.error) > 50 else result.error or ''
    )

    table.add_row(
      env_name,
      status_str,
      str(result.migrations_applied),
      duration,
      error,
    )

  console.print(table)


if __name__ == '__main__':
  app()
