"""Advanced migration command implementations.

Provides sub-app Typer instances and async implementations for snapshot,
rollback, and version commands.
"""

import asyncio
from pathlib import Path
from typing import Annotated

import typer

from surql.cli.common import (
  OutputFormat,
  confirm_destructive,
  console,
  directory_option,
  display_error,
  display_info,
  display_success,
  display_warning,
  format_output,
  get_migrations_directory,
  handle_error,
  spinner,
  verbose_option,
)
from surql.connection.client import get_client
from surql.migration.discovery import discover_migrations
from surql.migration.history import get_applied_migrations
from surql.migration.rollback import create_rollback_plan, execute_rollback
from surql.migration.versioning import (
  compare_snapshots,
  create_snapshot,
  list_snapshots,
  load_snapshot,
  store_snapshot,
)
from surql.settings import get_db_config

# -- Snapshot sub-app --

snapshot_app = typer.Typer(name='snapshot', help='Schema snapshot commands')


@snapshot_app.command('create')
def create_snapshot_cmd(
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Create a schema snapshot at current version.

  Examples:
    Create snapshot:
    $ surql migrate snapshot create
  """
  try:
    asyncio.run(_create_snapshot_async(directory, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _create_snapshot_async(directory: Path | None, _verbose: bool) -> None:
  """Async implementation of create snapshot."""
  migrations_dir = get_migrations_directory(directory)
  config = get_db_config()

  _migrations = discover_migrations(migrations_dir)

  async with get_client(config) as client:
    # Get current version
    history = await get_applied_migrations(client)
    if not history:
      display_error('No migrations applied yet')
      raise typer.Exit(1)

    current_version = history[-1].version
    migration_count = len(history)

    display_info(f'Creating snapshot for version {current_version}')

    with spinner() as progress:
      task = progress.add_task('Creating snapshot...', total=None)

      snapshot = await create_snapshot(client, current_version, migration_count)
      await store_snapshot(client, snapshot)

      progress.update(task, completed=True)

    display_success(f'Snapshot created: {current_version}')
    display_info(f'Checksum: {snapshot.checksum[:16]}...')


@snapshot_app.command('list')
def list_snapshots_cmd(
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TABLE,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """List all schema snapshots.

  Examples:
    List snapshots:
    $ surql migrate snapshot list
  """
  try:
    asyncio.run(_list_snapshots_async(output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _list_snapshots_async(output_format: OutputFormat, _verbose: bool) -> None:
  """Async implementation of list snapshots."""
  config = get_db_config()

  async with get_client(config) as client:
    snapshots = await list_snapshots(client)

    if not snapshots:
      display_info('No snapshots found')
      return

    data = [
      {
        'version': s.version,
        'created_at': s.created_at.isoformat(),
        'migration_count': s.migration_count,
        'checksum': s.checksum[:16] + '...',
      }
      for s in snapshots
    ]

    format_output(data, output_format, title='Schema Snapshots')


# -- Rollback sub-app --

rollback_app = typer.Typer(name='rollback', help='Migration rollback commands')


@rollback_app.command('plan')
def plan_rollback_cmd(
  to_version: Annotated[str, typer.Option('--to', help='Target version to rollback to')],
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Plan rollback to a specific version.

  Examples:
    Plan rollback:
    $ surql migrate rollback plan --to 20260108_120000
  """
  try:
    asyncio.run(_plan_rollback_async(to_version, directory, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _plan_rollback_async(
  to_version: str,
  directory: Path | None,
  _verbose: bool,
) -> None:
  """Async implementation of plan rollback."""
  migrations_dir = get_migrations_directory(directory)
  config = get_db_config()

  migrations = discover_migrations(migrations_dir)

  async with get_client(config) as client:
    display_info(f'Planning rollback to {to_version}')

    plan = await create_rollback_plan(client, migrations, to_version)

    display_info(f'Will rollback {plan.migration_count} migration(s):')
    for migration in plan.migrations:
      display_info(f'  - {migration.version}: {migration.description}')

    display_info(f'\nSafety: {plan.overall_safety.value.upper()}')

    if plan.issues:
      display_warning(f'\nFound {len(plan.issues)} safety issue(s):')
      for issue in plan.issues:
        style = 'red' if issue.safety.value == 'unsafe' else 'yellow'
        console.print(
          f'  [{style}]{issue.safety.value.upper()}[/{style}] '
          f'{issue.migration}: {issue.description}'
        )
        if issue.recommendation:
          console.print(f'    Recommendation: {issue.recommendation}')

    if plan.requires_approval:
      display_warning('\nThis rollback requires approval due to safety concerns')


@rollback_app.command('execute')
def execute_rollback_cmd(
  to_version: Annotated[str, typer.Option('--to', help='Target version to rollback to')],
  directory: Annotated[Path | None, directory_option] = None,
  force: Annotated[bool, typer.Option('--force', help='Force execution despite warnings')] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Execute rollback to a specific version.

  Examples:
    Execute rollback:
    $ surql migrate rollback execute --to 20260108_120000

    Force rollback despite warnings:
    $ surql migrate rollback execute --to 20260108_120000 --force
  """
  try:
    asyncio.run(_execute_rollback_async(to_version, directory, force, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _execute_rollback_async(
  to_version: str,
  directory: Path | None,
  force: bool,
  _verbose: bool,
) -> None:
  """Async implementation of execute rollback."""
  migrations_dir = get_migrations_directory(directory)
  config = get_db_config()

  migrations = discover_migrations(migrations_dir)

  async with get_client(config) as client:
    # Create plan
    plan = await create_rollback_plan(client, migrations, to_version)

    display_info(f'Rollback plan: {plan.migration_count} migration(s)')
    display_info(f'Safety: {plan.overall_safety.value.upper()}')

    # Require confirmation if not safe
    if (
      plan.has_data_loss
      and not force
      and not confirm_destructive(
        f'This rollback may cause data loss. Proceed with {plan.migration_count} migration(s)?'
      )
    ):
      display_info('Rollback cancelled')
      return

    # Execute
    with spinner() as progress:
      task = progress.add_task(f'Rolling back to {to_version}...', total=None)

      result = await execute_rollback(client, plan, force=force)

      progress.update(task, completed=True)

    if result.success:
      display_success(f'Successfully rolled back {result.rolled_back_count} migration(s)')
      display_info(f'Duration: {result.actual_duration_ms}ms')
    else:
      display_error(f'Rollback failed after {result.rolled_back_count} migration(s)')
      for error in result.errors:
        display_error(f'  - {error}')
      raise typer.Exit(1)


# -- Version sub-app --

version_app = typer.Typer(name='version', help='Schema version commands')


@version_app.command('show')
def show_version_cmd(
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Show current schema version.

  Examples:
    Show version:
    $ surql migrate version show
  """
  try:
    asyncio.run(_show_version_async(directory, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _show_version_async(directory: Path | None, _verbose: bool) -> None:
  """Async implementation of show version."""
  _migrations_dir = get_migrations_directory(directory)
  config = get_db_config()

  async with get_client(config) as client:
    history = await get_applied_migrations(client)

    if not history:
      display_info('No migrations applied')
      return

    current = history[-1]
    display_success(f'Current version: {current.version}')
    display_info(f'Description: {current.description}')
    display_info(f'Applied at: {current.applied_at.isoformat()}')
    display_info(f'Total migrations: {len(history)}')


@version_app.command('compare')
def compare_versions_cmd(
  version1: Annotated[str, typer.Argument(help='First version')],
  version2: Annotated[str, typer.Argument(help='Second version')],
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Compare two schema versions.

  Examples:
    Compare versions:
    $ surql migrate version compare 20260108_120000 20260109_120000
  """
  try:
    asyncio.run(_compare_versions_async(version1, version2, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _compare_versions_async(
  version1: str,
  version2: str,
  _verbose: bool,
) -> None:
  """Async implementation of compare versions."""
  config = get_db_config()

  async with get_client(config) as client:
    # Load snapshots
    snapshot1 = await load_snapshot(client, version1)
    snapshot2 = await load_snapshot(client, version2)

    if not snapshot1:
      display_error(f'Snapshot not found for version {version1}')
      raise typer.Exit(1)

    if not snapshot2:
      display_error(f'Snapshot not found for version {version2}')
      raise typer.Exit(1)

    # Compare
    diff = compare_snapshots(snapshot1, snapshot2)

    display_info(f'Comparing {version1} → {version2}')
    console.print()

    if diff['checksum_match']:
      display_success('Schemas are identical')
      return

    # Display differences
    if diff['tables_added']:
      console.print('[green]Tables added:[/green]')
      for table in diff['tables_added']:
        console.print(f'  + {table}')
      console.print()

    if diff['tables_removed']:
      console.print('[red]Tables removed:[/red]')
      for table in diff['tables_removed']:
        console.print(f'  - {table}')
      console.print()

    if diff['tables_modified']:
      console.print('[yellow]Tables modified:[/yellow]')
      for table in diff['tables_modified']:
        console.print(f'  ~ {table}')
      console.print()

    if diff['edges_added']:
      console.print('[green]Edges added:[/green]')
      for edge in diff['edges_added']:
        console.print(f'  + {edge}')
      console.print()

    if diff['edges_removed']:
      console.print('[red]Edges removed:[/red]')
      for edge in diff['edges_removed']:
        console.print(f'  - {edge}')
      console.print()
