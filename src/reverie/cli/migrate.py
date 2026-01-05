"""Migration CLI commands.

This module provides CLI commands for managing database migrations including
applying, rolling back, creating, and viewing migration status.
"""

import asyncio
from pathlib import Path
from typing import Annotated

import structlog
import typer
from rich.panel import Panel
from rich.table import Table

from reverie.cli.common import (
  OutputFormat,
  confirm,
  confirm_destructive,
  console,
  directory_option,
  display_error,
  display_info,
  display_panel,
  display_success,
  display_warning,
  format_output,
  get_migrations_directory,
  handle_error,
  spinner,
  verbose_option,
)
from reverie.connection.client import get_client
from reverie.migration.discovery import discover_migrations, validate_migration_name
from reverie.migration.executor import (
  create_migration_plan,
  execute_migration_plan,
  get_migration_status,
  validate_migrations,
)
from reverie.migration.generator import create_blank_migration
from reverie.migration.history import ensure_migration_table, get_applied_migrations
from reverie.migration.models import Migration, MigrationDirection, MigrationState
from reverie.migration.squash import (
  SquashError,
  SquashResult,
  SquashWarning,
  squash_migrations,
  validate_squash_safety,
)
from reverie.settings import get_db_config

logger = structlog.get_logger(__name__)

app = typer.Typer(
  name='migrate',
  help='Database migration commands',
  no_args_is_help=True,
)


@app.command('up')
def migrate_up(
  directory: Annotated[Path | None, directory_option] = None,
  steps: Annotated[
    int | None, typer.Option('--steps', '-n', help='Number of migrations to apply (default: all)')
  ] = None,
  dry_run: bool = typer.Option(False, '--dry-run', help='Preview changes without applying'),
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Apply pending migrations to the database.

  Examples:
    Apply all pending migrations:
    $ reverie migrate up

    Apply only the next migration:
    $ reverie migrate up --steps 1

    Preview migrations without applying:
    $ reverie migrate up --dry-run
  """
  try:
    asyncio.run(_migrate_up_async(directory, steps, dry_run, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _migrate_up_async(
  directory: Path | None,
  steps: int | None,
  dry_run: bool,
  _verbose: bool,
) -> None:
  """Async implementation of migrate up."""
  migrations_dir = get_migrations_directory(directory)
  config = get_db_config()

  # Discover migrations
  display_info(f'Discovering migrations in {migrations_dir}')
  migrations = discover_migrations(migrations_dir)

  if not migrations:
    display_warning('No migration files found')
    return

  # Validate migrations
  errors = await validate_migrations(migrations)
  if errors:
    display_error('Migration validation failed:')
    for error in errors:
      display_error(f'  - {error}')
    raise typer.Exit(1)

  # Connect to database
  async with get_client(config) as client:
    # Ensure migration table exists
    await ensure_migration_table(client)

    # Create migration plan
    plan = await create_migration_plan(
      client,
      migrations,
      MigrationDirection.UP,
      steps,
    )

    if plan.is_empty():
      display_success('All migrations are already applied')
      return

    # Display plan
    display_info(f'Found {plan.count} pending migration(s):')
    for migration in plan.migrations:
      display_info(f'  - {migration.version}: {migration.description}')

    if dry_run:
      display_warning('Dry run mode - no changes will be made')

      # Show SQL that would be executed
      for migration in plan.migrations:
        statements = migration.up()
        sql_text = '\n'.join(statements)
        display_panel(
          sql_text,
          title=f'{migration.version}: {migration.description}',
          style='yellow',
        )
      return

    # Execute migrations
    with spinner() as progress:
      task = progress.add_task(
        f'Applying {plan.count} migration(s)...',
        total=None,
      )

      await execute_migration_plan(client, plan)
      progress.update(task, completed=True)

    display_success(f'Successfully applied {plan.count} migration(s)')


@app.command('down')
def migrate_down(
  directory: Annotated[Path | None, directory_option] = None,
  steps: Annotated[int, typer.Option('--steps', '-n', help='Number of migrations to rollback')] = 1,
  dry_run: bool = typer.Option(False, '--dry-run', help='Preview changes without applying'),
  confirm: Annotated[bool, typer.Option('--yes', '-y', help='Skip confirmation prompt')] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Rollback applied migrations.

  Examples:
    Rollback the last migration:
    $ reverie migrate down

    Rollback the last 3 migrations:
    $ reverie migrate down --steps 3

    Preview rollback without executing:
    $ reverie migrate down --dry-run

    Rollback without confirmation:
    $ reverie migrate down --yes
  """
  try:
    asyncio.run(_migrate_down_async(directory, steps, dry_run, confirm, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _migrate_down_async(
  directory: Path | None,
  steps: int,
  dry_run: bool,
  skip_confirm: bool,
  _verbose: bool,
) -> None:
  """Async implementation of migrate down."""
  migrations_dir = get_migrations_directory(directory)
  config = get_db_config()

  # Discover migrations
  migrations = discover_migrations(migrations_dir)

  if not migrations:
    display_warning('No migration files found')
    return

  # Connect to database
  async with get_client(config) as client:
    # Create rollback plan
    plan = await create_migration_plan(
      client,
      migrations,
      MigrationDirection.DOWN,
      steps,
    )

    if plan.is_empty():
      display_warning('No migrations to rollback')
      return

    # Display plan
    display_warning(f'Will rollback {plan.count} migration(s):')
    for migration in reversed(plan.migrations):
      display_warning(f'  - {migration.version}: {migration.description}')

    if dry_run:
      display_warning('Dry run mode - no changes will be made')

      # Show SQL that would be executed
      for migration in reversed(plan.migrations):
        statements = migration.down()
        sql_text = '\n'.join(statements)
        display_panel(
          sql_text,
          title=f'Rollback: {migration.version}',
          style='yellow',
        )
      return

    # Confirm destructive action (skip if --yes flag provided)
    if not skip_confirm and not confirm_destructive('Rollback migrations?'):
      display_info('Rollback cancelled')
      return

    # Execute rollback
    with spinner() as progress:
      task = progress.add_task(
        f'Rolling back {plan.count} migration(s)...',
        total=None,
      )

      await execute_migration_plan(client, plan)
      progress.update(task, completed=True)

    display_success(f'Successfully rolled back {plan.count} migration(s)')


@app.command('status')
def migration_status(
  directory: Annotated[Path | None, directory_option] = None,
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TABLE,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Show migration status.

  Displays which migrations have been applied and which are pending.

  Examples:
    Show status:
    $ reverie migrate status

    Show status as JSON:
    $ reverie migrate status --format json
  """
  try:
    asyncio.run(_migration_status_async(directory, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _migration_status_async(
  directory: Path | None,
  output_format: OutputFormat,
  _verbose: bool,
) -> None:
  """Async implementation of migration status."""
  migrations_dir = get_migrations_directory(directory)
  config = get_db_config()

  # Discover migrations
  migrations = discover_migrations(migrations_dir)

  if not migrations:
    display_warning('No migration files found')
    return

  # Connect to database
  async with get_client(config) as client:
    # Ensure migration table exists
    await ensure_migration_table(client)

    # Get status
    statuses = await get_migration_status(client, migrations)

    # Format output
    data = [
      {
        'version': status.migration.version,
        'description': status.migration.description,
        'status': status.state.value,
        'path': str(status.migration.path.name),
      }
      for status in statuses
    ]

    format_output(data, output_format, title='Migration Status')

    # Summary
    applied_count = sum(1 for s in statuses if s.state == MigrationState.APPLIED)
    pending_count = sum(1 for s in statuses if s.state == MigrationState.PENDING)

    display_info(f'Total: {len(statuses)} | Applied: {applied_count} | Pending: {pending_count}')


@app.command('history')
def migration_history(
  directory: Annotated[Path | None, directory_option] = None,
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TABLE,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Show migration history from database.

  Displays all migrations that have been applied to the database.

  Examples:
    Show history:
    $ reverie migrate history

    Show history as JSON:
    $ reverie migrate history --format json
  """
  try:
    asyncio.run(_migration_history_async(directory, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _migration_history_async(
  directory: Path | None,
  output_format: OutputFormat,
  _verbose: bool,
) -> None:
  """Async implementation of migration history."""
  _migrations_dir = get_migrations_directory(directory)
  config = get_db_config()

  async with get_client(config) as client:
    # Ensure migration table exists
    await ensure_migration_table(client)

    # Get history
    history = await get_applied_migrations(client)

    if not history:
      display_info('No migrations have been applied yet')
      return

    # Format output
    data = [
      {
        'version': h.version,
        'description': h.description,
        'applied_at': h.applied_at.isoformat(),
        'execution_time_ms': h.execution_time_ms or 'N/A',
      }
      for h in history
    ]

    format_output(data, output_format, title='Migration History')


@app.command('create')
def create_migration(
  description: Annotated[str, typer.Argument(help='Migration description')],
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Create a new blank migration file.

  Creates a migration file with empty up() and down() functions for manual editing.

  Examples:
    Create migration:
    $ reverie migrate create "Add user table"

    Create in custom directory:
    $ reverie migrate create "Add indexes" --directory ./db/migrations
  """
  try:
    migrations_dir = get_migrations_directory(directory)

    # Create blank migration
    file_path = create_blank_migration(migrations_dir, description)

    display_success(f'Created migration: {file_path.name}')
    display_info('Edit the file to add your migration SQL')
    display_info(f'Path: {file_path}')

  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('validate')
def validate_migration_files(
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Validate migration files.

  Checks migration files for:
  - Valid naming format
  - No duplicate versions
  - Valid dependencies
  - Loadable Python files

  Examples:
    Validate migrations:
    $ reverie migrate validate
  """
  try:
    asyncio.run(_validate_migrations_async(directory, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _validate_migrations_async(
  directory: Path | None,
  verbose: bool,
) -> None:
  """Async implementation of validate migrations."""
  migrations_dir = get_migrations_directory(directory)

  display_info(f'Validating migrations in {migrations_dir}')

  # Check directory
  if not migrations_dir.exists():
    display_error('Migrations directory does not exist')
    raise typer.Exit(1)

  # Get all Python files
  py_files = list(migrations_dir.glob('*.py'))
  py_files = [f for f in py_files if not f.name.startswith('_')]

  if not py_files:
    display_warning('No migration files found')
    return

  # Validate filenames
  invalid_names = []
  for py_file in py_files:
    if not validate_migration_name(py_file.name):
      invalid_names.append(py_file.name)

  if invalid_names:
    display_error('Invalid migration filenames:')
    for name in invalid_names:
      display_error(f'  - {name}')
    display_info('Expected format: YYYYMMDD_HHMMSS_description.py')
    raise typer.Exit(1)

  # Try to load migrations
  try:
    migrations = discover_migrations(migrations_dir)
  except Exception as e:
    display_error(f'Failed to load migrations: {e}')
    raise typer.Exit(1) from e

  # Validate migration consistency
  errors = await validate_migrations(migrations)

  if errors:
    display_error('Validation errors:')
    for error in errors:
      display_error(f'  - {error}')
    raise typer.Exit(1)

  # Success
  display_success(f'All {len(migrations)} migration(s) are valid')

  if verbose:
    for migration in migrations:
      display_info(f'  [OK] {migration.version}: {migration.description}')


@app.command('generate')
def generate_migration(
  description: Annotated[str, typer.Argument(help='Migration description')],
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Generate migration from schema changes.

  NOTE: Auto-generation requires schema registry implementation.
  For now, this creates a blank migration file.

  Examples:
    Generate migration:
    $ reverie migrate generate "Update user schema"
  """
  display_warning('Auto-generation from schema not yet implemented')
  display_info('Creating blank migration instead...')

  # For now, just create a blank migration
  create_migration(description, directory, verbose)


@app.command('squash')
def squash_migration_files(
  directory: Annotated[
    Path | None,
    typer.Option(
      '--migrations',
      '-m',
      help='Path to migrations directory',
    ),
  ] = None,
  output: Annotated[
    Path | None,
    typer.Option(
      '--output',
      '-o',
      help='Output file path (auto-generated if not provided)',
    ),
  ] = None,
  from_version: Annotated[
    str | None,
    typer.Option(
      '--from',
      help='Start version (inclusive), squash from beginning if not specified',
    ),
  ] = None,
  to_version: Annotated[
    str | None,
    typer.Option(
      '--to',
      help='End version (inclusive), squash to latest if not specified',
    ),
  ] = None,
  dry_run: Annotated[
    bool,
    typer.Option(
      '--dry-run',
      help='Preview without writing file',
    ),
  ] = False,
  no_optimize: Annotated[
    bool,
    typer.Option(
      '--no-optimize',
      help='Disable statement optimization',
    ),
  ] = False,
  keep_originals: Annotated[
    bool,
    typer.Option(
      '--keep-originals',
      help='Keep original migration files (do not delete them)',
    ),
  ] = False,
  force: Annotated[
    bool,
    typer.Option(
      '--force',
      help='Proceed even if safety warnings are detected',
    ),
  ] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Squash multiple migrations into a single consolidated migration.

  Combines multiple migration files into one, optionally optimizing redundant
  operations. The squashed migration preserves all schema changes while
  removing intermediate states.

  Examples:
    Squash all migrations:
    $ reverie migrate squash --migrations migrations/

    Squash specific range:
    $ reverie migrate squash --from 20260101_000000 --to 20260131_235959

    Preview squash:
    $ reverie migrate squash --dry-run

    Keep original files:
    $ reverie migrate squash --keep-originals

    Custom output path:
    $ reverie migrate squash --output migrations/0001_initial.py
  """
  try:
    asyncio.run(
      _squash_migrations_async(
        directory,
        output,
        from_version,
        to_version,
        dry_run,
        no_optimize,
        keep_originals,
        force,
        verbose,
      )
    )
  except SquashError as e:
    display_error(str(e))
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _squash_migrations_async(
  directory: Path | None,
  output: Path | None,
  from_version: str | None,
  to_version: str | None,
  dry_run: bool,
  no_optimize: bool,
  keep_originals: bool,
  force: bool,
  verbose: bool,
) -> None:
  """Async implementation of squash migrations."""
  migrations_dir = get_migrations_directory(directory)

  display_info(f'Discovering migrations in {migrations_dir}')

  # Discover migrations
  migrations = discover_migrations(migrations_dir)

  if not migrations:
    display_warning('No migration files found')
    return

  # Filter by version range
  from reverie.migration.squash import _filter_migrations_by_version

  filtered = _filter_migrations_by_version(migrations, from_version, to_version)

  if not filtered:
    display_error('No migrations match the specified version range')
    raise typer.Exit(1)

  if len(filtered) < 2:
    display_error('At least 2 migrations required for squashing')
    raise typer.Exit(1)

  # Display migrations to be squashed
  _display_migrations_table(filtered)

  # Validate safety and display warnings
  warnings = validate_squash_safety(filtered)
  high_severity_count = 0

  if warnings:
    _display_warnings(warnings)
    high_severity_count = sum(1 for w in warnings if w.severity == 'high')

    # Exit if high severity warnings without --force
    if high_severity_count > 0 and not force:
      display_error(
        f'{high_severity_count} high severity warning(s) detected. Use --force to proceed anyway.'
      )
      raise typer.Exit(3)

  # Confirmation prompt (unless --force or --dry-run)
  if not dry_run and not force and not confirm(f'Squash {len(filtered)} migrations into one?'):
    display_info('Squash cancelled')
    raise typer.Exit(2)

  # Execute squash
  with spinner() as progress:
    task = progress.add_task(
      f'Squashing {len(filtered)} migration(s)...',
      total=None,
    )

    result = await squash_migrations(
      directory=migrations_dir,
      from_version=from_version,
      to_version=to_version,
      output_path=output,
      optimize=not no_optimize,
      dry_run=dry_run,
    )

    progress.update(task, completed=True)

  # Display result
  _display_squash_result(result, dry_run)

  # Delete original files if not keeping them and not dry run
  if not dry_run and not keep_originals:
    deleted_count = _delete_original_migrations(migrations_dir, result.original_migrations)
    if deleted_count > 0:
      display_info(f'Deleted {deleted_count} original migration file(s)')

  if verbose:
    display_info(f'Squashed migrations: {", ".join(result.original_migrations)}')


def _display_migrations_table(migrations: list[Migration]) -> None:
  """Display table of migrations to be squashed.

  Args:
    migrations: List of Migration objects to display
  """
  table = Table(
    title='Migrations to Squash',
    show_header=True,
    header_style='bold cyan',
  )

  table.add_column('#', style='dim', width=4)
  table.add_column('Version', style='cyan')
  table.add_column('Description')
  table.add_column('File', style='dim')

  for idx, migration in enumerate(migrations, 1):
    table.add_row(
      str(idx),
      migration.version,
      migration.description,
      migration.path.name,
    )

  console.print(table)
  console.print()


def _display_warnings(warnings: list[SquashWarning]) -> None:
  """Display squash warnings with colored output.

  Args:
    warnings: List of SquashWarning objects to display
  """
  severity_styles = {
    'low': 'blue',
    'medium': 'yellow',
    'high': 'red',
  }

  severity_icons = {
    'low': '[i]',
    'medium': '[!]',
    'high': '[X]',
  }

  console.print()
  console.print('[bold]Safety Warnings:[/bold]')

  for warning in warnings:
    style = severity_styles.get(warning.severity, 'white')
    icon = severity_icons.get(warning.severity, '[?]')
    console.print(
      f'  [{style}]{icon}[/{style}] '
      f'[{style}][{warning.severity.upper()}][/{style}] '
      f'{warning.migration}: {warning.message}'
    )

  console.print()

  # Summary by severity
  low_count = sum(1 for w in warnings if w.severity == 'low')
  medium_count = sum(1 for w in warnings if w.severity == 'medium')
  high_count = sum(1 for w in warnings if w.severity == 'high')

  summary_parts = []
  if low_count > 0:
    summary_parts.append(f'[blue]{low_count} low[/blue]')
  if medium_count > 0:
    summary_parts.append(f'[yellow]{medium_count} medium[/yellow]')
  if high_count > 0:
    summary_parts.append(f'[red]{high_count} high[/red]')

  if summary_parts:
    console.print(f'  Total warnings: {", ".join(summary_parts)}')
    console.print()


def _display_squash_result(result: SquashResult, dry_run: bool) -> None:
  """Display squash result with statistics.

  Args:
    result: SquashResult from squash operation
    dry_run: Whether this was a dry run
  """
  # Build statistics content
  stats_lines = [
    f'Original migrations: {result.original_count}',
    f'Total statements: {result.statement_count}',
    f'Optimizations applied: {result.optimizations_applied}',
  ]

  if dry_run:
    stats_lines.append('')
    stats_lines.append(f'Would write to: {result.squashed_path}')
    title = 'Squash Preview (Dry Run)'
    style = 'yellow'
  else:
    stats_lines.append('')
    stats_lines.append(f'Output file: {result.squashed_path}')
    title = 'Squash Complete'
    style = 'green'

  panel = Panel(
    '\n'.join(stats_lines),
    title=title,
    border_style=style,
  )

  console.print()
  console.print(panel)

  if dry_run:
    display_warning('Dry run mode - no files were modified')
  else:
    display_success(f'Successfully squashed {result.original_count} migrations into 1')


def _delete_original_migrations(
  migrations_dir: Path,
  migration_versions: list[str],
) -> int:
  """Delete original migration files after successful squash.

  Args:
    migrations_dir: Directory containing migration files
    migration_versions: List of version strings of migrations to delete

  Returns:
    Count of files deleted
  """
  deleted_count = 0

  for version in migration_versions:
    # Find files matching this version
    pattern = f'{version}_*.py'
    matching_files = list(migrations_dir.glob(pattern))

    for file_path in matching_files:
      try:
        file_path.unlink()
        deleted_count += 1
        logger.debug('deleted_original_migration', path=str(file_path))
      except OSError as e:
        display_warning(f'Failed to delete {file_path.name}: {e}')

  return deleted_count


# Snapshot commands
snapshot_app = typer.Typer(name='snapshot', help='Schema snapshot commands')
app.add_typer(snapshot_app)


@snapshot_app.command('create')
def create_snapshot_cmd(
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Create a schema snapshot at current version.

  Examples:
    Create snapshot:
    $ reverie migrate snapshot create
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
    from reverie.migration.versioning import create_snapshot, store_snapshot

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
    $ reverie migrate snapshot list
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
    from reverie.migration.versioning import list_snapshots

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


# Rollback commands
rollback_app = typer.Typer(name='rollback', help='Migration rollback commands')
app.add_typer(rollback_app)


@rollback_app.command('plan')
def plan_rollback_cmd(
  to_version: Annotated[str, typer.Option('--to', help='Target version to rollback to')],
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Plan rollback to a specific version.

  Examples:
    Plan rollback:
    $ reverie migrate rollback plan --to 20260108_120000
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
    from reverie.migration.rollback import create_rollback_plan

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
    $ reverie migrate rollback execute --to 20260108_120000

    Force rollback despite warnings:
    $ reverie migrate rollback execute --to 20260108_120000 --force
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
    from reverie.migration.rollback import create_rollback_plan, execute_rollback

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


# Version commands
version_app = typer.Typer(name='version', help='Schema version commands')
app.add_typer(version_app)


@version_app.command('show')
def show_version_cmd(
  directory: Annotated[Path | None, directory_option] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Show current schema version.

  Examples:
    Show version:
    $ reverie migrate version show
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
    $ reverie migrate version compare 20260108_120000 20260109_120000
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
    from reverie.migration.versioning import compare_snapshots, load_snapshot

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
