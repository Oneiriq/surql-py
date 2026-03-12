"""Core migration command implementations.

Provides async implementations and helpers for the primary migration commands:
up, down, status, history, create, validate, and generate.
"""

from pathlib import Path

import structlog
import typer

from reverie.cli.common import (
  OutputFormat,
  confirm_destructive,
  display_error,
  display_info,
  display_panel,
  display_success,
  display_warning,
  format_output,
  get_migrations_directory,
  handle_error,
  spinner,
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
from reverie.migration.models import MigrationDirection, MigrationState
from reverie.settings import get_db_config

logger = structlog.get_logger(__name__)


async def migrate_up_async(
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


async def migrate_down_async(
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


async def migration_status_async(
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


async def migration_history_async(
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


def create_migration_impl(
  description: str,
  directory: Path | None,
  verbose: bool,
) -> None:
  """Implementation of create migration command.

  Args:
    description: Migration description
    directory: Optional migrations directory path
    verbose: Enable verbose output
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


async def validate_migrations_async(
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


def generate_migration_impl(
  description: str,
  directory: Path | None,
  verbose: bool,
) -> None:
  """Implementation of generate migration command.

  Args:
    description: Migration description
    directory: Optional migrations directory path
    verbose: Enable verbose output
  """
  display_warning('Auto-generation from schema not yet implemented')
  display_info('Creating blank migration instead...')

  # For now, just create a blank migration
  create_migration_impl(description, directory, verbose)
