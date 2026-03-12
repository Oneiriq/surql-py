"""Squash migration command implementation.

Provides the async implementation and display helpers for squashing
multiple migrations into a single consolidated migration.
"""

from pathlib import Path

import structlog
import typer
from rich.panel import Panel
from rich.table import Table

from reverie.cli.common import (
  confirm,
  console,
  display_error,
  display_info,
  display_success,
  display_warning,
  get_migrations_directory,
  spinner,
)
from reverie.migration.discovery import discover_migrations
from reverie.migration.models import Migration
from reverie.migration.squash import (
  SquashResult,
  SquashWarning,
  _filter_migrations_by_version,
  squash_migrations,
  validate_squash_safety,
)

logger = structlog.get_logger(__name__)


async def squash_migrations_async(
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
  filtered = _filter_migrations_by_version(migrations, from_version, to_version)

  if not filtered:
    display_error('No migrations match the specified version range')
    raise typer.Exit(1)

  if len(filtered) < 2:
    display_error('At least 2 migrations required for squashing')
    raise typer.Exit(1)

  # Display migrations to be squashed
  display_migrations_table(filtered)

  # Validate safety and display warnings
  warnings = validate_squash_safety(filtered)
  high_severity_count = 0

  if warnings:
    display_warnings(warnings)
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
  display_squash_result(result, dry_run)

  # Delete original files if not keeping them and not dry run
  if not dry_run and not keep_originals:
    deleted_count = delete_original_migrations(migrations_dir, result.original_migrations)
    if deleted_count > 0:
      display_info(f'Deleted {deleted_count} original migration file(s)')

  if verbose:
    display_info(f'Squashed migrations: {", ".join(result.original_migrations)}')


def display_migrations_table(migrations: list[Migration]) -> None:
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


def display_warnings(warnings: list[SquashWarning]) -> None:
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


def display_squash_result(result: SquashResult, dry_run: bool) -> None:
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


def delete_original_migrations(
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
