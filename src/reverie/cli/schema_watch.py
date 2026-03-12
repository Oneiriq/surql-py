"""Schema watch and monitoring implementations.

Async handler for the watch command and all supporting helpers
for change processing, auto-generation, prompting, and display.
"""

import asyncio
from pathlib import Path
from typing import Any

import structlog
import typer

from reverie.cli.common import (
  confirm,
  console,
  display_error,
  display_info,
  display_success,
  display_warning,
)
from reverie.cli.schema_diff import _display_diff, _load_schemas_from_file
from reverie.cli.schema_inspect import _fetch_db_tables
from reverie.connection.client import get_client
from reverie.settings import get_db_config

logger = structlog.get_logger(__name__)

# Exit codes for watch command
WATCH_EXIT_SUCCESS = 0
WATCH_EXIT_ERROR = 1
WATCH_EXIT_INTERRUPTED = 130


async def _watch_schema_async(
  schema_path: Path,
  migrations_dir: Path,
  debounce: float,
  auto_generate: bool,
  no_prompt: bool,
  verbose: bool,
) -> None:
  """Async implementation of schema watch.

  Args:
    schema_path: Path to schema file or directory to watch
    migrations_dir: Path to migrations directory
    debounce: Debounce delay in seconds
    auto_generate: Automatically generate migrations
    no_prompt: Skip prompts, just report changes
    verbose: Enable verbose output
  """
  from rich.text import Text

  from reverie.migration.watcher import SchemaWatcher

  # Validate schema path exists
  if not schema_path.exists():
    display_error(f'Schema path not found: {schema_path}')
    raise typer.Exit(WATCH_EXIT_ERROR)

  # Ensure migrations directory exists
  if not migrations_dir.exists():
    migrations_dir.mkdir(parents=True, exist_ok=True)
    display_info(f'Created migrations directory: {migrations_dir}')

  # Session statistics
  session_stats = {
    'changes_detected': 0,
    'migrations_generated': 0,
    'migrations_skipped': 0,
  }

  # Create change handler callback
  async def handle_changes(changed_paths: list[Path]) -> None:
    """Handle detected schema changes."""
    for changed_path in changed_paths:
      session_stats['changes_detected'] += 1
      await _process_schema_change(
        changed_path=changed_path,
        schema_path=schema_path,
        migrations_dir=migrations_dir,
        auto_generate=auto_generate,
        no_prompt=no_prompt,
        verbose=verbose,
        session_stats=session_stats,
      )

  # Create watcher
  watcher = SchemaWatcher(
    schema_paths=[schema_path],
    migrations_dir=migrations_dir,
    on_change=handle_changes,
    debounce_seconds=debounce,
  )

  # Display startup message
  _display_watch_header(schema_path, migrations_dir, debounce, auto_generate, no_prompt)

  try:
    # Start watching
    await watcher.start()

    # Show watching status with spinner
    status_text = Text()
    status_text.append('[', style='dim')
    status_text.append('*', style='bold cyan')
    status_text.append('] ', style='dim')
    status_text.append('Watching for schema changes... ', style='cyan')
    status_text.append('(Press Ctrl+C to stop)', style='dim')

    console.print(status_text)
    console.print()

    # Keep running until interrupted
    while True:
      await asyncio.sleep(1)

  except asyncio.CancelledError:
    pass
  finally:
    # Stop watcher
    await watcher.stop()

    # Display session summary
    _display_watch_summary(session_stats)


def _display_watch_header(
  schema_path: Path,
  migrations_dir: Path,
  debounce: float,
  auto_generate: bool,
  no_prompt: bool,
) -> None:
  """Display the watch command header.

  Args:
    schema_path: Path being watched
    migrations_dir: Migrations directory
    debounce: Debounce delay
    auto_generate: Auto-generate mode
    no_prompt: No-prompt mode
  """
  from rich.panel import Panel
  from rich.text import Text

  # Build header text
  header = Text()
  header.append('Schema Watcher\n', style='bold cyan')
  header.append('\n')
  header.append('Watching: ', style='bold')
  header.append(str(schema_path), style='green')
  header.append('\n')
  header.append('Migrations: ', style='bold')
  header.append(str(migrations_dir), style='green')
  header.append('\n')
  header.append('Debounce: ', style='bold')
  header.append(f'{debounce}s', style='yellow')

  # Show mode
  if auto_generate:
    header.append('\n')
    header.append('Mode: ', style='bold')
    header.append('Auto-generate migrations', style='bold green')
  elif no_prompt:
    header.append('\n')
    header.append('Mode: ', style='bold')
    header.append('Report only (no prompts)', style='yellow')
  else:
    header.append('\n')
    header.append('Mode: ', style='bold')
    header.append('Interactive prompts', style='cyan')

  console.print(Panel(header, border_style='cyan'))
  console.print()


async def _process_schema_change(
  changed_path: Path,
  schema_path: Path,
  migrations_dir: Path,
  auto_generate: bool,
  no_prompt: bool,
  verbose: bool,
  session_stats: dict[str, int],
) -> None:
  """Process a detected schema change.

  Args:
    changed_path: Path to the changed file
    schema_path: Original schema path being watched
    migrations_dir: Migrations directory
    auto_generate: Automatically generate migrations
    no_prompt: Skip prompts
    verbose: Enable verbose output
    session_stats: Session statistics dictionary
  """
  from reverie.migration.diff import diff_tables
  from reverie.schema.registry import clear_registry

  # Display change notification
  _display_change_notification(changed_path)

  # Try to load schema and check for differences
  try:
    # Determine which file to check
    file_to_check = changed_path if changed_path.is_file() else schema_path

    if not file_to_check.exists() or not file_to_check.is_file():
      display_warning(f'Cannot analyze: {changed_path}')
      return

    # Clear registry and load new schemas
    clear_registry()
    code_tables = _load_schemas_from_file(file_to_check)

    if not code_tables:
      display_info('No schema definitions found in changed file')
      return

    # Fetch database schemas and compare
    config = get_db_config()

    async with get_client(config) as client:
      db_tables = await _fetch_db_tables(client)

      # Compare schemas
      all_table_names = set(code_tables.keys()) | set(db_tables.keys())
      all_diffs = []

      for table_name in sorted(all_table_names):
        code_table = code_tables.get(table_name)
        db_table = db_tables.get(table_name)

        if code_table is not None:  # Only check tables defined in this file
          table_diffs = diff_tables(db_table, code_table)
          all_diffs.extend(table_diffs)

      if not all_diffs:
        display_success('Schema is in sync with database - no migration needed')
        return

      # Display diff summary
      display_warning(f'Found {len(all_diffs)} schema differences')

      if verbose:
        for diff in all_diffs[:5]:  # Limit to first 5
          _display_diff(diff, verbose=False)
        if len(all_diffs) > 5:
          display_info(f'  ... and {len(all_diffs) - 5} more changes')

      console.print()

      # Handle based on mode
      if auto_generate:
        # Auto-generate migration
        await _auto_generate_migration(
          file_to_check=file_to_check,
          migrations_dir=migrations_dir,
          all_diffs=all_diffs,
          session_stats=session_stats,
        )
      elif no_prompt:
        # Just report, don't prompt
        display_info('Migration may be needed. Run "reverie schema generate" to create one.')
        session_stats['migrations_skipped'] += 1
      else:
        # Interactive prompt
        await _prompt_for_migration(
          file_to_check=file_to_check,
          migrations_dir=migrations_dir,
          all_diffs=all_diffs,
          session_stats=session_stats,
        )

  except Exception as e:
    display_error(f'Error processing change: {e}')
    logger.exception('schema_change_processing_error', path=str(changed_path))


def _display_change_notification(changed_path: Path) -> None:
  """Display a notification panel for a schema change.

  Args:
    changed_path: Path to the changed file
  """
  from datetime import UTC, datetime

  from rich.panel import Panel
  from rich.text import Text

  timestamp = datetime.now(UTC).strftime('%H:%M:%S')

  notification = Text()
  notification.append('Schema Change Detected\n', style='bold yellow')
  notification.append('\n')
  notification.append('File: ', style='bold')
  notification.append(str(changed_path), style='cyan')
  notification.append('\n')
  notification.append('Time: ', style='bold')
  notification.append(timestamp, style='dim')

  console.print()
  console.print(Panel(notification, border_style='yellow'))
  console.print()


async def _auto_generate_migration(
  file_to_check: Path,
  migrations_dir: Path,
  all_diffs: list[Any],
  session_stats: dict[str, int],
) -> None:
  """Automatically generate a migration file.

  Args:
    file_to_check: Schema file being checked
    migrations_dir: Directory to write migration
    all_diffs: List of schema differences
    session_stats: Session statistics dictionary
  """
  from reverie.migration.generator import generate_migration_from_diffs

  try:
    # Generate description from file name
    description = f'auto_update_{file_to_check.stem}'

    migration_path = generate_migration_from_diffs(
      directory=migrations_dir,
      description=description,
      diffs=all_diffs,
      author='reverie-watch',
    )

    display_success(f'Migration auto-generated: {migration_path}')
    session_stats['migrations_generated'] += 1

  except Exception as e:
    display_error(f'Failed to auto-generate migration: {e}')
    session_stats['migrations_skipped'] += 1


async def _prompt_for_migration(
  file_to_check: Path,
  migrations_dir: Path,
  all_diffs: list[Any],
  session_stats: dict[str, int],
) -> None:
  """Prompt the user to generate a migration.

  Args:
    file_to_check: Schema file being checked
    migrations_dir: Directory to write migration
    all_diffs: List of schema differences
    session_stats: Session statistics dictionary
  """
  from reverie.migration.generator import generate_migration_from_diffs

  # Prompt user
  should_generate = confirm('Generate migration?', default=False)

  if not should_generate:
    display_info('Migration skipped')
    session_stats['migrations_skipped'] += 1
    return

  # Prompt for description
  description = typer.prompt(
    'Migration description',
    default=f'update_{file_to_check.stem}',
  )

  try:
    migration_path = generate_migration_from_diffs(
      directory=migrations_dir,
      description=description,
      diffs=all_diffs,
      author='reverie-watch',
    )

    display_success(f'Migration generated: {migration_path}')
    display_info('Use "reverie migrate up" to apply the migration')
    session_stats['migrations_generated'] += 1

  except Exception as e:
    display_error(f'Failed to generate migration: {e}')
    session_stats['migrations_skipped'] += 1


def _display_watch_summary(session_stats: dict[str, int]) -> None:
  """Display summary of the watch session.

  Args:
    session_stats: Session statistics dictionary
  """
  from rich.panel import Panel
  from rich.text import Text

  console.print()

  summary = Text()
  summary.append('Watch Session Summary\n', style='bold cyan')
  summary.append('\n')
  summary.append('Changes detected: ', style='bold')
  summary.append(str(session_stats['changes_detected']), style='yellow')
  summary.append('\n')
  summary.append('Migrations generated: ', style='bold')
  summary.append(str(session_stats['migrations_generated']), style='green')
  summary.append('\n')
  summary.append('Migrations skipped: ', style='bold')
  summary.append(str(session_stats['migrations_skipped']), style='dim')

  console.print(Panel(summary, border_style='cyan'))
  display_info('Schema watcher stopped')
