"""Migration CLI commands.

This module provides CLI commands for managing database migrations including
applying, rolling back, creating, and viewing migration status. Command
implementations are delegated to sub-modules.
"""

import asyncio
from pathlib import Path
from typing import Annotated

import typer

from surql.cli.common import (
  OutputFormat,
  directory_option,
  display_error,
  handle_error,
  verbose_option,
)
from surql.cli.migrate_advanced import rollback_app, snapshot_app, version_app
from surql.cli.migrate_core import (
  create_migration_impl,
  generate_migration_impl,
  migrate_down_async,
  migrate_up_async,
  migration_history_async,
  migration_status_async,
  validate_migrations_async,
)
from surql.cli.migrate_squash import squash_migrations_async
from surql.migration.squash import SquashError

app = typer.Typer(
  name='migrate',
  help='Database migration commands',
  no_args_is_help=True,
)

# Register sub-apps
app.add_typer(snapshot_app)
app.add_typer(rollback_app)
app.add_typer(version_app)


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
    $ surql migrate up

    Apply only the next migration:
    $ surql migrate up --steps 1

    Preview migrations without applying:
    $ surql migrate up --dry-run
  """
  try:
    asyncio.run(migrate_up_async(directory, steps, dry_run, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


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
    $ surql migrate down

    Rollback the last 3 migrations:
    $ surql migrate down --steps 3

    Preview rollback without executing:
    $ surql migrate down --dry-run

    Rollback without confirmation:
    $ surql migrate down --yes
  """
  try:
    asyncio.run(migrate_down_async(directory, steps, dry_run, confirm, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


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
    $ surql migrate status

    Show status as JSON:
    $ surql migrate status --format json
  """
  try:
    asyncio.run(migration_status_async(directory, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


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
    $ surql migrate history

    Show history as JSON:
    $ surql migrate history --format json
  """
  try:
    asyncio.run(migration_history_async(directory, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


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
    $ surql migrate create "Add user table"

    Create in custom directory:
    $ surql migrate create "Add indexes" --directory ./db/migrations
  """
  create_migration_impl(description, directory, verbose)


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
    $ surql migrate validate
  """
  try:
    asyncio.run(validate_migrations_async(directory, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


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
    $ surql migrate generate "Update user schema"
  """
  generate_migration_impl(description, directory, verbose)


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
    $ surql migrate squash --migrations migrations/

    Squash specific range:
    $ surql migrate squash --from 20260101_000000 --to 20260131_235959

    Preview squash:
    $ surql migrate squash --dry-run

    Keep original files:
    $ surql migrate squash --keep-originals

    Custom output path:
    $ surql migrate squash --output migrations/0001_initial.py
  """
  try:
    asyncio.run(
      squash_migrations_async(
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
