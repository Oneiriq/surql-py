"""Schema inspection CLI commands.

This module provides CLI commands for inspecting and managing database schemas
including viewing, comparing, and exporting schema definitions.
"""

import asyncio
from pathlib import Path
from typing import Annotated

import typer

from surql.cli.common import (
  OutputFormat,
  display_error,
  display_info,
  display_warning,
  handle_error,
  verbose_option,
)
from surql.cli.schema_diff import _diff_schema_async, _generate_migration_async
from surql.cli.schema_inspect import (
  _export_schema_async,
  _inspect_table_async,
  _list_tables_async,
  _show_schema_async,
)
from surql.cli.schema_validate import (
  CHECK_EXIT_DRIFT_DETECTED as CHECK_EXIT_DRIFT_DETECTED,
)
from surql.cli.schema_validate import (
  CHECK_EXIT_ERROR as CHECK_EXIT_ERROR,
)
from surql.cli.schema_validate import (
  CHECK_EXIT_NO_DRIFT as CHECK_EXIT_NO_DRIFT,
)
from surql.cli.schema_validate import (
  VALIDATE_EXIT_CONNECTION_ERROR,
  VALIDATE_EXIT_ERRORS,
  VALIDATE_EXIT_SUCCESS,
  _check_schema_async,
  _validate_schema_async,
  generate_hook_config_impl,
)
from surql.cli.schema_visualize import _visualize_schema
from surql.cli.schema_watch import WATCH_EXIT_ERROR, WATCH_EXIT_SUCCESS, _watch_schema_async

app = typer.Typer(
  name='schema',
  help='Schema inspection and management commands',
  no_args_is_help=True,
)


@app.command('show')
def show_schema(
  table: Annotated[
    str | None, typer.Argument(help='Specific table name to inspect (default: show all)')
  ] = None,
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TEXT,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Show current database schema.

  Displays schema information from the database using INFO statements.

  Examples:
    Show all schema:
    $ surql schema show

    Show specific table:
    $ surql schema show user

    Show as JSON:
    $ surql schema show --format json
  """
  try:
    asyncio.run(_show_schema_async(table, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('diff')
def diff_schema(
  schema_file: Annotated[
    Path | None,
    typer.Option('--schema', '-s', help='Path to Python schema file'),
  ] = None,
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TEXT,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Compare code schema definitions with database schema.

  Load schemas from a Python file and compare against database.
  Shows what changes would be needed to sync code to database.

  Examples:
    Compare schemas from file:
    $ surql schema diff --schema schemas/models.py

    Compare with JSON output:
    $ surql schema diff --schema schemas/models.py --format json
  """
  try:
    asyncio.run(_diff_schema_async(schema_file, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('generate')
def generate_migration(
  schema_file: Annotated[
    Path,
    typer.Option('--schema', '-s', help='Path to Python schema file'),
  ],
  description: Annotated[
    str,
    typer.Option('--message', '-m', help='Migration description'),
  ],
  directory: Annotated[
    Path,
    typer.Option('--directory', '-d', help='Directory to write migration file'),
  ] = Path('migrations'),
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Generate a migration from code schema differences.

  Compares schema definitions in a Python file against the database
  and generates a migration file with the necessary changes.

  Examples:
    Generate migration from schema file:
    $ surql schema generate --schema schemas/models.py -m "Add user table"

    Generate to specific directory:
    $ surql schema generate -s schemas/models.py -m "Add email field" -d db/migrations
  """
  try:
    asyncio.run(_generate_migration_async(schema_file, description, directory, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('sync')
def sync_schema(
  _dry_run: Annotated[
    bool, typer.Option('--dry-run', help='Show what would be synced without making changes')
  ] = False,
  _verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Synchronize code schema to database.

  NOTE: This is a potentially destructive operation.
  Use 'schema generate' to create migrations instead.

  Examples:
    Preview sync:
    $ surql schema sync --dry-run
  """
  display_warning('Schema sync not recommended - use migrations instead')
  display_info('Use "surql schema generate" to create a migration from schema diff')
  display_info('Then use "surql migrate up" to apply changes safely')


@app.command('export')
def export_schema(
  output: Annotated[
    str | None, typer.Option('--output', '-o', help='Output file path (default: stdout)')
  ] = None,
  table: Annotated[
    str | None, typer.Option('--table', '-t', help='Export specific table only')
  ] = None,
  format: Annotated[str, typer.Option('--format', '-f', help='Export format (sql, json)')] = 'sql',
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Export database schema to file.

  Exports the current database schema as SQL or JSON.

  Examples:
    Export all schema to SQL:
    $ surql schema export --output schema.sql

    Export specific table as JSON:
    $ surql schema export --table user --format json --output user.json
  """
  try:
    asyncio.run(_export_schema_async(output, table, format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('tables')
def list_tables(
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TABLE,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """List all tables in the database.

  Examples:
    List tables:
    $ surql schema tables

    List as JSON:
    $ surql schema tables --format json
  """
  try:
    asyncio.run(_list_tables_async(output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('inspect')
def inspect_table(
  table: Annotated[str, typer.Argument(help='Table name to inspect')],
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Inspect detailed information about a table.

  Shows fields, indexes, events, and permissions for a table.

  Examples:
    Inspect table:
    $ surql schema inspect user
  """
  try:
    asyncio.run(_inspect_table_async(table, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('validate')
def validate_schema(
  schema_file: Annotated[
    Path,
    typer.Option('--schema', '-s', help='Path to Python schema file'),
  ],
  strict: Annotated[
    bool,
    typer.Option('--strict', help='Exit with non-zero code on any drift (for CI/CD)'),
  ] = False,
  strict_warnings: Annotated[
    bool,
    typer.Option('--strict-warnings', help='Also fail on warnings'),
  ] = False,
  output_format: Annotated[
    str,
    typer.Option('--format', '-f', help='Output format (text, json)'),
  ] = 'text',
  output: Annotated[
    Path | None,
    typer.Option('--output', '-o', help='Output file path (default: stdout)'),
  ] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Validate Python schema definitions against the database schema.

  Compares code-defined schemas with the actual database state and reports
  any schema drift, mismatches, or inconsistencies. Useful for CI/CD pipelines.

  Exit codes:
    0: Schema is valid, no drift detected
    1: Schema drift detected (errors)
    2: Warnings detected (with --strict-warnings)
    3: Cannot connect to database

  Examples:
    Validate schemas from file:
    $ surql schema validate --schema schemas/models.py

    Validate with strict mode for CI/CD:
    $ surql schema validate --schema schemas/models.py --strict

    Output as JSON:
    $ surql schema validate --schema schemas/models.py --format json

    Write report to file:
    $ surql schema validate --schema schemas/models.py --output report.txt
  """
  try:
    exit_code = asyncio.run(
      _validate_schema_async(
        schema_file,
        strict,
        strict_warnings,
        output_format,
        output,
        verbose,
      )
    )
    if exit_code != VALIDATE_EXIT_SUCCESS:
      raise typer.Exit(exit_code)
  except typer.Exit:
    raise
  except ConnectionRefusedError:
    display_error('Cannot connect to database')
    raise typer.Exit(VALIDATE_EXIT_CONNECTION_ERROR) from None
  except Exception as e:
    # Check if it's a connection error
    error_str = str(e).lower()
    if 'connection' in error_str or 'connect' in error_str or 'refused' in error_str:
      display_error(f'Cannot connect to database: {e}')
      raise typer.Exit(VALIDATE_EXIT_CONNECTION_ERROR) from e
    handle_error(e, verbose)
    raise typer.Exit(VALIDATE_EXIT_ERRORS) from e


@app.command('check')
def check_schema(
  schema_path: Annotated[
    Path,
    typer.Option(
      '--schema',
      '-s',
      help='Path to schema files or directory',
    ),
  ] = Path('schemas'),
  migrations_dir: Annotated[
    Path | None,
    typer.Option(
      '--migrations',
      '-m',
      help='Path to migrations directory (auto-detected if not specified)',
    ),
  ] = None,
  fail_on_drift: Annotated[
    bool,
    typer.Option(
      '--fail-on-drift/--no-fail-on-drift',
      help='Exit with non-zero code when drift is detected',
    ),
  ] = True,
  show_diff: Annotated[
    bool,
    typer.Option(
      '--show-diff',
      help='Show detailed diff information',
    ),
  ] = False,
  output_format: Annotated[
    str,
    typer.Option('--format', '-f', help='Output format (text, json)'),
  ] = 'text',
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Check for unmigrated schema changes.

  Designed for pre-commit hooks and CI/CD pipelines to detect schema drift
  without requiring a database connection. Compares schema files against
  migration state.

  Exit codes:
    0: No drift detected
    1: Drift detected (schema changes not yet migrated)
    2: Error (e.g., cannot find files)

  Examples:
    Check for drift (default behavior):
    $ surql schema check --schema schemas/

    Check without failing on drift:
    $ surql schema check --schema schemas/ --no-fail-on-drift

    Check with specific migrations directory:
    $ surql schema check --schema schemas/ --migrations db/migrations

    Output as JSON:
    $ surql schema check --schema schemas/ --format json
  """
  try:
    exit_code = asyncio.run(
      _check_schema_async(
        schema_path,
        migrations_dir,
        fail_on_drift,
        show_diff,
        output_format,
        verbose,
      )
    )
    if exit_code != CHECK_EXIT_NO_DRIFT:
      raise typer.Exit(exit_code)
  except typer.Exit:
    raise
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(CHECK_EXIT_ERROR) from e


@app.command('hook-config')
def generate_hook_config(
  schema_path: Annotated[
    str,
    typer.Option(
      '--schema',
      '-s',
      help='Path to schema files',
    ),
  ] = 'schemas/',
  fail_on_drift: Annotated[
    bool,
    typer.Option(
      '--fail-on-drift/--no-fail-on-drift',
      help='Configure hook to fail on drift',
    ),
  ] = True,
) -> None:
  """Generate pre-commit hook configuration.

  Outputs YAML configuration that can be added to .pre-commit-config.yaml
  for automatic schema drift detection on commits.

  Examples:
    Generate config for default paths:
    $ surql schema hook-config

    Generate config for custom schema path:
    $ surql schema hook-config --schema src/schemas/

    Generate config that doesn't fail on drift:
    $ surql schema hook-config --no-fail-on-drift
  """
  generate_hook_config_impl(schema_path, fail_on_drift)


@app.command('watch')
def watch_schema(
  schema_path: Annotated[
    Path,
    typer.Option(
      '--schema',
      '-s',
      help='Path to schema file or directory to watch',
    ),
  ],
  migrations_dir: Annotated[
    Path,
    typer.Option(
      '--migrations',
      '-m',
      help='Path to migrations directory',
    ),
  ] = Path('migrations'),
  debounce: Annotated[
    float,
    typer.Option(
      '--debounce',
      help='Debounce delay in seconds before processing changes',
    ),
  ] = 1.0,
  auto_generate: Annotated[
    bool,
    typer.Option(
      '--auto-generate',
      help='Automatically generate migrations without prompting',
    ),
  ] = False,
  no_prompt: Annotated[
    bool,
    typer.Option(
      '--no-prompt',
      help='Skip prompts, just report changes',
    ),
  ] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Watch schema files for changes and prompt for migration generation.

  Monitors schema files or directories for changes and notifies when
  migrations may be needed. Can automatically generate migrations or
  prompt the user interactively.

  Use Ctrl+C to stop watching.

  Examples:
    Watch schema directory:
    $ surql schema watch --schema schemas/

    Watch with auto-generation:
    $ surql schema watch --schema schemas/ --auto-generate

    Custom debounce delay:
    $ surql schema watch --schema schemas/ --debounce 2.0

    Just report changes without prompting:
    $ surql schema watch --schema schemas/ --no-prompt
  """
  try:
    asyncio.run(
      _watch_schema_async(
        schema_path,
        migrations_dir,
        debounce,
        auto_generate,
        no_prompt,
        verbose,
      )
    )
  except KeyboardInterrupt:
    # Graceful exit on Ctrl+C - handled in async function
    raise typer.Exit(WATCH_EXIT_SUCCESS) from None
  except typer.Exit:
    raise
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(WATCH_EXIT_ERROR) from e


@app.command('visualize')
def visualize_schema_cmd(
  schema_file: Annotated[
    Path,
    typer.Option('--schema', '-s', help='Path to Python schema file'),
  ],
  format: Annotated[
    str,
    typer.Option(
      '--format',
      '-f',
      help='Output format: mermaid, graphviz, ascii',
    ),
  ] = 'mermaid',
  output: Annotated[
    Path | None,
    typer.Option('--output', '-o', help='Output file path (default: stdout)'),
  ] = None,
  tables: Annotated[
    str | None,
    typer.Option(
      '--tables',
      '-t',
      help='Comma-separated list of tables to include (default: all)',
    ),
  ] = None,
  no_fields: Annotated[
    bool,
    typer.Option('--no-fields', help='Exclude field definitions from diagram'),
  ] = False,
  no_edges: Annotated[
    bool,
    typer.Option('--no-edges', help='Exclude edge relationships from diagram'),
  ] = False,
  theme: Annotated[
    str,
    typer.Option('--theme', help='Theme to use for visualization (default: modern)'),
  ] = 'modern',
  no_gradients: Annotated[
    bool,
    typer.Option('--no-gradients', help='Disable gradient styling in GraphViz output'),
  ] = False,
  ascii_style: Annotated[
    str,
    typer.Option('--ascii-style', help='ASCII box drawing style: single, double, rounded, heavy'),
  ] = 'rounded',
  no_unicode: Annotated[
    bool,
    typer.Option('--no-unicode', help='Force basic ASCII characters (no Unicode)'),
  ] = False,
  no_colors: Annotated[
    bool,
    typer.Option('--no-colors', help='Disable ANSI colors in ASCII output'),
  ] = False,
  no_icons: Annotated[
    bool,
    typer.Option('--no-icons', help='Disable emoji/Unicode icons in ASCII output'),
  ] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Generate visual diagrams of the database schema with modern theming.

  Creates Mermaid ER diagrams, GraphViz DOT files, or ASCII art
  representations of schema definitions from a Python file. Supports
  multiple preset themes and format-specific customization options.

  Available themes: modern (default), dark, forest, minimal, none

  Examples:
    Generate Mermaid diagram with default modern theme:
    $ surql schema visualize --schema schemas/models.py

    Generate with dark theme:
    $ surql schema visualize --schema schemas/models.py --theme dark

    GraphViz with forest theme, no gradients:
    $ surql schema visualize --schema schemas/models.py -f graphviz --theme forest --no-gradients

    ASCII with minimal theme, custom box style:
    $ surql schema visualize --schema schemas/models.py -f ascii --theme minimal --ascii-style double

    ASCII with modern theme but no icons:
    $ surql schema visualize --schema schemas/models.py -f ascii --theme modern --no-icons

    Backward compatible (no theme):
    $ surql schema visualize --schema schemas/models.py --theme none

    Filter to specific tables:
    $ surql schema visualize --schema schemas/models.py --tables user,post,comment
  """
  try:
    _visualize_schema(
      schema_file=schema_file,
      format=format,
      output=output,
      tables=tables,
      no_fields=no_fields,
      no_edges=no_edges,
      theme=theme,
      no_gradients=no_gradients,
      ascii_style=ascii_style,
      no_unicode=no_unicode,
      no_colors=no_colors,
      no_icons=no_icons,
      verbose=verbose,
    )
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e
