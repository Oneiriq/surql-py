"""Schema inspection CLI commands.

This module provides CLI commands for inspecting and managing database schemas
including viewing, comparing, and exporting schema definitions.
"""

import asyncio
import importlib.util
from pathlib import Path
from typing import Annotated, Any

import structlog
import typer

from reverie.cli.common import (
  OutputFormat,
  display_code,
  display_error,
  display_info,
  display_panel,
  display_success,
  display_warning,
  format_output,
  handle_error,
  spinner,
  verbose_option,
)
from reverie.connection.client import get_client
from reverie.settings import get_db_config

logger = structlog.get_logger(__name__)

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
    $ reverie schema show

    Show specific table:
    $ reverie schema show user

    Show as JSON:
    $ reverie schema show --format json
  """
  try:
    asyncio.run(_show_schema_async(table, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _show_schema_async(
  table: str | None,
  output_format: OutputFormat,
  _verbose: bool,
) -> None:
  """Async implementation of show schema."""
  config = get_db_config()

  async with get_client(config) as client:
    if table:
      # Show specific table info
      display_info(f'Fetching schema for table: {table}')

      query = f'INFO FOR TABLE {table};'
      result = await client.execute(query)

      if output_format == OutputFormat.JSON:
        format_output(result, OutputFormat.JSON)
      else:
        display_panel(
          str(result),
          title=f'Table: {table}',
          style='cyan',
        )
    else:
      # Show database info
      display_info('Fetching database schema')

      query = 'INFO FOR DB;'
      result = await client.execute(query)

      if output_format == OutputFormat.JSON:
        format_output(result, OutputFormat.JSON)
      else:
        display_panel(
          str(result),
          title='Database Schema',
          style='cyan',
        )


async def _fetch_db_tables(client: Any) -> dict[str, Any]:
  """Fetch table definitions from database.

  Args:
    client: Database client

  Returns:
    Dictionary of table name to TableDefinition
  """
  from reverie.schema.parser import parse_table_info
  from reverie.schema.table import TableDefinition

  db_tables: dict[str, TableDefinition] = {}

  # Get list of tables from database
  db_info = await client.execute('INFO FOR DB;')

  # Parse database info - handle both direct dict and wrapped result
  result = db_info
  if isinstance(db_info, list) and len(db_info) > 0:
    result = db_info[0].get('result', db_info[0]) if isinstance(db_info[0], dict) else db_info
  if isinstance(result, dict):
    # SurrealDB may use 'tables' or 'tb' depending on version
    tb_dict = result.get('tables') or result.get('tb') or {}

    # For each table, get detailed info (skip internal tables)
    for table_name in tb_dict:
      if table_name.startswith('_'):
        continue  # Skip internal tables like _migration_history
      try:
        table_info = await client.execute(f'INFO FOR TABLE {table_name};')
        info_result = table_info
        if isinstance(table_info, list) and len(table_info) > 0:
          info_result = (
            table_info[0].get('result', table_info[0])
            if isinstance(table_info[0], dict)
            else table_info
          )
        if isinstance(info_result, dict):
          db_tables[table_name] = parse_table_info(table_name, info_result)
      except Exception as e:
        logger.warning('table_fetch_failed', table=table_name, error=str(e))

  return db_tables


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
    $ reverie schema diff --schema schemas/models.py

    Compare with JSON output:
    $ reverie schema diff --schema schemas/models.py --format json
  """
  try:
    asyncio.run(_diff_schema_async(schema_file, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _diff_schema_async(
  schema_file: Path | None,
  output_format: OutputFormat,
  verbose: bool,
) -> None:
  """Async implementation of schema diff."""
  from reverie.migration.diff import diff_tables
  from reverie.schema.registry import clear_registry

  # Load schemas from file if provided
  if schema_file is None:
    display_error('Schema file required. Use --schema to specify a Python file.')
    display_info('Example: reverie schema diff --schema schemas/models.py')
    raise typer.Exit(1)

  if not schema_file.exists():
    display_error(f'Schema file not found: {schema_file}')
    raise typer.Exit(1)

  display_info(f'Loading schemas from: {schema_file}')

  # Clear registry and load new schemas
  clear_registry()
  code_tables = _load_schemas_from_file(schema_file)

  if not code_tables:
    display_warning('No schemas found in the specified file')
    display_info('Make sure your file registers schemas using register_table()')
    raise typer.Exit(1)

  display_success(f'Loaded {len(code_tables)} table schemas from file')

  # Fetch database schemas
  config = get_db_config()

  async with get_client(config) as client:
    display_info('Fetching database schema...')

    db_tables = await _fetch_db_tables(client)

    display_info(f'Found {len(db_tables)} tables in database')

    # Compare schemas
    all_table_names = set(code_tables.keys()) | set(db_tables.keys())
    all_diffs = []

    for table_name in sorted(all_table_names):
      code_table = code_tables.get(table_name)
      db_table = db_tables.get(table_name)

      table_diffs = diff_tables(db_table, code_table)
      all_diffs.extend(table_diffs)

    # Display results
    if not all_diffs:
      display_success('No schema differences found - code and database are in sync!')
      return

    display_warning(f'Found {len(all_diffs)} schema differences:')

    if output_format == OutputFormat.JSON:
      import json

      diff_data = [
        {
          'operation': d.operation.value,
          'table': d.table,
          'field': d.field,
          'index': d.index,
          'description': d.description,
          'forward_sql': d.forward_sql,
          'backward_sql': d.backward_sql,
        }
        for d in all_diffs
      ]
      print(json.dumps(diff_data, indent=2))
    else:
      for diff in all_diffs:
        _display_diff(diff, verbose)


def _load_schemas_from_file(file_path: Path) -> dict[str, Any]:
  """Load schema definitions from a Python file.

  Args:
    file_path: Path to Python file containing schema definitions

  Returns:
    Dictionary of table name to TableDefinition
  """
  from reverie.schema.registry import get_registered_tables
  from reverie.schema.table import TableDefinition

  # Import the module dynamically
  spec = importlib.util.spec_from_file_location('schema_module', file_path)
  if spec is None or spec.loader is None:
    raise ValueError(f'Could not load module from {file_path}')

  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)

  # Get registered tables from registry
  registered = get_registered_tables()
  if registered:
    return registered

  # Fallback: scan module for TableDefinition objects
  tables = {}
  for name in dir(module):
    obj = getattr(module, name)
    if isinstance(obj, TableDefinition):
      tables[obj.name] = obj

  return tables


def _display_diff(diff: Any, verbose: bool) -> None:
  """Display a schema diff in human-readable format."""
  from rich.console import Console
  from rich.text import Text

  console = Console()

  # Color coding by operation type
  op = diff.operation.value
  color = 'green' if 'add' in op else 'red' if 'drop' in op else 'yellow'

  # Build display text
  text = Text()
  text.append(f'  [{op.upper()}] ', style=f'bold {color}')
  text.append(diff.description)

  console.print(text)

  if verbose and diff.forward_sql:
    console.print(f'    SQL: {diff.forward_sql}', style='dim')


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
    $ reverie schema generate --schema schemas/models.py -m "Add user table"

    Generate to specific directory:
    $ reverie schema generate -s schemas/models.py -m "Add email field" -d db/migrations
  """
  try:
    asyncio.run(_generate_migration_async(schema_file, description, directory, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _generate_migration_async(
  schema_file: Path,
  description: str,
  directory: Path,
  verbose: bool,
) -> None:
  """Async implementation of generate migration."""
  from reverie.migration.diff import diff_tables
  from reverie.migration.generator import generate_migration_from_diffs
  from reverie.schema.registry import clear_registry

  if not schema_file.exists():
    display_error(f'Schema file not found: {schema_file}')
    raise typer.Exit(1)

  display_info(f'Loading schemas from: {schema_file}')

  # Clear registry and load new schemas
  clear_registry()
  code_tables = _load_schemas_from_file(schema_file)

  if not code_tables:
    display_warning('No schemas found in the specified file')
    display_info('Make sure your file registers schemas using register_table()')
    raise typer.Exit(1)

  display_success(f'Loaded {len(code_tables)} table schemas from file')

  # Fetch database schemas
  config = get_db_config()

  async with get_client(config) as client:
    display_info('Fetching database schema...')

    db_tables = await _fetch_db_tables(client)

    display_info(f'Found {len(db_tables)} tables in database')

    # Compare schemas (database is old, code is new)
    all_table_names = set(code_tables.keys()) | set(db_tables.keys())
    all_diffs = []

    for table_name in sorted(all_table_names):
      code_table = code_tables.get(table_name)
      db_table = db_tables.get(table_name)

      table_diffs = diff_tables(db_table, code_table)
      all_diffs.extend(table_diffs)

    # Check if there are any differences
    if not all_diffs:
      display_success('No schema differences found - code and database are in sync!')
      display_info('No migration needed.')
      return

    display_info(f'Found {len(all_diffs)} schema changes to migrate')

    if verbose:
      for diff in all_diffs:
        _display_diff(diff, verbose)

    # Generate migration file
    try:
      migration_path = generate_migration_from_diffs(
        directory=directory,
        description=description,
        diffs=all_diffs,
        author='reverie-cli',
      )
      display_success(f'Migration generated: {migration_path}')
      display_info('Use "reverie migrate up" to apply the migration')
    except Exception as e:
      display_error(f'Failed to generate migration: {e}')
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
    $ reverie schema sync --dry-run
  """
  display_warning('Schema sync not recommended - use migrations instead')
  display_info('Use "reverie schema generate" to create a migration from schema diff')
  display_info('Then use "reverie migrate up" to apply changes safely')


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
    $ reverie schema export --output schema.sql

    Export specific table as JSON:
    $ reverie schema export --table user --format json --output user.json
  """
  try:
    asyncio.run(_export_schema_async(output, table, format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _export_schema_async(
  output: str | None,
  table: str | None,
  format: str,
  _verbose: bool,
) -> None:
  """Async implementation of export schema."""
  config = get_db_config()

  async with get_client(config) as client:
    # Get schema info
    if table:
      query = f'INFO FOR TABLE {table};'
      title = f'Schema for table: {table}'
    else:
      query = 'INFO FOR DB;'
      title = 'Database schema'

    display_info(f'Exporting {title}')

    with spinner() as progress:
      task = progress.add_task('Fetching schema...', total=None)
      result = await client.execute(query)
      progress.update(task, completed=True)

    # Format output
    if format.lower() == 'json':
      import json

      content = json.dumps(result, indent=2, default=str)
    else:
      # SQL format - just stringify
      content = str(result)

    # Write to file or stdout
    if output:
      output_path = Path(output)
      output_path.write_text(content, encoding='utf-8')
      display_success(f'Schema exported to: {output}')
    else:
      # Print to stdout
      if format.lower() == 'json':
        format_output(result, OutputFormat.JSON)
      else:
        display_code(content, language='sql', title=title)


@app.command('tables')
def list_tables(
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TABLE,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """List all tables in the database.

  Examples:
    List tables:
    $ reverie schema tables

    List as JSON:
    $ reverie schema tables --format json
  """
  try:
    asyncio.run(_list_tables_async(output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _list_tables_async(
  output_format: OutputFormat,
  _verbose: bool,
) -> None:
  """Async implementation of list tables."""
  config = get_db_config()

  async with get_client(config) as client:
    display_info('Fetching tables...')

    # Get database info which includes tables
    result = await client.execute('INFO FOR DB;')

    # Extract table names from result - handle both direct dict and wrapped
    db_info = result
    if isinstance(result, list) and len(result) > 0:
      db_info = result[0].get('result', result[0]) if isinstance(result[0], dict) else result

    if isinstance(db_info, dict):
      # Try both 'tables' and 'tb' keys
      tables = db_info.get('tables', db_info.get('tb', {}))

      if tables:
        data = [{'name': name, 'definition': str(defn)} for name, defn in tables.items()]
        format_output(data, output_format, title='Database Tables')
      else:
        display_info('No tables found in database')
    else:
      # Show raw result
      format_output(result, OutputFormat.JSON)


@app.command('inspect')
def inspect_table(
  table: Annotated[str, typer.Argument(help='Table name to inspect')],
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Inspect detailed information about a table.

  Shows fields, indexes, events, and permissions for a table.

  Examples:
    Inspect table:
    $ reverie schema inspect user
  """
  try:
    asyncio.run(_inspect_table_async(table, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _inspect_table_async(
  table: str,
  _verbose: bool,
) -> None:
  """Async implementation of inspect table."""
  config = get_db_config()

  async with get_client(config) as client:
    display_info(f'Inspecting table: {table}')

    with spinner() as progress:
      task = progress.add_task('Fetching table info...', total=None)

      # Get table info
      result = await client.execute(f'INFO FOR TABLE {table};')

      progress.update(task, completed=True)

    # Display result - handle both direct dict and wrapped
    table_info = result
    if isinstance(result, list) and len(result) > 0:
      table_info = result[0].get('result', result[0]) if isinstance(result[0], dict) else result

    if isinstance(table_info, dict):
      # Display formatted info
      display_panel(
        str(table_info),
        title=f'Table: {table}',
        style='cyan',
      )

      # Try to extract and display specific sections if available
      # SurrealDB may use 'fields' or 'fd', 'indexes' or 'ix', 'events' or 'ev'
      fields = table_info.get('fields', table_info.get('fd', {}))
      indexes = table_info.get('indexes', table_info.get('ix', {}))
      events = table_info.get('events', table_info.get('ev', {}))

      if fields:
        display_info(f'\nFields: {len(fields)}')
      if indexes:
        display_info(f'Indexes: {len(indexes)}')
      if events:
        display_info(f'Events: {len(events)}')
    else:
      display_warning(f'No information found for table: {table}')
