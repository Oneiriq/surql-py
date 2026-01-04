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
  confirm,
  console,
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


# Exit codes for validate command
VALIDATE_EXIT_SUCCESS = 0
VALIDATE_EXIT_ERRORS = 1
VALIDATE_EXIT_WARNINGS = 2
VALIDATE_EXIT_CONNECTION_ERROR = 3


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
    $ reverie schema validate --schema schemas/models.py

    Validate with strict mode for CI/CD:
    $ reverie schema validate --schema schemas/models.py --strict

    Output as JSON:
    $ reverie schema validate --schema schemas/models.py --format json

    Write report to file:
    $ reverie schema validate --schema schemas/models.py --output report.txt
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


async def _validate_schema_async(
  schema_file: Path,
  strict: bool,
  strict_warnings: bool,
  output_format: str,
  output: Path | None,
  verbose: bool,
) -> int:
  """Async implementation of schema validation.

  Args:
    schema_file: Path to Python schema file
    strict: Exit with non-zero code on any drift
    strict_warnings: Also fail on warnings
    output_format: Output format (text, json)
    output: Output file path
    verbose: Enable verbose output

  Returns:
    Exit code based on validation results
  """
  from reverie.schema.registry import clear_registry
  from reverie.schema.validator import (
    filter_warnings,
    format_validation_report,
    get_validation_summary,
    has_errors,
  )
  from reverie.schema.validator import (
    validate_schema as run_validation,
  )

  # Validate schema file exists
  if not schema_file.exists():
    display_error(f'Schema file not found: {schema_file}')
    return VALIDATE_EXIT_ERRORS

  display_info(f'Loading schemas from: {schema_file}')

  # Clear registry and load new schemas
  clear_registry()
  code_tables = _load_schemas_from_file(schema_file)

  if not code_tables:
    display_warning('No schemas found in the specified file')
    display_info('Make sure your file registers schemas using register_table()')
    return VALIDATE_EXIT_ERRORS

  display_success(f'Loaded {len(code_tables)} table schemas from file')

  # Fetch database schemas and run validation
  config = get_db_config()

  async with get_client(config) as client:
    display_info('Validating schema against database...')

    results = await run_validation(code_tables, client)

    # Get summary statistics
    summary = get_validation_summary(results)

    # Determine exit code
    exit_code = VALIDATE_EXIT_SUCCESS
    if strict:
      if has_errors(results):
        exit_code = VALIDATE_EXIT_ERRORS
      elif strict_warnings and filter_warnings(results):
        exit_code = VALIDATE_EXIT_WARNINGS
    elif strict_warnings and filter_warnings(results):
      exit_code = VALIDATE_EXIT_WARNINGS

    # Format output
    if output_format.lower() == 'json':
      content = _format_validation_json(results, summary)
    else:
      content = format_validation_report(results, include_info=verbose)

    # Write output
    if output:
      output.write_text(content, encoding='utf-8')
      display_success(f'Validation report written to: {output}')
    else:
      # Print to stdout
      if output_format.lower() == 'json':
        print(content)
      else:
        _display_validation_results(results, summary, verbose)

    return exit_code


def _format_validation_json(
  results: list[Any],
  summary: dict[str, Any],
) -> str:
  """Format validation results as JSON.

  Args:
    results: List of ValidationResult objects
    summary: Summary statistics dictionary

  Returns:
    JSON string
  """
  import json

  from reverie.schema.validator import has_errors

  output_data = {
    'valid': not has_errors(results),
    'summary': {
      'total': summary['total'],
      'errors': summary['errors'],
      'warnings': summary['warnings'],
      'info': summary['info'],
    },
    'results': [
      {
        'severity': r.severity.value,
        'table': r.table,
        'field': r.field,
        'message': r.message,
        'code_value': r.code_value,
        'db_value': r.db_value,
      }
      for r in results
    ],
  }
  return json.dumps(output_data, indent=2)


def _display_validation_results(
  results: list[Any],
  summary: dict[str, Any],
  verbose: bool,
) -> None:
  """Display validation results to console with rich formatting.

  Args:
    results: List of ValidationResult objects
    summary: Summary statistics dictionary
    verbose: Include INFO severity results
  """
  from rich.console import Console
  from rich.text import Text

  from reverie.schema.validator import (
    ValidationSeverity,
    group_by_table,
  )

  console = Console()

  if not results:
    display_success('No schema validation issues found - code and database are in sync!')
    return

  # Filter results based on verbose flag
  filtered = results
  if not verbose:
    filtered = [r for r in results if r.severity != ValidationSeverity.INFO]

  if not filtered:
    display_success('No significant schema validation issues found.')
    return

  # Display summary header
  error_count = summary['errors']
  warning_count = summary['warnings']

  if error_count > 0:
    display_error(f'Schema validation found {error_count} errors, {warning_count} warnings')
  elif warning_count > 0:
    display_warning(f'Schema validation found {warning_count} warnings')
  else:
    display_info(f'Schema validation found {summary["info"]} informational items')

  # Group and display by table
  grouped = group_by_table(filtered)

  for table_name, table_results in sorted(grouped.items()):
    console.print(f'\n  [{table_name}]', style='bold')

    for result in table_results:
      # Color coding by severity
      if result.severity == ValidationSeverity.ERROR:
        style = 'bold red'
        icon = '[!]'
      elif result.severity == ValidationSeverity.WARNING:
        style = 'yellow'
        icon = '[~]'
      else:
        style = 'dim'
        icon = '[i]'

      text = Text()
      text.append(f'    {icon} ', style=style)
      text.append(result.message)
      if result.field:
        text.append(f' ({result.field})', style='cyan')
      console.print(text)

      if result.code_value or result.db_value:
        console.print(f'        code: {result.code_value}, db: {result.db_value}', style='dim')


# Exit codes for check command
CHECK_EXIT_NO_DRIFT = 0
CHECK_EXIT_DRIFT_DETECTED = 1
CHECK_EXIT_ERROR = 2


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
    $ reverie schema check --schema schemas/

    Check without failing on drift:
    $ reverie schema check --schema schemas/ --no-fail-on-drift

    Check with specific migrations directory:
    $ reverie schema check --schema schemas/ --migrations db/migrations

    Output as JSON:
    $ reverie schema check --schema schemas/ --format json
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


async def _check_schema_async(
  schema_path: Path,
  migrations_dir: Path | None,
  fail_on_drift: bool,
  show_diff: bool,
  output_format: str,
  verbose: bool,
) -> int:
  """Async implementation of schema check.

  Args:
    schema_path: Path to schema files or directory
    migrations_dir: Path to migrations directory (auto-detected if None)
    fail_on_drift: Exit with non-zero code when drift detected
    show_diff: Show detailed diff information
    output_format: Output format (text, json)
    verbose: Enable verbose output

  Returns:
    Exit code based on drift detection results
  """
  from reverie.migration.hooks import check_schema_drift

  # Validate schema path exists
  if not schema_path.exists():
    display_error(f'Schema path not found: {schema_path}')
    return CHECK_EXIT_ERROR

  # Build list of schema paths
  schema_paths = [schema_path]

  if verbose:
    display_info(f'Checking schema files in: {schema_path}')
    if migrations_dir:
      display_info(f'Using migrations directory: {migrations_dir}')
    else:
      display_info('Migrations directory will be auto-detected')

  # Run drift check
  result = await check_schema_drift(
    schema_paths=schema_paths,
    migrations_dir=migrations_dir,
    fail_on_drift=fail_on_drift,
  )

  # Format and display results
  if output_format.lower() == 'json':
    _display_check_result_json(result, show_diff)
  else:
    _display_check_result_text(result, show_diff, verbose)

  # Return appropriate exit code
  if result.passed:
    return CHECK_EXIT_NO_DRIFT
  return CHECK_EXIT_DRIFT_DETECTED


def _display_check_result_text(
  result: Any,
  show_diff: bool,
  verbose: bool,
) -> None:
  """Display check result as text.

  Args:
    result: HookCheckResult object
    show_diff: Show detailed diff information
    verbose: Enable verbose output
  """
  from rich.console import Console

  console = Console()

  if result.passed:
    display_success(result.message)
    return

  # Display drift detected
  display_warning('Schema drift detected!')
  console.print()

  if result.unmigrated_files:
    console.print('  Files with unmigrated changes:', style='bold')
    for file_path in result.unmigrated_files:
      console.print(f'    - {file_path}', style='yellow')

  if show_diff and verbose:
    console.print()
    console.print('  Details:', style='bold')
    # The message contains details when drift is found
    for line in result.message.split('\n')[1:]:  # Skip first line (header)
      if line.strip():
        console.print(f'  {line}', style='dim')

  if result.suggested_action:
    console.print()
    console.print('  Suggested action:', style='bold blue')
    console.print(f'    {result.suggested_action}', style='cyan')


def _display_check_result_json(result: Any, show_diff: bool) -> None:
  """Display check result as JSON.

  Args:
    result: HookCheckResult object
    show_diff: Show detailed diff information
  """
  import json

  output_data = {
    'passed': result.passed,
    'message': result.message,
    'unmigrated_files': [str(f) for f in result.unmigrated_files],
    'suggested_action': result.suggested_action,
  }

  if show_diff:
    output_data['details'] = result.message

  print(json.dumps(output_data, indent=2))


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
    $ reverie schema hook-config

    Generate config for custom schema path:
    $ reverie schema hook-config --schema src/schemas/

    Generate config that doesn't fail on drift:
    $ reverie schema hook-config --no-fail-on-drift
  """
  from reverie.migration.hooks import generate_precommit_config

  config = generate_precommit_config(
    schema_path=schema_path,
    fail_on_drift=fail_on_drift,
  )

  display_info('Add the following to your .pre-commit-config.yaml:')
  print()
  display_code(config, language='yaml', title='Pre-commit Configuration')


# Exit codes for watch command
WATCH_EXIT_SUCCESS = 0
WATCH_EXIT_ERROR = 1
WATCH_EXIT_INTERRUPTED = 130


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
    $ reverie schema watch --schema schemas/

    Watch with auto-generation:
    $ reverie schema watch --schema schemas/ --auto-generate

    Custom debounce delay:
    $ reverie schema watch --schema schemas/ --debounce 2.0

    Just report changes without prompting:
    $ reverie schema watch --schema schemas/ --no-prompt
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


# Visualization format enum (separate from CLI OutputFormat)
VISUALIZE_FORMATS = ['mermaid', 'graphviz', 'ascii']


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
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Generate visual diagrams of the database schema.

  Creates Mermaid ER diagrams, GraphViz DOT files, or ASCII art
  representations of schema definitions from a Python file.

  Examples:
    Generate Mermaid diagram to stdout:
    $ reverie schema visualize --schema schemas/models.py

    Generate GraphViz DOT file:
    $ reverie schema visualize --schema schemas/models.py --format graphviz --output schema.dot

    Generate ASCII art (terminal display):
    $ reverie schema visualize --schema schemas/models.py --format ascii

    Filter to specific tables:
    $ reverie schema visualize --schema schemas/models.py --tables user,post,comment

    Exclude edges:
    $ reverie schema visualize --schema schemas/models.py --no-edges
  """
  try:
    _visualize_schema(
      schema_file=schema_file,
      format=format,
      output=output,
      tables=tables,
      no_fields=no_fields,
      no_edges=no_edges,
      verbose=verbose,
    )
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


def _visualize_schema(
  schema_file: Path,
  format: str,
  output: Path | None,
  tables: str | None,
  no_fields: bool,
  no_edges: bool,
  verbose: bool,
) -> None:
  """Implementation of schema visualization.

  Args:
    schema_file: Path to Python schema file
    format: Output format (mermaid, graphviz, ascii)
    output: Output file path (None for stdout)
    tables: Comma-separated list of tables to include
    no_fields: Exclude field definitions
    no_edges: Exclude edge relationships
    verbose: Enable verbose output
  """
  from reverie.schema.registry import clear_registry, get_registered_edges
  from reverie.schema.visualize import OutputFormat as VisualizeFormat
  from reverie.schema.visualize import visualize_schema as generate_diagram

  # Validate format
  format_lower = format.lower()
  if format_lower not in VISUALIZE_FORMATS:
    display_error(f'Invalid format: {format}')
    display_info(f'Valid formats: {", ".join(VISUALIZE_FORMATS)}')
    raise typer.Exit(1)

  # Validate schema file exists
  if not schema_file.exists():
    display_error(f'Schema file not found: {schema_file}')
    raise typer.Exit(1)

  display_info(f'Loading schemas from: {schema_file}')

  # Clear registry and load schemas
  clear_registry()
  code_tables = _load_schemas_from_file(schema_file)
  code_edges = get_registered_edges()

  if not code_tables:
    display_warning('No schemas found in the specified file')
    display_info('Make sure your file registers schemas using register_table()')
    raise typer.Exit(1)

  display_success(f'Loaded {len(code_tables)} table schemas from file')
  if code_edges:
    display_info(f'Found {len(code_edges)} edge definitions')

  # Filter tables if specified
  if tables:
    table_list = [t.strip() for t in tables.split(',') if t.strip()]
    filtered_tables = {name: defn for name, defn in code_tables.items() if name in table_list}

    # Warn about missing tables
    missing = set(table_list) - set(filtered_tables.keys())
    if missing:
      display_warning(f'Tables not found: {", ".join(missing)}')

    if not filtered_tables:
      display_error('No matching tables found')
      raise typer.Exit(1)

    code_tables = filtered_tables
    display_info(f'Filtered to {len(code_tables)} tables')

    # Also filter edges to only include those between filtered tables
    if code_edges and not no_edges:
      table_names = set(code_tables.keys())
      filtered_edges = {
        name: edge
        for name, edge in code_edges.items()
        if edge.from_table in table_names and edge.to_table in table_names
      }
      code_edges = filtered_edges

  # Map format string to enum
  format_map = {
    'mermaid': VisualizeFormat.MERMAID,
    'graphviz': VisualizeFormat.GRAPHVIZ,
    'ascii': VisualizeFormat.ASCII,
  }
  output_format = format_map[format_lower]

  # Generate diagram
  if verbose:
    display_info(f'Generating {format_lower} diagram...')

  diagram = generate_diagram(
    tables=code_tables,
    edges=code_edges if not no_edges else None,
    output_format=output_format,
    include_fields=not no_fields,
    include_edges=not no_edges,
  )

  # Output handling
  if output:
    # Write to file
    output.write_text(diagram, encoding='utf-8')
    display_success(f'Diagram written to: {output}')
  else:
    # Output to stdout
    if format_lower == 'ascii':
      # Use Rich panel for ASCII art
      display_panel(diagram, title='Schema Diagram', style='cyan')
    elif format_lower == 'mermaid':
      # Display Mermaid as code block
      display_code(diagram, language='mermaid', title='Mermaid ER Diagram')
    else:
      # GraphViz DOT
      display_code(diagram, language='dot', title='GraphViz DOT Diagram')
