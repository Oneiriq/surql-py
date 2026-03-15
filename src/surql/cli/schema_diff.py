"""Schema diff and migration generation implementations.

Async handlers for diff and generate commands,
plus the shared _load_schemas_from_file helper used by multiple sub-modules.
"""

import importlib.util
from pathlib import Path
from typing import Any

import typer

from surql.cli.common import (
  OutputFormat,
  display_error,
  display_info,
  display_success,
  display_warning,
)
from surql.cli.schema_inspect import _fetch_db_tables
from surql.connection.client import get_client
from surql.settings import get_db_config


def _load_schemas_from_file(file_path: Path) -> dict[str, Any]:
  """Load schema definitions from a Python file.

  Args:
    file_path: Path to Python file containing schema definitions

  Returns:
    Dictionary of table name to TableDefinition
  """
  from surql.schema.registry import get_registered_tables
  from surql.schema.table import TableDefinition

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


async def _diff_schema_async(
  schema_file: Path | None,
  output_format: OutputFormat,
  verbose: bool,
) -> None:
  """Async implementation of schema diff."""
  from surql.migration.diff import diff_tables
  from surql.schema.registry import clear_registry

  # Load schemas from file if provided
  if schema_file is None:
    display_error('Schema file required. Use --schema to specify a Python file.')
    display_info('Example: surql schema diff --schema schemas/models.py')
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


async def _generate_migration_async(
  schema_file: Path,
  description: str,
  directory: Path,
  verbose: bool,
) -> None:
  """Async implementation of generate migration."""
  from surql.migration.diff import diff_tables
  from surql.migration.generator import generate_migration_from_diffs
  from surql.schema.registry import clear_registry

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
        author='surql-cli',
      )
      display_success(f'Migration generated: {migration_path}')
      display_info('Use "surql migrate up" to apply the migration')
    except Exception as e:
      display_error(f'Failed to generate migration: {e}')
      raise typer.Exit(1) from e
