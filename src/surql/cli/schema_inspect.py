"""Schema inspection and export implementations.

Async handlers for show, tables, inspect, and export commands,
plus the shared _fetch_db_tables helper used by multiple sub-modules.
"""

from pathlib import Path
from typing import Any

import structlog

from surql.cli.common import (
  OutputFormat,
  display_code,
  display_info,
  display_panel,
  display_success,
  display_warning,
  format_output,
  spinner,
)
from surql.connection.client import get_client
from surql.settings import get_db_config

logger = structlog.get_logger(__name__)


async def _fetch_db_tables(client: Any) -> dict[str, Any]:
  """Fetch table definitions from database.

  Args:
    client: Database client

  Returns:
    Dictionary of table name to TableDefinition
  """
  from surql.schema.parser import parse_table_info
  from surql.schema.table import TableDefinition

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
