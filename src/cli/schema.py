"""Schema inspection CLI commands.

This module provides CLI commands for inspecting and managing database schemas
including viewing, comparing, and exporting schema definitions.
"""

import asyncio
from typing import Annotated, Optional

import structlog
import typer

from src.cli.common import (
  confirm_destructive,
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
  OutputFormat,
)
from src.connection.client import get_client
from src.settings import get_db_config

logger = structlog.get_logger(__name__)

app = typer.Typer(
  name='schema',
  help='Schema inspection and management commands',
  no_args_is_help=True,
)


@app.command('show')
def show_schema(
  table: Annotated[
    Optional[str],
    typer.Argument(help='Specific table name to inspect (default: show all)')
  ] = None,
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TEXT,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Show current database schema.
  
  Displays schema information from the database using INFO statements.
  
  Examples:
    Show all schema:
    $ ethereal schema show
    
    Show specific table:
    $ ethereal schema show user
    
    Show as JSON:
    $ ethereal schema show --format json
  """
  try:
    asyncio.run(_show_schema_async(table, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1)


async def _show_schema_async(
  table: Optional[str],
  output_format: OutputFormat,
  verbose: bool,
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


@app.command('diff')
def diff_schema(
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Compare code schema definitions with database schema.
  
  NOTE: This requires schema registry implementation.
  Currently not available.
  
  Examples:
    Compare schemas:
    $ ethereal schema diff
  """
  display_warning('Schema diff not yet implemented')
  display_info('This feature requires schema registry implementation')
  display_info('Use "ethereal schema show" to view current database schema')


@app.command('sync')
def sync_schema(
  dry_run: Annotated[
    bool,
    typer.Option('--dry-run', help='Show what would be synced without making changes')
  ] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Synchronize code schema to database.
  
  NOTE: This is a potentially destructive operation that requires
  schema registry implementation. Currently not available.
  
  Use migrations instead for safe schema changes.
  
  Examples:
    Preview sync:
    $ ethereal schema sync --dry-run
  """
  display_warning('Schema sync not yet implemented')
  display_info('This feature requires schema registry implementation')
  display_info('Use "ethereal migrate" commands to safely manage schema changes')


@app.command('export')
def export_schema(
  output: Annotated[
    Optional[str],
    typer.Option('--output', '-o', help='Output file path (default: stdout)')
  ] = None,
  table: Annotated[
    Optional[str],
    typer.Option('--table', '-t', help='Export specific table only')
  ] = None,
  format: Annotated[
    str,
    typer.Option('--format', '-f', help='Export format (sql, json)')
  ] = 'sql',
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Export database schema to file.
  
  Exports the current database schema as SQL or JSON.
  
  Examples:
    Export all schema to SQL:
    $ ethereal schema export --output schema.sql
    
    Export specific table as JSON:
    $ ethereal schema export --table user --format json --output user.json
  """
  try:
    asyncio.run(_export_schema_async(output, table, format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1)


async def _export_schema_async(
  output: Optional[str],
  table: Optional[str],
  format: str,
  verbose: bool,
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
      from pathlib import Path
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
    $ ethereal schema tables
    
    List as JSON:
    $ ethereal schema tables --format json
  """
  try:
    asyncio.run(_list_tables_async(output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1)


async def _list_tables_async(
  output_format: OutputFormat,
  verbose: bool,
) -> None:
  """Async implementation of list tables."""
  config = get_db_config()
  
  async with get_client(config) as client:
    display_info('Fetching tables...')
    
    # Get database info which includes tables
    result = await client.execute('INFO FOR DB;')
    
    # Extract table names from result
    # Note: Result format may vary, this is a simplified approach
    if isinstance(result, list) and len(result) > 0:
      if isinstance(result[0], dict) and 'result' in result[0]:
        db_info = result[0]['result']
        
        # Try to extract tables
        if isinstance(db_info, dict) and 'tb' in db_info:
          tables = db_info['tb']
          
          if tables:
            data = [
              {'name': name, 'definition': str(defn)}
              for name, defn in tables.items()
            ]
            format_output(data, output_format, title='Database Tables')
          else:
            display_info('No tables found in database')
        else:
          # Fallback: show raw result
          display_warning('Could not parse table list from database info')
          format_output(db_info, OutputFormat.JSON)
      else:
        # Show raw result
        format_output(result, OutputFormat.JSON)
    else:
      display_warning('No schema information available')


@app.command('inspect')
def inspect_table(
  table: Annotated[str, typer.Argument(help='Table name to inspect')],
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Inspect detailed information about a table.
  
  Shows fields, indexes, events, and permissions for a table.
  
  Examples:
    Inspect table:
    $ ethereal schema inspect user
  """
  try:
    asyncio.run(_inspect_table_async(table, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1)


async def _inspect_table_async(
  table: str,
  verbose: bool,
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
    
    # Display result
    if isinstance(result, list) and len(result) > 0:
      if isinstance(result[0], dict) and 'result' in result[0]:
        table_info = result[0]['result']
        
        # Display formatted info
        display_panel(
          str(table_info),
          title=f'Table: {table}',
          style='cyan',
        )
        
        # Try to extract and display specific sections if available
        if isinstance(table_info, dict):
          if 'fd' in table_info and table_info['fd']:
            display_info(f'\nFields: {len(table_info["fd"])}')
          
          if 'ix' in table_info and table_info['ix']:
            display_info(f'Indexes: {len(table_info["ix"])}')
          
          if 'ev' in table_info and table_info['ev']:
            display_info(f'Events: {len(table_info["ev"])}')
      else:
        format_output(result, OutputFormat.JSON)
    else:
      display_warning(f'No information found for table: {table}')
