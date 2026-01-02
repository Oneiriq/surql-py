"""Database utility CLI commands.

This module provides CLI commands for database utilities including
initialization, reset, and connection testing.
"""

import asyncio
from typing import Annotated

import structlog
import typer

from src.cli.common import (
  OutputFormat,
  confirm_destructive,
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
from src.connection.client import ConnectionError as DBConnectionError
from src.connection.client import get_client
from src.migration.history import create_migration_table
from src.settings import get_db_config

logger = structlog.get_logger(__name__)

app = typer.Typer(
  name='db',
  help='Database utility commands',
  no_args_is_help=True,
)


@app.command('init')
def init_database(
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Initialize database and create migration tracking table.

  Creates the _migration_history table used to track applied migrations.
  Safe to run multiple times - will not recreate if already exists.

  Examples:
    Initialize database:
    $ reverie db init
  """
  try:
    asyncio.run(_init_database_async(verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _init_database_async(_verbose: bool) -> None:
  """Async implementation of init database."""
  config = get_db_config()

  display_info(f'Connecting to database: {config.namespace}/{config.database}')

  try:
    async with get_client(config) as client:
      display_info('Creating migration tracking table...')

      with spinner() as progress:
        task = progress.add_task('Initializing...', total=None)

        await create_migration_table(client)

        progress.update(task, completed=True)

      display_success('Database initialized successfully')
      display_info('Migration tracking table created: _migration_history')

  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    display_info('Check your database configuration in environment variables')
    raise typer.Exit(1) from e


@app.command('ping')
def ping_database(
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Test database connectivity.

  Attempts to connect to the database and execute a simple query.
  Useful for verifying configuration and connectivity.

  Examples:
    Test connection:
    $ reverie db ping
  """
  try:
    asyncio.run(_ping_database_async(verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _ping_database_async(verbose: bool) -> None:
  """Async implementation of ping database."""
  config = get_db_config()

  display_info(f'Testing connection to: {config.url}')
  display_info(f'Namespace: {config.namespace}')
  display_info(f'Database: {config.database}')

  try:
    with spinner() as progress:
      task = progress.add_task('Connecting...', total=None)

      async with get_client(config) as client:
        # Try a simple query
        result = await client.execute('SELECT 1 as ping;')

        progress.update(task, completed=True)

    display_success('Connection successful!')

    if verbose:
      display_info(f'Query result: {result}')

  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    display_info('Verify your database is running and configuration is correct')
    raise typer.Exit(1) from e


@app.command('info')
def database_info(
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TEXT,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Show database connection information.

  Displays current database configuration (without sensitive data).

  Examples:
    Show database info:
    $ reverie db info

    Show as JSON:
    $ reverie db info --format json
  """
  try:
    config = get_db_config()

    # Prepare info (mask password)
    info = {
      'url': config.url,
      'namespace': config.namespace,
      'database': config.database,
      'username': config.username or 'None',
      'password': '***' if config.password else 'None',
      'timeout': f'{config.timeout}s',
      'max_connections': config.max_connections,
      'retry_max_attempts': config.retry_max_attempts,
    }

    if output_format == OutputFormat.JSON:
      format_output(info, OutputFormat.JSON)
    else:
      # Format as text
      content = '\n'.join(
        [f'{key.replace("_", " ").title()}: {value}' for key, value in info.items()]
      )

      display_panel(
        content,
        title='Database Configuration',
        style='cyan',
      )

      display_info('\nConfiguration is loaded from environment variables with DB_ prefix')

  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('reset')
def reset_database(
  confirm: Annotated[bool, typer.Option('--yes', '-y', help='Skip confirmation prompt')] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Reset database by removing all tables.

  WARNING: This is a destructive operation that will delete ALL data and tables.
  Use with extreme caution, especially in production environments.

  Examples:
    Reset database (with confirmation):
    $ reverie db reset

    Reset without confirmation prompt:
    $ reverie db reset --yes
  """
  try:
    asyncio.run(_reset_database_async(confirm, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _reset_database_async(skip_confirm: bool, verbose: bool) -> None:
  """Async implementation of reset database."""
  config = get_db_config()

  display_warning('=' * 60)
  display_warning('DANGER: Database Reset Operation')
  display_warning('=' * 60)
  display_warning(f'Database: {config.namespace}/{config.database}')
  display_warning('This will DELETE ALL tables and data')
  display_warning('=' * 60)

  # Require confirmation
  if not skip_confirm and not confirm_destructive('Reset database and delete all tables?'):
    display_info('Reset cancelled')
    return

  try:
    async with get_client(config) as client:
      display_info('Fetching list of tables...')

      # Get database info to find all tables
      result = await client.execute('INFO FOR DB;')

      # Extract table names
      tables = []
      if (
        isinstance(result, list)
        and len(result) > 0
        and isinstance(result[0], dict)
        and 'result' in result[0]
      ):
        db_info = result[0]['result']

        if isinstance(db_info, dict) and 'tb' in db_info:
          tables = list(db_info['tb'].keys())

      if not tables:
        display_info('No tables found to remove')
        return

      display_warning(f'Found {len(tables)} table(s) to remove')

      if verbose:
        for table in tables:
          display_info(f'  - {table}')

      # Remove each table
      with spinner() as progress:
        task = progress.add_task(
          f'Removing {len(tables)} table(s)...',
          total=len(tables),
        )

        for table in tables:
          await client.execute(f'REMOVE TABLE {table};')
          progress.update(task, advance=1)

      display_success(f'Successfully removed {len(tables)} table(s)')
      display_info('Database has been reset')
      display_info('Run "reverie db init" to reinitialize migration tracking')

  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e


@app.command('query')
def execute_query(
  query: Annotated[str, typer.Argument(help='SurrealQL query to execute')],
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.JSON,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Execute a raw SurrealQL query.

  Executes a query against the database and displays the results.

  Examples:
    Execute query:
    $ reverie db query "SELECT * FROM user LIMIT 5"

    Execute with table format:
    $ reverie db query "SELECT * FROM user" --format table
  """
  try:
    asyncio.run(_execute_query_async(query, output_format, verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _execute_query_async(
  query: str,
  output_format: OutputFormat,
  verbose: bool,
) -> None:
  """Async implementation of execute query."""
  config = get_db_config()

  try:
    async with get_client(config) as client:
      if verbose:
        display_info(f'Executing query: {query}')

      with spinner() as progress:
        task = progress.add_task('Executing...', total=None)

        result = await client.execute(query)

        progress.update(task, completed=True)

      # Display result
      if result:
        format_output(result, output_format, title='Query Results')
      else:
        display_info('Query executed successfully (no results)')

  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    display_error(f'Query failed: {e}')
    raise typer.Exit(1) from e


@app.command('version')
def database_version(
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Show database version information.

  Displays SurrealDB version and other database information.

  Examples:
    Show version:
    $ reverie db version
  """
  try:
    asyncio.run(_database_version_async(verbose))
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


async def _database_version_async(verbose: bool) -> None:
  """Async implementation of database version."""
  config = get_db_config()

  try:
    async with get_client(config) as client:
      # Try to get version info
      # Note: SurrealDB may not have a direct version query
      # This is a placeholder implementation

      display_info(f'Connected to: {config.url}')
      display_info(f'Namespace: {config.namespace}')
      display_info(f'Database: {config.database}')

      # Try to get some database info
      result = await client.execute('INFO FOR DB;')

      display_success('Database is accessible')

      if verbose and result:
        format_output(result, OutputFormat.JSON, title='Database Info')

  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
