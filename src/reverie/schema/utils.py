"""Shared schema utility functions.

This module provides internal utility functions for fetching and processing
schema information from the database. These functions are shared across
schema validation and migration watching components.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import structlog

from reverie.schema.parser import parse_table_info as default_parse_table_info
from reverie.schema.table import TableDefinition

if TYPE_CHECKING:
  from reverie.connection.client import DatabaseClient

logger = structlog.get_logger(__name__)


async def fetch_db_tables(
  client: 'DatabaseClient',
  parse_table_info: Callable[[str, dict[str, Any]], TableDefinition] | None = None,
) -> dict[str, TableDefinition]:
  """Fetch table definitions from database.

  Queries the database for schema information and parses it into
  TableDefinition objects for comparison.

  This is an internal utility function used by schema validation and
  migration detection components.

  Args:
    client: Connected database client
    parse_table_info: Optional custom parser function. If not provided,
      uses the default parse_table_info from reverie.schema.parser.
      This parameter allows callers to inject their own parser for
      testing purposes.

  Returns:
    Dictionary of table name to TableDefinition

  Examples:
    >>> async with get_client(config) as client:
    ...   tables = await fetch_db_tables(client)
    ...   for name, table_def in tables.items():
    ...     print(f'{name}: {len(table_def.fields)} fields')
  """
  # Use default parser if none provided
  parser = parse_table_info if parse_table_info is not None else default_parse_table_info
  db_tables: dict[str, TableDefinition] = {}

  # Get list of tables from database
  db_info = await client.execute('INFO FOR DB;')

  # Parse database info - handle both direct dict and wrapped result
  result = db_info
  if isinstance(db_info, list) and len(db_info) > 0:
    first_item = db_info[0]
    result = first_item.get('result', first_item) if isinstance(first_item, dict) else db_info
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
          first_info = table_info[0]
          info_result = (
            first_info.get('result', first_info) if isinstance(first_info, dict) else table_info
          )
        if isinstance(info_result, dict):
          db_tables[table_name] = parser(table_name, info_result)
      except Exception as e:
        logger.warning('table_fetch_failed', table=table_name, error=str(e))

  return db_tables
