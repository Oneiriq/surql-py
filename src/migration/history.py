"""Migration history tracking in database.

This module provides functions for tracking migration history in the database,
including creating the migration history table and recording applied migrations.
"""

from datetime import datetime
from typing import Any

import structlog

from src.connection.client import DatabaseClient, QueryError
from src.migration.models import MigrationHistory

logger = structlog.get_logger(__name__)


class MigrationHistoryError(Exception):
  """Raised when migration history operations fail."""

  pass


MIGRATION_TABLE_NAME = '_migration_history'


async def create_migration_table(client: DatabaseClient) -> None:
  """Create the migration history table if it doesn't exist.

  This table tracks all applied migrations with their metadata.

  Args:
    client: Database client

  Raises:
    MigrationHistoryError: If table creation fails

  Examples:
    >>> async with get_client(config) as client:
    ...   await create_migration_table(client)
  """
  log = logger.bind(table=MIGRATION_TABLE_NAME)

  try:
    log.info('creating_migration_history_table')

    # Define the migration history table
    statements = [
      f'DEFINE TABLE {MIGRATION_TABLE_NAME} SCHEMAFULL;',
      f'DEFINE FIELD version ON TABLE {MIGRATION_TABLE_NAME} TYPE string;',
      f'DEFINE FIELD description ON TABLE {MIGRATION_TABLE_NAME} TYPE string;',
      f'DEFINE FIELD applied_at ON TABLE {MIGRATION_TABLE_NAME} TYPE datetime;',
      f'DEFINE FIELD checksum ON TABLE {MIGRATION_TABLE_NAME} TYPE string;',
      f'DEFINE FIELD execution_time_ms ON TABLE {MIGRATION_TABLE_NAME} TYPE int;',
      f'DEFINE INDEX version_idx ON TABLE {MIGRATION_TABLE_NAME} COLUMNS version UNIQUE;',
    ]

    for statement in statements:
      await client.execute(statement)

    log.info('migration_history_table_created')

  except QueryError as e:
    log.error('failed_to_create_migration_table', error=str(e))
    raise MigrationHistoryError(f'Failed to create migration history table: {e}') from e
  except Exception as e:
    log.error('unexpected_error_creating_migration_table', error=str(e))
    raise MigrationHistoryError(f'Unexpected error creating migration table: {e}') from e


async def ensure_migration_table(client: DatabaseClient) -> None:
  """Ensure migration history table exists, creating it if needed.

  Args:
    client: Database client

  Raises:
    MigrationHistoryError: If operation fails
  """
  try:
    # Try to query the table to see if it exists
    await client.execute(f'SELECT * FROM {MIGRATION_TABLE_NAME} LIMIT 1')
  except QueryError:
    # Table doesn't exist, create it
    await create_migration_table(client)


async def record_migration(
  client: DatabaseClient,
  version: str,
  description: str,
  checksum: str,
  execution_time_ms: int | None = None,
) -> None:
  """Record a migration as applied in the history table.

  Args:
    client: Database client
    version: Migration version
    description: Migration description
    checksum: Migration content checksum
    execution_time_ms: Optional execution time in milliseconds

  Raises:
    MigrationHistoryError: If recording fails

  Examples:
    >>> await record_migration(
    ...   client,
    ...   '20260102_120000',
    ...   'Create user table',
    ...   'abc123',
    ...   150,
    ... )
  """
  log = logger.bind(version=version)

  try:
    log.info('recording_migration', description=description)

    # Ensure table exists
    await ensure_migration_table(client)

    # Create migration history record
    data = {
      'version': version,
      'description': description,
      'applied_at': datetime.utcnow().isoformat(),
      'checksum': checksum,
    }

    if execution_time_ms is not None:
      data['execution_time_ms'] = execution_time_ms

    await client.create(MIGRATION_TABLE_NAME, data)

    log.info('migration_recorded', version=version)

  except QueryError as e:
    log.error('failed_to_record_migration', error=str(e))
    raise MigrationHistoryError(f'Failed to record migration {version}: {e}') from e
  except Exception as e:
    log.error('unexpected_error_recording_migration', error=str(e))
    raise MigrationHistoryError(f'Unexpected error recording migration: {e}') from e


async def remove_migration_record(
  client: DatabaseClient,
  version: str,
) -> None:
  """Remove a migration record from history (used during rollback).

  Args:
    client: Database client
    version: Migration version to remove

  Raises:
    MigrationHistoryError: If removal fails

  Examples:
    >>> await remove_migration_record(client, '20260102_120000')
  """
  log = logger.bind(version=version)

  try:
    log.info('removing_migration_record')

    # Query to find the record
    query = f'SELECT * FROM {MIGRATION_TABLE_NAME} WHERE version = $version'
    result = await client.execute(query, {'version': version})

    # Extract records from result
    records = _extract_records(result)

    if not records:
      log.warning('migration_record_not_found')
      return

    # Delete the record
    record_id = records[0].get('id')
    if record_id:
      await client.delete(record_id)
      log.info('migration_record_removed')

  except QueryError as e:
    log.error('failed_to_remove_migration_record', error=str(e))
    raise MigrationHistoryError(f'Failed to remove migration record {version}: {e}') from e
  except Exception as e:
    log.error('unexpected_error_removing_migration_record', error=str(e))
    raise MigrationHistoryError(f'Unexpected error removing migration record: {e}') from e


async def get_applied_migrations(client: DatabaseClient) -> list[MigrationHistory]:
  """Get all applied migrations from history.

  Args:
    client: Database client

  Returns:
    List of MigrationHistory objects, sorted by applied_at

  Raises:
    MigrationHistoryError: If query fails

  Examples:
    >>> applied = await get_applied_migrations(client)
    >>> for migration in applied:
    ...   print(migration.version, migration.applied_at)
  """
  log = logger.bind(table=MIGRATION_TABLE_NAME)

  try:
    log.debug('fetching_applied_migrations')

    # Ensure table exists
    await ensure_migration_table(client)

    # Query all migration records
    query = f'SELECT * FROM {MIGRATION_TABLE_NAME} ORDER BY applied_at ASC'
    result = await client.execute(query)

    # Extract records from result
    records = _extract_records(result)

    # Convert to MigrationHistory objects
    migrations = []
    for record in records:
      try:
        history = MigrationHistory(
          version=record['version'],
          description=record['description'],
          applied_at=_parse_datetime(record['applied_at']),
          checksum=record['checksum'],
          execution_time_ms=record.get('execution_time_ms'),
        )
        migrations.append(history)
      except Exception as e:
        log.warning('skipping_invalid_migration_record', record=record, error=str(e))

    log.debug('applied_migrations_fetched', count=len(migrations))
    return migrations

  except QueryError as e:
    log.error('failed_to_fetch_applied_migrations', error=str(e))
    raise MigrationHistoryError(f'Failed to fetch applied migrations: {e}') from e
  except Exception as e:
    log.error('unexpected_error_fetching_applied_migrations', error=str(e))
    raise MigrationHistoryError(f'Unexpected error fetching applied migrations: {e}') from e


async def get_applied_versions(client: DatabaseClient) -> set[str]:
  """Get set of applied migration versions.

  Args:
    client: Database client

  Returns:
    Set of version strings

  Raises:
    MigrationHistoryError: If query fails

  Examples:
    >>> versions = await get_applied_versions(client)
    >>> '20260102_120000' in versions
    True
  """
  migrations = await get_applied_migrations(client)
  return {m.version for m in migrations}


async def is_migration_applied(
  client: DatabaseClient,
  version: str,
) -> bool:
  """Check if a migration has been applied.

  Args:
    client: Database client
    version: Migration version to check

  Returns:
    True if migration has been applied, False otherwise

  Raises:
    MigrationHistoryError: If query fails

  Examples:
    >>> await is_migration_applied(client, '20260102_120000')
    True
  """
  versions = await get_applied_versions(client)
  return version in versions


async def get_migration_history(
  client: DatabaseClient,
  version: str,
) -> MigrationHistory | None:
  """Get history record for a specific migration.

  Args:
    client: Database client
    version: Migration version

  Returns:
    MigrationHistory object or None if not found

  Raises:
    MigrationHistoryError: If query fails

  Examples:
    >>> history = await get_migration_history(client, '20260102_120000')
    >>> if history:
    ...   print(f'Applied at: {history.applied_at}')
  """
  log = logger.bind(version=version)

  try:
    # Ensure table exists
    await ensure_migration_table(client)

    # Query for specific version
    query = f'SELECT * FROM {MIGRATION_TABLE_NAME} WHERE version = $version'
    result = await client.execute(query, {'version': version})

    # Extract records from result
    records = _extract_records(result)

    if not records:
      return None

    record = records[0]
    return MigrationHistory(
      version=record['version'],
      description=record['description'],
      applied_at=_parse_datetime(record['applied_at']),
      checksum=record['checksum'],
      execution_time_ms=record.get('execution_time_ms'),
    )

  except QueryError as e:
    log.error('failed_to_get_migration_history', error=str(e))
    raise MigrationHistoryError(f'Failed to get migration history for {version}: {e}') from e
  except Exception as e:
    log.error('unexpected_error_getting_migration_history', error=str(e))
    raise MigrationHistoryError(f'Unexpected error getting migration history: {e}') from e


def _extract_records(result: Any) -> list[dict[str, Any]]:
  """Extract records from SurrealDB query result.

  Args:
    result: Raw query result

  Returns:
    List of record dictionaries
  """
  # SurrealDB query results can be in different formats
  # Handle common cases
  if isinstance(result, list):
    if len(result) > 0 and isinstance(result[0], dict):
      if 'result' in result[0]:
        return result[0]['result'] or []
      return result
    return result
  elif isinstance(result, dict):
    if 'result' in result:
      return result['result'] or []
    return [result]

  return []


def _parse_datetime(value: Any) -> datetime:
  """Parse datetime from various formats.

  Args:
    value: Datetime value (string, datetime, or other)

  Returns:
    datetime object
  """
  if isinstance(value, datetime):
    return value
  elif isinstance(value, str):
    # Try ISO format
    try:
      return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except Exception:
      # Fallback to current time if parsing fails
      return datetime.utcnow()
  else:
    return datetime.utcnow()
