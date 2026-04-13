"""Migration execution engine.

This module provides functions for executing migrations, including applying
pending migrations and rolling back applied migrations.
"""

import time

import structlog

from surql.connection.client import DatabaseClient, QueryError
from surql.migration.history import (
  get_applied_versions,
  record_migration,
  remove_migration_record,
)
from surql.migration.models import (
  Migration,
  MigrationDirection,
  MigrationPlan,
  MigrationState,
  MigrationStatus,
)

logger = structlog.get_logger(__name__)


# Embedded URL schemes route through the SurrealDB Python SDK's
# AsyncEmbeddedSurrealConnection. That path currently crashes on BEGIN/COMMIT
# TRANSACTION with `IndexError: list index out of range` because the SDK's
# query() method assumes response["result"][0]["result"] is populated, which
# isn't the case for transaction-control statements in embedded mode. Until
# the upstream SDK is fixed we skip the transaction wrapper in embedded mode;
# embedded migrations are still effectively atomic because the engine lives
# in the application process (a crash during migration takes the whole process
# with it, rather than leaving a partial remote schema).
_EMBEDDED_URL_SCHEMES = ('mem://', 'memory://', 'file://', 'surrealkv://')


def _is_embedded_client(client: DatabaseClient) -> bool:
  """Return True when the client is connected via an embedded engine."""
  try:
    url = client._config.db_url
  except AttributeError:
    return False
  return any(url.startswith(scheme) for scheme in _EMBEDDED_URL_SCHEMES)


class MigrationExecutionError(Exception):
  """Raised when migration execution fails."""

  pass


async def execute_migration(
  client: DatabaseClient,
  migration: Migration,
  direction: MigrationDirection = MigrationDirection.UP,
) -> int:
  """Execute a single migration in the specified direction.

  Args:
    client: Database client
    migration: Migration to execute
    direction: Direction to execute (UP or DOWN)

  Returns:
    Execution time in milliseconds

  Raises:
    MigrationExecutionError: If execution fails

  Examples:
    >>> duration = await execute_migration(client, migration, MigrationDirection.UP)
    >>> print(f'Executed in {duration}ms')
  """
  log = logger.bind(
    version=migration.version,
    direction=direction.value,
    description=migration.description,
  )

  try:
    log.info('executing_migration')

    # Get SQL statements based on direction
    statements = migration.up() if direction == MigrationDirection.UP else migration.down()

    # Record start time
    start_time = time.time()

    # Execute statements within a transaction for atomicity on remote
    # connections. Embedded engines skip the wrapper (see
    # _is_embedded_client note at module top).
    embedded = _is_embedded_client(client)

    async def _run_statements() -> None:
      for i, statement in enumerate(statements):
        try:
          log.debug('executing_statement', statement_index=i, statement=statement)
          await client.execute(statement)
        except QueryError as e:
          log.error(
            'statement_execution_failed',
            statement_index=i,
            statement=statement,
            error=str(e),
          )
          raise MigrationExecutionError(
            f'Failed to execute statement {i} in migration {migration.version}: {e}'
          ) from e

    if embedded:
      log.debug('migration_transaction_skipped', reason='embedded_connection')
      await _run_statements()
    else:
      await client.execute('BEGIN TRANSACTION;')
      try:
        await _run_statements()
        await client.execute('COMMIT TRANSACTION;')
      except BaseException:
        try:
          await client.execute('CANCEL TRANSACTION;')
        except Exception as cancel_err:
          log.error('transaction_cancel_failed', error=str(cancel_err))
        raise

    # Calculate execution time
    execution_time_ms = int((time.time() - start_time) * 1000)

    # Update migration history
    if direction == MigrationDirection.UP:
      await record_migration(
        client,
        migration.version,
        migration.description,
        migration.checksum or '',
        execution_time_ms,
      )
    else:
      await remove_migration_record(client, migration.version)

    log.info('migration_executed', execution_time_ms=execution_time_ms)
    return execution_time_ms

  except MigrationExecutionError:
    raise
  except Exception as e:
    log.error('migration_execution_failed', error=str(e))
    raise MigrationExecutionError(f'Failed to execute migration {migration.version}: {e}') from e


async def migrate_up(
  client: DatabaseClient,
  migrations: list[Migration],
  steps: int | None = None,
) -> list[Migration]:
  """Apply pending migrations.

  Args:
    client: Database client
    migrations: List of all available migrations
    steps: Optional number of migrations to apply (None = all)

  Returns:
    List of applied migrations

  Raises:
    MigrationExecutionError: If any migration fails

  Examples:
    >>> applied = await migrate_up(client, all_migrations)
    >>> print(f'Applied {len(applied)} migrations')

    >>> applied = await migrate_up(client, all_migrations, steps=1)
    >>> print(f'Applied 1 migration')
  """
  log = logger.bind(total_migrations=len(migrations), steps=steps)

  try:
    log.info('starting_migrate_up')

    # Get pending migrations
    pending = await get_pending_migrations(client, migrations)

    if not pending:
      log.info('no_pending_migrations')
      return []

    # Limit to requested steps
    to_apply = pending[:steps] if steps else pending

    log.info('applying_migrations', count=len(to_apply))

    # Execute each migration
    applied: list[Migration] = []
    for migration in to_apply:
      await execute_migration(client, migration, MigrationDirection.UP)
      applied.append(migration)

    log.info('migrate_up_complete', applied_count=len(applied))
    return applied

  except Exception as e:
    log.error('migrate_up_failed', error=str(e))
    raise MigrationExecutionError(f'Failed to migrate up: {e}') from e


async def migrate_down(
  client: DatabaseClient,
  migrations: list[Migration],
  steps: int = 1,
) -> list[Migration]:
  """Rollback applied migrations.

  Args:
    client: Database client
    migrations: List of all available migrations
    steps: Number of migrations to rollback (default: 1)

  Returns:
    List of rolled back migrations

  Raises:
    MigrationExecutionError: If any rollback fails

  Examples:
    >>> rolled_back = await migrate_down(client, all_migrations, steps=1)
    >>> print(f'Rolled back {len(rolled_back)} migrations')
  """
  log = logger.bind(total_migrations=len(migrations), steps=steps)

  try:
    log.info('starting_migrate_down')

    # Get applied migrations (in reverse order)
    applied = await get_applied_migrations_ordered(client, migrations)

    if not applied:
      log.info('no_applied_migrations_to_rollback')
      return []

    # Take the last N applied migrations to rollback
    to_rollback = applied[-steps:]

    log.info('rolling_back_migrations', count=len(to_rollback))

    # Execute rollbacks in reverse order
    rolled_back: list[Migration] = []
    for migration in reversed(to_rollback):
      await execute_migration(client, migration, MigrationDirection.DOWN)
      rolled_back.append(migration)

    log.info('migrate_down_complete', rolled_back_count=len(rolled_back))
    return rolled_back

  except Exception as e:
    log.error('migrate_down_failed', error=str(e))
    raise MigrationExecutionError(f'Failed to migrate down: {e}') from e


async def get_pending_migrations(
  client: DatabaseClient,
  migrations: list[Migration],
) -> list[Migration]:
  """Get migrations that have not been applied.

  Args:
    client: Database client
    migrations: List of all available migrations

  Returns:
    List of pending migrations in execution order

  Examples:
    >>> pending = await get_pending_migrations(client, all_migrations)
    >>> for migration in pending:
    ...   print(migration.version, migration.description)
  """
  log = logger.bind(total_migrations=len(migrations))

  try:
    # Get set of applied versions
    applied_versions = await get_applied_versions(client)

    # Filter to pending migrations
    pending = [m for m in migrations if m.version not in applied_versions]

    # Sort by version
    pending.sort(key=lambda m: m.version)

    log.info('pending_migrations_retrieved', count=len(pending))
    return pending

  except Exception as e:
    log.error('failed_to_get_pending_migrations', error=str(e))
    raise MigrationExecutionError(f'Failed to get pending migrations: {e}') from e


async def get_applied_migrations_ordered(
  client: DatabaseClient,
  migrations: list[Migration],
) -> list[Migration]:
  """Get applied migrations in application order.

  Args:
    client: Database client
    migrations: List of all available migrations

  Returns:
    List of applied migrations sorted by version

  Examples:
    >>> applied = await get_applied_migrations_ordered(client, all_migrations)
    >>> for migration in applied:
    ...   print(migration.version)
  """
  try:
    # Get set of applied versions
    applied_versions = await get_applied_versions(client)

    # Filter to applied migrations
    applied = [m for m in migrations if m.version in applied_versions]

    # Sort by version
    applied.sort(key=lambda m: m.version)

    return applied

  except Exception as e:
    logger.error('failed_to_get_applied_migrations', error=str(e))
    raise MigrationExecutionError(f'Failed to get applied migrations: {e}') from e


async def get_migration_status(
  client: DatabaseClient,
  migrations: list[Migration],
) -> list[MigrationStatus]:
  """Get status of all migrations.

  Args:
    client: Database client
    migrations: List of all available migrations

  Returns:
    List of MigrationStatus objects

  Examples:
    >>> statuses = await get_migration_status(client, all_migrations)
    >>> for status in statuses:
    ...   print(status.migration.version, status.state.value)
  """
  log = logger.bind(total_migrations=len(migrations))

  try:
    # Get applied versions
    applied_versions = await get_applied_versions(client)

    # Create status for each migration
    statuses: list[MigrationStatus] = []
    for migration in migrations:
      if migration.version in applied_versions:
        state = MigrationState.APPLIED
      else:
        state = MigrationState.PENDING

      statuses.append(
        MigrationStatus(
          migration=migration,
          state=state,
        )
      )

    log.info('migration_status_retrieved', count=len(statuses))
    return statuses

  except Exception as e:
    log.error('failed_to_get_migration_status', error=str(e))
    raise MigrationExecutionError(f'Failed to get migration status: {e}') from e


async def execute_migration_plan(
  client: DatabaseClient,
  plan: MigrationPlan,
) -> None:
  """Execute a migration plan.

  Args:
    client: Database client
    plan: Migration plan to execute

  Raises:
    MigrationExecutionError: If execution fails

  Examples:
    >>> plan = MigrationPlan(
    ...   migrations=[migration1, migration2],
    ...   direction=MigrationDirection.UP,
    ... )
    >>> await execute_migration_plan(client, plan)
  """
  log = logger.bind(
    migration_count=plan.count,
    direction=plan.direction.value,
  )

  try:
    log.info('executing_migration_plan')

    if plan.is_empty():
      log.info('migration_plan_empty')
      return

    # Execute migrations in order (or reverse for DOWN)
    migrations_to_execute = (
      plan.migrations
      if plan.direction == MigrationDirection.UP
      else list(reversed(plan.migrations))
    )

    for migration in migrations_to_execute:
      await execute_migration(client, migration, plan.direction)

    log.info('migration_plan_executed')

  except Exception as e:
    log.error('migration_plan_execution_failed', error=str(e))
    raise MigrationExecutionError(f'Failed to execute migration plan: {e}') from e


async def validate_migrations(migrations: list[Migration]) -> list[str]:
  """Validate migration consistency.

  Checks for duplicate versions, invalid dependencies, etc.

  Args:
    migrations: List of migrations to validate

  Returns:
    List of validation error messages (empty if valid)

  Examples:
    >>> errors = await validate_migrations(all_migrations)
    >>> if errors:
    ...   for error in errors:
    ...     print(f'Validation error: {error}')
  """
  errors: list[str] = []

  # Check for duplicate versions
  versions = [m.version for m in migrations]
  duplicates = {v for v in versions if versions.count(v) > 1}

  if duplicates:
    errors.append(f'Duplicate migration versions found: {", ".join(duplicates)}')

  # Check dependencies
  version_set = set(versions)
  for migration in migrations:
    for dep in migration.depends_on:
      if dep not in version_set:
        errors.append(f'Migration {migration.version} depends on missing migration {dep}')

  return errors


async def create_migration_plan(
  client: DatabaseClient,
  migrations: list[Migration],
  direction: MigrationDirection,
  steps: int | None = None,
) -> MigrationPlan:
  """Create a migration execution plan.

  Args:
    client: Database client
    migrations: List of all available migrations
    direction: Direction to execute (UP or DOWN)
    steps: Optional number of migrations (None = all)

  Returns:
    MigrationPlan object

  Examples:
    >>> plan = await create_migration_plan(
    ...   client,
    ...   all_migrations,
    ...   MigrationDirection.UP,
    ... )
    >>> print(f'Plan has {plan.count} migrations')
  """
  if direction == MigrationDirection.UP:
    pending = await get_pending_migrations(client, migrations)
    to_execute = pending[:steps] if steps else pending
  else:
    applied = await get_applied_migrations_ordered(client, migrations)
    to_execute = applied[-steps:] if steps else applied

  return MigrationPlan(
    migrations=to_execute,
    direction=direction,
  )
