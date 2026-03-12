"""Safe rollback planning and execution.

This module provides comprehensive rollback functionality with safety analysis,
enabling safe schema rollbacks with data loss warnings and prevention.
"""

from datetime import UTC, datetime
from enum import Enum

import structlog
from pydantic import BaseModel, ConfigDict, Field

from reverie.connection.client import DatabaseClient
from reverie.migration.executor import execute_migration
from reverie.migration.models import Migration, MigrationDirection

logger = structlog.get_logger(__name__)


class RollbackSafety(str, Enum):
  """Safety level of rollback operation.

  Indicates potential data loss or schema destruction risks.
  """

  SAFE = 'safe'  # No data loss expected
  DATA_LOSS = 'data_loss'  # Some data may be lost
  UNSAFE = 'unsafe'  # Significant data loss likely


class RollbackIssue(BaseModel):
  """Issue that may occur during rollback.

  Describes specific safety concerns with recommendations for mitigation.

  Examples:
    >>> issue = RollbackIssue(
    ...   safety=RollbackSafety.DATA_LOSS,
    ...   migration='20260109_120000',
    ...   description='Dropping column will lose data',
    ...   affected_data='user.profile_picture',
    ... )
  """

  safety: RollbackSafety = Field(..., description='Safety level')
  migration: str = Field(..., description='Migration version')
  description: str = Field(..., description='Issue description')
  affected_data: str | None = Field(None, description='Affected data description')
  recommendation: str | None = Field(None, description='Recommendation for user')

  model_config = ConfigDict(frozen=True)


class RollbackPlan(BaseModel):
  """Plan for rolling back to a specific version.

  Contains ordered list of migrations to rollback with comprehensive
  safety analysis and warnings.

  Examples:
    >>> plan = RollbackPlan(
    ...   from_version='20260109_120000',
    ...   to_version='20260108_120000',
    ...   migrations=[migration1],
    ...   overall_safety=RollbackSafety.SAFE,
    ... )
  """

  from_version: str = Field(..., description='Current version')
  to_version: str = Field(..., description='Target version')
  migrations: list[Migration] = Field(..., description='Migrations to rollback')
  overall_safety: RollbackSafety = Field(..., description='Overall safety level')
  issues: list[RollbackIssue] = Field(default_factory=list, description='Potential issues')
  requires_approval: bool = Field(default=False, description='Requires manual approval')
  estimated_duration_ms: int | None = Field(None, description='Estimated duration')

  model_config = ConfigDict(frozen=True)

  @property
  def migration_count(self) -> int:
    """Get number of migrations to rollback."""
    return len(self.migrations)

  @property
  def is_safe(self) -> bool:
    """Check if rollback is completely safe."""
    return self.overall_safety == RollbackSafety.SAFE

  @property
  def has_data_loss(self) -> bool:
    """Check if rollback will cause data loss."""
    return self.overall_safety in (RollbackSafety.DATA_LOSS, RollbackSafety.UNSAFE)


class RollbackResult(BaseModel):
  """Result of rollback execution.

  Contains execution statistics and any errors encountered.

  Examples:
    >>> result = RollbackResult(
    ...   plan=plan,
    ...   success=True,
    ...   actual_duration_ms=1500,
    ... )
  """

  plan: RollbackPlan = Field(..., description='Executed plan')
  success: bool = Field(..., description='Whether rollback succeeded')
  actual_duration_ms: int = Field(..., description='Actual execution time')
  rolled_back_count: int = Field(..., description='Number of migrations rolled back')
  errors: list[str] = Field(default_factory=list, description='Errors encountered')

  model_config = ConfigDict(frozen=True)

  @property
  def completed_all(self) -> bool:
    """Check if all planned migrations were rolled back."""
    return self.rolled_back_count == self.plan.migration_count


async def create_rollback_plan(
  client: DatabaseClient,
  migrations: list[Migration],
  target_version: str,
  current_version: str | None = None,
) -> RollbackPlan:
  """Create safe rollback plan to target version.

  Analyzes rollback safety and identifies potential data loss issues.

  Args:
    client: Database client
    migrations: All available migrations
    target_version: Version to rollback to
    current_version: Current version (auto-detected if None)

  Returns:
    Rollback plan with safety analysis

  Raises:
    ValueError: If target version not found or invalid

  Examples:
    >>> plan = await create_rollback_plan(
    ...   client,
    ...   all_migrations,
    ...   '20260108_120000'
    ... )
  """
  from reverie.migration.history import get_applied_migrations

  logger.info('creating_rollback_plan', target_version=target_version)

  # Determine current version if not provided
  if not current_version:
    applied = await get_applied_migrations(client)
    if applied:
      current_version = applied[-1].version
    else:
      raise ValueError('No migrations have been applied')

  # Validate target version exists
  target_found = any(m.version == target_version for m in migrations)
  if not target_found:
    raise ValueError(f'Target version {target_version} not found in migrations')

  # Find migrations to rollback (in reverse order)
  to_rollback: list[Migration] = []
  found_current = False

  # Sort migrations by version
  sorted_migrations = sorted(migrations, key=lambda m: m.version)

  # Collect migrations between target and current
  for migration in sorted_migrations:
    if migration.version == target_version:
      # Stop before target version
      break
    if migration.version == current_version:
      found_current = True
    if found_current or migration.version > target_version:
      to_rollback.append(migration)

  # If we haven't found current yet, collect all after target
  if not found_current:
    to_rollback = [
      m for m in sorted_migrations if target_version < m.version <= (current_version or '')
    ]

  # Reverse to get rollback order (newest first)
  to_rollback.reverse()

  # Analyze safety
  issues: list[RollbackIssue] = []
  overall_safety = RollbackSafety.SAFE

  for migration in to_rollback:
    migration_issues = await _analyze_migration_safety(client, migration)
    issues.extend(migration_issues)

    # Update overall safety to worst case
    for issue in migration_issues:
      if issue.safety == RollbackSafety.UNSAFE:
        overall_safety = RollbackSafety.UNSAFE
      elif issue.safety == RollbackSafety.DATA_LOSS and overall_safety == RollbackSafety.SAFE:
        overall_safety = RollbackSafety.DATA_LOSS

  requires_approval = overall_safety != RollbackSafety.SAFE

  plan = RollbackPlan(
    from_version=current_version,
    to_version=target_version,
    migrations=to_rollback,
    overall_safety=overall_safety,
    issues=issues,
    requires_approval=requires_approval,
    estimated_duration_ms=None,
  )

  logger.info(
    'rollback_plan_created',
    from_version=current_version,
    to_version=target_version,
    migration_count=len(to_rollback),
    safety=overall_safety.value,
    issues_count=len(issues),
  )

  return plan


async def execute_rollback(
  client: DatabaseClient,
  plan: RollbackPlan,
  force: bool = False,
) -> RollbackResult:
  """Execute rollback plan.

  Rolls back migrations in reverse order with safety checks.

  Args:
    client: Database client
    plan: Rollback plan to execute
    force: Force execution despite safety warnings

  Returns:
    Rollback result

  Raises:
    ValueError: If plan is unsafe and force is False

  Examples:
    >>> result = await execute_rollback(client, plan)
    >>> if result.success:
    ...   print(f'Rolled back {result.rolled_back_count} migrations')
  """
  logger.info(
    'executing_rollback_plan',
    from_version=plan.from_version,
    to_version=plan.to_version,
    migration_count=plan.migration_count,
    safety=plan.overall_safety.value,
  )

  # Check safety
  if plan.overall_safety == RollbackSafety.UNSAFE and not force:
    raise ValueError(
      'Rollback is unsafe and may cause significant data loss. Use force=True to proceed anyway.'
    )

  if plan.overall_safety == RollbackSafety.DATA_LOSS and not force:
    raise ValueError('Rollback may cause data loss. Review issues and use force=True to proceed.')

  start_time = datetime.now(UTC)
  errors: list[str] = []
  rolled_back_count = 0

  try:
    # Execute migrations in reverse order
    for migration in plan.migrations:
      try:
        logger.info('rolling_back_migration', version=migration.version)
        await execute_migration(client, migration, MigrationDirection.DOWN)
        rolled_back_count += 1
        logger.info('migration_rolled_back', version=migration.version)

      except Exception as e:
        error_msg = f'{migration.version}: {e}'
        errors.append(error_msg)
        logger.error(
          'rollback_migration_failed',
          version=migration.version,
          error=str(e),
        )
        # Stop on first failure
        break

    end_time = datetime.now(UTC)
    duration_ms = int((end_time - start_time).total_seconds() * 1000)

    success = rolled_back_count == len(plan.migrations)

    result = RollbackResult(
      plan=plan,
      success=success,
      actual_duration_ms=duration_ms,
      rolled_back_count=rolled_back_count,
      errors=errors,
    )

    logger.info(
      'rollback_execution_complete',
      success=success,
      rolled_back_count=rolled_back_count,
      duration_ms=duration_ms,
    )

    return result

  except Exception as e:
    logger.error('rollback_execution_failed', error=str(e))
    end_time = datetime.now(UTC)
    duration_ms = int((end_time - start_time).total_seconds() * 1000)

    return RollbackResult(
      plan=plan,
      success=False,
      actual_duration_ms=duration_ms,
      rolled_back_count=rolled_back_count,
      errors=[*errors, str(e)],
    )


async def analyze_rollback_safety(
  client: DatabaseClient,
  migrations: list[Migration],
  target_version: str,
) -> list[RollbackIssue]:
  """Analyze safety of rolling back to target version.

  Args:
    client: Database client
    migrations: All available migrations
    target_version: Version to rollback to

  Returns:
    List of safety issues

  Examples:
    >>> issues = await analyze_rollback_safety(client, migrations, '20260108_120000')
    >>> for issue in issues:
    ...   print(f'{issue.safety}: {issue.description}')
  """
  plan = await create_rollback_plan(client, migrations, target_version)
  return plan.issues


async def _analyze_migration_safety(
  _client: DatabaseClient,
  migration: Migration,
) -> list[RollbackIssue]:
  """Analyze safety of rolling back a migration.

  Examines migration SQL to identify destructive operations.

  Args:
    _client: Database client (reserved for future use)
    migration: Migration to analyze

  Returns:
    List of safety issues
  """
  issues: list[RollbackIssue] = []

  # Get rollback SQL
  try:
    down_statements = migration.down()
  except Exception as e:
    logger.warning(
      'failed_to_get_down_migration',
      version=migration.version,
      error=str(e),
    )
    issues.append(
      RollbackIssue(
        safety=RollbackSafety.UNSAFE,
        migration=migration.version,
        description=f'Cannot retrieve rollback statements: {e}',
        affected_data=None,
        recommendation='Check migration file for errors',
      )
    )
    return issues

  # Analyze each statement for potential data loss
  for statement in down_statements:
    statement_upper = statement.upper().strip()

    # Check for table drops
    if 'DROP TABLE' in statement_upper or 'REMOVE TABLE' in statement_upper:
      # Extract table name
      table_name = _extract_table_name(statement)
      issues.append(
        RollbackIssue(
          safety=RollbackSafety.UNSAFE,
          migration=migration.version,
          description=f'Dropping table: {table_name}',
          affected_data=f'All records in table {table_name}',
          recommendation='Export table data before rollback',
        )
      )

    # Check for field drops
    elif 'DROP FIELD' in statement_upper or 'REMOVE FIELD' in statement_upper:
      field_name = _extract_field_name(statement)
      issues.append(
        RollbackIssue(
          safety=RollbackSafety.DATA_LOSS,
          migration=migration.version,
          description=f'Dropping field: {field_name}',
          affected_data=f'Field data in {field_name}',
          recommendation='Backup affected field data',
        )
      )

    # Check for index drops (usually safe, but note it)
    elif 'DROP INDEX' in statement_upper or 'REMOVE INDEX' in statement_upper:
      # Index drops are generally safe as they don't affect data
      # But we can log for awareness
      logger.debug(
        'rollback_will_drop_index',
        migration=migration.version,
        statement=statement,
      )

    # Check for field type changes (potentially unsafe)
    elif 'ALTER' in statement_upper and 'TYPE' in statement_upper:
      issues.append(
        RollbackIssue(
          safety=RollbackSafety.DATA_LOSS,
          migration=migration.version,
          description='Altering field type may cause data conversion issues',
          affected_data=None,
          recommendation='Review data compatibility before rollback',
        )
      )

  return issues


def _extract_table_name(statement: str) -> str:
  """Extract table name from DROP/REMOVE TABLE statement.

  Args:
    statement: SQL statement

  Returns:
    Table name or 'unknown'
  """
  parts = statement.upper().split()
  try:
    if 'TABLE' in parts:
      idx = parts.index('TABLE')
      if idx + 1 < len(parts):
        return parts[idx + 1].strip(';').lower()
  except (ValueError, IndexError):
    pass
  return 'unknown'


def _extract_field_name(statement: str) -> str:
  """Extract field name from DROP/REMOVE FIELD statement.

  Args:
    statement: SQL statement

  Returns:
    Field name or 'unknown'
  """
  parts = statement.upper().split()
  try:
    if 'FIELD' in parts:
      idx = parts.index('FIELD')
      if idx + 1 < len(parts):
        field_part = parts[idx + 1]
        # Field might be in format table.field
        if '.' in field_part:
          return field_part.strip(';').lower()
        # Or just field name with ON TABLE following
        return field_part.strip(';').lower()
  except (ValueError, IndexError):
    pass
  return 'unknown'


async def plan_rollback_to_version(
  client: DatabaseClient,
  migrations: list[Migration],
  target_version: str,
) -> RollbackPlan:
  """Convenience function to plan rollback to a specific version.

  Args:
    client: Database client
    migrations: All available migrations
    target_version: Target version to rollback to

  Returns:
    Rollback plan

  Examples:
    >>> plan = await plan_rollback_to_version(client, migrations, '20260108_120000')
    >>> print(f'Will rollback {plan.migration_count} migrations')
    >>> print(f'Safety: {plan.overall_safety.value}')
  """
  return await create_rollback_plan(client, migrations, target_version)
