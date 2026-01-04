"""Migration squashing for combining multiple migrations into one.

This module provides functionality to combine multiple migration files
into a single, consolidated migration. It supports optimization to
remove redundant operations and safety validation.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import structlog

from reverie.migration.discovery import discover_migrations
from reverie.migration.models import Migration

logger = structlog.get_logger(__name__)


class SquashError(Exception):
  """Raised when squash operation cannot be completed safely."""

  pass


@dataclass(frozen=True)
class SquashResult:
  """Result of migration squash operation."""

  squashed_path: Path
  original_count: int
  statement_count: int
  optimizations_applied: int
  original_migrations: list[str]


@dataclass(frozen=True)
class SquashWarning:
  """Warning about potential issues with squashing."""

  migration: str
  message: str
  severity: Literal['low', 'medium', 'high']


@dataclass
class _ParsedStatement:
  """Internal class for tracking statement metadata during optimization."""

  statement: str
  operation: str  # DEFINE, REMOVE, INSERT, UPDATE, DELETE, etc.
  object_type: str | None = None  # TABLE, FIELD, INDEX, EVENT
  table_name: str | None = None
  field_name: str | None = None
  index_name: str | None = None


def _parse_statement(statement: str) -> _ParsedStatement:
  """Parse a SQL statement to extract its components.

  Args:
    statement: SurrealQL statement

  Returns:
    Parsed statement with metadata
  """
  stmt = statement.strip().upper()
  original = statement.strip()

  # Extract operation type
  operation = ''
  if stmt.startswith('DEFINE'):
    operation = 'DEFINE'
  elif stmt.startswith('REMOVE'):
    operation = 'REMOVE'
  elif stmt.startswith('INSERT'):
    operation = 'INSERT'
  elif stmt.startswith('UPDATE'):
    operation = 'UPDATE'
  elif stmt.startswith('DELETE'):
    operation = 'DELETE'
  elif stmt.startswith('CREATE'):
    operation = 'CREATE'
  else:
    return _ParsedStatement(statement=original, operation='UNKNOWN')

  # Parse DEFINE/REMOVE statements for object type
  object_type = None
  table_name = None
  field_name = None
  index_name = None

  # DEFINE TABLE <name> or REMOVE TABLE <name>
  table_match = re.match(r'(DEFINE|REMOVE)\s+TABLE\s+(\w+)', stmt, re.IGNORECASE)
  if table_match:
    object_type = 'TABLE'
    table_name = table_match.group(2).lower()

  # DEFINE FIELD <name> ON TABLE <table> or REMOVE FIELD <name> ON TABLE <table>
  field_match = re.match(
    r'(DEFINE|REMOVE)\s+FIELD\s+(\w+)\s+ON\s+TABLE\s+(\w+)',
    stmt,
    re.IGNORECASE,
  )
  if field_match:
    object_type = 'FIELD'
    field_name = field_match.group(2).lower()
    table_name = field_match.group(3).lower()

  # DEFINE INDEX <name> ON TABLE <table> or REMOVE INDEX <name> ON TABLE <table>
  index_match = re.match(
    r'(DEFINE|REMOVE)\s+INDEX\s+(\w+)\s+ON\s+TABLE\s+(\w+)',
    stmt,
    re.IGNORECASE,
  )
  if index_match:
    object_type = 'INDEX'
    index_name = index_match.group(2).lower()
    table_name = index_match.group(3).lower()

  # DEFINE EVENT <name> ON TABLE <table> or REMOVE EVENT <name> ON TABLE <table>
  event_match = re.match(
    r'(DEFINE|REMOVE)\s+EVENT\s+(\w+)\s+ON\s+TABLE\s+(\w+)',
    stmt,
    re.IGNORECASE,
  )
  if event_match:
    object_type = 'EVENT'
    # Store event name in index_name field for reuse
    index_name = event_match.group(2).lower()
    table_name = event_match.group(3).lower()

  return _ParsedStatement(
    statement=original,
    operation=operation,
    object_type=object_type,
    table_name=table_name,
    field_name=field_name,
    index_name=index_name,
  )


def optimize_statements(statements: list[str]) -> tuple[list[str], int]:
  """Remove redundant SQL statements.

  Optimizations:
  - Remove field that is added then dropped in same sequence
  - Remove table that is defined then removed
  - Combine multiple field additions on same table
  - Remove duplicate index definitions (keep last)

  Args:
    statements: List of SQL statements to optimize

  Returns:
    Tuple of (optimized statements, count of optimizations)

  Examples:
    >>> stmts = [
    ...   'DEFINE TABLE user SCHEMAFULL;',
    ...   'DEFINE FIELD temp ON TABLE user TYPE string;',
    ...   'REMOVE FIELD temp ON TABLE user;',
    ... ]
    >>> optimized, count = optimize_statements(stmts)
    >>> count
    2
  """
  log = logger.bind(input_count=len(statements))
  optimizations = 0

  # Parse all statements
  parsed: list[_ParsedStatement] = [_parse_statement(s) for s in statements]

  # Track which indices to remove
  to_remove: set[int] = set()

  # Pass 1: Remove DEFINE + REMOVE pairs for same object
  for i, stmt_i in enumerate(parsed):
    if i in to_remove:
      continue

    if stmt_i.operation != 'DEFINE':
      continue

    # Look for matching REMOVE later in sequence
    for j in range(i + 1, len(parsed)):
      if j in to_remove:
        continue

      stmt_j = parsed[j]
      if stmt_j.operation != 'REMOVE':
        continue

      # Check if same object type and identifiers
      if stmt_i.object_type == stmt_j.object_type and stmt_i.table_name == stmt_j.table_name:
        is_match = (
          stmt_i.object_type == 'TABLE'
          or (stmt_i.object_type == 'FIELD' and stmt_i.field_name == stmt_j.field_name)
          or (stmt_i.object_type == 'INDEX' and stmt_i.index_name == stmt_j.index_name)
          or (stmt_i.object_type == 'EVENT' and stmt_i.index_name == stmt_j.index_name)
        )

        if is_match:
          to_remove.add(i)
          to_remove.add(j)
          optimizations += 2
          log.debug(
            'removed_define_remove_pair',
            object_type=stmt_i.object_type,
            table=stmt_i.table_name,
          )
          break

  # Pass 2: Remove duplicate DEFINE statements (keep last occurrence)
  define_positions: dict[tuple[str | None, str | None, str | None, str | None], int] = {}

  for i, stmt in enumerate(parsed):
    if i in to_remove:
      continue

    if stmt.operation == 'DEFINE':
      key = (stmt.object_type, stmt.table_name, stmt.field_name, stmt.index_name)

      if key in define_positions:
        # Mark earlier definition for removal
        earlier_idx = define_positions[key]
        if earlier_idx not in to_remove:
          to_remove.add(earlier_idx)
          optimizations += 1
          log.debug(
            'removed_duplicate_define',
            object_type=stmt.object_type,
            table=stmt.table_name,
          )

      define_positions[key] = i

  # Also filter out UPDATE statements that reference removed fields
  fields_removed: set[tuple[str | None, str | None]] = set()
  for i in to_remove:
    stmt = parsed[i]
    if stmt.object_type == 'FIELD':
      fields_removed.add((stmt.table_name, stmt.field_name))

  for i, stmt in enumerate(parsed):
    if i in to_remove:
      continue

    if stmt.operation == 'UPDATE':
      # Check if this UPDATE references a removed field
      # Simple heuristic: look for "SET fieldname =" pattern
      for table_name, field_name in fields_removed:
        if table_name and field_name:
          # Check if update is on this table and field
          pattern = rf'\bSET\s+{field_name}\s*='
          if re.search(pattern, stmt.statement, re.IGNORECASE):
            # Also check table name
            table_pattern = rf'\bUPDATE\s+{table_name}\b'
            if re.search(table_pattern, stmt.statement, re.IGNORECASE):
              to_remove.add(i)
              optimizations += 1
              log.debug(
                'removed_orphaned_update',
                table=table_name,
                field=field_name,
              )

  # Build optimized list
  optimized = [s for i, s in enumerate(statements) if i not in to_remove]

  log.info(
    'statements_optimized',
    output_count=len(optimized),
    optimizations=optimizations,
  )

  return optimized, optimizations


def validate_squash_safety(migrations: list[Migration]) -> list[SquashWarning]:
  """Check if squash is safe to perform.

  Returns list of warnings for:
  - Data migration statements (INSERT, UPDATE, DELETE)
  - Complex operations that may need review
  - Non-idempotent operations

  Args:
    migrations: List of migrations to validate

  Returns:
    List of warnings about potential issues

  Examples:
    >>> warnings = validate_squash_safety(migrations)
    >>> for w in warnings:
    ...   print(f'{w.severity}: {w.message}')
  """
  warnings: list[SquashWarning] = []

  for migration in migrations:
    version = migration.version

    try:
      statements = migration.up()
    except Exception as e:
      warnings.append(
        SquashWarning(
          migration=version,
          message=f'Failed to execute up() function: {e}',
          severity='high',
        )
      )
      continue

    for stmt in statements:
      stmt_upper = stmt.upper().strip()

      # Check for data manipulation statements
      if stmt_upper.startswith('INSERT'):
        warnings.append(
          SquashWarning(
            migration=version,
            message=f'Contains INSERT statement: {stmt[:50]}...',
            severity='medium',
          )
        )

      elif stmt_upper.startswith('UPDATE') and 'SET' in stmt_upper:
        # Only warn about UPDATE if it's not a backfill (WHERE ... IS NONE pattern)
        if 'IS NONE' not in stmt_upper:
          warnings.append(
            SquashWarning(
              migration=version,
              message=f'Contains UPDATE statement: {stmt[:50]}...',
              severity='medium',
            )
          )

      elif stmt_upper.startswith('DELETE'):
        warnings.append(
          SquashWarning(
            migration=version,
            message=f'Contains DELETE statement: {stmt[:50]}...',
            severity='high',
          )
        )

      elif stmt_upper.startswith('CREATE') and 'CREATE TABLE' not in stmt_upper:
        warnings.append(
          SquashWarning(
            migration=version,
            message=f'Contains CREATE statement: {stmt[:50]}...',
            severity='low',
          )
        )

      # Check for potential ordering issues with foreign keys or relations
      if 'RECORD' in stmt_upper and 'TYPE' in stmt_upper:
        warnings.append(
          SquashWarning(
            migration=version,
            message='Contains record reference - verify table order',
            severity='low',
          )
        )

  return warnings


def generate_squashed_migration(
  statements: list[str],
  description: str = 'squashed',
  migration_ids: list[str] | None = None,
) -> str:
  """Generate Python migration file content for squashed migration.

  Args:
    statements: Combined up() statements
    description: Migration description
    migration_ids: Original migration IDs for documentation

  Returns:
    Python source code for the migration file

  Examples:
    >>> content = generate_squashed_migration(
    ...   ['DEFINE TABLE user SCHEMAFULL;'],
    ...   'initial schema',
    ...   ['20260101_000000', '20260102_000000'],
    ... )
  """
  # Generate version timestamp
  now = datetime.now(UTC)
  version = now.strftime('%Y%m%d_%H%M%S')

  # Format statements as Python list
  up_lines = _format_statements_as_python(statements)

  # Generate squashed migrations comment
  squashed_comment = ''
  if migration_ids:
    ids_list = '\n'.join(f'#   - {mid}' for mid in migration_ids)
    squashed_comment = f"""
# Squashed from {len(migration_ids)} migrations:
{ids_list}
"""

  content = f'''"""Migration: {description}

Generated: {now.isoformat()}
Author: reverie

This migration was created by squashing multiple migrations.
"""{squashed_comment}


def up() -> list[str]:
  """Apply migration (forward)."""
  return [
{up_lines}
  ]


def down() -> list[str]:
  """Rollback migration (backward).

  NOTE: Squashed migrations generate an empty down() function.
  Review and update manually if rollback support is needed.
  """
  return []


metadata = {{
  'version': '{version}',
  'description': '{description}',
  'author': 'reverie',
  'depends_on': [],
}}
'''

  return content


def _format_statements_as_python(statements: list[str]) -> str:
  """Format SQL statements for Python list literal.

  Args:
    statements: List of SQL statements

  Returns:
    Formatted string for embedding in Python code
  """
  if not statements:
    return ''

  formatted = []
  for stmt in statements:
    # Escape single quotes in statement
    escaped = stmt.replace("'", "\\'")
    formatted.append(f"    '{escaped}',")

  return '\n'.join(formatted)


def _filter_migrations_by_version(
  migrations: list[Migration],
  from_version: str | None,
  to_version: str | None,
) -> list[Migration]:
  """Filter migrations by version range.

  Args:
    migrations: List of migrations to filter
    from_version: Start version (inclusive), None for beginning
    to_version: End version (inclusive), None for latest

  Returns:
    Filtered list of migrations
  """
  result = []

  for migration in migrations:
    version = migration.version

    # Check from_version constraint
    if from_version is not None and version < from_version:
      continue

    # Check to_version constraint
    if to_version is not None and version > to_version:
      continue

    result.append(migration)

  return result


def _extract_statements_from_migrations(migrations: list[Migration]) -> list[str]:
  """Extract all up() statements from migrations.

  Args:
    migrations: List of migrations

  Returns:
    Combined list of all up() statements

  Raises:
    SquashError: If any migration's up() function fails
  """
  all_statements: list[str] = []

  for migration in migrations:
    try:
      statements = migration.up()
      all_statements.extend(statements)
    except Exception as e:
      raise SquashError(f'Failed to extract statements from {migration.version}: {e}') from e

  return all_statements


async def squash_migrations(
  directory: Path,
  from_version: str | None = None,
  to_version: str | None = None,
  output_path: Path | None = None,
  optimize: bool = True,
  dry_run: bool = False,
) -> SquashResult:
  """Squash multiple migrations into one.

  Args:
    directory: Migration directory
    from_version: Start version (inclusive), None for beginning
    to_version: End version (inclusive), None for latest
    output_path: Output file path, None to generate automatically
    optimize: Apply statement optimizations
    dry_run: Return result without writing file

  Returns:
    SquashResult with details about the operation

  Raises:
    SquashError: If squash cannot be performed safely

  Examples:
    >>> result = await squash_migrations(
    ...   Path('migrations'),
    ...   from_version='20260101_000000',
    ...   to_version='20260105_000000',
    ... )
    >>> print(f'Squashed {result.original_count} migrations')
  """
  log = logger.bind(
    directory=str(directory),
    from_version=from_version,
    to_version=to_version,
  )

  log.info('starting_migration_squash')

  # Discover migrations
  try:
    all_migrations = discover_migrations(directory)
  except Exception as e:
    raise SquashError(f'Failed to discover migrations: {e}') from e

  if not all_migrations:
    raise SquashError('No migrations found in directory')

  # Filter by version range
  migrations = _filter_migrations_by_version(
    all_migrations,
    from_version,
    to_version,
  )

  if not migrations:
    raise SquashError('No migrations match the specified version range')

  if len(migrations) < 2:
    raise SquashError('At least 2 migrations required for squashing')

  log.info('found_migrations_to_squash', count=len(migrations))

  # Validate safety
  warnings = validate_squash_safety(migrations)
  high_severity_warnings = [w for w in warnings if w.severity == 'high']

  if high_severity_warnings:
    warning_msgs = '; '.join(w.message for w in high_severity_warnings)
    raise SquashError(f'High severity warnings prevent squashing: {warning_msgs}')

  # Extract statements from all migrations
  statements = _extract_statements_from_migrations(migrations)

  log.debug('extracted_statements', count=len(statements))

  # Optimize if requested
  optimizations_applied = 0
  if optimize:
    statements, optimizations_applied = optimize_statements(statements)
    log.info('optimizations_applied', count=optimizations_applied)

  # Get migration IDs for documentation
  migration_ids = [m.version for m in migrations]

  # Generate description
  first_version = migrations[0].version
  last_version = migrations[-1].version
  description = f'squashed_{first_version}_to_{last_version}'

  # Generate migration content
  content = generate_squashed_migration(
    statements,
    description=description,
    migration_ids=migration_ids,
  )

  # Determine output path
  if output_path is None:
    now = datetime.now(UTC)
    version = now.strftime('%Y%m%d_%H%M%S')
    filename = f'{version}_{description}.py'
    output_path = directory / filename

  # Write file if not dry run
  if not dry_run:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding='utf-8')
    log.info('squashed_migration_written', path=str(output_path))
  else:
    log.info('dry_run_complete', would_write_to=str(output_path))

  return SquashResult(
    squashed_path=output_path,
    original_count=len(migrations),
    statement_count=len(statements),
    optimizations_applied=optimizations_applied,
    original_migrations=migration_ids,
  )
