"""Migration file generation from schema changes.

This module provides functions for auto-generating migration files by
comparing schema definitions and generating appropriate SQL statements.
"""

from datetime import datetime
from pathlib import Path

import structlog

from reverie.migration.diff import diff_edges, diff_tables
from reverie.migration.models import SchemaDiff
from reverie.schema.edge import EdgeDefinition
from reverie.schema.table import TableDefinition

logger = structlog.get_logger(__name__)


class MigrationGenerationError(Exception):
  """Raised when migration generation fails."""

  pass


def generate_migration(
  directory: Path,
  description: str,
  *,
  old_tables: dict[str, TableDefinition] | None = None,
  new_tables: dict[str, TableDefinition] | None = None,
  old_edges: dict[str, EdgeDefinition] | None = None,
  new_edges: dict[str, EdgeDefinition] | None = None,
  author: str = 'reverie',
) -> Path:
  """Generate a migration file from schema changes.

  Args:
    directory: Directory to write migration file
    description: Human-readable description of migration
    old_tables: Previous table definitions
    new_tables: New table definitions
    old_edges: Previous edge definitions
    new_edges: New edge definitions
    author: Migration author

  Returns:
    Path to generated migration file

  Raises:
    MigrationGenerationError: If generation fails

  Examples:
    >>> path = generate_migration(
    ...   Path('migrations'),
    ...   'Create user table',
    ...   new_tables={'user': user_table},
    ... )
    >>> print(f'Created: {path}')
  """
  log = logger.bind(description=description, directory=str(directory))

  try:
    log.info('generating_migration')

    # Generate version timestamp
    version = _generate_version()

    # Calculate diffs
    diffs = _calculate_schema_diffs(
      old_tables or {},
      new_tables or {},
      old_edges or {},
      new_edges or {},
    )

    if not diffs:
      log.warning('no_schema_changes_detected')
      raise MigrationGenerationError('No schema changes detected')

    # Generate migration content
    content = _generate_migration_content(
      version,
      description,
      diffs,
      author,
    )

    # Create migration file
    filename = _generate_filename(version, description)
    file_path = directory / filename

    # Ensure directory exists
    directory.mkdir(parents=True, exist_ok=True)

    # Write migration file
    file_path.write_text(content, encoding='utf-8')

    log.info('migration_generated', path=str(file_path), version=version)
    return file_path

  except MigrationGenerationError:
    raise
  except Exception as e:
    log.error('migration_generation_failed', error=str(e))
    raise MigrationGenerationError(f'Failed to generate migration: {e}') from e


def generate_initial_migration(
  directory: Path,
  tables: dict[str, TableDefinition],
  edges: dict[str, EdgeDefinition] | None = None,
  description: str = 'Initial schema',
  author: str = 'reverie',
) -> Path:
  """Generate initial migration from schema definitions.

  Convenience function for creating the first migration.

  Args:
    directory: Directory to write migration file
    tables: Table definitions
    edges: Optional edge definitions
    description: Migration description
    author: Migration author

  Returns:
    Path to generated migration file

  Examples:
    >>> path = generate_initial_migration(
    ...   Path('migrations'),
    ...   {'user': user_table, 'post': post_table},
    ...   description='Initial database schema',
    ... )
  """
  return generate_migration(
    directory,
    description,
    new_tables=tables,
    new_edges=edges,
    author=author,
  )


def _calculate_schema_diffs(
  old_tables: dict[str, TableDefinition],
  new_tables: dict[str, TableDefinition],
  old_edges: dict[str, EdgeDefinition],
  new_edges: dict[str, EdgeDefinition],
) -> list[SchemaDiff]:
  """Calculate differences between schema versions.

  Args:
    old_tables: Previous table definitions
    new_tables: New table definitions
    old_edges: Previous edge definitions
    new_edges: New edge definitions

  Returns:
    List of schema differences
  """
  diffs: list[SchemaDiff] = []

  # Compare tables
  all_table_names = set(old_tables.keys()) | set(new_tables.keys())

  for table_name in sorted(all_table_names):
    old_table = old_tables.get(table_name)
    new_table = new_tables.get(table_name)

    table_diffs = diff_tables(old_table, new_table)
    diffs.extend(table_diffs)

  # Compare edges
  all_edge_names = set(old_edges.keys()) | set(new_edges.keys())

  for edge_name in sorted(all_edge_names):
    old_edge = old_edges.get(edge_name)
    new_edge = new_edges.get(edge_name)

    edge_diffs = diff_edges(old_edge, new_edge)
    diffs.extend(edge_diffs)

  return diffs


def _generate_migration_content(
  version: str,
  description: str,
  diffs: list[SchemaDiff],
  author: str,
) -> str:
  """Generate Python migration file content.

  Args:
    version: Migration version
    description: Migration description
    diffs: List of schema differences
    author: Migration author

  Returns:
    Python file content as string
  """
  # Generate up statements
  up_statements = [diff.forward_sql for diff in diffs if diff.forward_sql]

  # Generate down statements (in reverse order)
  down_statements = [diff.backward_sql for diff in reversed(diffs) if diff.backward_sql]

  # Format statements as Python list
  up_lines = _format_statements(up_statements)
  down_lines = _format_statements(down_statements)

  # Generate file content
  content = f'''"""Migration: {description}

Generated: {datetime.utcnow().isoformat()}
Author: {author}
"""


def up() -> list[str]:
  """Apply migration (forward)."""
  return [
{up_lines}
  ]


def down() -> list[str]:
  """Rollback migration (backward)."""
  return [
{down_lines}
  ]


metadata = {{
  'version': '{version}',
  'description': '{description}',
  'author': '{author}',
  'depends_on': [],
}}
'''

  return content


def _format_statements(statements: list[str]) -> str:
  """Format SQL statements for Python list.

  Args:
    statements: List of SQL statements

  Returns:
    Formatted string for Python list
  """
  if not statements:
    return ''

  # Format each statement with proper indentation and quotes
  formatted = []
  for stmt in statements:
    # Escape single quotes in statement
    escaped = stmt.replace("'", "\\'")
    formatted.append(f"    '{escaped}',")

  return '\n'.join(formatted)


def _generate_version() -> str:
  """Generate timestamp-based version string.

  Format: YYYYMMDD_HHMMSS

  Returns:
    Version string

  Examples:
    >>> version = _generate_version()
    >>> len(version)
    15
  """
  now = datetime.utcnow()
  return now.strftime('%Y%m%d_%H%M%S')


def _generate_filename(version: str, description: str) -> str:
  """Generate migration filename.

  Args:
    version: Migration version
    description: Migration description

  Returns:
    Filename string

  Examples:
    >>> _generate_filename('20260102_120000', 'Create user table')
    '20260102_120000_create_user_table.py'
  """
  # Sanitize description for filename
  sanitized = description.lower()
  sanitized = sanitized.replace(' ', '_')
  sanitized = ''.join(c for c in sanitized if c.isalnum() or c == '_')

  return f'{version}_{sanitized}.py'


def create_blank_migration(
  directory: Path,
  description: str,
  author: str = 'reverie',
) -> Path:
  """Create a blank migration file for manual editing.

  Args:
    directory: Directory to write migration file
    description: Migration description
    author: Migration author

  Returns:
    Path to created migration file

  Examples:
    >>> path = create_blank_migration(
    ...   Path('migrations'),
    ...   'Custom data migration',
    ... )
  """
  log = logger.bind(description=description)

  try:
    log.info('creating_blank_migration')

    # Generate version and filename
    version = _generate_version()
    filename = _generate_filename(version, description)
    file_path = directory / filename

    # Generate blank template
    content = f'''"""Migration: {description}

Generated: {datetime.utcnow().isoformat()}
Author: {author}
"""


def up() -> list[str]:
  """Apply migration (forward).

  Add your forward migration SQL statements here.
  """
  return [
    # Example: 'DEFINE TABLE example SCHEMAFULL;',
    # Example: 'DEFINE FIELD name ON TABLE example TYPE string;',
  ]


def down() -> list[str]:
  """Rollback migration (backward).

  Add your rollback SQL statements here (in reverse order).
  """
  return [
    # Example: 'REMOVE TABLE example;',
  ]


metadata = {{
  'version': '{version}',
  'description': '{description}',
  'author': '{author}',
  'depends_on': [],
}}
'''

    # Ensure directory exists
    directory.mkdir(parents=True, exist_ok=True)

    # Write file
    file_path.write_text(content, encoding='utf-8')

    log.info('blank_migration_created', path=str(file_path))
    return file_path

  except Exception as e:
    log.error('failed_to_create_blank_migration', error=str(e))
    raise MigrationGenerationError(f'Failed to create blank migration: {e}') from e


def generate_migration_from_diffs(
  directory: Path,
  description: str,
  diffs: list[SchemaDiff],
  author: str = 'reverie',
) -> Path:
  """Generate migration file from a list of diffs.

  Args:
    directory: Directory to write migration file
    description: Migration description
    diffs: List of schema differences
    author: Migration author

  Returns:
    Path to generated migration file

  Examples:
    >>> diffs = diff_tables(old_table, new_table)
    >>> path = generate_migration_from_diffs(
    ...   Path('migrations'),
    ...   'Update user table',
    ...   diffs,
    ... )
  """
  log = logger.bind(description=description)

  try:
    log.info('generating_migration_from_diffs', diff_count=len(diffs))

    if not diffs:
      raise MigrationGenerationError('No diffs provided')

    # Generate version
    version = _generate_version()

    # Generate content
    content = _generate_migration_content(version, description, diffs, author)

    # Create file
    filename = _generate_filename(version, description)
    file_path = directory / filename

    # Ensure directory exists
    directory.mkdir(parents=True, exist_ok=True)

    # Write file
    file_path.write_text(content, encoding='utf-8')

    log.info('migration_generated_from_diffs', path=str(file_path))
    return file_path

  except MigrationGenerationError:
    raise
  except Exception as e:
    log.error('failed_to_generate_migration_from_diffs', error=str(e))
    raise MigrationGenerationError(f'Failed to generate migration from diffs: {e}') from e
