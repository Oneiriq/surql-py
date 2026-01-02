"""Migration file discovery and loading.

This module provides functions for discovering migration files in a directory
and loading them into Migration objects.
"""

import hashlib
import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import structlog

from src.migration.models import Migration, MigrationMetadata

logger = structlog.get_logger(__name__)


class MigrationDiscoveryError(Exception):
  """Raised when migration discovery fails."""

  pass


class MigrationLoadError(Exception):
  """Raised when loading a migration file fails."""

  pass


def discover_migrations(directory: Path) -> list[Migration]:
  """Discover all migration files in a directory.

  Pure function that scans a directory for migration files and loads them
  in sorted order by version (timestamp).

  Args:
    directory: Path to migrations directory

  Returns:
    Sorted list of Migration objects

  Raises:
    MigrationDiscoveryError: If directory doesn't exist or is invalid

  Examples:
    >>> migrations = discover_migrations(Path('migrations'))
    >>> len(migrations)
    5
    >>> migrations[0].version
    '20260102_120000'
  """
  log = logger.bind(directory=str(directory))

  if not directory.exists():
    log.warning('migration_directory_not_found')
    return []

  if not directory.is_dir():
    raise MigrationDiscoveryError(f'Path is not a directory: {directory}')

  try:
    # Find all Python files (excluding __init__.py and private files)
    migration_files = sorted([f for f in directory.glob('*.py') if not f.name.startswith('_')])

    log.info('discovered_migration_files', count=len(migration_files))

    # Load each migration file
    migrations: list[Migration] = []
    for file_path in migration_files:
      try:
        migration = load_migration(file_path)
        migrations.append(migration)
        log.debug('loaded_migration', version=migration.version, path=str(file_path))
      except MigrationLoadError as e:
        log.error('failed_to_load_migration', path=str(file_path), error=str(e))
        raise

    # Sort by version (timestamp-based sorting)
    migrations.sort(key=lambda m: m.version)

    log.info('migrations_discovered', count=len(migrations))
    return migrations

  except Exception as e:
    log.error('migration_discovery_failed', error=str(e))
    raise MigrationDiscoveryError(f'Failed to discover migrations: {e}') from e


def load_migration(path: Path) -> Migration:
  """Load a single migration file.

  Imports the migration module and extracts the up, down functions
  and metadata.

  Args:
    path: Path to migration file

  Returns:
    Migration object

  Raises:
    MigrationLoadError: If the file cannot be loaded or is invalid

  Examples:
    >>> migration = load_migration(Path('migrations/20260102_120000_create_user.py'))
    >>> migration.version
    '20260102_120000'
    >>> migration.description
    'Create user table'
  """
  log = logger.bind(path=str(path))

  if not path.exists():
    raise MigrationLoadError(f'Migration file not found: {path}')

  if not path.is_file():
    raise MigrationLoadError(f'Path is not a file: {path}')

  try:
    # Load the module dynamically
    module = _load_module_from_path(path)

    # Extract required components
    up_fn = _get_migration_function(module, 'up', path)
    down_fn = _get_migration_function(module, 'down', path)
    metadata = _get_migration_metadata(module, path)

    # Calculate checksum of file content
    checksum = _calculate_checksum(path)

    # Create Migration object
    migration = Migration(
      version=metadata.version,
      description=metadata.description,
      path=path,
      up=up_fn,
      down=down_fn,
      checksum=checksum,
      depends_on=metadata.depends_on,
    )

    log.debug('migration_loaded', version=migration.version)
    return migration

  except MigrationLoadError:
    raise
  except Exception as e:
    log.error('failed_to_load_migration', error=str(e))
    raise MigrationLoadError(f'Failed to load migration from {path}: {e}') from e


def _load_module_from_path(path: Path) -> Any:
  """Load a Python module from a file path.

  Args:
    path: Path to Python file

  Returns:
    Loaded module object

  Raises:
    MigrationLoadError: If module cannot be loaded
  """
  try:
    module_name = f'_migration_{path.stem}'

    # Create module spec
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
      raise MigrationLoadError(f'Cannot create module spec for {path}')

    # Load the module
    module = importlib.util.module_from_spec(spec)

    # Add to sys.modules temporarily
    sys.modules[module_name] = module

    # Execute the module
    spec.loader.exec_module(module)

    return module

  except Exception as e:
    raise MigrationLoadError(f'Failed to load module from {path}: {e}') from e


def _get_migration_function(
  module: Any,
  name: str,
  path: Path,
) -> Callable[[], list[str]]:
  """Extract a migration function from a module.

  Args:
    module: Loaded migration module
    name: Function name ('up' or 'down')
    path: Path to migration file (for error messages)

  Returns:
    Migration function

  Raises:
    MigrationLoadError: If function is missing or invalid
  """
  if not hasattr(module, name):
    raise MigrationLoadError(f'Migration {path} missing {name}() function')

  fn = getattr(module, name)

  if not callable(fn):
    raise MigrationLoadError(f'Migration {path} {name} is not callable')

  return fn


def _get_migration_metadata(module: Any, path: Path) -> MigrationMetadata:
  """Extract metadata from a migration module.

  Args:
    module: Loaded migration module
    path: Path to migration file

  Returns:
    MigrationMetadata object

  Raises:
    MigrationLoadError: If metadata is missing or invalid
  """
  if not hasattr(module, 'metadata'):
    raise MigrationLoadError(f'Migration {path} missing metadata dict')

  metadata_dict = module.metadata

  if not isinstance(metadata_dict, dict):
    raise MigrationLoadError(f'Migration {path} metadata must be a dict')

  try:
    return MigrationMetadata(**metadata_dict)
  except Exception as e:
    raise MigrationLoadError(f'Invalid metadata in {path}: {e}') from e


def _calculate_checksum(path: Path) -> str:
  """Calculate SHA256 checksum of a file.

  Args:
    path: Path to file

  Returns:
    Hexadecimal checksum string
  """
  sha256 = hashlib.sha256()

  with open(path, 'rb') as f:
    for chunk in iter(lambda: f.read(4096), b''):
      sha256.update(chunk)

  return sha256.hexdigest()


def validate_migration_name(filename: str) -> bool:
  """Validate migration filename format.

  Expected format: YYYYMMDD_HHMMSS_description.py

  Args:
    filename: Migration filename

  Returns:
    True if valid, False otherwise

  Examples:
    >>> validate_migration_name('20260102_120000_create_user.py')
    True
    >>> validate_migration_name('invalid.py')
    False
  """
  if not filename.endswith('.py'):
    return False

  # Remove .py extension
  name = filename[:-3]

  # Check for underscore separators
  parts = name.split('_')
  if len(parts) < 3:
    return False

  # First part should be YYYYMMDD (8 digits)
  if len(parts[0]) != 8 or not parts[0].isdigit():
    return False

  # Second part should be HHMMSS (6 digits)
  if len(parts[1]) != 6 or not parts[1].isdigit():
    return False

  # Rest is description
  return True


def get_version_from_filename(filename: str) -> str | None:
  """Extract version from migration filename.

  Args:
    filename: Migration filename

  Returns:
    Version string (YYYYMMDD_HHMMSS) or None if invalid

  Examples:
    >>> get_version_from_filename('20260102_120000_create_user.py')
    '20260102_120000'
    >>> get_version_from_filename('invalid.py')
    None
  """
  if not validate_migration_name(filename):
    return None

  parts = filename[:-3].split('_')
  return f'{parts[0]}_{parts[1]}'


def get_description_from_filename(filename: str) -> str | None:
  """Extract description from migration filename.

  Args:
    filename: Migration filename

  Returns:
    Description string or None if invalid

  Examples:
    >>> get_description_from_filename('20260102_120000_create_user.py')
    'create_user'
    >>> get_description_from_filename('invalid.py')
    None
  """
  if not validate_migration_name(filename):
    return None

  parts = filename[:-3].split('_')
  return '_'.join(parts[2:])
