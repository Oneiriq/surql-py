"""Git hook utilities for schema drift detection.

This module provides utilities for integrating reverie schema drift detection
into git pre-commit hooks and CI/CD pipelines. It allows for detecting
unmigrated schema changes before commits are made.
"""

import importlib.util
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
  from reverie.schema.table import TableDefinition

logger = structlog.get_logger(__name__)


@dataclass
class HookCheckResult:
  """Result of a pre-commit hook check.

  Attributes:
    passed: Whether the check passed (no drift detected)
    message: Human-readable message describing the result
    unmigrated_files: List of schema files with unmigrated changes
    suggested_action: Suggested action to resolve drift (if any)
  """

  passed: bool
  message: str
  unmigrated_files: list[Path]
  suggested_action: str | None = None


@dataclass
class SchemaDriftInfo:
  """Information about detected schema drift.

  Attributes:
    file_path: Path to the schema file with drift
    table_name: Name of the table with drift
    diff_descriptions: List of diff descriptions
  """

  file_path: Path
  table_name: str
  diff_descriptions: list[str] = field(default_factory=list)


async def check_schema_drift(
  schema_paths: Sequence[Path | str],
  migrations_dir: Path | str | None = None,
  fail_on_drift: bool = True,
) -> HookCheckResult:
  """Check for unmigrated schema changes.

  This function is designed to be called from pre-commit hooks
  or CI/CD pipelines to detect schema drift. Unlike database-connected
  drift detection, this version compares code state against applied
  migrations without requiring a database connection.

  Args:
    schema_paths: Paths to schema files to check
    migrations_dir: Directory containing migrations (auto-detected if None)
    fail_on_drift: If True, returns failed result when drift detected

  Returns:
    HookCheckResult with pass/fail status and details

  Examples:
    >>> result = await check_schema_drift([Path('schemas/')], Path('migrations'))
    >>> if not result.passed:
    ...   print(f'Drift detected: {result.message}')
  """
  log = logger.bind(component='check_schema_drift')
  log.info('starting_schema_drift_check', paths=[str(p) for p in schema_paths])

  # Normalize paths
  paths = [Path(p) if isinstance(p, str) else p for p in schema_paths]
  mig_dir = Path(migrations_dir) if migrations_dir else _find_migrations_dir()

  if not mig_dir or not mig_dir.exists():
    log.warning('migrations_dir_not_found', dir=str(mig_dir))
    return HookCheckResult(
      passed=True,
      message='No migrations directory found - skipping drift check',
      unmigrated_files=[],
      suggested_action='Create a migrations directory to enable drift detection',
    )

  # Collect all schema files to check
  schema_files = _collect_schema_files(paths)

  if not schema_files:
    log.info('no_schema_files_found')
    return HookCheckResult(
      passed=True,
      message='No schema files found to check',
      unmigrated_files=[],
    )

  log.info('checking_schema_files', count=len(schema_files))

  # Detect drift by comparing code schemas against migration state
  drift_info = _detect_code_drift(schema_files, mig_dir)

  if not drift_info:
    return HookCheckResult(
      passed=True,
      message='No schema drift detected - all changes are migrated',
      unmigrated_files=[],
    )

  # Format result message
  unmigrated = [info.file_path for info in drift_info]
  message_parts = [f'Schema drift detected in {len(unmigrated)} file(s):']

  for info in drift_info:
    message_parts.append(f'  - {info.file_path}: {info.table_name}')
    for desc in info.diff_descriptions[:3]:  # Limit descriptions
      message_parts.append(f'      {desc}')

  message = '\n'.join(message_parts)

  if fail_on_drift:
    return HookCheckResult(
      passed=False,
      message=message,
      unmigrated_files=unmigrated,
      suggested_action=(
        "Generate a migration with: reverie schema generate -s <schema-file> -m '<description>'"
      ),
    )

  return HookCheckResult(
    passed=True,
    message=f'Schema drift detected (non-blocking):\n{message}',
    unmigrated_files=unmigrated,
    suggested_action=(
      'Consider generating a migration with: '
      "reverie schema generate -s <schema-file> -m '<description>'"
    ),
  )


def get_staged_schema_files(
  schema_dir: Path | str,
  extensions: tuple[str, ...] = ('.py',),
) -> list[Path]:
  """Get list of staged Python files that contain schema definitions.

  Uses git diff --cached to find staged files in the schema directory.

  Args:
    schema_dir: Directory containing schema files
    extensions: File extensions to include (default: .py)

  Returns:
    List of Path objects for staged schema files

  Examples:
    >>> staged = get_staged_schema_files(Path('schemas/'))
    >>> for path in staged:
    ...   print(f'Staged: {path}')
  """
  log = logger.bind(component='get_staged_schema_files')
  schema_path = Path(schema_dir) if isinstance(schema_dir, str) else schema_dir

  if not schema_path.exists():
    log.warning('schema_dir_not_found', dir=str(schema_path))
    return []

  try:
    # Run git diff --cached to get staged files
    result = subprocess.run(
      ['git', 'diff', '--cached', '--name-only', '--diff-filter=ACMR'],
      capture_output=True,
      text=True,
      check=False,
      cwd=schema_path.parent if schema_path.is_file() else schema_path,
    )

    if result.returncode != 0:
      log.warning(
        'git_diff_failed',
        returncode=result.returncode,
        stderr=result.stderr,
      )
      return []

    staged_files: list[Path] = []
    schema_dir_absolute = schema_path.resolve()

    for filename in result.stdout.strip().split('\n'):
      if not filename:
        continue

      # Check if file is in schema directory and has correct extension
      file_path = Path(filename)

      if not any(file_path.suffix == ext for ext in extensions):
        continue

      # Check if file is under schema directory
      try:
        file_absolute = file_path.resolve()
        is_in_schema_dir = (
          schema_dir_absolute in file_absolute.parents
          or file_absolute == schema_dir_absolute
          or file_path.is_relative_to(schema_path)
        )
        if is_in_schema_dir and _is_schema_file(file_path):
          staged_files.append(file_path)
      except (ValueError, OSError):
        # Path resolution failed, try relative comparison
        if str(file_path).startswith(str(schema_path)) and _is_schema_file(file_path):
          staged_files.append(file_path)

    log.info('staged_schema_files_found', count=len(staged_files))
    return staged_files

  except FileNotFoundError:
    log.warning('git_not_found')
    return []
  except subprocess.SubprocessError as e:
    log.warning('git_command_error', error=str(e))
    return []


def generate_precommit_config(
  schema_path: str = 'schemas/',
  fail_on_drift: bool = True,
) -> str:
  """Generate .pre-commit-config.yaml entry for reverie schema check.

  Returns YAML string that can be added to .pre-commit-config.yaml.

  Args:
    schema_path: Path to schema files (default: 'schemas/')
    fail_on_drift: Whether to fail on drift detection (default: True)

  Returns:
    YAML string for pre-commit configuration

  Examples:
    >>> config = generate_precommit_config('src/schemas/')
    >>> print(config)
    repos:
      - repo: local
        hooks:
          - id: reverie-schema-check
            ...
  """
  fail_flag = '--fail-on-drift' if fail_on_drift else ''

  config = f"""repos:
  - repo: local
    hooks:
      - id: reverie-schema-check
        name: Check schema migrations
        entry: reverie schema check --schema {schema_path} {fail_flag}
        language: python
        types: [python]
        pass_filenames: false
"""
  return config.strip()


def _find_migrations_dir() -> Path | None:
  """Find migrations directory using settings or common locations.

  Resolution order:
  1. REVERIE_MIGRATION_PATH environment variable
  2. .env file (REVERIE_MIGRATION_PATH)
  3. pyproject.toml [tool.reverie] migration_path
  4. Common locations (migrations/, db/migrations/, etc.)

  Returns:
    Path to migrations directory or None if not found
  """
  # First, check configured path from settings
  try:
    from reverie.settings import get_migration_path

    configured_path = get_migration_path()
    # If relative path, resolve relative to current directory
    if not configured_path.is_absolute():
      resolved_path = Path.cwd() / configured_path
    else:
      resolved_path = configured_path

    if resolved_path.exists() and resolved_path.is_dir():
      return resolved_path
  except Exception:
    # Fall back to common locations if settings fail
    pass

  # Fallback: check common locations
  common_locations = [
    Path('migrations'),
    Path('db/migrations'),
    Path('src/migrations'),
    Path('database/migrations'),
  ]

  for location in common_locations:
    if location.exists() and location.is_dir():
      return location

  return None


def _collect_schema_files(paths: Sequence[Path]) -> list[Path]:
  """Collect all Python schema files from given paths.

  Args:
    paths: List of paths (files or directories)

  Returns:
    List of schema file paths
  """
  schema_files: list[Path] = []

  for path in paths:
    if not path.exists():
      continue

    if path.is_file():
      if _is_schema_file(path):
        schema_files.append(path)
    elif path.is_dir():
      for file_path in path.rglob('*.py'):
        if _is_schema_file(file_path):
          schema_files.append(file_path)

  return schema_files


def _is_schema_file(path: Path) -> bool:
  """Check if a path is a Python schema file.

  Excludes test files, __pycache__, migrations, and other non-schema files.

  Args:
    path: Path to check

  Returns:
    True if the path is a potential schema file
  """
  if path.suffix != '.py':
    return False

  path_str = str(path)

  exclusions = [
    '__pycache__',
    '__init__.py',
    'test_',
    '_test.py',
    'conftest.py',
    'migrations/',
    'migrations\\',
  ]

  return all(exclusion not in path_str for exclusion in exclusions)


def _detect_code_drift(
  schema_files: list[Path],
  migrations_dir: Path,
) -> list[SchemaDriftInfo]:
  """Detect schema drift by comparing schema file timestamps against migrations.

  This is a heuristic approach that checks if schema files have been modified
  more recently than the latest migration. For more precise drift detection,
  use the database-connected drift check via `reverie schema validate`.

  Args:
    schema_files: List of schema files to check
    migrations_dir: Path to migrations directory

  Returns:
    List of SchemaDriftInfo for files with potential drift
  """
  from reverie.schema.registry import clear_registry

  log = logger.bind(component='detect_code_drift')
  drift_info: list[SchemaDriftInfo] = []

  # Get latest migration timestamp
  latest_migration_time = _get_latest_migration_time(migrations_dir)

  for schema_file in schema_files:
    try:
      # Check if schema file is newer than latest migration
      schema_mtime = schema_file.stat().st_mtime
      file_is_newer = latest_migration_time is None or schema_mtime > latest_migration_time

      if not file_is_newer:
        continue

      # Clear registry and load schemas from file
      clear_registry()
      code_tables = _load_schemas_from_file(schema_file)

      if not code_tables:
        continue

      # File has schemas and is newer than latest migration - potential drift
      table_names = list(code_tables.keys())
      drift_descriptions = [
        'Schema file modified after latest migration',
        f'Tables defined: {", ".join(table_names)}',
        'Run "reverie schema validate" for detailed drift detection',
      ]

      drift_info.append(
        SchemaDriftInfo(
          file_path=schema_file,
          table_name=table_names[0] if table_names else 'unknown',
          diff_descriptions=drift_descriptions,
        )
      )

    except Exception as e:
      log.warning(
        'failed_to_check_schema_file',
        path=str(schema_file),
        error=str(e),
      )

  return drift_info


def _get_latest_migration_time(migrations_dir: Path) -> float | None:
  """Get the modification time of the latest migration file.

  Args:
    migrations_dir: Path to migrations directory

  Returns:
    Latest migration modification time as Unix timestamp, or None if no migrations
  """
  if not migrations_dir.exists():
    return None

  migration_files = sorted(
    [f for f in migrations_dir.glob('*.py') if not f.name.startswith('_')],
    reverse=True,
  )

  if not migration_files:
    return None

  # Return the modification time of the newest migration file
  return migration_files[0].stat().st_mtime


def _load_schemas_from_file(file_path: Path) -> dict[str, 'TableDefinition']:
  """Load schema definitions from a Python file.

  Args:
    file_path: Path to Python file containing schema definitions

  Returns:
    Dictionary of table name to TableDefinition
  """
  from reverie.schema.registry import get_registered_tables
  from reverie.schema.table import TableDefinition

  spec = importlib.util.spec_from_file_location('schema_module', file_path)
  if spec is None or spec.loader is None:
    return {}

  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)

  # Get registered tables from registry
  registered = get_registered_tables()
  if registered:
    return registered

  # Fallback: scan module for TableDefinition objects
  tables: dict[str, TableDefinition] = {}
  for name in dir(module):
    obj = getattr(module, name)
    if isinstance(obj, TableDefinition):
      tables[obj.name] = obj

  return tables
