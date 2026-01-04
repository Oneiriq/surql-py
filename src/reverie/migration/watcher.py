"""Schema file watcher for auto-migration detection.

Uses watchdog library for cross-platform file monitoring to detect schema
changes and prompt for migration generation.
"""

import asyncio
import contextlib
import importlib.util
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Literal

import structlog
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver

if TYPE_CHECKING:
  from reverie.connection.client import DatabaseClient
  from reverie.schema.table import TableDefinition

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SchemaChange:
  """Represents a detected schema change.

  Attributes:
    file_path: Path to the changed schema file
    change_type: Type of change (modified, created, deleted)
    timestamp: When the change was detected
    requires_migration: Whether this change requires a migration
    diff_summary: Optional summary of the schema diff
  """

  file_path: Path
  change_type: Literal['modified', 'created', 'deleted']
  timestamp: datetime
  requires_migration: bool
  diff_summary: str | None = None


@dataclass
class PendingChange:
  """Internal representation of a pending file change for debouncing.

  Attributes:
    file_path: Path to the changed file
    change_type: Type of change
    timestamp: When the change was detected
  """

  file_path: Path
  change_type: Literal['modified', 'created', 'deleted']
  timestamp: datetime


class WatcherEventHandler(FileSystemEventHandler):
  """Handle file system events for schema files.

  Filters events to only Python files and forwards them to the SchemaWatcher
  for debounced processing.
  """

  def __init__(self, watcher: 'SchemaWatcher') -> None:
    """Initialize event handler.

    Args:
      watcher: Parent SchemaWatcher instance to notify of changes
    """
    super().__init__()
    self._watcher = watcher
    self._log = logger.bind(handler='WatcherEventHandler')

  def on_modified(self, event: FileSystemEvent) -> None:
    """Handle file modification events.

    Args:
      event: File system event from watchdog
    """
    if event.is_directory:
      return

    path = Path(str(event.src_path))
    if is_schema_file(path):
      self._log.debug('file_modified', path=str(path))
      self._watcher._queue_change(path, 'modified')

  def on_created(self, event: FileSystemEvent) -> None:
    """Handle file creation events.

    Args:
      event: File system event from watchdog
    """
    if event.is_directory:
      return

    path = Path(str(event.src_path))
    if is_schema_file(path):
      self._log.debug('file_created', path=str(path))
      self._watcher._queue_change(path, 'created')

  def on_deleted(self, event: FileSystemEvent) -> None:
    """Handle file deletion events.

    Args:
      event: File system event from watchdog
    """
    if event.is_directory:
      return

    path = Path(str(event.src_path))
    if is_schema_file(path):
      self._log.debug('file_deleted', path=str(path))
      self._watcher._queue_change(path, 'deleted')


@dataclass
class SchemaWatcher:
  """Watch schema files for changes and detect migration needs.

  Uses watchdog to monitor schema files or directories for changes.
  Implements debouncing to avoid triggering on rapid successive changes.

  Examples:
    Basic usage:
    >>> watcher = SchemaWatcher(
    ...   schema_paths=[Path('schemas/')],
    ...   migrations_dir=Path('migrations'),
    ...   on_change=handle_changes,
    ... )
    >>> await watcher.start()
    >>> # ... later
    >>> await watcher.stop()

    Check for changes manually:
    >>> changes = await watcher.check_for_changes()
    >>> for change in changes:
    ...   print(f'{change.file_path}: {change.change_type}')
  """

  schema_paths: list[Path]
  migrations_dir: Path
  on_change: Callable[[list[Path]], Awaitable[None]] | None = None
  debounce_seconds: float = 1.0

  # Private fields
  _observer: BaseObserver | None = field(default=None, init=False, repr=False)
  _running: bool = field(default=False, init=False, repr=False)
  _pending_changes: dict[Path, PendingChange] = field(default_factory=dict, init=False, repr=False)
  _lock: Lock = field(default_factory=Lock, init=False, repr=False)
  _debounce_task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)
  _log: structlog.stdlib.BoundLogger = field(init=False, repr=False)

  def __post_init__(self) -> None:
    """Initialize logger after dataclass creation."""
    # Use object.__setattr__ to work around frozen fields if needed
    object.__setattr__(
      self,
      '_log',
      logger.bind(
        component='SchemaWatcher',
        paths=[str(p) for p in self.schema_paths],
      ),
    )

  async def start(self) -> None:
    """Start watching for file changes.

    Creates a watchdog Observer and begins monitoring all configured
    schema paths for changes.

    Raises:
      RuntimeError: If watcher is already running
    """
    if self._running:
      raise RuntimeError('SchemaWatcher is already running')

    self._log.info('starting_schema_watcher')

    self._observer = Observer()
    handler = WatcherEventHandler(self)

    # Schedule handlers for each schema path
    for path in self.schema_paths:
      if path.exists():
        if path.is_dir():
          self._observer.schedule(handler, str(path), recursive=True)
          self._log.debug('watching_directory', path=str(path))
        else:
          # Watch parent directory for single files
          parent = path.parent
          self._observer.schedule(handler, str(parent), recursive=False)
          self._log.debug('watching_file', path=str(path))
      else:
        self._log.warning('path_not_found', path=str(path))

    self._observer.start()
    self._running = True
    self._log.info('schema_watcher_started')

  async def stop(self) -> None:
    """Stop watching for file changes.

    Stops the watchdog Observer and cancels any pending debounce tasks.
    """
    if not self._running:
      self._log.debug('watcher_not_running')
      return

    self._log.info('stopping_schema_watcher')

    # Cancel debounce task if running
    if self._debounce_task and not self._debounce_task.done():
      self._debounce_task.cancel()
      with contextlib.suppress(asyncio.CancelledError):
        await self._debounce_task

    # Stop observer
    if self._observer:
      self._observer.stop()
      self._observer.join(timeout=5.0)

    self._running = False
    self._pending_changes.clear()
    self._log.info('schema_watcher_stopped')

  async def check_for_changes(self) -> list[SchemaChange]:
    """Check if schema changes require new migrations.

    Analyzes pending changes and determines if they require migrations
    by comparing the loaded schemas against the database state.

    Returns:
      List of SchemaChange objects representing detected changes
    """
    with self._lock:
      if not self._pending_changes:
        return []

      # Copy and clear pending changes
      changes_to_process = list(self._pending_changes.values())
      self._pending_changes.clear()

    self._log.info('checking_schema_changes', count=len(changes_to_process))

    schema_changes: list[SchemaChange] = []

    for pending in changes_to_process:
      # Determine if migration is needed based on change type
      requires_migration = pending.change_type != 'deleted'

      # Generate diff summary for modified files
      diff_summary: str | None = None
      if pending.change_type == 'modified' and pending.file_path.exists():
        try:
          diff_summary = _generate_diff_summary(pending.file_path)
        except Exception as e:
          self._log.warning(
            'failed_to_generate_diff_summary',
            path=str(pending.file_path),
            error=str(e),
          )

      schema_changes.append(
        SchemaChange(
          file_path=pending.file_path,
          change_type=pending.change_type,
          timestamp=pending.timestamp,
          requires_migration=requires_migration,
          diff_summary=diff_summary,
        )
      )

    return schema_changes

  def _queue_change(
    self,
    path: Path,
    change_type: Literal['modified', 'created', 'deleted'],
  ) -> None:
    """Queue a file change for debounced processing.

    Internal method called by WatcherEventHandler.

    Args:
      path: Path to the changed file
      change_type: Type of change detected
    """
    with self._lock:
      self._pending_changes[path] = PendingChange(
        file_path=path,
        change_type=change_type,
        timestamp=datetime.now(UTC),
      )

    # Schedule debounce callback
    self._schedule_debounce()

  def _schedule_debounce(self) -> None:
    """Schedule the debounce callback.

    Cancels any existing debounce task and schedules a new one.
    """
    # Run in the existing event loop if available
    try:
      loop = asyncio.get_running_loop()
    except RuntimeError:
      # No event loop running, skip scheduling
      return

    # Cancel existing task
    if self._debounce_task and not self._debounce_task.done():
      self._debounce_task.cancel()

    # Schedule new debounce
    self._debounce_task = loop.create_task(self._debounce_callback())

  async def _debounce_callback(self) -> None:
    """Execute callback after debounce period.

    Waits for the debounce period and then triggers the on_change callback
    if one is configured.
    """
    try:
      await asyncio.sleep(self.debounce_seconds)

      changes = await self.check_for_changes()
      if changes and self.on_change:
        # Extract just the paths that changed
        changed_paths = [c.file_path for c in changes]
        await self.on_change(changed_paths)

    except asyncio.CancelledError:
      # Normal cancellation during shutdown or reschedule
      pass


def is_schema_file(path: Path) -> bool:
  """Check if a path is a Python schema file.

  Determines if a file path represents a Python file that could contain
  schema definitions. Excludes test files, __pycache__, and migration files.

  Args:
    path: Path to check

  Returns:
    True if the path is a potential schema file, False otherwise

  Examples:
    >>> is_schema_file(Path('schemas/user.py'))
    True
    >>> is_schema_file(Path('tests/test_user.py'))
    False
    >>> is_schema_file(Path('migrations/001_init.py'))
    False
  """
  # Must be a Python file
  if path.suffix != '.py':
    return False

  # Get the string representation for pattern matching
  path_str = str(path)

  # Exclude common non-schema files
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


async def detect_schema_drift(
  schema_paths: list[Path],
  client: 'DatabaseClient',
) -> list[SchemaChange]:
  """Detect if schema files have changes that need migrations.

  Loads schema definitions from the specified paths and compares them
  against the current database state to detect drift.

  Args:
    schema_paths: Paths to schema files or directories
    client: Connected database client for comparison

  Returns:
    List of SchemaChange objects for files with detected drift

  Examples:
    >>> async with get_client(config) as client:
    ...   drift = await detect_schema_drift([Path('schemas/')], client)
    ...   for change in drift:
    ...     print(f'Drift detected in {change.file_path}')
  """
  from reverie.migration.diff import diff_tables
  from reverie.schema.registry import clear_registry

  log = logger.bind(component='detect_schema_drift')
  changes: list[SchemaChange] = []

  # Collect all Python schema files
  schema_files: list[Path] = []
  for path in schema_paths:
    if path.is_dir():
      schema_files.extend(f for f in path.rglob('*.py') if is_schema_file(f))
    elif path.is_file() and is_schema_file(path):
      schema_files.append(path)

  log.info('checking_schema_files', count=len(schema_files))

  # Get database tables for comparison
  db_tables = await _fetch_db_tables(client)

  for schema_file in schema_files:
    try:
      # Clear registry before loading each file
      clear_registry()

      # Load schemas from file
      code_tables = _load_schemas_from_file(schema_file)

      if not code_tables:
        continue

      # Compare against database
      all_table_names = set(code_tables.keys()) | set(db_tables.keys())
      has_drift = False
      drift_descriptions: list[str] = []

      for table_name in all_table_names:
        code_table = code_tables.get(table_name)
        db_table = db_tables.get(table_name)

        # Only check tables defined in this file
        if code_table is None:
          continue

        diffs = diff_tables(db_table, code_table)
        if diffs:
          has_drift = True
          drift_descriptions.extend(d.description for d in diffs)

      if has_drift:
        changes.append(
          SchemaChange(
            file_path=schema_file,
            change_type='modified',
            timestamp=datetime.now(UTC),
            requires_migration=True,
            diff_summary='; '.join(drift_descriptions[:5]),  # Limit summary length
          )
        )

    except Exception as e:
      log.warning('failed_to_check_schema_file', path=str(schema_file), error=str(e))

  return changes


def _load_schemas_from_file(file_path: Path) -> dict[str, 'TableDefinition']:
  """Load schema definitions from a Python file.

  Dynamically imports the specified Python file and extracts any
  registered table definitions.

  Args:
    file_path: Path to Python file containing schema definitions

  Returns:
    Dictionary of table name to TableDefinition
  """
  from reverie.schema.registry import get_registered_tables
  from reverie.schema.table import TableDefinition

  # Import the module dynamically
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


async def _fetch_db_tables(client: 'DatabaseClient') -> dict[str, 'TableDefinition']:
  """Fetch table definitions from database.

  Delegates to the shared utility function in schema.utils.

  Args:
    client: Connected database client

  Returns:
    Dictionary of table name to TableDefinition
  """
  from reverie.schema.utils import fetch_db_tables

  return await fetch_db_tables(client)


def _generate_diff_summary(file_path: Path) -> str | None:
  """Generate a summary of schema changes in a file.

  Attempts to load the schema file and generate a human-readable
  summary of the tables defined within.

  Args:
    file_path: Path to the schema file

  Returns:
    Summary string or None if unable to generate
  """
  from reverie.schema.registry import clear_registry

  try:
    clear_registry()
    tables = _load_schemas_from_file(file_path)

    if not tables:
      return None

    table_names = list(tables.keys())
    if len(table_names) == 1:
      return f'Schema file defines table: {table_names[0]}'
    return f'Schema file defines tables: {", ".join(table_names)}'

  except Exception:
    return None
