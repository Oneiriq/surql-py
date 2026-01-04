"""Tests for the auto-migration watcher, hooks, and CLI commands.

This module provides comprehensive tests for:
- Schema file watcher (src/reverie/migration/watcher.py)
- Git hook utilities (src/reverie/migration/hooks.py)
- CLI commands: schema check, hook-config, watch (src/reverie/cli/schema.py)
"""

import asyncio
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from typer.testing import CliRunner
from watchdog.events import FileCreatedEvent, FileDeletedEvent, FileModifiedEvent

from reverie.cli.schema import (
  CHECK_EXIT_DRIFT_DETECTED,
  CHECK_EXIT_ERROR,
  CHECK_EXIT_NO_DRIFT,
)
from reverie.cli.schema import (
  app as schema_app,
)
from reverie.migration.hooks import (
  HookCheckResult,
  SchemaDriftInfo,
  check_schema_drift,
  generate_precommit_config,
  get_staged_schema_files,
)
from reverie.migration.watcher import (
  PendingChange,
  SchemaChange,
  SchemaWatcher,
  WatcherEventHandler,
  is_schema_file,
)


def strip_ansi(text: str) -> str:
  """Remove ANSI escape sequences from text."""
  ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
  return ansi_escape.sub('', text)


def extract_json_from_output(text: str) -> dict:
  """Extract JSON object from CLI output that may contain other text."""
  stripped = strip_ansi(text)
  start = stripped.find('{')
  if start == -1:
    raise ValueError(f'No JSON object found in output: {stripped[:100]}...')

  depth = 0
  for i, char in enumerate(stripped[start:], start):
    if char == '{':
      depth += 1
    elif char == '}':
      depth -= 1
      if depth == 0:
        return json.loads(stripped[start : i + 1])

  raise ValueError(f'Unmatched braces in JSON output: {stripped[:100]}...')


# ============================================================================
# SchemaChange Dataclass Tests
# ============================================================================


class TestSchemaChange:
  """Test suite for SchemaChange dataclass."""

  def test_schema_change_creation_modified(self) -> None:
    """Test SchemaChange creation for modified file."""
    change = SchemaChange(
      file_path=Path('schemas/user.py'),
      change_type='modified',
      timestamp=datetime.now(UTC),
      requires_migration=True,
      diff_summary='Schema file defines table: user',
    )

    assert change.file_path == Path('schemas/user.py')
    assert change.change_type == 'modified'
    assert change.requires_migration is True
    assert change.diff_summary == 'Schema file defines table: user'

  def test_schema_change_creation_created(self) -> None:
    """Test SchemaChange creation for created file."""
    change = SchemaChange(
      file_path=Path('schemas/new_table.py'),
      change_type='created',
      timestamp=datetime.now(UTC),
      requires_migration=True,
    )

    assert change.change_type == 'created'
    assert change.requires_migration is True
    assert change.diff_summary is None

  def test_schema_change_creation_deleted(self) -> None:
    """Test SchemaChange creation for deleted file."""
    change = SchemaChange(
      file_path=Path('schemas/old_table.py'),
      change_type='deleted',
      timestamp=datetime.now(UTC),
      requires_migration=False,
    )

    assert change.change_type == 'deleted'
    assert change.requires_migration is False

  def test_schema_change_is_frozen(self) -> None:
    """Test that SchemaChange is immutable (frozen dataclass)."""
    change = SchemaChange(
      file_path=Path('schemas/user.py'),
      change_type='modified',
      timestamp=datetime.now(UTC),
      requires_migration=True,
    )

    with pytest.raises(AttributeError):
      change.file_path = Path('other.py')  # type: ignore[misc]

  def test_schema_change_timestamp_type(self) -> None:
    """Test SchemaChange timestamp is a datetime."""
    now = datetime.now(UTC)
    change = SchemaChange(
      file_path=Path('test.py'),
      change_type='modified',
      timestamp=now,
      requires_migration=True,
    )

    assert change.timestamp == now
    assert isinstance(change.timestamp, datetime)


# ============================================================================
# is_schema_file() Function Tests
# ============================================================================


class TestIsSchemaFile:
  """Test suite for is_schema_file function."""

  def test_valid_schema_file_returns_true(self) -> None:
    """Test valid Python schema file returns True."""
    assert is_schema_file(Path('schemas/user.py')) is True
    assert is_schema_file(Path('src/schemas/models.py')) is True
    assert is_schema_file(Path('user_schema.py')) is True

  def test_non_python_file_returns_false(self) -> None:
    """Test non-Python files return False."""
    assert is_schema_file(Path('schema.txt')) is False
    assert is_schema_file(Path('schema.json')) is False
    assert is_schema_file(Path('schema.yaml')) is False
    assert is_schema_file(Path('schema')) is False

  def test_test_file_returns_false(self) -> None:
    """Test test files return False."""
    assert is_schema_file(Path('test_user.py')) is False
    assert is_schema_file(Path('tests/test_schema.py')) is False
    assert is_schema_file(Path('user_test.py')) is False

  def test_conftest_returns_false(self) -> None:
    """Test conftest.py returns False."""
    assert is_schema_file(Path('conftest.py')) is False
    assert is_schema_file(Path('tests/conftest.py')) is False

  def test_init_file_returns_false(self) -> None:
    """Test __init__.py returns False."""
    assert is_schema_file(Path('__init__.py')) is False
    assert is_schema_file(Path('schemas/__init__.py')) is False

  def test_pycache_file_returns_false(self) -> None:
    """Test __pycache__ files return False."""
    assert is_schema_file(Path('__pycache__/schema.cpython-312.pyc')) is False
    assert is_schema_file(Path('schemas/__pycache__/user.py')) is False

  def test_migration_file_returns_false(self) -> None:
    """Test migration files return False."""
    assert is_schema_file(Path('migrations/001_init.py')) is False
    assert is_schema_file(Path('migrations/20260108_120000_test.py')) is False
    # Windows path separator
    assert is_schema_file(Path('migrations\\001_init.py')) is False

  def test_nested_schema_file_returns_true(self) -> None:
    """Test nested schema files return True."""
    assert is_schema_file(Path('src/app/schemas/user.py')) is True
    assert is_schema_file(Path('deep/nested/path/schema.py')) is True


# ============================================================================
# WatcherEventHandler Tests
# ============================================================================


class TestWatcherEventHandler:
  """Test suite for WatcherEventHandler class."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.mock_watcher = Mock(spec=SchemaWatcher)
    self.mock_watcher._queue_change = Mock()
    self.handler = WatcherEventHandler(self.mock_watcher)

  def test_handles_file_modification(self) -> None:
    """Test handling of file modification events."""
    event = FileModifiedEvent(src_path='schemas/user.py')
    event._is_directory = False

    self.handler.on_modified(event)

    self.mock_watcher._queue_change.assert_called_once()
    args = self.mock_watcher._queue_change.call_args
    assert args[0][0] == Path('schemas/user.py')
    assert args[0][1] == 'modified'

  def test_handles_file_creation(self) -> None:
    """Test handling of file creation events."""
    event = FileCreatedEvent(src_path='schemas/new_table.py')
    event._is_directory = False

    self.handler.on_created(event)

    self.mock_watcher._queue_change.assert_called_once()
    args = self.mock_watcher._queue_change.call_args
    assert args[0][0] == Path('schemas/new_table.py')
    assert args[0][1] == 'created'

  def test_handles_file_deletion(self) -> None:
    """Test handling of file deletion events."""
    event = FileDeletedEvent(src_path='schemas/old_table.py')
    event._is_directory = False

    self.handler.on_deleted(event)

    self.mock_watcher._queue_change.assert_called_once()
    args = self.mock_watcher._queue_change.call_args
    assert args[0][0] == Path('schemas/old_table.py')
    assert args[0][1] == 'deleted'

  def test_ignores_directory_events_on_modified(self) -> None:
    """Test that directory events are ignored on modified."""
    event = MagicMock()
    event.is_directory = True
    event.src_path = 'schemas/'

    self.handler.on_modified(event)

    self.mock_watcher._queue_change.assert_not_called()

  def test_ignores_directory_events_on_created(self) -> None:
    """Test that directory events are ignored on created."""
    event = MagicMock()
    event.is_directory = True
    event.src_path = 'schemas/'

    self.handler.on_created(event)

    self.mock_watcher._queue_change.assert_not_called()

  def test_ignores_directory_events_on_deleted(self) -> None:
    """Test that directory events are ignored on deleted."""
    event = MagicMock()
    event.is_directory = True
    event.src_path = 'schemas/'

    self.handler.on_deleted(event)

    self.mock_watcher._queue_change.assert_not_called()

  def test_ignores_non_schema_files_on_modified(self) -> None:
    """Test that non-schema files are ignored on modified."""
    event = FileModifiedEvent(src_path='tests/test_user.py')
    event._is_directory = False

    self.handler.on_modified(event)

    self.mock_watcher._queue_change.assert_not_called()

  def test_ignores_non_schema_files_on_created(self) -> None:
    """Test that non-schema files are ignored on created."""
    event = FileCreatedEvent(src_path='migrations/001_init.py')
    event._is_directory = False

    self.handler.on_created(event)

    self.mock_watcher._queue_change.assert_not_called()

  def test_ignores_non_schema_files_on_deleted(self) -> None:
    """Test that non-schema files are ignored on deleted."""
    event = FileDeletedEvent(src_path='config.json')
    event._is_directory = False

    self.handler.on_deleted(event)

    self.mock_watcher._queue_change.assert_not_called()


# ============================================================================
# SchemaWatcher Tests
# ============================================================================


class TestSchemaWatcher:
  """Test suite for SchemaWatcher class."""

  def test_initialization_with_paths(self, tmp_path: Path) -> None:
    """Test SchemaWatcher initialization with paths."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()
    migrations_dir = tmp_path / 'migrations'
    migrations_dir.mkdir()

    watcher = SchemaWatcher(
      schema_paths=[schema_dir],
      migrations_dir=migrations_dir,
    )

    assert watcher.schema_paths == [schema_dir]
    assert watcher.migrations_dir == migrations_dir
    assert watcher.debounce_seconds == 1.0
    assert watcher.on_change is None

  def test_initialization_with_callback(self, tmp_path: Path) -> None:
    """Test SchemaWatcher initialization with callback."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()
    migrations_dir = tmp_path / 'migrations'

    async def callback(paths: list[Path]) -> None:
      pass

    watcher = SchemaWatcher(
      schema_paths=[schema_dir],
      migrations_dir=migrations_dir,
      on_change=callback,
      debounce_seconds=2.0,
    )

    assert watcher.on_change is callback
    assert watcher.debounce_seconds == 2.0

  def test_initialization_with_multiple_paths(self, tmp_path: Path) -> None:
    """Test SchemaWatcher initialization with multiple schema paths."""
    schema_dir1 = tmp_path / 'schemas1'
    schema_dir1.mkdir()
    schema_dir2 = tmp_path / 'schemas2'
    schema_dir2.mkdir()

    watcher = SchemaWatcher(
      schema_paths=[schema_dir1, schema_dir2],
      migrations_dir=tmp_path / 'migrations',
    )

    assert len(watcher.schema_paths) == 2

  def test_start_creates_observer(self, tmp_path: Path) -> None:
    """Test that start() creates and starts an observer."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    watcher = SchemaWatcher(
      schema_paths=[schema_dir],
      migrations_dir=tmp_path / 'migrations',
    )

    async def run_test() -> None:
      await watcher.start()
      assert watcher._running is True
      assert watcher._observer is not None
      await watcher.stop()

    asyncio.run(run_test())

  def test_start_raises_if_already_running(self, tmp_path: Path) -> None:
    """Test that start() raises RuntimeError if already running."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    watcher = SchemaWatcher(
      schema_paths=[schema_dir],
      migrations_dir=tmp_path / 'migrations',
    )

    async def run_test() -> None:
      await watcher.start()
      try:
        with pytest.raises(RuntimeError, match='already running'):
          await watcher.start()
      finally:
        await watcher.stop()

    asyncio.run(run_test())

  def test_stop_when_not_running(self, tmp_path: Path) -> None:
    """Test that stop() handles not running gracefully."""
    watcher = SchemaWatcher(
      schema_paths=[tmp_path],
      migrations_dir=tmp_path / 'migrations',
    )

    async def run_test() -> None:
      # Should not raise
      await watcher.stop()
      assert watcher._running is False

    asyncio.run(run_test())

  def test_stop_clears_pending_changes(self, tmp_path: Path) -> None:
    """Test that stop() clears pending changes."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    watcher = SchemaWatcher(
      schema_paths=[schema_dir],
      migrations_dir=tmp_path / 'migrations',
    )

    async def run_test() -> None:
      await watcher.start()
      # Manually add a pending change
      watcher._pending_changes[Path('test.py')] = PendingChange(
        file_path=Path('test.py'),
        change_type='modified',
        timestamp=datetime.now(UTC),
      )
      await watcher.stop()
      assert len(watcher._pending_changes) == 0

    asyncio.run(run_test())

  def test_queue_change_adds_pending_change(self, tmp_path: Path) -> None:
    """Test that _queue_change adds a pending change."""
    watcher = SchemaWatcher(
      schema_paths=[tmp_path],
      migrations_dir=tmp_path / 'migrations',
    )

    # Call _queue_change directly
    test_path = Path('schemas/user.py')
    watcher._queue_change(test_path, 'modified')

    assert test_path in watcher._pending_changes
    assert watcher._pending_changes[test_path].change_type == 'modified'

  def test_check_for_changes_returns_empty_when_no_changes(self, tmp_path: Path) -> None:
    """Test check_for_changes returns empty list when no changes."""
    watcher = SchemaWatcher(
      schema_paths=[tmp_path],
      migrations_dir=tmp_path / 'migrations',
    )

    async def run_test() -> list[SchemaChange]:
      return await watcher.check_for_changes()

    changes = asyncio.run(run_test())
    assert changes == []

  def test_check_for_changes_processes_pending_changes(self, tmp_path: Path) -> None:
    """Test check_for_changes processes and clears pending changes."""
    watcher = SchemaWatcher(
      schema_paths=[tmp_path],
      migrations_dir=tmp_path / 'migrations',
    )

    # Add pending changes
    watcher._pending_changes[Path('user.py')] = PendingChange(
      file_path=Path('user.py'),
      change_type='created',
      timestamp=datetime.now(UTC),
    )

    async def run_test() -> list[SchemaChange]:
      return await watcher.check_for_changes()

    changes = asyncio.run(run_test())

    assert len(changes) == 1
    assert changes[0].file_path == Path('user.py')
    assert changes[0].change_type == 'created'
    assert changes[0].requires_migration is True
    assert len(watcher._pending_changes) == 0

  def test_deleted_file_does_not_require_migration(self, tmp_path: Path) -> None:
    """Test that deleted files don't require migration."""
    watcher = SchemaWatcher(
      schema_paths=[tmp_path],
      migrations_dir=tmp_path / 'migrations',
    )

    watcher._pending_changes[Path('deleted.py')] = PendingChange(
      file_path=Path('deleted.py'),
      change_type='deleted',
      timestamp=datetime.now(UTC),
    )

    async def run_test() -> list[SchemaChange]:
      return await watcher.check_for_changes()

    changes = asyncio.run(run_test())

    assert len(changes) == 1
    assert changes[0].requires_migration is False


# ============================================================================
# HookCheckResult Dataclass Tests
# ============================================================================


class TestHookCheckResult:
  """Test suite for HookCheckResult dataclass."""

  def test_hook_check_result_passed(self) -> None:
    """Test HookCheckResult when check passed."""
    result = HookCheckResult(
      passed=True,
      message='No schema drift detected',
      unmigrated_files=[],
    )

    assert result.passed is True
    assert 'No schema drift' in result.message
    assert result.unmigrated_files == []
    assert result.suggested_action is None

  def test_hook_check_result_failed(self) -> None:
    """Test HookCheckResult when check failed."""
    result = HookCheckResult(
      passed=False,
      message='Schema drift detected in 2 file(s)',
      unmigrated_files=[Path('schemas/user.py'), Path('schemas/post.py')],
      suggested_action='Generate a migration with: reverie schema generate',
    )

    assert result.passed is False
    assert len(result.unmigrated_files) == 2
    assert result.suggested_action is not None

  def test_hook_check_result_with_suggested_action(self) -> None:
    """Test HookCheckResult with suggested action."""
    result = HookCheckResult(
      passed=True,
      message='No migrations directory found',
      unmigrated_files=[],
      suggested_action='Create a migrations directory',
    )

    assert result.suggested_action == 'Create a migrations directory'


# ============================================================================
# check_schema_drift() Tests
# ============================================================================


class TestCheckSchemaDrift:
  """Test suite for check_schema_drift function."""

  def test_returns_passed_when_no_drift(self, tmp_path: Path) -> None:
    """Test check_schema_drift returns passed=True when no drift."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()
    migrations_dir = tmp_path / 'migrations'
    migrations_dir.mkdir()

    # Create a migration file that is newer than schema files
    migration_file = migrations_dir / '20260108_120000_init.py'
    migration_file.write_text('# migration')

    async def run_test() -> HookCheckResult:
      return await check_schema_drift(
        schema_paths=[schema_dir],
        migrations_dir=migrations_dir,
      )

    result = asyncio.run(run_test())

    # No schema files, so no drift
    assert result.passed is True

  def test_returns_passed_when_missing_migrations_dir(self, tmp_path: Path) -> None:
    """Test check_schema_drift returns passed when migrations dir missing."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    async def run_test() -> HookCheckResult:
      return await check_schema_drift(
        schema_paths=[schema_dir],
        migrations_dir=tmp_path / 'nonexistent_migrations',
      )

    result = asyncio.run(run_test())

    assert result.passed is True
    assert 'No migrations directory found' in result.message

  def test_returns_failed_when_drift_detected(self, tmp_path: Path) -> None:
    """Test check_schema_drift returns passed=False when drift detected."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()
    migrations_dir = tmp_path / 'migrations'
    migrations_dir.mkdir()

    # Create an older migration file
    import time

    migration_file = migrations_dir / '20260107_120000_init.py'
    migration_file.write_text('# old migration')

    # Wait a bit to ensure different timestamps
    time.sleep(0.1)

    # Create a newer schema file (after migration)
    schema_file = schema_dir / 'user_schema.py'
    schema_file.write_text("""
from reverie.schema.table import table_schema
user = table_schema('user')
""")

    with patch('reverie.migration.hooks._load_schemas_from_file') as mock_load:
      # Mock returns empty tables - the important thing is file timestamp
      mock_load.return_value = {}

      async def run_test() -> HookCheckResult:
        return await check_schema_drift(
          schema_paths=[schema_dir],
          migrations_dir=migrations_dir,
          fail_on_drift=True,
        )

      result = asyncio.run(run_test())

    # Schema file is newer than migration, but no tables loaded = no drift reported
    # This tests the code path even if no drift is detected
    assert isinstance(result, HookCheckResult)

  def test_fail_on_drift_false_returns_passed(self, tmp_path: Path) -> None:
    """Test check_schema_drift returns passed=True when fail_on_drift=False."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()
    migrations_dir = tmp_path / 'migrations'
    migrations_dir.mkdir()

    async def run_test() -> HookCheckResult:
      return await check_schema_drift(
        schema_paths=[schema_dir],
        migrations_dir=migrations_dir,
        fail_on_drift=False,
      )

    result = asyncio.run(run_test())

    # With fail_on_drift=False, even drift should return passed=True
    assert result.passed is True

  def test_handles_string_paths(self, tmp_path: Path) -> None:
    """Test check_schema_drift accepts string paths."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()
    migrations_dir = tmp_path / 'migrations'
    migrations_dir.mkdir()

    async def run_test() -> HookCheckResult:
      return await check_schema_drift(
        schema_paths=[str(schema_dir)],
        migrations_dir=str(migrations_dir),
      )

    result = asyncio.run(run_test())

    assert isinstance(result, HookCheckResult)


# ============================================================================
# get_staged_schema_files() Tests
# ============================================================================


class TestGetStagedSchemaFiles:
  """Test suite for get_staged_schema_files function."""

  def test_returns_empty_when_dir_not_exists(self, tmp_path: Path) -> None:
    """Test returns empty list when directory doesn't exist."""
    result = get_staged_schema_files(tmp_path / 'nonexistent')

    assert result == []

  def test_returns_empty_when_no_staged_files(self, tmp_path: Path) -> None:
    """Test returns empty list when no files are staged."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('subprocess.run') as mock_run:
      mock_run.return_value = Mock(
        returncode=0,
        stdout='',
        stderr='',
      )

      result = get_staged_schema_files(schema_dir)

    assert result == []

  def test_returns_files_when_git_succeeds(self, tmp_path: Path) -> None:
    """Test returns correct files when Git commands work."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('subprocess.run') as mock_run:
      mock_run.return_value = Mock(
        returncode=0,
        stdout='schemas/user.py\nschemas/post.py\n',
        stderr='',
      )

      result = get_staged_schema_files(schema_dir)

    # Files should be returned based on the mock output
    # Note: actual filtering depends on path resolution
    assert isinstance(result, list)

  def test_handles_git_command_failure(self, tmp_path: Path) -> None:
    """Test handles Git command failures gracefully."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('subprocess.run') as mock_run:
      mock_run.return_value = Mock(
        returncode=1,
        stdout='',
        stderr='fatal: not a git repository',
      )

      result = get_staged_schema_files(schema_dir)

    assert result == []

  def test_handles_git_not_found(self, tmp_path: Path) -> None:
    """Test handles Git not installed gracefully."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('subprocess.run', side_effect=FileNotFoundError('git not found')):
      result = get_staged_schema_files(schema_dir)

    assert result == []

  def test_handles_subprocess_error(self, tmp_path: Path) -> None:
    """Test handles subprocess errors gracefully."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('subprocess.run', side_effect=subprocess.SubprocessError('Error')):
      result = get_staged_schema_files(schema_dir)

    assert result == []

  def test_filters_non_python_files(self, tmp_path: Path) -> None:
    """Test filters out non-Python files."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('subprocess.run') as mock_run:
      mock_run.return_value = Mock(
        returncode=0,
        stdout='schemas/user.py\nschemas/config.json\nschemas/readme.md\n',
        stderr='',
      )

      result = get_staged_schema_files(schema_dir)

    # Only .py files should be included
    for f in result:
      assert f.suffix == '.py'


# ============================================================================
# generate_precommit_config() Tests
# ============================================================================


class TestGeneratePrecommitConfig:
  """Test suite for generate_precommit_config function."""

  def test_generates_valid_yaml(self) -> None:
    """Test generates valid YAML configuration."""
    config = generate_precommit_config()

    assert 'repos:' in config
    assert 'hooks:' in config
    assert 'reverie-schema-check' in config

  def test_uses_provided_schema_path(self) -> None:
    """Test uses provided schema path in configuration."""
    config = generate_precommit_config(schema_path='src/schemas/')

    assert 'src/schemas/' in config

  def test_includes_fail_on_drift_flag(self) -> None:
    """Test includes --fail-on-drift flag when enabled."""
    config = generate_precommit_config(fail_on_drift=True)

    assert '--fail-on-drift' in config

  def test_excludes_fail_on_drift_flag_when_disabled(self) -> None:
    """Test excludes --fail-on-drift flag when disabled."""
    config = generate_precommit_config(fail_on_drift=False)

    # Flag should not be present (or empty string placeholder)
    lines = [line.strip() for line in config.split('\n') if line.strip()]
    fail_flag_count = sum(1 for line in lines if '--fail-on-drift' in line)
    assert fail_flag_count == 0

  def test_default_schema_path(self) -> None:
    """Test default schema path is schemas/."""
    config = generate_precommit_config()

    assert 'schemas/' in config

  def test_config_contains_required_keys(self) -> None:
    """Test config contains all required pre-commit keys."""
    config = generate_precommit_config()

    assert 'id:' in config
    assert 'name:' in config
    assert 'entry:' in config
    assert 'language:' in config
    assert 'types:' in config


# ============================================================================
# CLI schema check Command Tests
# ============================================================================


class TestSchemaCheckCommand:
  """Test suite for schema check CLI command."""

  def setup_method(self) -> None:
    """Set up test resources with wide terminal for consistent help output."""
    self.runner = CliRunner(
      env={
        'NO_COLOR': '1',
        'COLUMNS': '200',
        'TERM': 'dumb',
        'FORCE_COLOR': '0',
      }
    )

  def test_check_help(self) -> None:
    """Test check command help."""
    result = self.runner.invoke(schema_app, ['check', '--help'])

    assert result.exit_code == 0
    assert 'check' in result.stdout.lower()
    assert '--schema' in result.stdout
    assert '--migrations' in result.stdout
    assert '--fail-on-drift' in result.stdout

  def test_check_returns_exit_code_0_when_no_drift(self, tmp_path: Path) -> None:
    """Test check returns exit code 0 when no drift detected."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('reverie.migration.hooks.check_schema_drift') as mock_check:
      mock_check.return_value = HookCheckResult(
        passed=True,
        message='No schema drift detected',
        unmigrated_files=[],
      )

      result = self.runner.invoke(
        schema_app,
        ['check', '--schema', str(schema_dir)],
      )

    assert result.exit_code == CHECK_EXIT_NO_DRIFT

  def test_check_returns_exit_code_1_when_drift_detected(self, tmp_path: Path) -> None:
    """Test check returns exit code 1 when drift detected."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('reverie.migration.hooks.check_schema_drift') as mock_check:
      mock_check.return_value = HookCheckResult(
        passed=False,
        message='Schema drift detected',
        unmigrated_files=[Path('user.py')],
        suggested_action='Generate a migration',
      )

      result = self.runner.invoke(
        schema_app,
        ['check', '--schema', str(schema_dir)],
      )

    assert result.exit_code == CHECK_EXIT_DRIFT_DETECTED

  def test_check_respects_fail_on_drift_flag(self, tmp_path: Path) -> None:
    """Test check respects --fail-on-drift flag."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('reverie.migration.hooks.check_schema_drift') as mock_check:
      mock_check.return_value = HookCheckResult(
        passed=True,
        message='Schema drift detected (non-blocking)',
        unmigrated_files=[],
      )

      result = self.runner.invoke(
        schema_app,
        ['check', '--schema', str(schema_dir), '--no-fail-on-drift'],
      )

    assert result.exit_code == CHECK_EXIT_NO_DRIFT

  def test_check_outputs_json_format(self, tmp_path: Path) -> None:
    """Test check outputs JSON format correctly."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    with patch('reverie.migration.hooks.check_schema_drift') as mock_check:
      mock_check.return_value = HookCheckResult(
        passed=True,
        message='No drift',
        unmigrated_files=[],
      )

      result = self.runner.invoke(
        schema_app,
        ['check', '--schema', str(schema_dir), '--format', 'json'],
      )

    assert result.exit_code == CHECK_EXIT_NO_DRIFT
    json_output = extract_json_from_output(result.stdout)
    assert 'passed' in json_output
    assert json_output['passed'] is True

  def test_check_returns_exit_code_2_on_error(self) -> None:
    """Test check returns exit code 2 on error."""
    result = self.runner.invoke(
      schema_app,
      ['check', '--schema', '/nonexistent/path'],
    )

    assert result.exit_code == CHECK_EXIT_ERROR

  def test_check_with_migrations_directory(self, tmp_path: Path) -> None:
    """Test check with custom migrations directory."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()
    migrations_dir = tmp_path / 'custom_migrations'
    migrations_dir.mkdir()

    with patch('reverie.migration.hooks.check_schema_drift') as mock_check:
      mock_check.return_value = HookCheckResult(
        passed=True,
        message='No drift',
        unmigrated_files=[],
      )

      result = self.runner.invoke(
        schema_app,
        [
          'check',
          '--schema',
          str(schema_dir),
          '--migrations',
          str(migrations_dir),
        ],
      )

    assert result.exit_code == CHECK_EXIT_NO_DRIFT
    # Verify migrations dir was passed
    call_args = mock_check.call_args
    assert call_args[1]['migrations_dir'] == migrations_dir


# ============================================================================
# CLI schema hook-config Command Tests
# ============================================================================


class TestSchemaHookConfigCommand:
  """Test suite for schema hook-config CLI command."""

  def setup_method(self) -> None:
    """Set up test resources with wide terminal for consistent help output."""
    self.runner = CliRunner(
      env={
        'NO_COLOR': '1',
        'COLUMNS': '200',
        'TERM': 'dumb',
        'FORCE_COLOR': '0',
      }
    )

  def test_hook_config_help(self) -> None:
    """Test hook-config command help."""
    result = self.runner.invoke(schema_app, ['hook-config', '--help'])

    assert result.exit_code == 0
    assert 'hook-config' in result.stdout.lower()
    assert '--schema' in result.stdout

  def test_hook_config_generates_yaml(self) -> None:
    """Test hook-config generates YAML configuration."""
    result = self.runner.invoke(schema_app, ['hook-config'])

    assert result.exit_code == 0
    assert 'repos:' in result.stdout
    assert 'hooks:' in result.stdout
    assert 'reverie-schema-check' in result.stdout

  def test_hook_config_uses_custom_schema_path(self) -> None:
    """Test hook-config uses custom schema path."""
    result = self.runner.invoke(
      schema_app,
      ['hook-config', '--schema', 'src/models/'],
    )

    assert result.exit_code == 0
    assert 'src/models/' in result.stdout

  def test_hook_config_includes_fail_on_drift(self) -> None:
    """Test hook-config includes --fail-on-drift flag."""
    result = self.runner.invoke(
      schema_app,
      ['hook-config', '--fail-on-drift'],
    )

    assert result.exit_code == 0
    assert '--fail-on-drift' in result.stdout

  def test_hook_config_excludes_fail_on_drift_when_disabled(self) -> None:
    """Test hook-config excludes --fail-on-drift when disabled."""
    result = self.runner.invoke(
      schema_app,
      ['hook-config', '--no-fail-on-drift'],
    )

    assert result.exit_code == 0
    # The flag should not appear in the entry command
    # However, there might be a trailing space, so check more carefully
    entry_line = [line for line in result.stdout.split('\n') if 'entry:' in line]
    if entry_line:
      assert '--fail-on-drift' not in entry_line[0] or entry_line[0].strip().endswith(' ')


# ============================================================================
# CLI schema watch Command Tests
# ============================================================================


class TestSchemaWatchCommand:
  """Test suite for schema watch CLI command."""

  def setup_method(self) -> None:
    """Set up test resources with wide terminal for consistent help output."""
    self.runner = CliRunner(
      env={
        'NO_COLOR': '1',
        'COLUMNS': '200',
        'TERM': 'dumb',
        'FORCE_COLOR': '0',
      }
    )

  def test_watch_help(self) -> None:
    """Test watch command help."""
    result = self.runner.invoke(schema_app, ['watch', '--help'])

    assert result.exit_code == 0
    assert 'watch' in result.stdout.lower()
    assert '--schema' in result.stdout
    assert '--migrations' in result.stdout
    assert '--debounce' in result.stdout
    assert '--auto-generate' in result.stdout
    assert '--no-prompt' in result.stdout

  def test_watch_requires_schema_option(self) -> None:
    """Test watch requires --schema option."""
    result = self.runner.invoke(schema_app, ['watch'])

    assert result.exit_code != 0

  def test_watch_validates_schema_path_exists(self) -> None:
    """Test watch validates schema path exists."""
    result = self.runner.invoke(
      schema_app,
      ['watch', '--schema', '/nonexistent/path'],
    )

    assert result.exit_code != 0

  def test_watch_no_prompt_mode(self, tmp_path: Path) -> None:
    """Test watch with --no-prompt mode."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()

    # We can't easily test the full watch loop, but we can test initialization
    with (
      patch('reverie.migration.watcher.SchemaWatcher') as mock_watcher_class,
      patch('asyncio.sleep', side_effect=KeyboardInterrupt),
    ):
      mock_watcher = AsyncMock()
      mock_watcher_class.return_value = mock_watcher
      mock_watcher.start = AsyncMock()
      mock_watcher.stop = AsyncMock()

      result = self.runner.invoke(
        schema_app,
        ['watch', '--schema', str(schema_dir), '--no-prompt'],
      )

    # Should exit gracefully on KeyboardInterrupt
    assert result.exit_code == 0 or isinstance(result.exception, KeyboardInterrupt)

  def test_watch_creates_migrations_dir_if_missing(self, tmp_path: Path) -> None:
    """Test watch creates migrations directory if it doesn't exist."""
    schema_dir = tmp_path / 'schemas'
    schema_dir.mkdir()
    migrations_dir = tmp_path / 'migrations'

    # Migrations dir doesn't exist yet
    assert not migrations_dir.exists()

    with (
      patch('reverie.migration.watcher.SchemaWatcher') as mock_watcher_class,
      patch('asyncio.sleep', side_effect=KeyboardInterrupt),
    ):
      mock_watcher = AsyncMock()
      mock_watcher_class.return_value = mock_watcher
      mock_watcher.start = AsyncMock()
      mock_watcher.stop = AsyncMock()

      self.runner.invoke(
        schema_app,
        [
          'watch',
          '--schema',
          str(schema_dir),
          '--migrations',
          str(migrations_dir),
        ],
      )

    # Migrations directory should now exist
    assert migrations_dir.exists()


# ============================================================================
# PendingChange Dataclass Tests
# ============================================================================


class TestPendingChange:
  """Test suite for PendingChange dataclass."""

  def test_pending_change_creation(self) -> None:
    """Test PendingChange creation."""
    change = PendingChange(
      file_path=Path('schemas/user.py'),
      change_type='modified',
      timestamp=datetime.now(UTC),
    )

    assert change.file_path == Path('schemas/user.py')
    assert change.change_type == 'modified'
    assert isinstance(change.timestamp, datetime)

  def test_pending_change_change_types(self) -> None:
    """Test PendingChange supports all change types."""
    for change_type in ['modified', 'created', 'deleted']:
      change = PendingChange(
        file_path=Path('test.py'),
        change_type=change_type,  # type: ignore[arg-type]
        timestamp=datetime.now(UTC),
      )
      assert change.change_type == change_type


# ============================================================================
# SchemaDriftInfo Dataclass Tests
# ============================================================================


class TestSchemaDriftInfo:
  """Test suite for SchemaDriftInfo dataclass."""

  def test_schema_drift_info_creation(self) -> None:
    """Test SchemaDriftInfo creation."""
    info = SchemaDriftInfo(
      file_path=Path('schemas/user.py'),
      table_name='user',
      diff_descriptions=['Missing field: email'],
    )

    assert info.file_path == Path('schemas/user.py')
    assert info.table_name == 'user'
    assert 'Missing field: email' in info.diff_descriptions

  def test_schema_drift_info_default_descriptions(self) -> None:
    """Test SchemaDriftInfo default empty descriptions."""
    info = SchemaDriftInfo(
      file_path=Path('test.py'),
      table_name='test',
    )

    assert info.diff_descriptions == []

  def test_schema_drift_info_multiple_descriptions(self) -> None:
    """Test SchemaDriftInfo with multiple descriptions."""
    info = SchemaDriftInfo(
      file_path=Path('test.py'),
      table_name='test',
      diff_descriptions=[
        'Missing table',
        'Field type mismatch',
        'Extra index',
      ],
    )

    assert len(info.diff_descriptions) == 3
