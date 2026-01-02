"""Tests for the migration module."""

from datetime import datetime
from pathlib import Path

import pytest

from src.migration.discovery import (
  MigrationDiscoveryError,
  MigrationLoadError,
  _calculate_checksum,
  discover_migrations,
  get_description_from_filename,
  get_version_from_filename,
  load_migration,
  validate_migration_name,
)
from src.migration.models import (
  DiffOperation,
  Migration,
  MigrationDirection,
  MigrationHistory,
  MigrationMetadata,
  MigrationPlan,
  MigrationState,
  MigrationStatus,
  SchemaDiff,
)


class TestMigrationState:
  """Test suite for MigrationState enum."""

  def test_migration_state_values(self) -> None:
    """Test MigrationState enum values."""
    assert MigrationState.PENDING.value == 'pending'
    assert MigrationState.APPLIED.value == 'applied'
    assert MigrationState.FAILED.value == 'failed'


class TestMigrationDirection:
  """Test suite for MigrationDirection enum."""

  def test_migration_direction_values(self) -> None:
    """Test MigrationDirection enum values."""
    assert MigrationDirection.UP.value == 'up'
    assert MigrationDirection.DOWN.value == 'down'


class TestDiffOperation:
  """Test suite for DiffOperation enum."""

  def test_diff_operation_values(self) -> None:
    """Test DiffOperation enum values."""
    assert DiffOperation.ADD_TABLE.value == 'add_table'
    assert DiffOperation.DROP_TABLE.value == 'drop_table'
    assert DiffOperation.ADD_FIELD.value == 'add_field'
    assert DiffOperation.DROP_FIELD.value == 'drop_field'
    assert DiffOperation.MODIFY_FIELD.value == 'modify_field'
    assert DiffOperation.ADD_INDEX.value == 'add_index'
    assert DiffOperation.DROP_INDEX.value == 'drop_index'
    assert DiffOperation.ADD_EVENT.value == 'add_event'
    assert DiffOperation.DROP_EVENT.value == 'drop_event'
    assert DiffOperation.MODIFY_PERMISSIONS.value == 'modify_permissions'


class TestMigration:
  """Test suite for Migration model."""

  def test_migration_creation(self, tmp_path: Path) -> None:
    """Test Migration model creation."""
    path = tmp_path / 'test_migration.py'
    path.write_text('# test')

    def up() -> list[str]:
      return ['CREATE TABLE user;']

    def down() -> list[str]:
      return ['DROP TABLE user;']

    migration = Migration(
      version='20260102_120000',
      description='Create user table',
      path=path,
      up=up,
      down=down,
      checksum='abc123',
    )

    assert migration.version == '20260102_120000'
    assert migration.description == 'Create user table'
    assert migration.path == path
    assert migration.up() == ['CREATE TABLE user;']
    assert migration.down() == ['DROP TABLE user;']
    assert migration.checksum == 'abc123'
    assert migration.depends_on == []

  def test_migration_with_dependencies(self, tmp_path: Path) -> None:
    """Test Migration with dependencies."""
    path = tmp_path / 'test_migration.py'
    path.write_text('# test')

    migration = Migration(
      version='20260102_120000',
      description='Test',
      path=path,
      up=lambda: [],
      down=lambda: [],
      depends_on=['20260101_120000'],
    )

    assert migration.depends_on == ['20260101_120000']

  def test_migration_immutability(self, tmp_path: Path) -> None:
    """Test that Migration is immutable."""
    path = tmp_path / 'test_migration.py'
    path.write_text('# test')

    migration = Migration(
      version='20260102_120000',
      description='Test',
      path=path,
      up=lambda: [],
      down=lambda: [],
    )

    with pytest.raises(Exception):
      migration.version = '20260103_120000'  # type: ignore[misc]


class TestMigrationHistory:
  """Test suite for MigrationHistory model."""

  def test_migration_history_creation(self) -> None:
    """Test MigrationHistory creation."""
    now = datetime.now()
    history = MigrationHistory(
      version='20260102_120000',
      description='Create user table',
      applied_at=now,
      checksum='abc123',
      execution_time_ms=150,
    )

    assert history.version == '20260102_120000'
    assert history.description == 'Create user table'
    assert history.applied_at == now
    assert history.checksum == 'abc123'
    assert history.execution_time_ms == 150

  def test_migration_history_immutability(self) -> None:
    """Test that MigrationHistory is immutable."""
    history = MigrationHistory(
      version='20260102_120000',
      description='Test',
      applied_at=datetime.now(),
      checksum='abc123',
    )

    with pytest.raises(Exception):
      history.version = '20260103_120000'  # type: ignore[misc]


class TestMigrationPlan:
  """Test suite for MigrationPlan model."""

  def test_migration_plan_creation(self, tmp_path: Path) -> None:
    """Test MigrationPlan creation."""
    path = tmp_path / 'test_migration.py'
    path.write_text('# test')

    migration = Migration(
      version='20260102_120000',
      description='Test',
      path=path,
      up=lambda: [],
      down=lambda: [],
    )

    plan = MigrationPlan(
      migrations=[migration],
      direction=MigrationDirection.UP,
    )

    assert len(plan.migrations) == 1
    assert plan.direction == MigrationDirection.UP
    assert plan.count == 1
    assert plan.is_empty() is False

  def test_migration_plan_empty(self) -> None:
    """Test empty MigrationPlan."""
    plan = MigrationPlan(
      migrations=[],
      direction=MigrationDirection.UP,
    )

    assert plan.count == 0
    assert plan.is_empty() is True

  def test_migration_plan_immutability(self, tmp_path: Path) -> None:
    """Test that MigrationPlan is immutable."""
    path = tmp_path / 'test_migration.py'
    path.write_text('# test')

    migration = Migration(
      version='20260102_120000',
      description='Test',
      path=path,
      up=lambda: [],
      down=lambda: [],
    )

    plan = MigrationPlan(
      migrations=[migration],
      direction=MigrationDirection.UP,
    )

    with pytest.raises(Exception):
      plan.direction = MigrationDirection.DOWN  # type: ignore[misc]


class TestMigrationMetadata:
  """Test suite for MigrationMetadata model."""

  def test_metadata_creation(self) -> None:
    """Test MigrationMetadata creation."""
    metadata = MigrationMetadata(
      version='20260102_120000',
      description='Create user table',
      author='dev',
      depends_on=['20260101_120000'],
    )

    assert metadata.version == '20260102_120000'
    assert metadata.description == 'Create user table'
    assert metadata.author == 'dev'
    assert metadata.depends_on == ['20260101_120000']

  def test_metadata_defaults(self) -> None:
    """Test MigrationMetadata with default values."""
    metadata = MigrationMetadata(
      version='20260102_120000',
      description='Test',
    )

    assert metadata.author == 'ethereal'
    assert metadata.depends_on == []


class TestMigrationStatus:
  """Test suite for MigrationStatus model."""

  def test_migration_status_pending(self, tmp_path: Path) -> None:
    """Test MigrationStatus for pending migration."""
    path = tmp_path / 'test_migration.py'
    path.write_text('# test')

    migration = Migration(
      version='20260102_120000',
      description='Test',
      path=path,
      up=lambda: [],
      down=lambda: [],
    )

    status = MigrationStatus(
      migration=migration,
      state=MigrationState.PENDING,
    )

    assert status.state == MigrationState.PENDING
    assert status.applied_at is None
    assert status.error is None

  def test_migration_status_applied(self, tmp_path: Path) -> None:
    """Test MigrationStatus for applied migration."""
    path = tmp_path / 'test_migration.py'
    path.write_text('# test')

    migration = Migration(
      version='20260102_120000',
      description='Test',
      path=path,
      up=lambda: [],
      down=lambda: [],
    )

    now = datetime.now()
    status = MigrationStatus(
      migration=migration,
      state=MigrationState.APPLIED,
      applied_at=now,
    )

    assert status.state == MigrationState.APPLIED
    assert status.applied_at == now

  def test_migration_status_failed(self, tmp_path: Path) -> None:
    """Test MigrationStatus for failed migration."""
    path = tmp_path / 'test_migration.py'
    path.write_text('# test')

    migration = Migration(
      version='20260102_120000',
      description='Test',
      path=path,
      up=lambda: [],
      down=lambda: [],
    )

    status = MigrationStatus(
      migration=migration,
      state=MigrationState.FAILED,
      error='Syntax error',
    )

    assert status.state == MigrationState.FAILED
    assert status.error == 'Syntax error'


class TestSchemaDiff:
  """Test suite for SchemaDiff model."""

  def test_schema_diff_add_table(self) -> None:
    """Test SchemaDiff for ADD_TABLE operation."""
    diff = SchemaDiff(
      operation=DiffOperation.ADD_TABLE,
      table='user',
      description='Add user table',
      forward_sql='DEFINE TABLE user SCHEMAFULL;',
      backward_sql='REMOVE TABLE user;',
    )

    assert diff.operation == DiffOperation.ADD_TABLE
    assert diff.table == 'user'
    assert diff.field is None
    assert diff.description == 'Add user table'

  def test_schema_diff_add_field(self) -> None:
    """Test SchemaDiff for ADD_FIELD operation."""
    diff = SchemaDiff(
      operation=DiffOperation.ADD_FIELD,
      table='user',
      field='email',
      description='Add email field',
      forward_sql='DEFINE FIELD email ON TABLE user TYPE string;',
      backward_sql='REMOVE FIELD email ON TABLE user;',
    )

    assert diff.operation == DiffOperation.ADD_FIELD
    assert diff.table == 'user'
    assert diff.field == 'email'

  def test_schema_diff_with_details(self) -> None:
    """Test SchemaDiff with additional details."""
    diff = SchemaDiff(
      operation=DiffOperation.MODIFY_FIELD,
      table='user',
      field='age',
      description='Modify age field type',
      forward_sql='DEFINE FIELD age ON TABLE user TYPE int;',
      backward_sql='DEFINE FIELD age ON TABLE user TYPE string;',
      details={'old_type': 'string', 'new_type': 'int'},
    )

    assert diff.details['old_type'] == 'string'
    assert diff.details['new_type'] == 'int'


class TestValidateMigrationName:
  """Test suite for validate_migration_name function."""

  def test_valid_migration_name(self) -> None:
    """Test validation of correct migration name."""
    assert validate_migration_name('20260102_120000_create_user_table.py') is True

  def test_valid_migration_name_short_description(self) -> None:
    """Test validation with short description."""
    assert validate_migration_name('20260102_120000_test.py') is True

  def test_valid_migration_name_long_description(self) -> None:
    """Test validation with long description."""
    assert validate_migration_name('20260102_120000_create_user_and_post_tables.py') is True

  def test_invalid_no_extension(self) -> None:
    """Test that files without .py extension are invalid."""
    assert validate_migration_name('20260102_120000_test') is False

  def test_invalid_wrong_extension(self) -> None:
    """Test that files with wrong extension are invalid."""
    assert validate_migration_name('20260102_120000_test.txt') is False

  def test_invalid_date_format(self) -> None:
    """Test that invalid date format is rejected."""
    assert validate_migration_name('2026_120000_test.py') is False

  def test_invalid_time_format(self) -> None:
    """Test that invalid time format is rejected."""
    assert validate_migration_name('20260102_1200_test.py') is False

  def test_invalid_missing_description(self) -> None:
    """Test that missing description is rejected."""
    assert validate_migration_name('20260102_120000.py') is False

  def test_invalid_missing_parts(self) -> None:
    """Test that missing parts are rejected."""
    assert validate_migration_name('20260102.py') is False


class TestGetVersionFromFilename:
  """Test suite for get_version_from_filename function."""

  def test_get_version_valid(self) -> None:
    """Test extracting version from valid filename."""
    version = get_version_from_filename('20260102_120000_create_user.py')
    assert version == '20260102_120000'

  def test_get_version_invalid(self) -> None:
    """Test extracting version from invalid filename."""
    version = get_version_from_filename('invalid.py')
    assert version is None


class TestGetDescriptionFromFilename:
  """Test suite for get_description_from_filename function."""

  def test_get_description_single_word(self) -> None:
    """Test extracting description with single word."""
    description = get_description_from_filename('20260102_120000_test.py')
    assert description == 'test'

  def test_get_description_multiple_words(self) -> None:
    """Test extracting description with multiple words."""
    description = get_description_from_filename('20260102_120000_create_user_table.py')
    assert description == 'create_user_table'

  def test_get_description_invalid(self) -> None:
    """Test extracting description from invalid filename."""
    description = get_description_from_filename('invalid.py')
    assert description is None


class TestCalculateChecksum:
  """Test suite for _calculate_checksum function."""

  def test_calculate_checksum(self, tmp_path: Path) -> None:
    """Test calculating checksum of a file."""
    test_file = tmp_path / 'test.txt'
    test_file.write_text('Hello, world!')

    checksum = _calculate_checksum(test_file)

    assert isinstance(checksum, str)
    assert len(checksum) == 64  # SHA256 hex length

  def test_calculate_checksum_consistent(self, tmp_path: Path) -> None:
    """Test that checksum is consistent for same content."""
    test_file = tmp_path / 'test.txt'
    test_file.write_text('Test content')

    checksum1 = _calculate_checksum(test_file)
    checksum2 = _calculate_checksum(test_file)

    assert checksum1 == checksum2

  def test_calculate_checksum_different_content(self, tmp_path: Path) -> None:
    """Test that different content produces different checksums."""
    file1 = tmp_path / 'file1.txt'
    file2 = tmp_path / 'file2.txt'

    file1.write_text('Content 1')
    file2.write_text('Content 2')

    checksum1 = _calculate_checksum(file1)
    checksum2 = _calculate_checksum(file2)

    assert checksum1 != checksum2


class TestLoadMigration:
  """Test suite for load_migration function."""

  def test_load_migration_success(self, temp_migration_dir: Path) -> None:
    """Test successfully loading a migration file."""
    # Create a valid migration file
    migration_file = temp_migration_dir / '20260102_120000_test.py'
    migration_content = """
metadata = {
  'version': '20260102_120000',
  'description': 'Test migration',
  'author': 'test',
}

def up():
  return ['CREATE TABLE test;']

def down():
  return ['DROP TABLE test;']
"""
    migration_file.write_text(migration_content)

    migration = load_migration(migration_file)

    assert migration.version == '20260102_120000'
    assert migration.description == 'Test migration'
    assert migration.up() == ['CREATE TABLE test;']
    assert migration.down() == ['DROP TABLE test;']
    assert migration.checksum is not None

  def test_load_migration_missing_up(self, temp_migration_dir: Path) -> None:
    """Test loading migration without up function."""
    migration_file = temp_migration_dir / '20260102_120000_test.py'
    migration_content = """
metadata = {
  'version': '20260102_120000',
  'description': 'Test',
}

def down():
  return []
"""
    migration_file.write_text(migration_content)

    with pytest.raises(MigrationLoadError) as exc_info:
      load_migration(migration_file)

    assert 'missing up()' in str(exc_info.value)

  def test_load_migration_missing_down(self, temp_migration_dir: Path) -> None:
    """Test loading migration without down function."""
    migration_file = temp_migration_dir / '20260102_120000_test.py'
    migration_content = """
metadata = {
  'version': '20260102_120000',
  'description': 'Test',
}

def up():
  return []
"""
    migration_file.write_text(migration_content)

    with pytest.raises(MigrationLoadError) as exc_info:
      load_migration(migration_file)

    assert 'missing down()' in str(exc_info.value)

  def test_load_migration_missing_metadata(self, temp_migration_dir: Path) -> None:
    """Test loading migration without metadata."""
    migration_file = temp_migration_dir / '20260102_120000_test.py'
    migration_content = """
def up():
  return []

def down():
  return []
"""
    migration_file.write_text(migration_content)

    with pytest.raises(MigrationLoadError) as exc_info:
      load_migration(migration_file)

    assert 'missing metadata' in str(exc_info.value)

  def test_load_migration_file_not_found(self, temp_migration_dir: Path) -> None:
    """Test loading non-existent migration file."""
    missing_file = temp_migration_dir / 'nonexistent.py'

    with pytest.raises(MigrationLoadError) as exc_info:
      load_migration(missing_file)

    assert 'not found' in str(exc_info.value)

  def test_load_migration_invalid_metadata(self, temp_migration_dir: Path) -> None:
    """Test loading migration with invalid metadata."""
    migration_file = temp_migration_dir / '20260102_120000_test.py'
    migration_content = """
metadata = {
  'version': '20260102_120000',
  # Missing required 'description' field
}

def up():
  return []

def down():
  return []
"""
    migration_file.write_text(migration_content)

    with pytest.raises(MigrationLoadError) as exc_info:
      load_migration(migration_file)

    assert 'Invalid metadata' in str(exc_info.value)


class TestDiscoverMigrations:
  """Test suite for discover_migrations function."""

  def test_discover_migrations_empty_directory(self, temp_migration_dir: Path) -> None:
    """Test discovering migrations in empty directory."""
    migrations = discover_migrations(temp_migration_dir)

    assert len(migrations) == 0

  def test_discover_migrations_single_file(self, temp_migration_dir: Path) -> None:
    """Test discovering single migration file."""
    migration_file = temp_migration_dir / '20260102_120000_test.py'
    migration_content = """
metadata = {
  'version': '20260102_120000',
  'description': 'Test',
}

def up():
  return []

def down():
  return []
"""
    migration_file.write_text(migration_content)

    migrations = discover_migrations(temp_migration_dir)

    assert len(migrations) == 1
    assert migrations[0].version == '20260102_120000'

  def test_discover_migrations_multiple_files(self, temp_migration_dir: Path) -> None:
    """Test discovering multiple migration files."""
    # Create migrations in non-sorted order
    for i, version in enumerate(['20260103_120000', '20260101_120000', '20260102_120000']):
      migration_file = temp_migration_dir / f'{version}_test{i}.py'
      migration_content = f"""
metadata = {{
  'version': '{version}',
  'description': 'Test {i}',
}}

def up():
  return []

def down():
  return []
"""
      migration_file.write_text(migration_content)

    migrations = discover_migrations(temp_migration_dir)

    assert len(migrations) == 3
    # Should be sorted by version
    assert migrations[0].version == '20260101_120000'
    assert migrations[1].version == '20260102_120000'
    assert migrations[2].version == '20260103_120000'

  def test_discover_migrations_ignores_init(self, temp_migration_dir: Path) -> None:
    """Test that __init__.py is ignored."""
    (temp_migration_dir / '__init__.py').write_text('# init')

    migration_file = temp_migration_dir / '20260102_120000_test.py'
    migration_content = """
metadata = {
  'version': '20260102_120000',
  'description': 'Test',
}

def up():
  return []

def down():
  return []
"""
    migration_file.write_text(migration_content)

    migrations = discover_migrations(temp_migration_dir)

    assert len(migrations) == 1

  def test_discover_migrations_nonexistent_directory(self, tmp_path: Path) -> None:
    """Test discovering migrations in non-existent directory."""
    nonexistent = tmp_path / 'nonexistent'

    migrations = discover_migrations(nonexistent)

    assert len(migrations) == 0

  def test_discover_migrations_not_a_directory(self, tmp_path: Path) -> None:
    """Test discovering migrations when path is not a directory."""
    file_path = tmp_path / 'file.txt'
    file_path.write_text('test')

    with pytest.raises(MigrationDiscoveryError) as exc_info:
      discover_migrations(file_path)

    assert 'not a directory' in str(exc_info.value)
