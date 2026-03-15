"""Tests for the migration executor module."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from surql.connection.client import QueryError
from surql.migration.executor import (
  MigrationExecutionError,
  create_migration_plan,
  execute_migration,
  execute_migration_plan,
  get_applied_migrations_ordered,
  get_migration_status,
  get_pending_migrations,
  migrate_down,
  migrate_up,
  validate_migrations,
)
from surql.migration.models import Migration, MigrationDirection, MigrationPlan, MigrationState


class TestExecuteMigration:
  """Test suite for execute_migration function."""

  @pytest.mark.anyio
  async def test_execute_migration_up_success(self, mock_db_client, tmp_path: Path):
    """Test successful execution of migration in UP direction."""
    migration = Migration(
      version='20260101_120000',
      description='Create test table',
      path=tmp_path / 'test.py',
      up=lambda: ['CREATE TABLE test;', 'CREATE TABLE test2;'],
      down=lambda: ['DROP TABLE test2;', 'DROP TABLE test;'],
      checksum='abc123',
    )

    with patch('surql.migration.executor.record_migration', new=AsyncMock()) as mock_record:
      result = await execute_migration(mock_db_client, migration, MigrationDirection.UP)

      # Verify execution time returned
      assert isinstance(result, int)
      assert result >= 0

      # Verify SQL statements were executed (BEGIN + 2 statements + COMMIT = 4)
      assert mock_db_client._client.query.call_count == 4

      # Verify migration was recorded
      mock_record.assert_called_once()
      call_args = mock_record.call_args
      assert call_args[0][1] == '20260101_120000'
      assert call_args[0][2] == 'Create test table'
      assert call_args[0][3] == 'abc123'

  @pytest.mark.anyio
  async def test_execute_migration_down_success(self, mock_db_client, tmp_path: Path):
    """Test successful execution of migration in DOWN direction."""
    migration = Migration(
      version='20260101_120000',
      description='Create test table',
      path=tmp_path / 'test.py',
      up=lambda: ['CREATE TABLE test;'],
      down=lambda: ['DROP TABLE test;'],
      checksum='abc123',
    )

    with patch('surql.migration.executor.remove_migration_record', new=AsyncMock()) as mock_remove:
      result = await execute_migration(mock_db_client, migration, MigrationDirection.DOWN)

      # Verify execution time returned
      assert isinstance(result, int)
      assert result >= 0

      # Verify migration record was removed
      mock_remove.assert_called_once_with(mock_db_client, '20260101_120000')

  @pytest.mark.anyio
  async def test_execute_migration_statement_failure(self, mock_db_client, tmp_path: Path):
    """Test migration execution with statement failure."""
    migration = Migration(
      version='20260101_120000',
      description='Test',
      path=tmp_path / 'test.py',
      up=lambda: ['CREATE TABLE test;', 'INVALID SQL;'],
      down=lambda: ['DROP TABLE test;'],
    )

    # Mock query to succeed for BEGIN TRANSACTION then fail on statements
    call_count = 0

    async def side_effect(query: str, _params: dict | None = None) -> list:
      nonlocal call_count
      call_count += 1
      if 'BEGIN' in query or 'CANCEL' in query:
        return [{'result': [], 'time': '0ns'}]
      raise QueryError('Syntax error')

    mock_db_client._client.query = AsyncMock(side_effect=side_effect)

    with pytest.raises(MigrationExecutionError) as exc_info:
      await execute_migration(mock_db_client, migration, MigrationDirection.UP)

    assert 'Failed to execute statement' in str(exc_info.value)
    assert '20260101_120000' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_execute_migration_empty_statements(self, mock_db_client, tmp_path: Path):
    """Test migration with no statements to execute."""
    migration = Migration(
      version='20260101_120000',
      description='Empty migration',
      path=tmp_path / 'test.py',
      up=lambda: [],
      down=lambda: [],
    )

    with patch('surql.migration.executor.record_migration', new=AsyncMock()) as mock_record:
      result = await execute_migration(mock_db_client, migration, MigrationDirection.UP)

      assert isinstance(result, int)
      mock_record.assert_called_once()


class TestMigrateUp:
  """Test suite for migrate_up function."""

  @pytest.mark.anyio
  async def test_migrate_up_all_pending(self, mock_db_client, tmp_path: Path):
    """Test applying all pending migrations."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: ['CREATE TABLE test1;'],
        down=lambda: ['DROP TABLE test1;'],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: ['CREATE TABLE test2;'],
        down=lambda: ['DROP TABLE test2;'],
      ),
    ]

    with (
      patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())),
      patch('surql.migration.executor.record_migration', new=AsyncMock()),
    ):
      applied = await migrate_up(mock_db_client, migrations)

      assert len(applied) == 2
      assert applied[0].version == '20260101_120000'
      assert applied[1].version == '20260102_120000'

  @pytest.mark.anyio
  async def test_migrate_up_with_steps(self, mock_db_client, tmp_path: Path):
    """Test applying migrations with step limit."""
    migrations = [
      Migration(
        version=f'2026010{i}_120000',
        description=f'Test {i}',
        path=tmp_path / f'test{i}.py',
        up=lambda i=i: [f'CREATE TABLE test{i};'],
        down=lambda i=i: [f'DROP TABLE test{i};'],
      )
      for i in range(1, 4)
    ]

    with (
      patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())),
      patch('surql.migration.executor.record_migration', new=AsyncMock()),
    ):
      applied = await migrate_up(mock_db_client, migrations, steps=2)

      assert len(applied) == 2
      assert applied[0].version == '20260101_120000'
      assert applied[1].version == '20260102_120000'

  @pytest.mark.anyio
  async def test_migrate_up_no_pending(self, mock_db_client, tmp_path: Path):
    """Test migrate_up when no migrations are pending."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test',
        path=tmp_path / 'test.py',
        up=lambda: ['CREATE TABLE test;'],
        down=lambda: ['DROP TABLE test;'],
      )
    ]

    # Mock all migrations as already applied
    with patch(
      'surql.migration.executor.get_applied_versions',
      new=AsyncMock(return_value={'20260101_120000'}),
    ):
      applied = await migrate_up(mock_db_client, migrations)

      assert len(applied) == 0

  @pytest.mark.anyio
  async def test_migrate_up_execution_failure(self, mock_db_client, tmp_path: Path):
    """Test migrate_up when migration execution fails."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test',
        path=tmp_path / 'test.py',
        up=lambda: ['INVALID SQL;'],
        down=lambda: ['DROP TABLE test;'],
      )
    ]

    mock_db_client._client.query = AsyncMock(side_effect=QueryError('Syntax error'))

    with patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())):
      with pytest.raises(MigrationExecutionError) as exc_info:
        await migrate_up(mock_db_client, migrations)

      assert 'Failed to migrate up' in str(exc_info.value)


class TestMigrateDown:
  """Test suite for migrate_down function."""

  @pytest.mark.anyio
  async def test_migrate_down_single_step(self, mock_db_client, tmp_path: Path):
    """Test rolling back one migration."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: ['CREATE TABLE test1;'],
        down=lambda: ['DROP TABLE test1;'],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: ['CREATE TABLE test2;'],
        down=lambda: ['DROP TABLE test2;'],
      ),
    ]

    # Mock both migrations as applied
    with (
      patch(
        'surql.migration.executor.get_applied_versions',
        new=AsyncMock(return_value={'20260101_120000', '20260102_120000'}),
      ),
      patch('surql.migration.executor.remove_migration_record', new=AsyncMock()),
    ):
      rolled_back = await migrate_down(mock_db_client, migrations, steps=1)

      assert len(rolled_back) == 1
      assert rolled_back[0].version == '20260102_120000'

  @pytest.mark.anyio
  async def test_migrate_down_multiple_steps(self, mock_db_client, tmp_path: Path):
    """Test rolling back multiple migrations."""
    migrations = [
      Migration(
        version=f'2026010{i}_120000',
        description=f'Test {i}',
        path=tmp_path / f'test{i}.py',
        up=lambda i=i: [f'CREATE TABLE test{i};'],
        down=lambda i=i: [f'DROP TABLE test{i};'],
      )
      for i in range(1, 4)
    ]

    # Mock all migrations as applied
    applied_versions = {m.version for m in migrations}
    with (
      patch(
        'surql.migration.executor.get_applied_versions',
        new=AsyncMock(return_value=applied_versions),
      ),
      patch('surql.migration.executor.remove_migration_record', new=AsyncMock()),
    ):
      rolled_back = await migrate_down(mock_db_client, migrations, steps=2)

      assert len(rolled_back) == 2
      # Should rollback in reverse order
      assert rolled_back[0].version == '20260103_120000'
      assert rolled_back[1].version == '20260102_120000'

  @pytest.mark.anyio
  async def test_migrate_down_no_applied(self, mock_db_client, tmp_path: Path):
    """Test migrate_down when no migrations are applied."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test',
        path=tmp_path / 'test.py',
        up=lambda: ['CREATE TABLE test;'],
        down=lambda: ['DROP TABLE test;'],
      )
    ]

    with patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())):
      rolled_back = await migrate_down(mock_db_client, migrations, steps=1)

      assert len(rolled_back) == 0

  @pytest.mark.anyio
  async def test_migrate_down_execution_failure(self, mock_db_client, tmp_path: Path):
    """Test migrate_down when rollback execution fails."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test',
        path=tmp_path / 'test.py',
        up=lambda: ['CREATE TABLE test;'],
        down=lambda: ['INVALID SQL;'],
      )
    ]

    mock_db_client._client.query = AsyncMock(side_effect=QueryError('Syntax error'))

    with patch(
      'surql.migration.executor.get_applied_versions',
      new=AsyncMock(return_value={'20260101_120000'}),
    ):
      with pytest.raises(MigrationExecutionError) as exc_info:
        await migrate_down(mock_db_client, migrations, steps=1)

      assert 'Failed to migrate down' in str(exc_info.value)


class TestGetPendingMigrations:
  """Test suite for get_pending_migrations function."""

  @pytest.mark.anyio
  async def test_get_pending_migrations_all_pending(self, mock_db_client, tmp_path: Path):
    """Test getting pending migrations when none are applied."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    with patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())):
      pending = await get_pending_migrations(mock_db_client, migrations)

      assert len(pending) == 2
      assert pending[0].version == '20260101_120000'
      assert pending[1].version == '20260102_120000'

  @pytest.mark.anyio
  async def test_get_pending_migrations_partial(self, mock_db_client, tmp_path: Path):
    """Test getting pending migrations with some applied."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    with patch(
      'surql.migration.executor.get_applied_versions',
      new=AsyncMock(return_value={'20260101_120000'}),
    ):
      pending = await get_pending_migrations(mock_db_client, migrations)

      assert len(pending) == 1
      assert pending[0].version == '20260102_120000'

  @pytest.mark.anyio
  async def test_get_pending_migrations_sorted(self, mock_db_client, tmp_path: Path):
    """Test that pending migrations are sorted by version."""
    migrations = [
      Migration(
        version='20260103_120000',
        description='Test 3',
        path=tmp_path / 'test3.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    with patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())):
      pending = await get_pending_migrations(mock_db_client, migrations)

      assert len(pending) == 3
      assert pending[0].version == '20260101_120000'
      assert pending[1].version == '20260102_120000'
      assert pending[2].version == '20260103_120000'


class TestGetAppliedMigrationsOrdered:
  """Test suite for get_applied_migrations_ordered function."""

  @pytest.mark.anyio
  async def test_get_applied_migrations_ordered(self, mock_db_client, tmp_path: Path):
    """Test getting applied migrations in order."""
    migrations = [
      Migration(
        version='20260103_120000',
        description='Test 3',
        path=tmp_path / 'test3.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    with patch(
      'surql.migration.executor.get_applied_versions',
      new=AsyncMock(return_value={'20260101_120000', '20260103_120000'}),
    ):
      applied = await get_applied_migrations_ordered(mock_db_client, migrations)

      assert len(applied) == 2
      # Should be sorted by version
      assert applied[0].version == '20260101_120000'
      assert applied[1].version == '20260103_120000'

  @pytest.mark.anyio
  async def test_get_applied_migrations_none(self, mock_db_client, tmp_path: Path):
    """Test getting applied migrations when none exist."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      )
    ]

    with patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())):
      applied = await get_applied_migrations_ordered(mock_db_client, migrations)

      assert len(applied) == 0


class TestGetMigrationStatus:
  """Test suite for get_migration_status function."""

  @pytest.mark.anyio
  async def test_get_migration_status_mixed(self, mock_db_client, tmp_path: Path):
    """Test getting status of migrations with mixed states."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Applied migration',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Pending migration',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    with patch(
      'surql.migration.executor.get_applied_versions',
      new=AsyncMock(return_value={'20260101_120000'}),
    ):
      statuses = await get_migration_status(mock_db_client, migrations)

      assert len(statuses) == 2
      assert statuses[0].migration.version == '20260101_120000'
      assert statuses[0].state == MigrationState.APPLIED
      assert statuses[1].migration.version == '20260102_120000'
      assert statuses[1].state == MigrationState.PENDING

  @pytest.mark.anyio
  async def test_get_migration_status_all_pending(self, mock_db_client, tmp_path: Path):
    """Test getting status when all migrations are pending."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test',
        path=tmp_path / 'test.py',
        up=lambda: [],
        down=lambda: [],
      )
    ]

    with patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())):
      statuses = await get_migration_status(mock_db_client, migrations)

      assert len(statuses) == 1
      assert statuses[0].state == MigrationState.PENDING


class TestExecuteMigrationPlan:
  """Test suite for execute_migration_plan function."""

  @pytest.mark.anyio
  async def test_execute_migration_plan_up(self, mock_db_client, tmp_path: Path):
    """Test executing migration plan in UP direction."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: ['CREATE TABLE test1;'],
        down=lambda: ['DROP TABLE test1;'],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: ['CREATE TABLE test2;'],
        down=lambda: ['DROP TABLE test2;'],
      ),
    ]

    plan = MigrationPlan(migrations=migrations, direction=MigrationDirection.UP)

    with patch('surql.migration.executor.record_migration', new=AsyncMock()):
      await execute_migration_plan(mock_db_client, plan)

      # Verify both migrations were executed
      # 2 migrations x 3 calls each (BEGIN + statement + COMMIT)
      assert mock_db_client._client.query.call_count == 6

  @pytest.mark.anyio
  async def test_execute_migration_plan_down(self, mock_db_client, tmp_path: Path):
    """Test executing migration plan in DOWN direction."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: ['CREATE TABLE test1;'],
        down=lambda: ['DROP TABLE test1;'],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: ['CREATE TABLE test2;'],
        down=lambda: ['DROP TABLE test2;'],
      ),
    ]

    plan = MigrationPlan(migrations=migrations, direction=MigrationDirection.DOWN)

    with patch('surql.migration.executor.remove_migration_record', new=AsyncMock()):
      await execute_migration_plan(mock_db_client, plan)

      # 2 migrations x 3 calls each (BEGIN + statement + COMMIT)
      assert mock_db_client._client.query.call_count == 6

  @pytest.mark.anyio
  async def test_execute_migration_plan_empty(self, mock_db_client):
    """Test executing empty migration plan."""
    plan = MigrationPlan(migrations=[], direction=MigrationDirection.UP)

    await execute_migration_plan(mock_db_client, plan)

    # Should complete without executing anything
    assert mock_db_client._client.query.call_count == 0

  @pytest.mark.anyio
  async def test_execute_migration_plan_failure(self, mock_db_client, tmp_path: Path):
    """Test executing migration plan with failure."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test',
        path=tmp_path / 'test.py',
        up=lambda: ['INVALID SQL;'],
        down=lambda: ['DROP TABLE test;'],
      )
    ]

    plan = MigrationPlan(migrations=migrations, direction=MigrationDirection.UP)

    mock_db_client._client.query = AsyncMock(side_effect=QueryError('Syntax error'))

    with pytest.raises(MigrationExecutionError) as exc_info:
      await execute_migration_plan(mock_db_client, plan)

    assert 'Failed to execute migration plan' in str(exc_info.value)


class TestValidateMigrations:
  """Test suite for validate_migrations function."""

  @pytest.mark.anyio
  async def test_validate_migrations_no_errors(self, tmp_path: Path):
    """Test validating migrations with no errors."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    errors = await validate_migrations(migrations)

    assert len(errors) == 0

  @pytest.mark.anyio
  async def test_validate_migrations_duplicate_versions(self, tmp_path: Path):
    """Test validating migrations with duplicate versions."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260101_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    errors = await validate_migrations(migrations)

    assert len(errors) == 1
    assert 'Duplicate migration versions' in errors[0]
    assert '20260101_120000' in errors[0]

  @pytest.mark.anyio
  async def test_validate_migrations_missing_dependency(self, tmp_path: Path):
    """Test validating migrations with missing dependency."""
    migrations = [
      Migration(
        version='20260102_120000',
        description='Test',
        path=tmp_path / 'test.py',
        up=lambda: [],
        down=lambda: [],
        depends_on=['20260101_120000'],  # This dependency doesn't exist
      )
    ]

    errors = await validate_migrations(migrations)

    assert len(errors) == 1
    assert 'depends on missing migration' in errors[0]
    assert '20260102_120000' in errors[0]
    assert '20260101_120000' in errors[0]

  @pytest.mark.anyio
  async def test_validate_migrations_multiple_errors(self, tmp_path: Path):
    """Test validating migrations with multiple errors."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260101_120000',
        description='Test 2 (duplicate)',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Test 3',
        path=tmp_path / 'test3.py',
        up=lambda: [],
        down=lambda: [],
        depends_on=['20260199_120000'],  # Missing dependency
      ),
    ]

    errors = await validate_migrations(migrations)

    assert len(errors) == 2
    assert any('Duplicate' in err for err in errors)
    assert any('missing migration' in err for err in errors)


class TestCreateMigrationPlan:
  """Test suite for create_migration_plan function."""

  @pytest.mark.anyio
  async def test_create_migration_plan_up_all(self, mock_db_client, tmp_path: Path):
    """Test creating migration plan for all pending migrations."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    with patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())):
      plan = await create_migration_plan(
        mock_db_client, migrations, MigrationDirection.UP, steps=None
      )

      assert plan.direction == MigrationDirection.UP
      assert plan.count == 2
      assert not plan.is_empty()

  @pytest.mark.anyio
  async def test_create_migration_plan_up_with_steps(self, mock_db_client, tmp_path: Path):
    """Test creating migration plan with step limit."""
    migrations = [
      Migration(
        version=f'2026010{i}_120000',
        description=f'Test {i}',
        path=tmp_path / f'test{i}.py',
        up=lambda: [],
        down=lambda: [],
      )
      for i in range(1, 4)
    ]

    with patch('surql.migration.executor.get_applied_versions', new=AsyncMock(return_value=set())):
      plan = await create_migration_plan(mock_db_client, migrations, MigrationDirection.UP, steps=2)

      assert plan.count == 2

  @pytest.mark.anyio
  async def test_create_migration_plan_down_all(self, mock_db_client, tmp_path: Path):
    """Test creating rollback plan for all migrations."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test 1',
        path=tmp_path / 'test1.py',
        up=lambda: [],
        down=lambda: [],
      ),
      Migration(
        version='20260102_120000',
        description='Test 2',
        path=tmp_path / 'test2.py',
        up=lambda: [],
        down=lambda: [],
      ),
    ]

    applied_versions = {m.version for m in migrations}
    with patch(
      'surql.migration.executor.get_applied_versions',
      new=AsyncMock(return_value=applied_versions),
    ):
      plan = await create_migration_plan(
        mock_db_client, migrations, MigrationDirection.DOWN, steps=None
      )

      assert plan.direction == MigrationDirection.DOWN
      assert plan.count == 2

  @pytest.mark.anyio
  async def test_create_migration_plan_down_with_steps(self, mock_db_client, tmp_path: Path):
    """Test creating rollback plan with step limit."""
    migrations = [
      Migration(
        version=f'2026010{i}_120000',
        description=f'Test {i}',
        path=tmp_path / f'test{i}.py',
        up=lambda: [],
        down=lambda: [],
      )
      for i in range(1, 4)
    ]

    applied_versions = {m.version for m in migrations}
    with patch(
      'surql.migration.executor.get_applied_versions',
      new=AsyncMock(return_value=applied_versions),
    ):
      plan = await create_migration_plan(
        mock_db_client, migrations, MigrationDirection.DOWN, steps=1
      )

      assert plan.count == 1

  @pytest.mark.anyio
  async def test_create_migration_plan_empty(self, mock_db_client, tmp_path: Path):
    """Test creating plan when no migrations to execute."""
    migrations = [
      Migration(
        version='20260101_120000',
        description='Test',
        path=tmp_path / 'test.py',
        up=lambda: [],
        down=lambda: [],
      )
    ]

    # All migrations already applied
    with patch(
      'surql.migration.executor.get_applied_versions',
      new=AsyncMock(return_value={'20260101_120000'}),
    ):
      plan = await create_migration_plan(
        mock_db_client, migrations, MigrationDirection.UP, steps=None
      )

      assert plan.is_empty()
      assert plan.count == 0
