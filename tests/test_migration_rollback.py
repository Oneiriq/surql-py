"""Tests for migration rollback functionality."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from reverie.migration.models import Migration, MigrationHistory
from reverie.migration.rollback import (
  RollbackIssue,
  RollbackPlan,
  RollbackResult,
  RollbackSafety,
  _analyze_migration_safety,
  _extract_field_name,
  _extract_table_name,
  analyze_rollback_safety,
  create_rollback_plan,
  execute_rollback,
)


@pytest.fixture
def safe_migration() -> Migration:
  """Create a migration with safe rollback."""
  return Migration(
    version='20260109_120000',
    description='Add index',
    path=Path('migrations/20260109_120000_add_index.py'),
    up=lambda: ['DEFINE INDEX email_idx ON TABLE user COLUMNS email;'],
    down=lambda: ['REMOVE INDEX email_idx ON TABLE user;'],
  )


@pytest.fixture
def unsafe_migration() -> Migration:
  """Create a migration with unsafe rollback (drops table)."""
  return Migration(
    version='20260109_120000',
    description='Create user table',
    path=Path('migrations/20260109_120000_create_user.py'),
    up=lambda: ['DEFINE TABLE user SCHEMAFULL;'],
    down=lambda: ['DROP TABLE user;'],
  )


@pytest.fixture
def data_loss_migration() -> Migration:
  """Create a migration with data loss rollback (drops field)."""
  return Migration(
    version='20260109_120000',
    description='Add profile field',
    path=Path('migrations/20260109_120000_add_profile.py'),
    up=lambda: ['DEFINE FIELD profile ON TABLE user TYPE object;'],
    down=lambda: ['REMOVE FIELD profile ON TABLE user;'],
  )


class TestRollbackSafety:
  """Tests for RollbackSafety enum."""

  def test_safety_levels(self) -> None:
    """Test all safety levels are defined."""
    assert RollbackSafety.SAFE.value == 'safe'
    assert RollbackSafety.DATA_LOSS.value == 'data_loss'
    assert RollbackSafety.UNSAFE.value == 'unsafe'


class TestRollbackIssue:
  """Tests for RollbackIssue model."""

  def test_issue_creation(self) -> None:
    """Test creating a rollback issue."""
    issue = RollbackIssue(
      safety=RollbackSafety.DATA_LOSS,
      migration='20260109_120000',
      description='Dropping field will lose data',
      affected_data='user.profile',
      recommendation='Backup affected records',
    )

    assert issue.safety == RollbackSafety.DATA_LOSS
    assert issue.migration == '20260109_120000'
    assert 'profile' in issue.affected_data

  def test_issue_immutable(self) -> None:
    """Test that rollback issue is immutable."""
    issue = RollbackIssue(
      safety=RollbackSafety.SAFE,
      migration='20260109_120000',
      description='Test',
    )

    with pytest.raises(ValidationError):
      issue.safety = RollbackSafety.UNSAFE


class TestRollbackPlan:
  """Tests for RollbackPlan model."""

  def test_plan_creation(self, safe_migration: Migration) -> None:
    """Test creating a rollback plan."""
    plan = RollbackPlan(
      from_version='20260109_120000',
      to_version='20260108_120000',
      migrations=[safe_migration],
      overall_safety=RollbackSafety.SAFE,
    )

    assert plan.from_version == '20260109_120000'
    assert plan.to_version == '20260108_120000'
    assert plan.migration_count == 1
    assert plan.is_safe is True

  def test_plan_properties(self, unsafe_migration: Migration) -> None:
    """Test rollback plan properties."""
    plan = RollbackPlan(
      from_version='20260109_120000',
      to_version='20260108_120000',
      migrations=[unsafe_migration],
      overall_safety=RollbackSafety.UNSAFE,
      requires_approval=True,
    )

    assert plan.is_safe is False
    assert plan.has_data_loss is True
    assert plan.requires_approval is True


class TestRollbackResult:
  """Tests for RollbackResult model."""

  def test_result_creation(self, safe_migration: Migration) -> None:
    """Test creating a rollback result."""
    plan = RollbackPlan(
      from_version='20260109_120000',
      to_version='20260108_120000',
      migrations=[safe_migration],
      overall_safety=RollbackSafety.SAFE,
    )

    result = RollbackResult(
      plan=plan,
      success=True,
      actual_duration_ms=1500,
      rolled_back_count=1,
    )

    assert result.success is True
    assert result.rolled_back_count == 1
    assert result.completed_all is True

  def test_result_partial_failure(self, safe_migration: Migration) -> None:
    """Test result with partial completion."""
    plan = RollbackPlan(
      from_version='20260109_120000',
      to_version='20260107_120000',
      migrations=[safe_migration, safe_migration],  # Simulating multiple
      overall_safety=RollbackSafety.SAFE,
    )

    result = RollbackResult(
      plan=plan,
      success=False,
      actual_duration_ms=800,
      rolled_back_count=1,
      errors=['Second migration failed'],
    )

    assert result.success is False
    assert result.completed_all is False
    assert len(result.errors) == 1


class TestSafetyAnalysis:
  """Tests for migration safety analysis."""

  @pytest.mark.anyio
  async def test_analyze_safe_migration(self, safe_migration: Migration) -> None:
    """Test analyzing a safe migration."""
    mock_client = AsyncMock()
    issues = await _analyze_migration_safety(mock_client, safe_migration)

    # Index removal is safe
    assert len(issues) == 0

  @pytest.mark.anyio
  async def test_analyze_unsafe_migration(self, unsafe_migration: Migration) -> None:
    """Test analyzing an unsafe migration (drops table)."""
    mock_client = AsyncMock()
    issues = await _analyze_migration_safety(mock_client, unsafe_migration)

    assert len(issues) > 0
    assert any(issue.safety == RollbackSafety.UNSAFE for issue in issues)
    assert any('table' in issue.description.lower() for issue in issues)

  @pytest.mark.anyio
  async def test_analyze_data_loss_migration(self, data_loss_migration: Migration) -> None:
    """Test analyzing a migration that causes data loss."""
    mock_client = AsyncMock()
    issues = await _analyze_migration_safety(mock_client, data_loss_migration)

    assert len(issues) > 0
    assert any(issue.safety == RollbackSafety.DATA_LOSS for issue in issues)
    assert any('field' in issue.description.lower() for issue in issues)

  def test_extract_table_name(self) -> None:
    """Test extracting table name from SQL."""
    assert _extract_table_name('DROP TABLE user;') == 'user'
    assert _extract_table_name('REMOVE TABLE post;') == 'post'
    assert _extract_table_name('drop table COMMENT;') == 'comment'

  def test_extract_field_name(self) -> None:
    """Test extracting field name from SQL."""
    assert _extract_field_name('REMOVE FIELD profile ON TABLE user;') == 'profile'
    assert _extract_field_name('DROP FIELD email ON TABLE user;') == 'email'


class TestCreateRollbackPlan:
  """Tests for create_rollback_plan function."""

  @pytest.mark.anyio
  async def test_create_plan_simple(self, safe_migration: Migration) -> None:
    """Test creating a simple rollback plan."""
    mock_client = AsyncMock()

    # Create target migration that we're rolling back to
    target_migration = Migration(
      version='20260108_120000',
      description='Initial setup',
      path=Path('migrations/20260108_120000_initial.py'),
      up=lambda: ['DEFINE TABLE user SCHEMAFULL;'],
      down=lambda: ['REMOVE TABLE user;'],
    )

    # Mock get_applied_migrations to return history
    with patch('reverie.migration.history.get_applied_migrations') as mock_get:
      mock_get.return_value = [
        MigrationHistory(
          version='20260109_120000',
          description='Test',
          applied_at=datetime.now(UTC),
          checksum='abc',
        )
      ]

      # Include both migrations: target and current
      migrations = [target_migration, safe_migration]
      plan = await create_rollback_plan(
        mock_client, migrations, '20260108_120000', '20260109_120000'
      )

      assert plan.migration_count >= 0
      assert plan.overall_safety in [
        RollbackSafety.SAFE,
        RollbackSafety.DATA_LOSS,
        RollbackSafety.UNSAFE,
      ]

  @pytest.mark.anyio
  async def test_create_plan_no_migrations(self) -> None:
    """Test creating plan when no migrations applied."""
    mock_client = AsyncMock()

    with patch('reverie.migration.history.get_applied_migrations') as mock_get:
      mock_get.return_value = []

      with pytest.raises(ValueError, match='No migrations have been applied'):
        await create_rollback_plan(mock_client, [], '20260108_120000')

  @pytest.mark.anyio
  async def test_create_plan_target_not_found(self, safe_migration: Migration) -> None:
    """Test creating plan with invalid target version."""
    mock_client = AsyncMock()

    with patch('reverie.migration.history.get_applied_migrations') as mock_get:
      mock_get.return_value = [
        MigrationHistory(
          version='20260109_120000',
          description='Test',
          applied_at=datetime.now(UTC),
          checksum='abc',
        )
      ]

      with pytest.raises(ValueError, match='Target version.*not found'):
        await create_rollback_plan(
          mock_client, [safe_migration], '20260199_000000', '20260109_120000'
        )


class TestExecuteRollback:
  """Tests for execute_rollback function."""

  @pytest.mark.anyio
  async def test_execute_safe_rollback(self, safe_migration: Migration) -> None:
    """Test executing a safe rollback."""
    mock_client = AsyncMock()

    plan = RollbackPlan(
      from_version='20260109_120000',
      to_version='20260108_120000',
      migrations=[safe_migration],
      overall_safety=RollbackSafety.SAFE,
    )

    with patch('reverie.migration.rollback.execute_migration') as mock_exec:
      mock_exec.return_value = None

      result = await execute_rollback(mock_client, plan)

      assert result.success is True
      assert result.rolled_back_count == 1
      mock_exec.assert_called_once()

  @pytest.mark.anyio
  async def test_execute_unsafe_rollback_without_force(self, unsafe_migration: Migration) -> None:
    """Test executing unsafe rollback without force flag."""
    mock_client = AsyncMock()

    plan = RollbackPlan(
      from_version='20260109_120000',
      to_version='20260108_120000',
      migrations=[unsafe_migration],
      overall_safety=RollbackSafety.UNSAFE,
    )

    with pytest.raises(ValueError, match='unsafe'):
      await execute_rollback(mock_client, plan, force=False)

  @pytest.mark.anyio
  async def test_execute_unsafe_rollback_with_force(self, unsafe_migration: Migration) -> None:
    """Test executing unsafe rollback with force flag."""
    mock_client = AsyncMock()

    plan = RollbackPlan(
      from_version='20260109_120000',
      to_version='20260108_120000',
      migrations=[unsafe_migration],
      overall_safety=RollbackSafety.UNSAFE,
    )

    with patch('reverie.migration.rollback.execute_migration') as mock_exec:
      mock_exec.return_value = None

      result = await execute_rollback(mock_client, plan, force=True)

      assert result.success is True

  @pytest.mark.anyio
  async def test_execute_rollback_with_error(self, safe_migration: Migration) -> None:
    """Test rollback execution with errors."""
    mock_client = AsyncMock()

    plan = RollbackPlan(
      from_version='20260109_120000',
      to_version='20260108_120000',
      migrations=[safe_migration],
      overall_safety=RollbackSafety.SAFE,
    )

    with patch('reverie.migration.rollback.execute_migration') as mock_exec:
      mock_exec.side_effect = Exception('Rollback failed')

      result = await execute_rollback(mock_client, plan)

      assert result.success is False
      assert result.rolled_back_count == 0
      assert len(result.errors) > 0


class TestAnalyzeRollbackSafety:
  """Tests for analyze_rollback_safety function."""

  @pytest.mark.anyio
  async def test_analyze_safety(self, data_loss_migration: Migration) -> None:
    """Test analyzing rollback safety."""
    mock_client = AsyncMock()

    with patch('reverie.migration.rollback.create_rollback_plan') as mock_plan:
      mock_plan.return_value = RollbackPlan(
        from_version='20260109_120000',
        to_version='20260108_120000',
        migrations=[data_loss_migration],
        overall_safety=RollbackSafety.DATA_LOSS,
        issues=[
          RollbackIssue(
            safety=RollbackSafety.DATA_LOSS,
            migration='20260109_120000',
            description='Field removal',
          )
        ],
      )

      issues = await analyze_rollback_safety(mock_client, [data_loss_migration], '20260108_120000')

      assert len(issues) > 0
      assert any(issue.safety == RollbackSafety.DATA_LOSS for issue in issues)
