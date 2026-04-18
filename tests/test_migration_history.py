"""Tests for migration history tracking module."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from surql.connection.client import QueryError
from surql.migration.history import (
  MIGRATION_TABLE_NAME,
  MigrationHistoryError,
  _extract_records,
  _parse_datetime,
  create_migration_table,
  ensure_migration_table,
  get_applied_migrations,
  get_applied_versions,
  get_migration_history,
  is_migration_applied,
  record_migration,
  remove_migration_record,
)
from surql.migration.models import MigrationHistory


class TestCreateMigrationTable:
  """Test suite for create_migration_table function."""

  @pytest.mark.anyio
  async def test_create_migration_table_success(self, mock_db_client):
    """Test successful creation of migration history table."""
    mock_db_client.execute = AsyncMock(return_value=[])

    await create_migration_table(mock_db_client)

    # Verify all statements were executed (1 table + 5 fields + 1 index = 7)
    assert mock_db_client.execute.call_count == 7
    calls = [call[0][0] for call in mock_db_client.execute.call_args_list]

    # Verify table definition
    assert any('DEFINE TABLE' in call and MIGRATION_TABLE_NAME in call for call in calls)

    # Verify field definitions (match on trailing field name, not a
    # fixed substring, so `IF NOT EXISTS` can sit in the middle).
    assert any(' version ON TABLE' in call for call in calls)
    assert any(' description ON TABLE' in call for call in calls)
    assert any(' applied_at ON TABLE' in call for call in calls)
    assert any(' checksum ON TABLE' in call for call in calls)
    assert any(' execution_time_ms ON TABLE' in call for call in calls)

    # Verify index definition
    assert any('version_idx ON TABLE' in call and 'UNIQUE' in call for call in calls)

  @pytest.mark.anyio
  async def test_create_migration_table_uses_if_not_exists(self, mock_db_client):
    """Regression (bug #16): every DEFINE must be ``IF NOT EXISTS``.

    Without it, repeated calls to ``create_migration_table`` on
    SurrealDB v3 fail with "table already exists" / "field already
    exists" / "index already exists". ``ensure_migration_table``
    invokes this helper on basically every migration call, so
    idempotency is required.
    """
    mock_db_client.execute = AsyncMock(return_value=[])

    await create_migration_table(mock_db_client)

    statements = [call[0][0] for call in mock_db_client.execute.call_args_list]
    for stmt in statements:
      assert 'IF NOT EXISTS' in stmt, f'DEFINE statement must be idempotent on v3; got {stmt!r}'

  @pytest.mark.anyio
  async def test_create_migration_table_query_error(self, mock_db_client):
    """Test create_migration_table handles QueryError."""
    mock_db_client.execute = AsyncMock(side_effect=QueryError('Database error'))

    with pytest.raises(MigrationHistoryError) as exc_info:
      await create_migration_table(mock_db_client)

    assert 'Failed to create migration history table' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_create_migration_table_unexpected_error(self, mock_db_client):
    """Test create_migration_table handles unexpected errors."""
    mock_db_client.execute = AsyncMock(side_effect=RuntimeError('Unexpected error'))

    with pytest.raises(MigrationHistoryError) as exc_info:
      await create_migration_table(mock_db_client)

    assert 'Unexpected error creating migration table' in str(exc_info.value)


class TestEnsureMigrationTable:
  """Test suite for ensure_migration_table function."""

  @pytest.mark.anyio
  async def test_ensure_migration_table_exists(self, mock_db_client):
    """Test ensure_migration_table when table already exists."""
    mock_db_client.execute = AsyncMock(return_value=[])

    await ensure_migration_table(mock_db_client)

    # Should only query, not create
    mock_db_client.execute.assert_called_once()
    call_args = mock_db_client.execute.call_args[0][0]
    assert 'SELECT' in call_args
    assert MIGRATION_TABLE_NAME in call_args

  @pytest.mark.anyio
  async def test_ensure_migration_table_creates_if_missing(self, mock_db_client):
    """Test ensure_migration_table creates table if it doesn't exist."""
    # First call fails (table doesn't exist), subsequent calls succeed
    mock_db_client.execute = AsyncMock(
      side_effect=[QueryError('Table does not exist')] + [None] * 10
    )

    await ensure_migration_table(mock_db_client)

    # Should query first, then create table (multiple DEFINE statements)
    assert mock_db_client.execute.call_count > 1


class TestRecordMigration:
  """Test suite for record_migration function."""

  @pytest.mark.anyio
  async def test_record_migration_success(self, mock_db_client):
    """Test successfully recording a migration."""
    mock_db_client.execute = AsyncMock(return_value=[])
    mock_db_client.create = AsyncMock(
      return_value={
        'id': f'{MIGRATION_TABLE_NAME}:20240101_000000',
        'version': '20240101_000000',
        'description': 'test migration',
        'applied_at': '2024-01-01T00:00:00Z',
        'checksum': 'abc123',
      }
    )

    await record_migration(
      mock_db_client,
      version='20240101_000000',
      description='test migration',
      checksum='abc123',
    )

    # Verify ensure_migration_table was called (execute)
    assert mock_db_client.execute.called

    # Verify create was called with correct data
    mock_db_client.create.assert_called_once()
    call_args = mock_db_client.create.call_args
    assert call_args[0][0] == MIGRATION_TABLE_NAME
    data = call_args[0][1]
    assert data['version'] == '20240101_000000'
    assert data['description'] == 'test migration'
    assert data['checksum'] == 'abc123'
    assert 'applied_at' in data

  @pytest.mark.anyio
  async def test_record_migration_with_execution_time(self, mock_db_client):
    """Test recording migration with execution time."""
    mock_db_client.execute = AsyncMock(return_value=[])
    mock_db_client.create = AsyncMock(return_value={})

    await record_migration(
      mock_db_client,
      version='20240101_000000',
      description='test',
      checksum='abc123',
      execution_time_ms=150,
    )

    data = mock_db_client.create.call_args[0][1]
    assert data['execution_time_ms'] == 150

  @pytest.mark.anyio
  async def test_record_migration_query_error(self, mock_db_client):
    """Test record_migration handles QueryError."""
    mock_db_client.execute = AsyncMock(return_value=[])
    mock_db_client.create = AsyncMock(side_effect=QueryError('Duplicate version'))

    with pytest.raises(MigrationHistoryError) as exc_info:
      await record_migration(
        mock_db_client, version='20240101_000000', description='test', checksum='abc123'
      )

    assert 'Failed to record migration 20240101_000000' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_record_migration_unexpected_error(self, mock_db_client):
    """Test record_migration handles unexpected errors."""
    mock_db_client.execute = AsyncMock(return_value=[])
    mock_db_client.create = AsyncMock(side_effect=RuntimeError('Unexpected'))

    with pytest.raises(MigrationHistoryError) as exc_info:
      await record_migration(
        mock_db_client, version='20240101_000000', description='test', checksum='abc123'
      )

    assert 'Unexpected error recording migration' in str(exc_info.value)


class TestRemoveMigrationRecord:
  """Test suite for remove_migration_record function."""

  @pytest.mark.anyio
  async def test_remove_migration_record_success(self, mock_db_client):
    """Test successfully removing a migration record."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'id': f'{MIGRATION_TABLE_NAME}:20240101_000000',
              'version': '20240101_000000',
            }
          ]
        }
      ]
    )
    mock_db_client.delete = AsyncMock(return_value=None)

    await remove_migration_record(mock_db_client, version='20240101_000000')

    # Verify query was made
    mock_db_client.execute.assert_called_once()
    call_args = mock_db_client.execute.call_args
    assert 'SELECT' in call_args[0][0]
    # Parameters are in second positional argument
    assert call_args[0][1]['version'] == '20240101_000000'

    # Verify delete was called
    mock_db_client.delete.assert_called_once_with(f'{MIGRATION_TABLE_NAME}:20240101_000000')

  @pytest.mark.anyio
  async def test_remove_migration_record_not_found(self, mock_db_client):
    """Test removing non-existent migration record."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])
    mock_db_client.delete = AsyncMock()

    # Should not raise error, just log warning
    await remove_migration_record(mock_db_client, version='nonexistent')

    # Delete should not be called
    mock_db_client.delete.assert_not_called()

  @pytest.mark.anyio
  async def test_remove_migration_record_query_error(self, mock_db_client):
    """Test remove_migration_record handles QueryError."""
    mock_db_client.execute = AsyncMock(side_effect=QueryError('Database error'))

    with pytest.raises(MigrationHistoryError) as exc_info:
      await remove_migration_record(mock_db_client, version='20240101_000000')

    assert 'Failed to remove migration record 20240101_000000' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_remove_migration_record_unexpected_error(self, mock_db_client):
    """Test remove_migration_record handles unexpected errors."""
    mock_db_client.execute = AsyncMock(side_effect=RuntimeError('Unexpected'))

    with pytest.raises(MigrationHistoryError) as exc_info:
      await remove_migration_record(mock_db_client, version='20240101_000000')

    assert 'Unexpected error removing migration record' in str(exc_info.value)


class TestGetAppliedMigrations:
  """Test suite for get_applied_migrations function."""

  @pytest.mark.anyio
  async def test_get_applied_migrations_success(self, mock_db_client):
    """Test getting applied migrations."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'version': '20240101_000000',
              'description': 'First migration',
              'applied_at': '2024-01-01T00:00:00Z',
              'checksum': 'abc123',
              'execution_time_ms': 100,
            },
            {
              'version': '20240102_000000',
              'description': 'Second migration',
              'applied_at': '2024-01-02T00:00:00Z',
              'checksum': 'def456',
              'execution_time_ms': 150,
            },
          ]
        }
      ]
    )

    migrations = await get_applied_migrations(mock_db_client)

    assert len(migrations) == 2
    assert isinstance(migrations[0], MigrationHistory)
    assert migrations[0].version == '20240101_000000'
    assert migrations[0].description == 'First migration'
    assert migrations[0].checksum == 'abc123'
    assert migrations[0].execution_time_ms == 100
    assert migrations[1].version == '20240102_000000'

  @pytest.mark.anyio
  async def test_get_applied_migrations_empty(self, mock_db_client):
    """Test getting applied migrations when none exist."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    migrations = await get_applied_migrations(mock_db_client)

    assert len(migrations) == 0
    assert migrations == []

  @pytest.mark.anyio
  async def test_get_applied_migrations_without_execution_time(self, mock_db_client):
    """Test getting migrations without execution_time_ms field."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'version': '20240101_000000',
              'description': 'Test',
              'applied_at': '2024-01-01T00:00:00Z',
              'checksum': 'abc123',
            }
          ]
        }
      ]
    )

    migrations = await get_applied_migrations(mock_db_client)

    assert len(migrations) == 1
    assert migrations[0].execution_time_ms is None

  @pytest.mark.anyio
  async def test_get_applied_migrations_skips_invalid_records(self, mock_db_client):
    """Test that invalid migration records are skipped."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'version': '20240101_000000',
              'description': 'Valid',
              'applied_at': '2024-01-01T00:00:00Z',
              'checksum': 'abc123',
            },
            {
              'version': '20240102_000000',
              # Missing required fields
            },
          ]
        }
      ]
    )

    migrations = await get_applied_migrations(mock_db_client)

    # Only valid migration should be returned
    assert len(migrations) == 1
    assert migrations[0].version == '20240101_000000'

  @pytest.mark.anyio
  async def test_get_applied_migrations_query_error(self, mock_db_client):
    """Test get_applied_migrations handles QueryError from ensure_migration_table."""
    mock_db_client.execute = AsyncMock(side_effect=QueryError('Database error'))

    with pytest.raises(MigrationHistoryError) as exc_info:
      await get_applied_migrations(mock_db_client)

    # Error is wrapped by ensure_migration_table, then caught as unexpected error
    assert 'Unexpected error fetching applied migrations' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_get_applied_migrations_unexpected_error(self, mock_db_client):
    """Test get_applied_migrations handles unexpected errors."""
    # First call succeeds (ensure_migration_table), second fails
    mock_db_client.execute = AsyncMock(side_effect=[[], RuntimeError('Unexpected')])

    with pytest.raises(MigrationHistoryError) as exc_info:
      await get_applied_migrations(mock_db_client)

    assert 'Unexpected error fetching applied migrations' in str(exc_info.value)


class TestGetAppliedVersions:
  """Test suite for get_applied_versions function."""

  @pytest.mark.anyio
  async def test_get_applied_versions_success(self, mock_db_client):
    """Test getting set of applied migration versions."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'version': '20240101_000000',
              'description': 'First',
              'applied_at': '2024-01-01T00:00:00Z',
              'checksum': 'abc',
            },
            {
              'version': '20240102_000000',
              'description': 'Second',
              'applied_at': '2024-01-02T00:00:00Z',
              'checksum': 'def',
            },
          ]
        }
      ]
    )

    versions = await get_applied_versions(mock_db_client)

    assert isinstance(versions, set)
    assert len(versions) == 2
    assert '20240101_000000' in versions
    assert '20240102_000000' in versions

  @pytest.mark.anyio
  async def test_get_applied_versions_empty(self, mock_db_client):
    """Test getting applied versions when none exist."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    versions = await get_applied_versions(mock_db_client)

    assert isinstance(versions, set)
    assert len(versions) == 0


class TestIsMigrationApplied:
  """Test suite for is_migration_applied function."""

  @pytest.mark.anyio
  async def test_is_migration_applied_true(self, mock_db_client):
    """Test checking if migration is applied when it exists."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'version': '20240101_000000',
              'description': 'Test',
              'applied_at': '2024-01-01T00:00:00Z',
              'checksum': 'abc',
            }
          ]
        }
      ]
    )

    result = await is_migration_applied(mock_db_client, '20240101_000000')

    assert result is True

  @pytest.mark.anyio
  async def test_is_migration_applied_false(self, mock_db_client):
    """Test checking if migration is applied when it doesn't exist."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    result = await is_migration_applied(mock_db_client, '20240101_000000')

    assert result is False


class TestGetMigrationHistory:
  """Test suite for get_migration_history function."""

  @pytest.mark.anyio
  async def test_get_migration_history_success(self, mock_db_client):
    """Test getting history for specific migration."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'version': '20240101_000000',
              'description': 'Test migration',
              'applied_at': '2024-01-01T00:00:00Z',
              'checksum': 'abc123',
              'execution_time_ms': 100,
            }
          ]
        }
      ]
    )

    history = await get_migration_history(mock_db_client, '20240101_000000')

    assert history is not None
    assert isinstance(history, MigrationHistory)
    assert history.version == '20240101_000000'
    assert history.description == 'Test migration'
    assert history.checksum == 'abc123'
    assert history.execution_time_ms == 100

  @pytest.mark.anyio
  async def test_get_migration_history_not_found(self, mock_db_client):
    """Test getting history for non-existent migration."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    history = await get_migration_history(mock_db_client, 'nonexistent')

    assert history is None

  @pytest.mark.anyio
  async def test_get_migration_history_query_error(self, mock_db_client):
    """Test get_migration_history handles QueryError from ensure_migration_table."""
    mock_db_client.execute = AsyncMock(side_effect=QueryError('Database error'))

    with pytest.raises(MigrationHistoryError) as exc_info:
      await get_migration_history(mock_db_client, '20240101_000000')

    # Error is wrapped by ensure_migration_table, then caught as unexpected error
    assert 'Unexpected error getting migration history' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_get_migration_history_unexpected_error(self, mock_db_client):
    """Test get_migration_history handles unexpected errors."""
    # First call succeeds (ensure_migration_table), second fails
    mock_db_client.execute = AsyncMock(side_effect=[[], RuntimeError('Unexpected')])

    with pytest.raises(MigrationHistoryError) as exc_info:
      await get_migration_history(mock_db_client, '20240101_000000')

    assert 'Unexpected error getting migration history' in str(exc_info.value)


class TestExtractRecords:
  """Test suite for _extract_records helper function."""

  def test_extract_records_list_with_result_key(self):
    """Test extracting records from list with result key."""
    result = [{'result': [{'id': '1', 'data': 'test'}]}]
    records = _extract_records(result)
    assert records == [{'id': '1', 'data': 'test'}]

  def test_extract_records_list_without_result_key(self):
    """Test extracting records from plain list of dicts."""
    result = [{'id': '1', 'data': 'test'}]
    records = _extract_records(result)
    assert records == [{'id': '1', 'data': 'test'}]

  def test_extract_records_dict_with_result_key(self):
    """Test extracting records from dict with result key."""
    result = {'result': [{'id': '1', 'data': 'test'}]}
    records = _extract_records(result)
    assert records == [{'id': '1', 'data': 'test'}]

  def test_extract_records_single_dict(self):
    """Test extracting records from single dict without result key."""
    result = {'id': '1', 'data': 'test'}
    records = _extract_records(result)
    assert records == [{'id': '1', 'data': 'test'}]

  def test_extract_records_empty_result(self):
    """Test extracting records from empty result."""
    result = [{'result': []}]
    records = _extract_records(result)
    assert records == []

  def test_extract_records_none_result(self):
    """Test extracting records when result is None."""
    result = [{'result': None}]
    records = _extract_records(result)
    assert records == []

  def test_extract_records_empty_list(self):
    """Test extracting records from empty list."""
    result = []
    records = _extract_records(result)
    assert records == []

  def test_extract_records_invalid_type(self):
    """Test extracting records from invalid type returns empty list."""
    result = 'invalid'
    records = _extract_records(result)
    assert records == []


class TestParseDatetime:
  """Test suite for _parse_datetime helper function."""

  def test_parse_datetime_from_datetime_object(self):
    """Test parsing datetime from datetime object."""
    now = datetime.now(UTC)
    result = _parse_datetime(now)
    assert result == now

  def test_parse_datetime_from_iso_string(self):
    """Test parsing datetime from ISO format string."""
    iso_string = '2024-01-01T12:00:00+00:00'
    result = _parse_datetime(iso_string)
    assert isinstance(result, datetime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 1

  def test_parse_datetime_from_iso_string_with_z(self):
    """Test parsing datetime from ISO string with Z suffix."""
    iso_string = '2024-01-01T12:00:00Z'
    result = _parse_datetime(iso_string)
    assert isinstance(result, datetime)
    assert result.year == 2024

  def test_parse_datetime_from_invalid_string(self):
    """Test parsing datetime from invalid string falls back to current time."""
    result = _parse_datetime('invalid-date-string')
    assert isinstance(result, datetime)
    # Should be close to current time (within a few seconds)
    now = datetime.now(UTC)
    assert abs((result - now).total_seconds()) < 5

  def test_parse_datetime_from_none(self):
    """Test parsing datetime from None falls back to current time."""
    result = _parse_datetime(None)
    assert isinstance(result, datetime)
    now = datetime.now(UTC)
    assert abs((result - now).total_seconds()) < 5

  def test_parse_datetime_from_int(self):
    """Test parsing datetime from int falls back to current time."""
    result = _parse_datetime(12345)
    assert isinstance(result, datetime)

  def test_parse_datetime_preserves_microseconds(self):
    """Test parsing datetime preserves microseconds."""
    iso_string = '2024-01-01T12:00:00.123456+00:00'
    result = _parse_datetime(iso_string)
    assert isinstance(result, datetime)
    assert result.microsecond == 123456


class TestMigrationHistoryError:
  """Test suite for MigrationHistoryError exception."""

  def test_migration_history_error_creation(self):
    """Test creating MigrationHistoryError."""
    error = MigrationHistoryError('Test error message')
    assert str(error) == 'Test error message'
    assert isinstance(error, Exception)

  def test_migration_history_error_with_cause(self):
    """Test creating MigrationHistoryError with cause."""
    cause = ValueError('Original error')
    try:
      raise MigrationHistoryError('Wrapped error') from cause
    except MigrationHistoryError as error:
      assert str(error) == 'Wrapped error'
      assert error.__cause__ == cause
