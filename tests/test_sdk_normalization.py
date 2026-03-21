"""Tests for SurrealDB 3.x SDK type normalization and single-record select unwrapping.

Covers two fixes:
1. select() unwraps single-record results (record ID targets) from list to dict
2. All CRUD responses normalize SDK RecordID objects to plain strings
"""

from unittest.mock import AsyncMock

import pytest
from surrealdb import RecordID as SdkRecordID

from surql.connection.client import (
  DatabaseClient,
  _is_record_id_target,
  _normalize_sdk_value,
)

# ============================================================================
# _is_record_id_target
# ============================================================================


class TestIsRecordIdTarget:
  """Tests for _is_record_id_target helper."""

  def test_simple_record_id(self) -> None:
    """Detects standard table:id format."""
    assert _is_record_id_target('user:alice') is True

  def test_numeric_id(self) -> None:
    """Detects table:123 format."""
    assert _is_record_id_target('user:123') is True

  def test_angle_bracket_id(self) -> None:
    """Detects table:<complex.id> format."""
    assert _is_record_id_target('outlet:<alaskabeacon.com>') is True

  def test_underscore_table(self) -> None:
    """Detects tables with underscores."""
    assert _is_record_id_target('user_profile:abc') is True

  def test_table_only(self) -> None:
    """Rejects plain table name without colon."""
    assert _is_record_id_target('user') is False

  def test_empty_string(self) -> None:
    """Rejects empty string."""
    assert _is_record_id_target('') is False

  def test_numeric_start_table(self) -> None:
    """Rejects table names starting with digits."""
    assert _is_record_id_target('123:abc') is False

  def test_ulid_style_id(self) -> None:
    """Detects ULID-style record IDs."""
    assert _is_record_id_target('file:01JQXYZ1234ABCDEF5678') is True


# ============================================================================
# _normalize_sdk_value
# ============================================================================


class TestNormalizeSdkValue:
  """Tests for _normalize_sdk_value helper."""

  def test_sdk_record_id_to_string(self) -> None:
    """Converts SDK RecordID to its string representation."""
    sdk_rid = SdkRecordID('user', 'alice')
    result = _normalize_sdk_value(sdk_rid)

    assert isinstance(result, str)
    assert result == 'user:alice'

  def test_dict_with_sdk_record_id(self) -> None:
    """Normalizes RecordID values inside dicts."""
    sdk_rid = SdkRecordID('file', 'abc123')
    data = {'id': sdk_rid, 'name': 'test.py', 'size': 42}

    result = _normalize_sdk_value(data)

    assert isinstance(result, dict)
    assert result['id'] == 'file:abc123'
    assert result['name'] == 'test.py'
    assert result['size'] == 42

  def test_nested_dict_with_sdk_record_id(self) -> None:
    """Normalizes RecordID values in nested dicts."""
    inner_rid = SdkRecordID('project', 'xyz')
    data = {'id': SdkRecordID('file', 'abc'), 'meta': {'project_id': inner_rid}}

    result = _normalize_sdk_value(data)

    assert result['id'] == 'file:abc'
    assert result['meta']['project_id'] == 'project:xyz'

  def test_list_with_sdk_record_ids(self) -> None:
    """Normalizes RecordID values inside lists."""
    data = [
      {'id': SdkRecordID('user', 'alice'), 'name': 'Alice'},
      {'id': SdkRecordID('user', 'bob'), 'name': 'Bob'},
    ]

    result = _normalize_sdk_value(data)

    assert result[0]['id'] == 'user:alice'
    assert result[1]['id'] == 'user:bob'

  def test_plain_values_unchanged(self) -> None:
    """Leaves plain Python types untouched."""
    assert _normalize_sdk_value('hello') == 'hello'
    assert _normalize_sdk_value(42) == 42
    assert _normalize_sdk_value(3.14) == 3.14
    assert _normalize_sdk_value(True) is True
    assert _normalize_sdk_value(None) is None

  def test_dict_without_record_ids_unchanged(self) -> None:
    """Leaves dicts without RecordID values intact."""
    data = {'name': 'test', 'count': 5}
    result = _normalize_sdk_value(data)

    assert result == data

  def test_empty_structures(self) -> None:
    """Handles empty dicts and lists."""
    assert _normalize_sdk_value({}) == {}
    assert _normalize_sdk_value([]) == []

  def test_deeply_nested_record_id(self) -> None:
    """Normalizes RecordID in deeply nested structures."""
    data = {'a': [{'b': {'c': SdkRecordID('tbl', 'deep')}}]}
    result = _normalize_sdk_value(data)

    assert result['a'][0]['b']['c'] == 'tbl:deep'


# ============================================================================
# DatabaseClient.select() -- single-record unwrap
# ============================================================================


class TestSelectSingleRecordUnwrap:
  """Tests for DatabaseClient.select() single-record unwrap behavior."""

  @pytest.mark.anyio
  async def test_select_record_id_unwraps_list(self, mock_db_client: DatabaseClient) -> None:
    """Selecting a record ID unwraps the single-element list to a dict."""
    mock_db_client._client.select = AsyncMock(return_value=[{'id': 'user:alice', 'name': 'Alice'}])

    result = await mock_db_client.select('user:alice')

    assert isinstance(result, dict)
    assert result['id'] == 'user:alice'
    assert result['name'] == 'Alice'

  @pytest.mark.anyio
  async def test_select_record_id_empty_list_returns_none(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Selecting a non-existent record ID returns None."""
    mock_db_client._client.select = AsyncMock(return_value=[])

    result = await mock_db_client.select('user:nonexistent')

    assert result is None

  @pytest.mark.anyio
  async def test_select_table_returns_list(self, mock_db_client: DatabaseClient) -> None:
    """Selecting a table returns the full list."""
    mock_db_client._client.select = AsyncMock(
      return_value=[
        {'id': 'user:alice', 'name': 'Alice'},
        {'id': 'user:bob', 'name': 'Bob'},
      ]
    )

    result = await mock_db_client.select('user')

    assert isinstance(result, list)
    assert len(result) == 2

  @pytest.mark.anyio
  async def test_select_record_id_normalizes_sdk_types(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Selecting a record ID normalizes SDK RecordID in the response."""
    sdk_rid = SdkRecordID('user', 'alice')
    mock_db_client._client.select = AsyncMock(return_value=[{'id': sdk_rid, 'name': 'Alice'}])

    result = await mock_db_client.select('user:alice')

    assert isinstance(result, dict)
    assert result['id'] == 'user:alice'
    assert isinstance(result['id'], str)

  @pytest.mark.anyio
  async def test_select_table_normalizes_sdk_types(self, mock_db_client: DatabaseClient) -> None:
    """Selecting a table normalizes SDK RecordID in the response list."""
    mock_db_client._client.select = AsyncMock(
      return_value=[
        {'id': SdkRecordID('user', 'alice'), 'name': 'Alice'},
        {'id': SdkRecordID('user', 'bob'), 'name': 'Bob'},
      ]
    )

    result = await mock_db_client.select('user')

    assert isinstance(result, list)
    assert result[0]['id'] == 'user:alice'
    assert result[1]['id'] == 'user:bob'


# ============================================================================
# DatabaseClient.create() -- SDK type normalization
# ============================================================================


class TestCreateNormalization:
  """Tests for DatabaseClient.create() SDK type normalization."""

  @pytest.mark.anyio
  async def test_create_normalizes_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Create normalizes SDK RecordID in the response."""
    sdk_rid = SdkRecordID('file', 'abc123')
    mock_db_client._client.create = AsyncMock(return_value={'id': sdk_rid, 'name': 'test.py'})

    result = await mock_db_client.create('file', {'name': 'test.py'})

    assert result['id'] == 'file:abc123'
    assert isinstance(result['id'], str)

  @pytest.mark.anyio
  async def test_create_normalizes_nested_record_ids(self, mock_db_client: DatabaseClient) -> None:
    """Create normalizes nested SDK RecordID values."""
    mock_db_client._client.create = AsyncMock(
      return_value={
        'id': SdkRecordID('symbol', 'xyz'),
        'file_id': SdkRecordID('file', 'abc123'),
        'name': 'MyClass',
      }
    )

    result = await mock_db_client.create('symbol', {'file_id': 'file:abc123', 'name': 'MyClass'})

    assert result['id'] == 'symbol:xyz'
    assert result['file_id'] == 'file:abc123'
    assert isinstance(result['file_id'], str)

  @pytest.mark.anyio
  async def test_create_result_id_reusable_in_subsequent_create(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Demonstrates the core fix: created record IDs can be reused as field values."""
    # First create returns a record with SDK RecordID
    mock_db_client._client.create = AsyncMock(
      return_value={'id': SdkRecordID('file', 'abc123'), 'path': '/src/main.py'}
    )

    file_record = await mock_db_client.create('file', {'path': '/src/main.py'})

    # The id should be a plain string, safe to pass back
    assert isinstance(file_record['id'], str)

    # Second create uses the id as a field value
    mock_db_client._client.create = AsyncMock(
      return_value={
        'id': SdkRecordID('symbol', 'xyz'),
        'file_id': file_record['id'],
        'name': 'MyClass',
      }
    )

    symbol_record = await mock_db_client.create(
      'symbol', {'file_id': file_record['id'], 'name': 'MyClass'}
    )

    # Both IDs should be plain strings
    assert isinstance(symbol_record['id'], str)
    assert isinstance(symbol_record['file_id'], str)
    assert symbol_record['file_id'] == 'file:abc123'


# ============================================================================
# DatabaseClient.execute() -- SDK type normalization
# ============================================================================


class TestExecuteNormalization:
  """Tests for DatabaseClient.execute() SDK type normalization."""

  @pytest.mark.anyio
  async def test_execute_normalizes_record_ids(self, mock_db_client: DatabaseClient) -> None:
    """Execute normalizes SDK RecordID in query results."""
    mock_db_client._client.query = AsyncMock(
      return_value=[{'result': [{'id': SdkRecordID('user', 'alice'), 'name': 'Alice'}]}]
    )

    result = await mock_db_client.execute('SELECT * FROM user')

    assert result[0]['result'][0]['id'] == 'user:alice'
    assert isinstance(result[0]['result'][0]['id'], str)


# ============================================================================
# DatabaseClient.update() / merge() -- SDK type normalization
# ============================================================================


class TestUpdateMergeNormalization:
  """Tests for update/merge SDK type normalization."""

  @pytest.mark.anyio
  async def test_update_normalizes_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Update normalizes SDK RecordID in the response."""
    mock_db_client._client.update = AsyncMock(
      return_value={'id': SdkRecordID('user', 'alice'), 'name': 'Alice Updated'}
    )

    result = await mock_db_client.update('user:alice', {'name': 'Alice Updated'})

    assert result['id'] == 'user:alice'
    assert isinstance(result['id'], str)

  @pytest.mark.anyio
  async def test_merge_normalizes_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Merge normalizes SDK RecordID in the response."""
    mock_db_client._client.merge = AsyncMock(
      return_value={'id': SdkRecordID('user', 'alice'), 'status': 'active'}
    )

    result = await mock_db_client.merge('user:alice', {'status': 'active'})

    assert result['id'] == 'user:alice'
    assert isinstance(result['id'], str)
