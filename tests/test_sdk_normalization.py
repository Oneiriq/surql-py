"""Tests for SurrealDB 3.x SDK type normalization, denormalization, and round-trip.

Covers three concerns:
1. select() unwraps single-record results (record ID targets) from list to dict
2. All CRUD responses normalize SDK RecordID objects to plain strings
3. Record ID strings in input data are denormalized back to SDK RecordID objects
"""

from unittest.mock import AsyncMock

import pytest
from surrealdb import RecordID as SdkRecordID

from surql.connection.client import (
  DatabaseClient,
  _denormalize_params,
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
    """Selecting a record ID unwraps the single-element list to a dict.

    Bug #15: record-id targets now route through raw
    ``SELECT * FROM type::record($table, $id)`` rather than the SDK's
    bare-string ``select``, so the mock is placed on ``query``.
    """
    mock_db_client._client.query = AsyncMock(return_value=[{'id': 'user:alice', 'name': 'Alice'}])

    result = await mock_db_client.select('user:alice')

    assert isinstance(result, dict)
    assert result['id'] == 'user:alice'
    assert result['name'] == 'Alice'

  @pytest.mark.anyio
  async def test_select_record_id_empty_list_returns_none(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Selecting a non-existent record ID returns None."""
    mock_db_client._client.query = AsyncMock(return_value=[])

    result = await mock_db_client.select('user:nonexistent')

    assert result is None

  @pytest.mark.anyio
  async def test_select_table_returns_list(self, mock_db_client: DatabaseClient) -> None:
    """Selecting a table returns the full list (still via SDK select())."""
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
    mock_db_client._client.query = AsyncMock(return_value=[{'id': sdk_rid, 'name': 'Alice'}])

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


# ============================================================================
# _denormalize_params
# ============================================================================


class TestDenormalizeParams:
  """Tests for _denormalize_params helper."""

  def test_record_id_string_to_sdk_object(self) -> None:
    """Converts a record ID string to an SDK RecordID."""
    result = _denormalize_params('repo:abc123')

    assert isinstance(result, SdkRecordID)
    assert str(result) == 'repo:abc123'

  def test_dict_with_record_id_string(self) -> None:
    """Converts record ID strings inside dict values."""
    data = {'repo_id': 'repo:abc123', 'name': 'test.py'}
    result = _denormalize_params(data)

    assert isinstance(result['repo_id'], SdkRecordID)
    assert str(result['repo_id']) == 'repo:abc123'
    assert result['name'] == 'test.py'

  def test_nested_dict_with_record_id_strings(self) -> None:
    """Converts record ID strings in nested dicts."""
    data = {'meta': {'project_id': 'project:xyz'}, 'name': 'foo'}
    result = _denormalize_params(data)

    assert isinstance(result['meta']['project_id'], SdkRecordID)
    assert str(result['meta']['project_id']) == 'project:xyz'

  def test_list_with_record_id_strings(self) -> None:
    """Converts record ID strings inside lists."""
    data = ['repo:abc', 'file:def']
    result = _denormalize_params(data)

    assert all(isinstance(item, SdkRecordID) for item in result)
    assert str(result[0]) == 'repo:abc'
    assert str(result[1]) == 'file:def'

  def test_plain_strings_unchanged(self) -> None:
    """Leaves non-record-ID strings untouched."""
    assert _denormalize_params('hello') == 'hello'
    assert _denormalize_params('just_a_name') == 'just_a_name'
    assert _denormalize_params('') == ''

  def test_non_string_values_unchanged(self) -> None:
    """Leaves non-string values untouched."""
    assert _denormalize_params(42) == 42
    assert _denormalize_params(3.14) == 3.14
    assert _denormalize_params(True) is True
    assert _denormalize_params(None) is None

  def test_empty_structures(self) -> None:
    """Handles empty dicts and lists."""
    assert _denormalize_params({}) == {}
    assert _denormalize_params([]) == []

  def test_mixed_dict_with_plain_and_record_id_strings(self) -> None:
    """Converts only record ID strings, leaving other values intact."""
    data = {
      'repo_id': 'repo:abc123',
      'path': '/src/main.py',
      'line_count': 42,
      'active': True,
    }
    result = _denormalize_params(data)

    assert isinstance(result['repo_id'], SdkRecordID)
    assert result['path'] == '/src/main.py'
    assert result['line_count'] == 42
    assert result['active'] is True

  def test_ulid_style_record_id(self) -> None:
    """Converts ULID-style record ID strings."""
    result = _denormalize_params('file:01JQXYZ1234ABCDEF5678')

    assert isinstance(result, SdkRecordID)
    assert str(result) == 'file:01JQXYZ1234ABCDEF5678'

  def test_underscore_table_record_id(self) -> None:
    """Converts record IDs with underscored table names."""
    result = _denormalize_params('user_profile:abc')

    assert isinstance(result, SdkRecordID)
    assert str(result) == 'user_profile:abc'

  def test_numeric_start_not_converted(self) -> None:
    """Strings starting with digits are not treated as record IDs."""
    result = _denormalize_params('123:abc')

    assert isinstance(result, str)
    assert result == '123:abc'

  def test_deeply_nested_record_id(self) -> None:
    """Converts record ID strings in deeply nested structures."""
    data = {'a': [{'b': {'c': 'tbl:deep'}}]}
    result = _denormalize_params(data)

    assert isinstance(result['a'][0]['b']['c'], SdkRecordID)
    assert str(result['a'][0]['b']['c']) == 'tbl:deep'


# ============================================================================
# Round-trip: normalize (response) -> denormalize (next request)
# ============================================================================


class TestRoundTrip:
  """Tests verifying the normalize -> denormalize round-trip is transparent."""

  def test_normalize_then_denormalize_preserves_identity(self) -> None:
    """A normalized RecordID string denormalizes back to an equivalent SDK RecordID."""
    original = SdkRecordID('repo', 'abc123')
    normalized = _normalize_sdk_value(original)
    denormalized = _denormalize_params(normalized)

    assert isinstance(denormalized, SdkRecordID)
    assert str(denormalized) == str(original)

  def test_round_trip_dict(self) -> None:
    """Round-trip works for dicts containing RecordID values."""
    sdk_response = {
      'id': SdkRecordID('file', 'abc'),
      'repo_id': SdkRecordID('repo', 'xyz'),
      'path': '/src/main.py',
    }
    normalized = _normalize_sdk_value(sdk_response)

    # Consumer takes the normalized response and passes fields back
    next_request = {'file_id': normalized['id'], 'repo_id': normalized['repo_id']}
    denormalized = _denormalize_params(next_request)

    assert isinstance(denormalized['file_id'], SdkRecordID)
    assert isinstance(denormalized['repo_id'], SdkRecordID)
    assert str(denormalized['file_id']) == 'file:abc'
    assert str(denormalized['repo_id']) == 'repo:xyz'


# ============================================================================
# DatabaseClient CRUD -- input denormalization
# ============================================================================


class TestCreateDenormalization:
  """Tests for DatabaseClient.create() input denormalization."""

  @pytest.mark.anyio
  async def test_create_denormalizes_record_id_string(self, mock_db_client: DatabaseClient) -> None:
    """Create converts record ID strings in data to SDK RecordID before sending."""
    mock_db_client._client.create = AsyncMock(
      return_value={
        'id': SdkRecordID('file', 'f1'),
        'repo_id': SdkRecordID('repo', 'r1'),
        'path': 'foo.py',
      }
    )

    await mock_db_client.create('file', {'repo_id': 'repo:r1', 'path': 'foo.py'})

    # Verify the SDK received a RecordID object, not a plain string
    call_args = mock_db_client._client.create.call_args
    sent_data = call_args[0][1]
    assert isinstance(sent_data['repo_id'], SdkRecordID)
    assert str(sent_data['repo_id']) == 'repo:r1'
    assert sent_data['path'] == 'foo.py'

  @pytest.mark.anyio
  async def test_create_round_trip(self, mock_db_client: DatabaseClient) -> None:
    """Full round-trip: create repo, use its ID to create file."""
    # Step 1: create repo -- SDK returns RecordID, client normalizes to string
    mock_db_client._client.create = AsyncMock(
      return_value={'id': SdkRecordID('repo', 'r1'), 'name': 'my-repo'}
    )
    repo = await mock_db_client.create('repo', {'name': 'my-repo'})
    assert repo['id'] == 'repo:r1'
    assert isinstance(repo['id'], str)

    # Step 2: create file using the string ID -- client denormalizes to RecordID
    mock_db_client._client.create = AsyncMock(
      return_value={
        'id': SdkRecordID('file', 'f1'),
        'repo_id': SdkRecordID('repo', 'r1'),
        'path': 'foo.py',
      }
    )
    file_record = await mock_db_client.create('file', {'repo_id': repo['id'], 'path': 'foo.py'})

    # Verify SDK received RecordID object
    call_args = mock_db_client._client.create.call_args
    sent_data = call_args[0][1]
    assert isinstance(sent_data['repo_id'], SdkRecordID)

    # Verify response is normalized back to strings
    assert file_record['id'] == 'file:f1'
    assert file_record['repo_id'] == 'repo:r1'
    assert isinstance(file_record['repo_id'], str)


class TestUpdateDenormalization:
  """Tests for DatabaseClient.update() input denormalization."""

  @pytest.mark.anyio
  async def test_update_denormalizes_record_id_string(self, mock_db_client: DatabaseClient) -> None:
    """Update converts record ID strings in data to SDK RecordID before sending."""
    mock_db_client._client.update = AsyncMock(
      return_value={'id': SdkRecordID('file', 'f1'), 'repo_id': SdkRecordID('repo', 'r2')}
    )

    await mock_db_client.update('file:f1', {'repo_id': 'repo:r2'})

    call_args = mock_db_client._client.update.call_args
    sent_data = call_args[0][1]
    assert isinstance(sent_data['repo_id'], SdkRecordID)
    assert str(sent_data['repo_id']) == 'repo:r2'


class TestMergeDenormalization:
  """Tests for DatabaseClient.merge() input denormalization."""

  @pytest.mark.anyio
  async def test_merge_denormalizes_record_id_string(self, mock_db_client: DatabaseClient) -> None:
    """Merge converts record ID strings in data to SDK RecordID before sending."""
    mock_db_client._client.merge = AsyncMock(
      return_value={'id': SdkRecordID('file', 'f1'), 'owner_id': SdkRecordID('user', 'u1')}
    )

    await mock_db_client.merge('file:f1', {'owner_id': 'user:u1'})

    call_args = mock_db_client._client.merge.call_args
    sent_data = call_args[0][1]
    assert isinstance(sent_data['owner_id'], SdkRecordID)
    assert str(sent_data['owner_id']) == 'user:u1'


class TestExecuteDenormalization:
  """Tests for DatabaseClient.execute() params denormalization."""

  @pytest.mark.anyio
  async def test_execute_denormalizes_params(self, mock_db_client: DatabaseClient) -> None:
    """Execute converts record ID strings in params to SDK RecordID before sending."""
    mock_db_client._client.query = AsyncMock(return_value=[{'result': []}])

    await mock_db_client.execute(
      'CREATE file SET repo_id = $repo_id',
      params={'repo_id': 'repo:r1'},
    )

    call_args = mock_db_client._client.query.call_args
    sent_params = call_args[0][1]
    assert isinstance(sent_params['repo_id'], SdkRecordID)
    assert str(sent_params['repo_id']) == 'repo:r1'

  @pytest.mark.anyio
  async def test_execute_no_params_unchanged(self, mock_db_client: DatabaseClient) -> None:
    """Execute without params passes empty dict."""
    mock_db_client._client.query = AsyncMock(return_value=[{'result': []}])

    await mock_db_client.execute('SELECT * FROM user')

    call_args = mock_db_client._client.query.call_args
    sent_params = call_args[0][1]
    assert sent_params == {}


class TestInsertRelationDenormalization:
  """Tests for DatabaseClient.insert_relation() input denormalization."""

  @pytest.mark.anyio
  async def test_insert_relation_denormalizes_in_out(self, mock_db_client: DatabaseClient) -> None:
    """insert_relation converts 'in' and 'out' record ID strings to SDK RecordID."""
    mock_db_client._client.insert_relation = AsyncMock(
      return_value={
        'id': SdkRecordID('likes', '123'),
        'in': SdkRecordID('user', 'alice'),
        'out': SdkRecordID('post', 'p1'),
      }
    )

    await mock_db_client.insert_relation('likes', {'in': 'user:alice', 'out': 'post:p1'})

    call_args = mock_db_client._client.insert_relation.call_args
    sent_data = call_args[0][1]
    assert isinstance(sent_data['in'], SdkRecordID)
    assert isinstance(sent_data['out'], SdkRecordID)
    assert str(sent_data['in']) == 'user:alice'
    assert str(sent_data['out']) == 'post:p1'
