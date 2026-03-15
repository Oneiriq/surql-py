"""Tests for the query executor module."""

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel, ValidationError

from surql.connection.client import QueryError
from surql.query.builder import Query
from surql.query.executor import (
  _extract_result_data,
  execute_query,
  execute_raw,
  execute_raw_typed,
  fetch_all,
  fetch_many,
  fetch_one,
  fetch_record,
  fetch_records,
)
from surql.query.results import ListResult, RecordResult


# Test models
class User(BaseModel):
  """Test user model."""

  name: str
  email: str
  age: int | None = None


class Post(BaseModel):
  """Test post model."""

  title: str
  content: str


class TestExecuteQuery:
  """Test suite for execute_query function."""

  @pytest.mark.anyio
  async def test_execute_query_success(self, mock_db_client):
    """Test successful query execution."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'id': 'user:1'}]}])

    query = Query[User]().select().from_table('user')
    result = await execute_query(query, client=mock_db_client)

    assert result == [{'result': [{'id': 'user:1'}]}]
    mock_db_client.execute.assert_called_once()

  @pytest.mark.anyio
  async def test_execute_query_with_context(self, mock_db_client):
    """Test query execution using context client."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    query = Query[User]().select().from_table('user')

    with patch('surql.query.executor.get_db', return_value=mock_db_client):
      result = await execute_query(query)

    assert result == [{'result': []}]

  @pytest.mark.anyio
  async def test_execute_query_failure(self, mock_db_client):
    """Test query execution failure."""
    mock_db_client.execute = AsyncMock(side_effect=Exception('Database error'))

    query = Query[User]().select().from_table('user')

    with pytest.raises(QueryError) as exc_info:
      await execute_query(query, client=mock_db_client)

    assert 'Query execution failed' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_execute_query_complex(self, mock_db_client):
    """Test complex query execution."""
    mock_result = [{'result': [{'name': 'Alice', 'age': 30}]}]
    mock_db_client.execute = AsyncMock(return_value=mock_result)

    query = Query[User]().select(['name', 'age']).from_table('user').where('age > 18').limit(10)
    result = await execute_query(query, client=mock_db_client)

    assert result == mock_result


class TestFetchOne:
  """Test suite for fetch_one function."""

  @pytest.mark.anyio
  async def test_fetch_one_success(self, mock_db_client):
    """Test fetching single record successfully."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Alice', 'email': 'alice@example.com', 'age': 30}]}]
    )

    query = Query[User]().select().from_table('user:alice')
    result = await fetch_one(query, User, client=mock_db_client)

    assert result is not None
    assert isinstance(result, User)
    assert result.name == 'Alice'
    assert result.email == 'alice@example.com'
    assert result.age == 30

  @pytest.mark.anyio
  async def test_fetch_one_none_result(self, mock_db_client):
    """Test fetch_one with None result."""
    mock_db_client.execute = AsyncMock(return_value=None)

    query = Query[User]().select().from_table('user:nonexistent')
    result = await fetch_one(query, User, client=mock_db_client)

    assert result is None

  @pytest.mark.anyio
  async def test_fetch_one_empty_result(self, mock_db_client):
    """Test fetch_one with empty result."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    query = Query[User]().select().from_table('user:nonexistent')
    result = await fetch_one(query, User, client=mock_db_client)

    assert result is None

  @pytest.mark.anyio
  async def test_fetch_one_from_list(self, mock_db_client):
    """Test fetch_one returns first item from list."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
            {'name': 'Bob', 'email': 'bob@example.com', 'age': 25},
          ]
        }
      ]
    )

    query = Query[User]().select().from_table('user').limit(1)
    result = await fetch_one(query, User, client=mock_db_client)

    assert result is not None
    assert result.name == 'Alice'

  @pytest.mark.anyio
  async def test_fetch_one_validation_error(self, mock_db_client):
    """Test fetch_one with validation error."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Alice'}]}]  # Missing required 'email'
    )

    query = Query[User]().select().from_table('user:alice')

    with pytest.raises(ValidationError):
      await fetch_one(query, User, client=mock_db_client)

  @pytest.mark.anyio
  async def test_fetch_one_flat_result(self, mock_db_client):
    """Test fetch_one with flat result format."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'name': 'Alice', 'email': 'alice@example.com'}]
    )

    query = Query[User]().select().from_table('user:alice')
    result = await fetch_one(query, User, client=mock_db_client)

    assert result is not None
    assert result.name == 'Alice'


class TestFetchAll:
  """Test suite for fetch_all function."""

  @pytest.mark.anyio
  async def test_fetch_all_success(self, mock_db_client):
    """Test fetching multiple records successfully."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
            {'name': 'Bob', 'email': 'bob@example.com', 'age': 25},
          ]
        }
      ]
    )

    query = Query[User]().select().from_table('user')
    results = await fetch_all(query, User, client=mock_db_client)

    assert len(results) == 2
    assert all(isinstance(r, User) for r in results)
    assert results[0].name == 'Alice'
    assert results[1].name == 'Bob'

  @pytest.mark.anyio
  async def test_fetch_all_empty(self, mock_db_client):
    """Test fetch_all with empty results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    query = Query[User]().select().from_table('user')
    results = await fetch_all(query, User, client=mock_db_client)

    assert results == []

  @pytest.mark.anyio
  async def test_fetch_all_single_record(self, mock_db_client):
    """Test fetch_all with single record."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Alice', 'email': 'alice@example.com'}]}]
    )

    query = Query[User]().select().from_table('user')
    results = await fetch_all(query, User, client=mock_db_client)

    assert len(results) == 1
    assert results[0].name == 'Alice'

  @pytest.mark.anyio
  async def test_fetch_all_validation_error(self, mock_db_client):
    """Test fetch_all with validation error in results."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Invalid'},  # Missing email
          ]
        }
      ]
    )

    query = Query[User]().select().from_table('user')

    with pytest.raises(ValidationError):
      await fetch_all(query, User, client=mock_db_client)

  @pytest.mark.anyio
  async def test_fetch_all_non_list_result(self, mock_db_client):
    """Test fetch_all converts non-list to list."""
    mock_db_client.execute = AsyncMock(
      return_value={'result': {'name': 'Alice', 'email': 'alice@example.com'}}
    )

    query = Query[User]().select().from_table('user:alice')
    results = await fetch_all(query, User, client=mock_db_client)

    assert len(results) == 1
    assert results[0].name == 'Alice'


class TestFetchMany:
  """Test suite for fetch_many function."""

  @pytest.mark.anyio
  async def test_fetch_many_success(self, mock_db_client):
    """Test streaming records with fetch_many."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Bob', 'email': 'bob@example.com'},
            {'name': 'Charlie', 'email': 'charlie@example.com'},
          ]
        }
      ]
    )

    query = Query[User]().select().from_table('user')
    results = []

    async for user in fetch_many(query, User, client=mock_db_client):
      results.append(user)

    assert len(results) == 3
    assert all(isinstance(r, User) for r in results)
    assert results[0].name == 'Alice'

  @pytest.mark.anyio
  async def test_fetch_many_empty(self, mock_db_client):
    """Test fetch_many with empty results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    query = Query[User]().select().from_table('user')
    results = []

    async for user in fetch_many(query, User, client=mock_db_client):
      results.append(user)

    assert results == []


class TestFetchRecord:
  """Test suite for fetch_record function."""

  @pytest.mark.anyio
  async def test_fetch_record_exists(self, mock_db_client):
    """Test fetch_record with existing record."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Alice', 'email': 'alice@example.com'}]}]
    )

    query = Query[User]().select().from_table('user:alice')
    result = await fetch_record(query, User, client=mock_db_client)

    assert isinstance(result, RecordResult)
    assert result.exists is True
    assert result.record is not None
    assert result.record.name == 'Alice'

  @pytest.mark.anyio
  async def test_fetch_record_not_exists(self, mock_db_client):
    """Test fetch_record with non-existent record."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    query = Query[User]().select().from_table('user:nonexistent')
    result = await fetch_record(query, User, client=mock_db_client)

    assert isinstance(result, RecordResult)
    assert result.exists is False
    assert result.record is None

  @pytest.mark.anyio
  async def test_fetch_record_unwrap(self, mock_db_client):
    """Test unwrapping fetch_record result."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Alice', 'email': 'alice@example.com'}]}]
    )

    query = Query[User]().select().from_table('user:alice')
    result = await fetch_record(query, User, client=mock_db_client)

    user = result.unwrap()
    assert isinstance(user, User)
    assert user.name == 'Alice'


class TestFetchRecords:
  """Test suite for fetch_records function."""

  @pytest.mark.anyio
  async def test_fetch_records_success(self, mock_db_client):
    """Test fetch_records with multiple records."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Bob', 'email': 'bob@example.com'},
          ]
        }
      ]
    )

    query = Query[User]().select().from_table('user').limit(10)
    result = await fetch_records(query, User, client=mock_db_client)

    assert isinstance(result, ListResult)
    assert len(result) == 2
    assert result.limit == 10
    assert result.offset is None

  @pytest.mark.anyio
  async def test_fetch_records_with_pagination(self, mock_db_client):
    """Test fetch_records with pagination info."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
          ]
        }
      ]
    )

    query = Query[User]().select().from_table('user').limit(10).offset(20)
    result = await fetch_records(query, User, client=mock_db_client)

    assert result.limit == 10
    assert result.offset == 20

  @pytest.mark.anyio
  async def test_fetch_records_empty(self, mock_db_client):
    """Test fetch_records with empty results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    query = Query[User]().select().from_table('user')
    result = await fetch_records(query, User, client=mock_db_client)

    assert isinstance(result, ListResult)
    assert len(result) == 0
    assert result.is_empty()


class TestExecuteRaw:
  """Test suite for execute_raw function."""

  @pytest.mark.anyio
  async def test_execute_raw_success(self, mock_db_client):
    """Test executing raw SQL successfully."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'id': 'user:1'}]}])

    result = await execute_raw('SELECT * FROM user', client=mock_db_client)

    assert result == [{'result': [{'id': 'user:1'}]}]
    mock_db_client.execute.assert_called_once_with('SELECT * FROM user', None)

  @pytest.mark.anyio
  async def test_execute_raw_with_params(self, mock_db_client):
    """Test executing raw SQL with parameters."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    params = {'age': 18}
    result = await execute_raw(
      'SELECT * FROM user WHERE age > $age', params=params, client=mock_db_client
    )

    assert result == [{'result': []}]
    mock_db_client.execute.assert_called_once_with('SELECT * FROM user WHERE age > $age', params)

  @pytest.mark.anyio
  async def test_execute_raw_failure(self, mock_db_client):
    """Test execute_raw with query failure."""
    mock_db_client.execute = AsyncMock(side_effect=Exception('Syntax error'))

    with pytest.raises(QueryError) as exc_info:
      await execute_raw('INVALID SQL', client=mock_db_client)

    assert 'Raw query execution failed' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_execute_raw_with_context(self, mock_db_client):
    """Test execute_raw using context client."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    with patch('surql.query.executor.get_db', return_value=mock_db_client):
      result = await execute_raw('SELECT * FROM user')

    assert result == [{'result': []}]


class TestExecuteRawTyped:
  """Test suite for execute_raw_typed function."""

  @pytest.mark.anyio
  async def test_execute_raw_typed_success(self, mock_db_client):
    """Test executing typed raw query successfully."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Bob', 'email': 'bob@example.com'},
          ]
        }
      ]
    )

    results = await execute_raw_typed('SELECT * FROM user', User, client=mock_db_client)

    assert len(results) == 2
    assert all(isinstance(r, User) for r in results)
    assert results[0].name == 'Alice'

  @pytest.mark.anyio
  async def test_execute_raw_typed_with_params(self, mock_db_client):
    """Test typed raw query with parameters."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Alice', 'email': 'alice@example.com', 'age': 30}]}]
    )

    results = await execute_raw_typed(
      'SELECT * FROM user WHERE age > $age', User, params={'age': 18}, client=mock_db_client
    )

    assert len(results) == 1
    assert results[0].age == 30

  @pytest.mark.anyio
  async def test_execute_raw_typed_empty(self, mock_db_client):
    """Test typed raw query with empty results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    results = await execute_raw_typed(
      'SELECT * FROM user WHERE age > 100', User, client=mock_db_client
    )

    assert results == []

  @pytest.mark.anyio
  async def test_execute_raw_typed_validation_error(self, mock_db_client):
    """Test typed raw query with validation error."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Invalid'}]}]  # Missing email
    )

    with pytest.raises(ValidationError):
      await execute_raw_typed('SELECT * FROM user', User, client=mock_db_client)

  @pytest.mark.anyio
  async def test_execute_raw_typed_single_result(self, mock_db_client):
    """Test typed raw query converting single result to list."""
    mock_db_client.execute = AsyncMock(
      return_value={'result': {'name': 'Alice', 'email': 'alice@example.com'}}
    )

    results = await execute_raw_typed('SELECT * FROM user:alice', User, client=mock_db_client)

    assert len(results) == 1
    assert results[0].name == 'Alice'


class TestExtractResultData:
  """Test suite for _extract_result_data helper function."""

  def test_extract_none(self):
    """Test extracting from None."""
    assert _extract_result_data(None) is None

  def test_extract_direct_list(self):
    """Test extracting from direct list."""
    data = [{'name': 'Alice'}, {'name': 'Bob'}]
    assert _extract_result_data(data) == data

  def test_extract_wrapped_result(self):
    """Test extracting from wrapped result."""
    wrapped = [{'result': [{'name': 'Alice'}]}]
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}]

  def test_extract_multiple_wrapped_results(self):
    """Test extracting from multiple wrapped results."""
    wrapped = [
      {'result': [{'name': 'Alice'}]},
      {'result': [{'name': 'Bob'}]},
    ]
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}, {'name': 'Bob'}]

  def test_extract_dict_with_result_key(self):
    """Test extracting from dict with result key."""
    wrapped = {'result': [{'name': 'Alice'}]}
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}]

  def test_extract_empty_wrapped_result(self):
    """Test extracting from empty wrapped result."""
    wrapped = [{'result': []}]
    result = _extract_result_data(wrapped)
    assert result is None

  def test_extract_nested_lists(self):
    """Test extracting nested list results."""
    wrapped = [
      {'result': [{'name': 'Alice'}, {'name': 'Bob'}]},
    ]
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}, {'name': 'Bob'}]

  def test_extract_single_non_list_result(self):
    """Test extracting single non-list wrapped result."""
    wrapped = [{'result': {'name': 'Alice'}}]
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}]

  def test_extract_mixed_results(self):
    """Test extracting mixed result formats."""
    wrapped = [
      {'result': [{'name': 'Alice'}]},
      {'result': {'name': 'Bob'}},
    ]
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}, {'name': 'Bob'}]

  def test_extract_empty_list(self):
    """Test extracting from empty list."""
    assert _extract_result_data([]) == []
