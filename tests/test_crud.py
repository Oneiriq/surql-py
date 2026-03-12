"""Comprehensive tests for CRUD operations module.

This module tests all CRUD operations including create, read, update, delete,
and query functions with various scenarios including happy paths, edge cases,
and error conditions.
"""

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from reverie.connection.client import DatabaseClient
from reverie.query.crud import (
  count_records,
  create_record,
  create_records,
  delete_record,
  delete_records,
  exists,
  first,
  get_record,
  last,
  merge_record,
  query_records,
  query_records_wrapped,
  update_record,
)
from reverie.types.operators import eq
from reverie.types.record_id import RecordID


# Test models
class User(BaseModel):
  """Test user model."""

  name: str
  email: str
  age: int | None = None


class Product(BaseModel):
  """Test product model."""

  name: str
  price: float
  stock: int


# ============================================================================
# CREATE OPERATIONS TESTS
# ============================================================================


class TestCreateRecord:
  """Test suite for create_record function."""

  @pytest.mark.anyio
  async def test_create_record_with_dict(self, mock_db_client: DatabaseClient) -> None:
    """Test creating a record with dictionary data."""
    mock_db_client.create = AsyncMock(
      return_value={'id': 'user:1', 'name': 'John', 'email': 'john@example.com'}
    )

    result = await create_record(
      'user', {'name': 'John', 'email': 'john@example.com'}, client=mock_db_client
    )

    assert result == {'id': 'user:1', 'name': 'John', 'email': 'john@example.com'}
    mock_db_client.create.assert_called_once_with(
      'user', {'name': 'John', 'email': 'john@example.com'}
    )

  @pytest.mark.anyio
  async def test_create_record_with_pydantic_model(self, mock_db_client: DatabaseClient) -> None:
    """Test creating a record with Pydantic model."""
    user = User(name='Alice', email='alice@example.com', age=30)
    mock_db_client.create = AsyncMock(
      return_value={'id': 'user:alice', 'name': 'Alice', 'email': 'alice@example.com', 'age': 30}
    )

    result = await create_record('user', user, client=mock_db_client)

    assert result['name'] == 'Alice'
    assert result['email'] == 'alice@example.com'
    assert result['age'] == 30
    mock_db_client.create.assert_called_once()
    # Verify Pydantic model was serialized to dict for the create call
    call_args = mock_db_client.create.call_args[0]
    assert call_args[0] == 'user'
    assert isinstance(call_args[1], dict)
    assert call_args[1]['name'] == 'Alice'
    assert call_args[1]['email'] == 'alice@example.com'
    assert call_args[1]['age'] == 30

  @pytest.mark.anyio
  async def test_create_record_with_context(self, mock_db_client: DatabaseClient) -> None:
    """Test creating a record using context client."""
    mock_db_client.create = AsyncMock(return_value={'id': 'user:1', 'name': 'Bob'})

    with patch('reverie.query.crud.get_db', return_value=mock_db_client):
      result = await create_record('user', {'name': 'Bob'})

    assert result['name'] == 'Bob'
    mock_db_client.create.assert_called_once()

  @pytest.mark.anyio
  async def test_create_record_returns_id(self, mock_db_client: DatabaseClient) -> None:
    """Test that create_record returns record with ID."""
    mock_db_client.create = AsyncMock(return_value={'id': 'user:123', 'name': 'Test'})

    result = await create_record('user', {'name': 'Test'}, client=mock_db_client)

    assert 'id' in result
    assert result['id'] == 'user:123'


class TestCreateRecords:
  """Test suite for create_records function."""

  @pytest.mark.anyio
  async def test_create_records_with_dicts(self, mock_db_client: DatabaseClient) -> None:
    """Test creating multiple records with dictionaries."""
    mock_db_client.create = AsyncMock(
      side_effect=[{'id': 'user:1', 'name': 'Alice'}, {'id': 'user:2', 'name': 'Bob'}]
    )

    data = [{'name': 'Alice'}, {'name': 'Bob'}]
    results = await create_records('user', data, client=mock_db_client)

    assert len(results) == 2
    assert results[0]['name'] == 'Alice'
    assert results[1]['name'] == 'Bob'
    assert mock_db_client.create.call_count == 2

  @pytest.mark.anyio
  async def test_create_records_with_models(self, mock_db_client: DatabaseClient) -> None:
    """Test creating multiple records with Pydantic models."""
    mock_db_client.create = AsyncMock(
      side_effect=[
        {'id': 'user:1', 'name': 'Alice', 'email': 'alice@example.com'},
        {'id': 'user:2', 'name': 'Bob', 'email': 'bob@example.com'},
      ]
    )

    users = [
      User(name='Alice', email='alice@example.com'),
      User(name='Bob', email='bob@example.com'),
    ]
    results = await create_records('user', users, client=mock_db_client)

    assert len(results) == 2
    assert all('id' in r for r in results)

  @pytest.mark.anyio
  async def test_create_records_empty_list(self, mock_db_client: DatabaseClient) -> None:
    """Test creating records with empty list."""
    mock_db_client.create = AsyncMock()

    results = await create_records('user', [], client=mock_db_client)

    assert results == []
    mock_db_client.create.assert_not_called()


# ============================================================================
# READ OPERATIONS TESTS
# ============================================================================


class TestGetRecord:
  """Test suite for get_record function."""

  @pytest.mark.anyio
  async def test_get_record_with_string_id(self, mock_db_client: DatabaseClient) -> None:
    """Test fetching a record by string ID."""
    mock_db_client.select = AsyncMock(
      return_value=[{'id': 'user:alice', 'name': 'Alice', 'email': 'alice@example.com'}]
    )

    result = await get_record('user', 'alice', User, client=mock_db_client)

    assert result is not None
    assert result.name == 'Alice'
    assert result.email == 'alice@example.com'
    mock_db_client.select.assert_called_once_with('user:alice')

  @pytest.mark.anyio
  async def test_get_record_with_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Test fetching a record with RecordID instance."""
    record_id = RecordID(table='user', id='alice')
    mock_db_client.select = AsyncMock(
      return_value=[{'id': 'user:alice', 'name': 'Alice', 'email': 'alice@example.com'}]
    )

    result = await get_record('user', record_id, User, client=mock_db_client)

    assert result is not None
    assert result.name == 'Alice'
    mock_db_client.select.assert_called_once_with('user:alice')

  @pytest.mark.anyio
  async def test_get_record_not_found(self, mock_db_client: DatabaseClient) -> None:
    """Test fetching a non-existent record returns None."""
    mock_db_client.select = AsyncMock(return_value=[])

    result = await get_record('user', 'nonexistent', User, client=mock_db_client)

    assert result is None

  @pytest.mark.anyio
  async def test_get_record_empty_result(self, mock_db_client: DatabaseClient) -> None:
    """Test handling empty result from database."""
    mock_db_client.select = AsyncMock(return_value=None)

    result = await get_record('user', 'test', User, client=mock_db_client)

    assert result is None

  @pytest.mark.anyio
  async def test_get_record_deserializes_to_model(self, mock_db_client: DatabaseClient) -> None:
    """Test that get_record properly deserializes to model."""
    mock_db_client.select = AsyncMock(
      return_value=[{'id': 'user:1', 'name': 'Test', 'email': 'test@example.com', 'age': 25}]
    )

    result = await get_record('user', '1', User, client=mock_db_client)

    assert isinstance(result, User)
    assert result.name == 'Test'
    assert result.age == 25


class TestQueryRecords:
  """Test suite for query_records function."""

  @pytest.mark.anyio
  async def test_query_records_no_filters(self, mock_db_client: DatabaseClient) -> None:
    """Test querying records without filters."""
    with patch(
      'reverie.query.crud.fetch_all',
      AsyncMock(
        return_value=[
          User(name='Alice', email='alice@example.com'),
          User(name='Bob', email='bob@example.com'),
        ]
      ),
    ):
      results = await query_records('user', User, client=mock_db_client)

    assert len(results) == 2
    assert all(isinstance(u, User) for u in results)

  @pytest.mark.anyio
  async def test_query_records_with_conditions(self, mock_db_client: DatabaseClient) -> None:
    """Test querying records with WHERE conditions."""
    with patch(
      'reverie.query.crud.fetch_all',
      AsyncMock(return_value=[User(name='Alice', email='alice@example.com', age=30)]),
    ):
      results = await query_records(
        'user', User, conditions=['age > 18', eq('status', 'active')], client=mock_db_client
      )

    assert len(results) == 1
    assert results[0].age == 30

  @pytest.mark.anyio
  async def test_query_records_with_order_by(self, mock_db_client: DatabaseClient) -> None:
    """Test querying records with ORDER BY."""
    with patch(
      'reverie.query.crud.fetch_all',
      AsyncMock(
        return_value=[
          User(name='Alice', email='alice@example.com'),
          User(name='Bob', email='bob@example.com'),
        ]
      ),
    ):
      results = await query_records('user', User, order_by=('name', 'ASC'), client=mock_db_client)

    assert len(results) == 2

  @pytest.mark.anyio
  async def test_query_records_with_limit(self, mock_db_client: DatabaseClient) -> None:
    """Test querying records with LIMIT."""
    with patch(
      'reverie.query.crud.fetch_all',
      AsyncMock(return_value=[User(name='Alice', email='alice@example.com')]),
    ):
      results = await query_records('user', User, limit=1, client=mock_db_client)

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_query_records_with_offset(self, mock_db_client: DatabaseClient) -> None:
    """Test querying records with OFFSET."""
    with patch(
      'reverie.query.crud.fetch_all',
      AsyncMock(return_value=[User(name='Bob', email='bob@example.com')]),
    ):
      results = await query_records('user', User, offset=1, client=mock_db_client)

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_query_records_complex_query(self, mock_db_client: DatabaseClient) -> None:
    """Test complex query with multiple parameters."""
    with patch(
      'reverie.query.crud.fetch_all',
      AsyncMock(return_value=[User(name='Alice', email='alice@example.com', age=30)]),
    ):
      results = await query_records(
        'user',
        User,
        conditions=['age > 18', 'status = "active"'],
        order_by=('created_at', 'DESC'),
        limit=10,
        offset=5,
        client=mock_db_client,
      )

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_query_records_empty_result(self, mock_db_client: DatabaseClient) -> None:
    """Test querying with no matching records."""
    with patch('reverie.query.crud.fetch_all', AsyncMock(return_value=[])):
      results = await query_records('user', User, conditions=['age > 100'], client=mock_db_client)

    assert results == []


class TestQueryRecordsWrapped:
  """Test suite for query_records_wrapped function."""

  @pytest.mark.anyio
  async def test_query_records_wrapped_returns_list_result(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Test that query_records_wrapped returns ListResult."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(return_value=[User(name='Alice', email='alice@example.com')]),
    ):
      result = await query_records_wrapped('user', User, limit=10, offset=0, client=mock_db_client)

    assert hasattr(result, 'records')
    assert len(result.records) == 1
    assert result.limit == 10
    assert result.offset == 0


# ============================================================================
# UPDATE OPERATIONS TESTS
# ============================================================================


class TestUpdateRecord:
  """Test suite for update_record function."""

  @pytest.mark.anyio
  async def test_update_record_with_dict(self, mock_db_client: DatabaseClient) -> None:
    """Test updating a record with dictionary data."""
    mock_db_client.update = AsyncMock(
      return_value={'id': 'user:alice', 'name': 'Alice Updated', 'email': 'alice@example.com'}
    )

    result = await update_record('user', 'alice', {'name': 'Alice Updated'}, client=mock_db_client)

    assert result['name'] == 'Alice Updated'
    mock_db_client.update.assert_called_once_with('user:alice', {'name': 'Alice Updated'})

  @pytest.mark.anyio
  async def test_update_record_with_model(self, mock_db_client: DatabaseClient) -> None:
    """Test updating a record with Pydantic model."""
    user = User(name='Alice Updated', email='alice@example.com', age=31)
    mock_db_client.update = AsyncMock(
      return_value={
        'id': 'user:alice',
        'name': 'Alice Updated',
        'email': 'alice@example.com',
        'age': 31,
      }
    )

    result = await update_record('user', 'alice', user, client=mock_db_client)

    assert result['age'] == 31
    mock_db_client.update.assert_called_once()

  @pytest.mark.anyio
  async def test_update_record_with_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Test updating a record using RecordID."""
    record_id = RecordID(table='user', id='alice')
    mock_db_client.update = AsyncMock(return_value={'id': 'user:alice', 'status': 'active'})

    result = await update_record('user', record_id, {'status': 'active'}, client=mock_db_client)

    assert result['status'] == 'active'
    mock_db_client.update.assert_called_once_with('user:alice', {'status': 'active'})


class TestMergeRecord:
  """Test suite for merge_record function."""

  @pytest.mark.anyio
  async def test_merge_record_partial_update(self, mock_db_client: DatabaseClient) -> None:
    """Test merging partial data into a record."""
    mock_db_client.merge = AsyncMock(
      return_value={'id': 'user:alice', 'name': 'Alice', 'status': 'active'}
    )

    result = await merge_record('user', 'alice', {'status': 'active'}, client=mock_db_client)

    assert result['status'] == 'active'
    mock_db_client.merge.assert_called_once_with('user:alice', {'status': 'active'})

  @pytest.mark.anyio
  async def test_merge_record_with_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Test merging with RecordID."""
    record_id = RecordID(table='user', id='bob')
    mock_db_client.merge = AsyncMock(return_value={'id': 'user:bob', 'age': 26})

    result = await merge_record('user', record_id, {'age': 26}, client=mock_db_client)

    assert result['age'] == 26
    mock_db_client.merge.assert_called_once_with('user:bob', {'age': 26})

  @pytest.mark.anyio
  async def test_merge_record_preserves_other_fields(self, mock_db_client: DatabaseClient) -> None:
    """Test that merge sends only partial data while response contains all fields."""
    mock_db_client.merge = AsyncMock(
      return_value={'id': 'user:alice', 'name': 'Alice', 'email': 'alice@example.com', 'age': 31}
    )

    result = await merge_record('user', 'alice', {'age': 31}, client=mock_db_client)

    # Verify only partial data was sent in the merge call
    mock_db_client.merge.assert_called_once_with('user:alice', {'age': 31})
    # Response contains all fields including non-merged ones
    assert 'name' in result
    assert 'email' in result
    assert result['age'] == 31


# ============================================================================
# DELETE OPERATIONS TESTS
# ============================================================================


class TestDeleteRecord:
  """Test suite for delete_record function."""

  @pytest.mark.anyio
  async def test_delete_record_with_string_id(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting a record by string ID."""
    mock_db_client.delete = AsyncMock(return_value=None)

    await delete_record('user', 'alice', client=mock_db_client)

    mock_db_client.delete.assert_called_once_with('user:alice')

  @pytest.mark.anyio
  async def test_delete_record_with_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting a record with RecordID."""
    record_id = RecordID(table='user', id='bob')
    mock_db_client.delete = AsyncMock(return_value=None)

    await delete_record('user', record_id, client=mock_db_client)

    mock_db_client.delete.assert_called_once_with('user:bob')

  @pytest.mark.anyio
  async def test_delete_record_returns_none(self, mock_db_client: DatabaseClient) -> None:
    """Test that delete_record returns None."""
    mock_db_client.delete = AsyncMock(return_value=None)

    result = await delete_record('user', 'test', client=mock_db_client)

    assert result is None


class TestDeleteRecords:
  """Test suite for delete_records function."""

  @pytest.mark.anyio
  async def test_delete_records_with_condition(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting multiple records with condition."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    await delete_records('user', 'status = "inactive"', client=mock_db_client)

    mock_db_client.execute.assert_called_once()
    query = mock_db_client.execute.call_args[0][0]
    assert 'DELETE' in query
    assert 'user' in query
    assert 'WHERE' in query
    assert 'status = "inactive"' in query

  @pytest.mark.anyio
  async def test_delete_records_with_operator(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting records with Operator condition."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    await delete_records('user', eq('status', 'inactive'), client=mock_db_client)

    mock_db_client.execute.assert_called_once()
    query = mock_db_client.execute.call_args[0][0]
    assert 'DELETE' in query
    assert 'user' in query
    assert "status = 'inactive'" in query

  @pytest.mark.anyio
  async def test_delete_records_no_condition(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting all records without condition."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    await delete_records('user', client=mock_db_client)

    mock_db_client.execute.assert_called_once()
    query = mock_db_client.execute.call_args[0][0]
    assert 'DELETE' in query
    assert 'user' in query
    assert 'WHERE' not in query


# ============================================================================
# COUNT AND EXISTENCE TESTS
# ============================================================================


class TestCountRecords:
  """Test suite for count_records function."""

  @pytest.mark.anyio
  async def test_count_records_all(self, mock_db_client: DatabaseClient) -> None:
    """Test counting all records in a table."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 42}]}])

    count = await count_records('user', client=mock_db_client)

    assert count == 42

  @pytest.mark.anyio
  async def test_count_records_with_condition(self, mock_db_client: DatabaseClient) -> None:
    """Test counting records with condition."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 15}]}])

    count = await count_records('user', 'age > 18', client=mock_db_client)

    assert count == 15
    mock_db_client.execute.assert_called_once()
    query = mock_db_client.execute.call_args[0][0]
    assert 'SELECT count() FROM user' in query
    assert 'WHERE' in query
    assert 'age > 18' in query

  @pytest.mark.anyio
  async def test_count_records_with_operator(self, mock_db_client: DatabaseClient) -> None:
    """Test counting records with Operator condition."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 10}]}])

    count = await count_records('user', eq('status', 'active'), client=mock_db_client)

    assert count == 10
    mock_db_client.execute.assert_called_once()
    query = mock_db_client.execute.call_args[0][0]
    assert 'SELECT count() FROM user' in query
    assert "status = 'active'" in query

  @pytest.mark.anyio
  async def test_count_records_empty_result(self, mock_db_client: DatabaseClient) -> None:
    """Test counting with no matching records."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 0}]}])

    count = await count_records('user', 'age > 200', client=mock_db_client)

    assert count == 0

  @pytest.mark.anyio
  async def test_count_records_invalid_result_format(self, mock_db_client: DatabaseClient) -> None:
    """Test count_records handles invalid result format."""
    mock_db_client.execute = AsyncMock(return_value=[])

    count = await count_records('user', client=mock_db_client)

    assert count == 0


class TestExists:
  """Test suite for exists function."""

  @pytest.mark.anyio
  async def test_exists_record_found(self, mock_db_client: DatabaseClient) -> None:
    """Test exists returns True when record exists."""
    with patch(
      'reverie.query.crud.get_record',
      AsyncMock(return_value=User(name='Alice', email='alice@example.com')),
    ):
      result = await exists('user', 'alice', client=mock_db_client)

    assert result is True

  @pytest.mark.anyio
  async def test_exists_record_not_found(self, mock_db_client: DatabaseClient) -> None:
    """Test exists returns False when record doesn't exist."""
    with patch('reverie.query.crud.get_record', AsyncMock(return_value=None)):
      result = await exists('user', 'nonexistent', client=mock_db_client)

    assert result is False

  @pytest.mark.anyio
  async def test_exists_with_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Test exists with RecordID passes RecordID through correctly."""
    record_id = RecordID(table='user', id='alice')
    with patch(
      'reverie.query.crud.get_record',
      AsyncMock(return_value=User(name='Alice', email='alice@example.com')),
    ) as mock_get:
      result = await exists('user', record_id, client=mock_db_client)

    assert result is True
    # Verify RecordID was passed through correctly
    mock_get.assert_called_once()
    call_args = mock_get.call_args
    assert call_args[0][1] == record_id


# ============================================================================
# FIRST AND LAST TESTS
# ============================================================================


class TestFirst:
  """Test suite for first function."""

  @pytest.mark.anyio
  async def test_first_with_results(self, mock_db_client: DatabaseClient) -> None:
    """Test first returns first matching record."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(
        return_value=[
          User(name='Alice', email='alice@example.com'),
          User(name='Bob', email='bob@example.com'),
        ]
      ),
    ):
      result = await first('user', User, client=mock_db_client)

    assert result is not None
    assert result.name == 'Alice'

  @pytest.mark.anyio
  async def test_first_with_condition(self, mock_db_client: DatabaseClient) -> None:
    """Test first with WHERE condition."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(return_value=[User(name='Bob', email='bob@example.com', age=30)]),
    ):
      result = await first('user', User, condition='age > 25', client=mock_db_client)

    assert result is not None
    assert result.name == 'Bob'

  @pytest.mark.anyio
  async def test_first_with_order_by(self, mock_db_client: DatabaseClient) -> None:
    """Test first with ORDER BY."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(return_value=[User(name='Alice', email='alice@example.com')]),
    ):
      result = await first('user', User, order_by=('name', 'ASC'), client=mock_db_client)

    assert result is not None
    assert result.name == 'Alice'

  @pytest.mark.anyio
  async def test_first_no_results(self, mock_db_client: DatabaseClient) -> None:
    """Test first returns None when no records match."""
    with patch('reverie.query.crud.query_records', AsyncMock(return_value=[])):
      result = await first('user', User, condition='age > 200', client=mock_db_client)

    assert result is None


class TestLast:
  """Test suite for last function."""

  @pytest.mark.anyio
  async def test_last_with_results(self, mock_db_client: DatabaseClient) -> None:
    """Test last returns last matching record."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(return_value=[User(name='Charlie', email='charlie@example.com')]),
    ):
      result = await last('user', User, client=mock_db_client)

    assert result is not None
    assert result.name == 'Charlie'

  @pytest.mark.anyio
  async def test_last_reverses_order(self, mock_db_client: DatabaseClient) -> None:
    """Test that last reverses the order direction."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(return_value=[User(name='Zoe', email='zoe@example.com')]),
    ) as mock_query:
      await last('user', User, order_by=('name', 'ASC'), client=mock_db_client)

    # Verify order was reversed to DESC
    call_args = mock_query.call_args
    assert call_args.kwargs['order_by'] == ('name', 'DESC')

  @pytest.mark.anyio
  async def test_last_reverses_desc_to_asc(self, mock_db_client: DatabaseClient) -> None:
    """Test that last reverses DESC to ASC."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(return_value=[User(name='Alice', email='alice@example.com')]),
    ) as mock_query:
      await last('user', User, order_by=('created_at', 'DESC'), client=mock_db_client)

    # Verify order was reversed to ASC
    call_args = mock_query.call_args
    assert call_args.kwargs['order_by'] == ('created_at', 'ASC')

  @pytest.mark.anyio
  async def test_last_with_condition(self, mock_db_client: DatabaseClient) -> None:
    """Test last with WHERE condition is passed through to query_records."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(return_value=[User(name='Bob', email='bob@example.com')]),
    ) as mock_query:
      result = await last('user', User, condition='status = "active"', client=mock_db_client)

    assert result is not None
    # Verify condition was passed to query_records
    call_args = mock_query.call_args
    assert call_args.kwargs.get('conditions') is not None or 'status' in str(call_args)

  @pytest.mark.anyio
  async def test_last_no_results(self, mock_db_client: DatabaseClient) -> None:
    """Test last returns None when no records match."""
    with patch('reverie.query.crud.query_records', AsyncMock(return_value=[])):
      result = await last('user', User, client=mock_db_client)

    assert result is None

  @pytest.mark.anyio
  async def test_last_no_order_by(self, mock_db_client: DatabaseClient) -> None:
    """Test last without order_by parameter."""
    with patch(
      'reverie.query.crud.query_records',
      AsyncMock(return_value=[User(name='Test', email='test@example.com')]),
    ):
      result = await last('user', User, client=mock_db_client)

    assert result is not None


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


class TestIntegration:
  """Integration tests for CRUD operations."""

  @pytest.mark.anyio
  async def test_create_then_get_workflow(self, mock_db_client: DatabaseClient) -> None:
    """Test creating and then retrieving a record."""
    # Create
    mock_db_client.create = AsyncMock(
      return_value={'id': 'user:test', 'name': 'Test', 'email': 'test@example.com'}
    )
    created = await create_record(
      'user', {'name': 'Test', 'email': 'test@example.com'}, client=mock_db_client
    )

    # Get
    mock_db_client.select = AsyncMock(return_value=[created])
    fetched = await get_record('user', 'test', User, client=mock_db_client)

    assert fetched is not None
    assert fetched.name == 'Test'

  @pytest.mark.anyio
  async def test_create_update_workflow(self, mock_db_client: DatabaseClient) -> None:
    """Test creating and then updating a record."""
    # Create
    mock_db_client.create = AsyncMock(
      return_value={'id': 'user:test', 'name': 'Test', 'email': 'test@example.com'}
    )
    await create_record(
      'user', {'name': 'Test', 'email': 'test@example.com'}, client=mock_db_client
    )

    # Update
    mock_db_client.update = AsyncMock(
      return_value={'id': 'user:test', 'name': 'Updated', 'email': 'test@example.com'}
    )
    updated = await update_record(
      'user', 'test', {'name': 'Updated', 'email': 'test@example.com'}, client=mock_db_client
    )

    assert updated['name'] == 'Updated'

  @pytest.mark.anyio
  async def test_query_count_workflow(self, mock_db_client: DatabaseClient) -> None:
    """Test querying and counting records."""
    # Query
    with patch(
      'reverie.query.crud.fetch_all',
      AsyncMock(
        return_value=[
          User(name='Alice', email='alice@example.com'),
          User(name='Bob', email='bob@example.com'),
        ]
      ),
    ):
      results = await query_records('user', User, client=mock_db_client)

    # Count
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': len(results)}]}])
    count = await count_records('user', client=mock_db_client)

    assert count == len(results)
