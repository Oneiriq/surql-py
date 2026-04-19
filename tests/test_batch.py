"""Comprehensive tests for batch operations module.

This module tests batch operations including upsert_many, relate_many,
insert_many, and delete_many functions with various scenarios.
"""

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from surql.connection.client import DatabaseClient
from surql.query.batch import (
  build_relate_query,
  build_upsert_query,
  delete_many,
  insert_many,
  relate_many,
  upsert_many,
)


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
# UPSERT_MANY TESTS
# ============================================================================


class TestUpsertMany:
  """Test suite for upsert_many function."""

  @pytest.mark.anyio
  async def test_upsert_many_with_dicts(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting multiple records with dictionaries."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:1', 'name': 'Alice', 'age': 30},
            {'id': 'user:2', 'name': 'Bob', 'age': 25},
          ]
        }
      ]
    )

    items = [
      {'id': 'user:1', 'name': 'Alice', 'age': 30},
      {'id': 'user:2', 'name': 'Bob', 'age': 25},
    ]
    results = await upsert_many(mock_db_client, 'users', items)

    assert len(results) == 2
    assert results[0]['name'] == 'Alice'
    assert results[1]['name'] == 'Bob'
    mock_db_client.execute.assert_called_once()

  @pytest.mark.anyio
  async def test_upsert_many_with_pydantic_models(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting multiple records with Pydantic models."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:1', 'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
            {'id': 'user:2', 'name': 'Bob', 'email': 'bob@example.com', 'age': None},
          ]
        }
      ]
    )

    users = [
      User(name='Alice', email='alice@example.com', age=30),
      User(name='Bob', email='bob@example.com'),
    ]
    results = await upsert_many(mock_db_client, 'users', users)

    assert len(results) == 2
    mock_db_client.execute.assert_called_once()

    # After the v3 refactor (Oneiriq/surql-py#32) payloads flow through
    # the params dict, not the inlined SQL string. Verify both paths.
    query, params = mock_db_client.execute.call_args[0]
    assert 'UPSERT' in query and 'CONTENT' in query
    payloads = [params[k] for k in sorted(params) if k.startswith('item_')]
    assert any(p.get('name') == 'Alice' for p in payloads)
    assert any(p.get('email') == 'alice@example.com' for p in payloads)
    assert any(p.get('name') == 'Bob' for p in payloads)

  @pytest.mark.anyio
  async def test_upsert_many_empty_list(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting empty list returns empty list."""
    mock_db_client.execute = AsyncMock()

    results = await upsert_many(mock_db_client, 'users', [])

    assert results == []
    mock_db_client.execute.assert_not_called()

  @pytest.mark.anyio
  async def test_upsert_many_with_conflict_fields(self, mock_db_client: DatabaseClient) -> None:
    """Test upsert with conflict fields generates correct query."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:1', 'name': 'Alice', 'email': 'alice@example.com'}]}]
    )

    items = [{'email': 'alice@example.com', 'name': 'Alice'}]
    await upsert_many(mock_db_client, 'users', items, conflict_fields=['email'])

    # After the v3 refactor each record is upserted individually; the
    # conflict WHERE clause still appears on each emitted statement but
    # it references the per-record bind `$item_<idx>.<field>`.
    call_args = mock_db_client.execute.call_args
    query = call_args[0][0]
    assert 'WHERE' in query
    assert 'email = $item_0.email' in query

  @pytest.mark.anyio
  async def test_upsert_many_with_context_client(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting using context client."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:1', 'name': 'Test'}]}]
    )

    with patch('surql.query.batch.get_db', return_value=mock_db_client):
      results = await upsert_many(None, 'users', [{'name': 'Test'}])

    assert len(results) == 1
    mock_db_client.execute.assert_called_once()

  @pytest.mark.anyio
  async def test_upsert_many_invalid_table_name(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting with empty table name raises error."""
    with pytest.raises(ValueError, match='Table name is required'):
      await upsert_many(mock_db_client, '', [{'name': 'Test'}])

  @pytest.mark.anyio
  async def test_upsert_many_nested_data(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting records with nested data structures."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:1', 'name': 'Alice', 'metadata': {'role': 'admin', 'tags': ['active']}}
          ]
        }
      ]
    )

    items = [{'name': 'Alice', 'metadata': {'role': 'admin', 'tags': ['active']}}]
    results = await upsert_many(mock_db_client, 'users', items)

    assert len(results) == 1

    # After the v3 refactor, nested structures flow through the params
    # dict rather than being inlined. Check the bound payload instead.
    _, params = mock_db_client.execute.call_args[0]
    payload = params['item_0']
    assert payload['metadata']['role'] == 'admin'
    assert payload['metadata']['tags'] == ['active']

  @pytest.mark.anyio
  async def test_upsert_many_handles_empty_result(self, mock_db_client: DatabaseClient) -> None:
    """Test upsert handles database returning empty result."""
    mock_db_client.execute = AsyncMock(return_value=[])

    results = await upsert_many(mock_db_client, 'users', [{'name': 'Test'}])

    assert results == []


# ============================================================================
# RELATE_MANY TESTS
# ============================================================================


class TestRelateMany:
  """Test suite for relate_many function."""

  @pytest.mark.anyio
  async def test_relate_many_basic(self, mock_db_client: DatabaseClient) -> None:
    """Test creating multiple relationships."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {'result': [{'id': 'knows:1', 'in': 'person:bob', 'out': 'person:alice'}]},
        {'result': [{'id': 'knows:2', 'in': 'person:charlie', 'out': 'person:alice'}]},
      ]
    )

    relations = [
      ('person:alice', 'person:bob', None),
      ('person:alice', 'person:charlie', None),
    ]
    results = await relate_many(mock_db_client, 'person', 'knows', 'person', relations)

    assert len(results) == 2
    mock_db_client.execute.assert_called_once()

    # Verify RELATE statements in query
    query = mock_db_client.execute.call_args[0][0]
    assert 'RELATE' in query
    assert 'person:alice->knows->person:bob' in query
    assert 'person:alice->knows->person:charlie' in query

  @pytest.mark.anyio
  async def test_relate_many_with_edge_data(self, mock_db_client: DatabaseClient) -> None:
    """Test creating relationships with edge data."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'knows:1', 'in': 'person:bob', 'out': 'person:alice', 'since': '2024-01-01'}
          ]
        },
        {
          'result': [
            {'id': 'knows:2', 'in': 'person:charlie', 'out': 'person:alice', 'strength': 0.8}
          ]
        },
      ]
    )

    relations = [
      ('person:alice', 'person:bob', {'since': '2024-01-01'}),
      ('person:alice', 'person:charlie', {'strength': 0.8}),
    ]
    results = await relate_many(mock_db_client, 'person', 'knows', 'person', relations)

    assert len(results) == 2
    # Verify the query contains SET clause with edge data
    call_args = mock_db_client.execute.call_args
    query = call_args[0][0]
    assert 'SET' in query
    assert 'since' in query
    assert 'strength' in query

    # Verify SET clauses with correct value formatting
    assert "since = '2024-01-01'" in query or 'since = "2024-01-01"' in query
    assert 'strength = 0.8' in query

  @pytest.mark.anyio
  async def test_relate_many_empty_list(self, mock_db_client: DatabaseClient) -> None:
    """Test relating empty list returns empty list."""
    mock_db_client.execute = AsyncMock()

    results = await relate_many(mock_db_client, 'person', 'knows', 'person', [])

    assert results == []
    mock_db_client.execute.assert_not_called()

  @pytest.mark.anyio
  async def test_relate_many_invalid_edge_name(self, mock_db_client: DatabaseClient) -> None:
    """Test relating with empty edge name raises error."""
    relations = [('person:alice', 'person:bob', None)]
    with pytest.raises(ValueError, match='Edge table name is required'):
      await relate_many(mock_db_client, 'person', '', 'person', relations)

  @pytest.mark.anyio
  async def test_relate_many_with_context_client(self, mock_db_client: DatabaseClient) -> None:
    """Test relating using context client."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'knows:1', 'in': 'person:bob', 'out': 'person:alice'}]}]
    )

    relations = [('person:alice', 'person:bob', None)]
    with patch('surql.query.batch.get_db', return_value=mock_db_client):
      results = await relate_many(None, 'person', 'knows', 'person', relations)

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_relate_many_mixed_data(self, mock_db_client: DatabaseClient) -> None:
    """Test creating relationships with mixed data presence."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {'result': [{'id': 'knows:1'}]},
        {'result': [{'id': 'knows:2', 'since': '2024-01-01'}]},
        {'result': [{'id': 'knows:3'}]},
      ]
    )

    relations = [
      ('person:alice', 'person:bob', None),  # No data
      ('person:alice', 'person:charlie', {'since': '2024-01-01'}),  # With data
      ('person:bob', 'person:charlie', None),  # No data
    ]
    results = await relate_many(mock_db_client, 'person', 'knows', 'person', relations)

    assert len(results) == 3

    query = mock_db_client.execute.call_args[0][0]
    # Verify query contains all three RELATE statements
    assert query.count('RELATE') == 3

  @pytest.mark.anyio
  async def test_relate_many_with_nested_edge_data(self, mock_db_client: DatabaseClient) -> None:
    """Test creating relationships with nested data in edge."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'knows:1', 'metadata': {'context': 'work'}}]}]
    )

    relations = [('person:alice', 'person:bob', {'metadata': {'context': 'work'}})]
    results = await relate_many(mock_db_client, 'person', 'knows', 'person', relations)

    assert len(results) == 1


# ============================================================================
# INSERT_MANY TESTS
# ============================================================================


class TestInsertMany:
  """Test suite for insert_many function."""

  @pytest.mark.anyio
  async def test_insert_many_with_dicts(self, mock_db_client: DatabaseClient) -> None:
    """Test inserting multiple records with dictionaries."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:1', 'name': 'Alice', 'email': 'alice@example.com'},
            {'id': 'user:2', 'name': 'Bob', 'email': 'bob@example.com'},
          ]
        }
      ]
    )

    items = [
      {'name': 'Alice', 'email': 'alice@example.com'},
      {'name': 'Bob', 'email': 'bob@example.com'},
    ]
    results = await insert_many(mock_db_client, 'users', items)

    assert len(results) == 2
    mock_db_client.execute.assert_called_once()

  @pytest.mark.anyio
  async def test_insert_many_with_pydantic_models(self, mock_db_client: DatabaseClient) -> None:
    """Test inserting multiple records with Pydantic models."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:1', 'name': 'Alice', 'email': 'alice@example.com'},
          ]
        }
      ]
    )

    users = [User(name='Alice', email='alice@example.com')]
    results = await insert_many(mock_db_client, 'users', users)

    assert len(results) == 1

    # Verify model data appears in the INSERT query
    query = mock_db_client.execute.call_args[0][0]
    assert 'INSERT INTO users' in query
    assert 'Alice' in query
    assert 'alice@example.com' in query

  @pytest.mark.anyio
  async def test_insert_many_empty_list(self, mock_db_client: DatabaseClient) -> None:
    """Test inserting empty list returns empty list."""
    mock_db_client.execute = AsyncMock()

    results = await insert_many(mock_db_client, 'users', [])

    assert results == []
    mock_db_client.execute.assert_not_called()

  @pytest.mark.anyio
  async def test_insert_many_invalid_table_name(self, mock_db_client: DatabaseClient) -> None:
    """Test inserting with empty table name raises error."""
    with pytest.raises(ValueError, match='Table name is required'):
      await insert_many(mock_db_client, '', [{'name': 'Test'}])

  @pytest.mark.anyio
  async def test_insert_many_with_context_client(self, mock_db_client: DatabaseClient) -> None:
    """Test inserting using context client."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:1', 'name': 'Test'}]}]
    )

    with patch('surql.query.batch.get_db', return_value=mock_db_client):
      results = await insert_many(None, 'users', [{'name': 'Test'}])

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_insert_many_generates_correct_query(self, mock_db_client: DatabaseClient) -> None:
    """Test insert generates INSERT INTO query."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    await insert_many(mock_db_client, 'users', [{'name': 'Test'}])

    call_args = mock_db_client.execute.call_args
    query = call_args[0][0]
    assert 'INSERT INTO users' in query


# ============================================================================
# DELETE_MANY TESTS
# ============================================================================


class TestDeleteMany:
  """Test suite for delete_many function."""

  @pytest.mark.anyio
  async def test_delete_many_with_simple_ids(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting multiple records with simple IDs."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        [{'result': [{'id': 'user:1', 'name': 'Alice'}]}],
        [{'result': [{'id': 'user:2', 'name': 'Bob'}]}],
      ]
    )

    results = await delete_many(mock_db_client, 'user', ['1', '2'])

    assert len(results) == 2
    assert mock_db_client.execute.call_count == 2

    # Verify DELETE queries were built with correct record IDs
    calls = mock_db_client.execute.call_args_list
    all_queries = ' '.join(c[0][0] for c in calls)
    assert 'DELETE' in all_queries
    assert 'user:1' in all_queries
    assert 'user:2' in all_queries

  @pytest.mark.anyio
  async def test_delete_many_with_full_record_ids(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting multiple records with full record IDs."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        [{'result': [{'id': 'user:alice', 'name': 'Alice'}]}],
        [{'result': [{'id': 'user:bob', 'name': 'Bob'}]}],
      ]
    )

    results = await delete_many(mock_db_client, 'user', ['user:alice', 'user:bob'])

    assert len(results) == 2

  @pytest.mark.anyio
  async def test_delete_many_empty_list(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting empty list returns empty list."""
    mock_db_client.execute = AsyncMock()

    results = await delete_many(mock_db_client, 'user', [])

    assert results == []
    mock_db_client.execute.assert_not_called()

  @pytest.mark.anyio
  async def test_delete_many_invalid_table_name(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting with empty table name raises error."""
    with pytest.raises(ValueError, match='Table name is required'):
      await delete_many(mock_db_client, '', ['1'])

  @pytest.mark.anyio
  async def test_delete_many_with_context_client(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting using context client."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:1', 'name': 'Test'}]}]
    )

    with patch('surql.query.batch.get_db', return_value=mock_db_client):
      results = await delete_many(None, 'user', ['1'])

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_delete_many_returns_before_state(self, mock_db_client: DatabaseClient) -> None:
    """Test delete_many returns records before deletion."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:1', 'name': 'Alice', 'status': 'active'}]}]
    )

    results = await delete_many(mock_db_client, 'user', ['1'])

    # Verify results contain the deleted record
    assert len(results) == 1
    assert results[0]['name'] == 'Alice'

    # Verify RETURN BEFORE is in the query
    call_args = mock_db_client.execute.call_args
    query = call_args[0][0]
    assert 'RETURN BEFORE' in query

  @pytest.mark.anyio
  async def test_delete_many_nonexistent_records(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting nonexistent records returns empty results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    results = await delete_many(mock_db_client, 'user', ['nonexistent'])

    assert results == []


# ============================================================================
# BUILD_UPSERT_QUERY TESTS
# ============================================================================


class TestBuildUpsertQuery:
  """Test suite for build_upsert_query function."""

  def test_build_upsert_query_basic(self) -> None:
    """Test building basic upsert query.

    Post Oneiriq/surql-py#32 the builder emits one ``UPSERT <target>
    CONTENT {...}`` per record rather than the v2 ``UPSERT INTO table
    [...]`` array form (rejected by v3).
    """
    items = [{'id': 'user:1', 'name': 'Alice'}]
    query = build_upsert_query('users', items)

    assert query.startswith('UPSERT user:1 CONTENT')
    assert 'Alice' in query
    # ``id`` should be stripped from the per-record CONTENT payload:
    # v3 rejects a redundant ``id`` field when targeting a specific
    # record.
    assert 'id:' not in query.replace('user:1', '')

  def test_build_upsert_query_multiple_items(self) -> None:
    """Test building upsert query with multiple items."""
    items = [
      {'id': 'user:1', 'name': 'Alice'},
      {'id': 'user:2', 'name': 'Bob'},
    ]
    query = build_upsert_query('users', items)

    assert 'UPSERT user:1 CONTENT' in query
    assert 'UPSERT user:2 CONTENT' in query
    assert 'Alice' in query
    assert 'Bob' in query

  def test_build_upsert_query_with_conflict_fields(self) -> None:
    """Test building upsert query with conflict fields."""
    items = [{'email': 'alice@example.com', 'name': 'Alice'}]
    query = build_upsert_query('users', items, conflict_fields=['email'])

    assert 'WHERE' in query
    assert 'email = $item.email' in query

  def test_build_upsert_query_empty_list(self) -> None:
    """Test building upsert query with empty list returns empty string."""
    query = build_upsert_query('users', [])

    assert query == ''

  def test_build_upsert_query_with_nested_data(self) -> None:
    """Test building upsert query with nested data structures."""
    items = [{'name': 'Alice', 'metadata': {'role': 'admin'}}]
    query = build_upsert_query('users', items)

    assert 'metadata' in query
    assert 'admin' in query
    assert '"role"' in query or "'role'" in query

  def test_build_upsert_query_with_null_values(self) -> None:
    """Test building upsert query with null values."""
    items = [{'name': 'Alice', 'age': None}]
    query = build_upsert_query('users', items)

    assert 'NULL' in query

  def test_build_upsert_query_with_boolean_values(self) -> None:
    """Test building upsert query with boolean values."""
    items = [{'name': 'Alice', 'active': True, 'deleted': False}]
    query = build_upsert_query('users', items)

    assert 'true' in query
    assert 'false' in query


# ============================================================================
# BUILD_RELATE_QUERY TESTS
# ============================================================================


class TestBuildRelateQuery:
  """Test suite for build_relate_query function."""

  def test_build_relate_query_basic(self) -> None:
    """Test building basic relate query."""
    query = build_relate_query('person:alice', 'knows', 'person:bob')

    assert query == 'RELATE person:alice->knows->person:bob;'

  def test_build_relate_query_with_data(self) -> None:
    """Test building relate query with edge data."""
    query = build_relate_query('person:alice', 'knows', 'person:bob', {'since': '2024-01-01'})

    assert 'RELATE person:alice->knows->person:bob' in query
    assert 'SET' in query
    assert "since = '2024-01-01'" in query or 'since = "2024-01-01"' in query

  def test_build_relate_query_with_numeric_data(self) -> None:
    """Test building relate query with numeric edge data."""
    query = build_relate_query('person:alice', 'knows', 'person:bob', {'strength': 0.8})

    assert 'strength = 0.8' in query

  def test_build_relate_query_with_boolean_data(self) -> None:
    """Test building relate query with boolean edge data."""
    query = build_relate_query('person:alice', 'knows', 'person:bob', {'mutual': True})

    assert 'mutual = true' in query

  def test_build_relate_query_with_nested_data(self) -> None:
    """Test building relate query with nested edge data."""
    query = build_relate_query(
      'person:alice', 'knows', 'person:bob', {'metadata': {'context': 'work'}}
    )

    assert 'SET' in query
    assert 'metadata' in query
    # Verify nested object is JSON-formatted
    assert '{' in query
    assert 'context' in query
    assert 'work' in query

  def test_build_relate_query_with_multiple_fields(self) -> None:
    """Test building relate query with multiple edge data fields."""
    query = build_relate_query(
      'person:alice',
      'knows',
      'person:bob',
      {'since': '2024-01-01', 'strength': 0.9, 'mutual': True},
    )

    assert 'since' in query
    assert 'strength' in query
    assert 'mutual' in query


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================


class TestErrorHandling:
  """Test suite for error handling in batch operations."""

  @pytest.mark.anyio
  async def test_upsert_many_handles_db_error(self, mock_db_client: DatabaseClient) -> None:
    """Test upsert_many propagates database errors."""
    mock_db_client.execute = AsyncMock(side_effect=Exception('Database error'))

    with pytest.raises(Exception, match='Database error'):
      await upsert_many(mock_db_client, 'users', [{'name': 'Test'}])

  @pytest.mark.anyio
  async def test_relate_many_handles_db_error(self, mock_db_client: DatabaseClient) -> None:
    """Test relate_many propagates database errors."""
    mock_db_client.execute = AsyncMock(side_effect=Exception('Connection failed'))

    with pytest.raises(Exception, match='Connection failed'):
      await relate_many(
        mock_db_client, 'person', 'knows', 'person', [('person:a', 'person:b', None)]
      )

  @pytest.mark.anyio
  async def test_insert_many_handles_db_error(self, mock_db_client: DatabaseClient) -> None:
    """Test insert_many propagates database errors."""
    mock_db_client.execute = AsyncMock(side_effect=Exception('Insert failed'))

    with pytest.raises(Exception, match='Insert failed'):
      await insert_many(mock_db_client, 'users', [{'name': 'Test'}])

  @pytest.mark.anyio
  async def test_delete_many_handles_db_error(self, mock_db_client: DatabaseClient) -> None:
    """Test delete_many propagates database errors."""
    mock_db_client.execute = AsyncMock(side_effect=Exception('Delete failed'))

    with pytest.raises(Exception, match='Delete failed'):
      await delete_many(mock_db_client, 'user', ['1'])


# ============================================================================
# EDGE CASE TESTS
# ============================================================================


class TestEdgeCases:
  """Test suite for edge cases in batch operations."""

  @pytest.mark.anyio
  async def test_upsert_many_single_item(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting a single item works correctly."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:1', 'name': 'Alice'}]}]
    )

    results = await upsert_many(mock_db_client, 'users', [{'name': 'Alice'}])

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_upsert_many_special_characters(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting records with special characters."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:1', 'name': "O'Brien", 'bio': 'Line1\nLine2'}]}]
    )

    items = [{'name': "O'Brien", 'bio': 'Line1\nLine2'}]
    results = await upsert_many(mock_db_client, 'users', items)

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_relate_many_single_relation(self, mock_db_client: DatabaseClient) -> None:
    """Test relating a single pair works correctly."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'id': 'knows:1'}]}])

    relations = [('person:alice', 'person:bob', None)]
    results = await relate_many(mock_db_client, 'person', 'knows', 'person', relations)

    assert len(results) == 1

  @pytest.mark.anyio
  async def test_delete_many_mixed_ids(self, mock_db_client: DatabaseClient) -> None:
    """Test deleting with mixed ID formats."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        [{'result': [{'id': 'user:1'}]}],  # Simple ID
        [{'result': [{'id': 'user:alice'}]}],  # Full record ID
      ]
    )

    results = await delete_many(mock_db_client, 'user', ['1', 'user:alice'])

    assert len(results) == 2

  @pytest.mark.anyio
  async def test_upsert_many_large_batch(self, mock_db_client: DatabaseClient) -> None:
    """Test upserting a large batch of items."""
    # Create 100 items
    items = [{'id': f'user:{i}', 'name': f'User{i}'} for i in range(100)]
    expected_results = [{'id': f'user:{i}', 'name': f'User{i}'} for i in range(100)]

    mock_db_client.execute = AsyncMock(return_value=[{'result': expected_results}])

    results = await upsert_many(mock_db_client, 'users', items)

    assert len(results) == 100
    mock_db_client.execute.assert_called_once()

  def test_build_upsert_query_with_array_values(self) -> None:
    """Test building upsert query with array values."""
    items = [{'name': 'Alice', 'tags': ['admin', 'active']}]
    query = build_upsert_query('users', items)

    assert 'tags' in query
    assert '[' in query  # Array bracket notation
    assert 'admin' in query
    assert 'active' in query

  def test_build_relate_query_with_array_in_data(self) -> None:
    """Test building relate query with array in edge data."""
    query = build_relate_query(
      'person:alice', 'knows', 'person:bob', {'contexts': ['work', 'social']}
    )

    assert 'SET' in query
    assert 'contexts' in query
    assert '[' in query  # Array bracket notation
    assert 'work' in query
    assert 'social' in query
