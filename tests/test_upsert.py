"""Tests for upsert functionality in query builder and crud module."""

from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from reverie.connection.client import DatabaseClient
from reverie.query.builder import Query, upsert
from reverie.query.crud import upsert_record
from reverie.types.record_id import RecordID


class User(BaseModel):
  """Test user model."""

  name: str
  status: str


class TestQueryUpsertMethod:
  """Tests for Query.upsert() method."""

  def test_upsert_generates_valid_sql_with_record_id(self) -> None:
    """Generates UPSERT SQL with record ID target."""
    sql = Query().upsert('user:alice', {'name': 'Alice'}).to_surql()

    assert sql.startswith('UPSERT user:alice CONTENT')
    assert '"name"' in sql or "'name'" in sql or 'name' in sql

  def test_upsert_generates_valid_sql_without_record_id(self) -> None:
    """Generates UPSERT SQL with table-only target."""
    sql = Query().upsert('user', {'name': 'Bob'}).to_surql()

    assert sql.startswith('UPSERT user CONTENT')

  def test_upsert_generates_content_sql(self) -> None:
    """to_surql produces UPSERT ... CONTENT ... statement."""
    query = Query().upsert('user:alice', {'name': 'Alice'})
    sql = query.to_surql()

    assert sql.startswith('UPSERT user:alice CONTENT')
    assert 'name' in sql

  def test_upsert_with_where_condition(self) -> None:
    """Chained .where() appends WHERE clause to upsert SQL."""
    query = Query().upsert('user', {'status': 'active'}).where('email = "alice@example.com"')
    sql = query.to_surql()

    assert 'WHERE' in sql
    assert 'email = "alice@example.com"' in sql

  def test_upsert_with_return_none(self) -> None:
    """Chained .return_none() appends RETURN NONE."""
    query = Query().upsert('user:alice', {'name': 'Alice'}).return_none()
    sql = query.to_surql()

    assert 'RETURN NONE' in sql

  def test_upsert_with_return_after(self) -> None:
    """Chained .return_after() appends RETURN AFTER."""
    query = Query().upsert('user:alice', {'name': 'Alice'}).return_after()
    sql = query.to_surql()

    assert 'RETURN AFTER' in sql

  def test_upsert_immutability(self) -> None:
    """Returns a new Query instance, leaving original unchanged."""
    original = Query()
    after = original.upsert('user:alice', {'name': 'Alice'})

    assert original.operation is None
    assert after.operation == 'UPSERT'

  def test_upsert_invalid_table_name_raises(self) -> None:
    """Raises ValueError for invalid table name (SQL injection attempt)."""
    with pytest.raises(ValueError):
      Query().upsert('user; DROP TABLE user', {'name': 'Alice'})

  def test_upsert_invalid_field_name_raises(self) -> None:
    """Raises ValueError for invalid field name (SQL injection attempt)."""
    with pytest.raises(ValueError):
      Query().upsert('user:alice', {'na me; DROP TABLE': 'value'})

  def test_upsert_multiple_fields_in_sql(self) -> None:
    """All provided fields appear in the generated CONTENT block."""
    query = Query().upsert('user:alice', {'name': 'Alice', 'status': 'active', 'age': 30})
    sql = query.to_surql()

    assert 'name' in sql
    assert 'status' in sql
    assert 'age' in sql


class TestUpsertFreeFunction:
  """Tests for the upsert() free function in builder module."""

  def test_upsert_free_function_generates_valid_sql(self) -> None:
    """Generates valid UPSERT SQL with all provided fields."""
    sql = upsert('user:bob', {'name': 'Bob', 'status': 'pending'}).to_surql()

    assert 'UPSERT user:bob CONTENT' in sql
    assert 'name' in sql
    assert 'status' in sql

  def test_upsert_free_function_invalid_table_raises(self) -> None:
    """Raises ValueError for invalid table name."""
    with pytest.raises(ValueError):
      upsert('1invalid', {'name': 'test'})


class TestUpsertRecord:
  """Tests for upsert_record crud function."""

  @pytest.mark.anyio
  async def test_upsert_record_calls_execute(self, mock_db_client: DatabaseClient) -> None:
    """Calls db.execute with UPSERT SQL."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:alice', 'name': 'Alice', 'status': 'active'}]}]
    )

    await upsert_record(
      'user', 'alice', {'name': 'Alice', 'status': 'active'}, client=mock_db_client
    )

    mock_db_client.execute.assert_called_once()
    call_args = mock_db_client.execute.call_args[0][0]
    assert 'UPSERT user:alice CONTENT' in call_args

  @pytest.mark.anyio
  async def test_upsert_record_with_string_id(self, mock_db_client: DatabaseClient) -> None:
    """Builds correct target from table + string record_id."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:alice', 'name': 'Alice'}]}]
    )

    await upsert_record('user', 'alice', {'name': 'Alice'}, client=mock_db_client)

    call_sql = mock_db_client.execute.call_args[0][0]
    assert 'user:alice' in call_sql

  @pytest.mark.anyio
  async def test_upsert_record_with_record_id(self, mock_db_client: DatabaseClient) -> None:
    """Accepts RecordID instance as record_id."""
    record_id = RecordID(table='user', id='alice')
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:alice', 'name': 'Alice'}]}]
    )

    await upsert_record('user', record_id, {'name': 'Alice'}, client=mock_db_client)

    call_sql = mock_db_client.execute.call_args[0][0]
    assert 'user:alice' in call_sql

  @pytest.mark.anyio
  async def test_upsert_record_with_pydantic_model(self, mock_db_client: DatabaseClient) -> None:
    """Accepts a Pydantic model as data and converts to dict."""
    user = User(name='Alice', status='active')
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:alice', 'name': 'Alice', 'status': 'active'}]}]
    )

    await upsert_record('user', 'alice', user, client=mock_db_client)

    call_sql = mock_db_client.execute.call_args[0][0]
    assert 'name' in call_sql
    assert 'status' in call_sql

  @pytest.mark.anyio
  async def test_upsert_record_returns_dict_from_result(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """Returns the first record from the result list."""
    expected = {'id': 'user:alice', 'name': 'Alice', 'status': 'active'}
    mock_db_client.execute = AsyncMock(return_value=[{'result': [expected]}])

    result = await upsert_record(
      'user', 'alice', {'name': 'Alice', 'status': 'active'}, client=mock_db_client
    )

    assert result == expected

  @pytest.mark.anyio
  async def test_upsert_record_returns_dict_direct(self, mock_db_client: DatabaseClient) -> None:
    """Handles result that is a list of dicts directly (no 'result' key)."""
    expected = {'id': 'user:alice', 'name': 'Alice'}
    mock_db_client.execute = AsyncMock(return_value=[expected])

    result = await upsert_record('user', 'alice', {'name': 'Alice'}, client=mock_db_client)

    assert result == expected


class TestUpsertSqlInjectionPrevention:
  """Tests verifying SQL injection prevention in upsert operations."""

  def test_semicolon_in_table_name_blocked(self) -> None:
    """Blocks semicolon in table name."""
    with pytest.raises(ValueError):
      Query().upsert('user; DROP TABLE user--', {'name': 'x'})

  def test_space_in_table_name_blocked(self) -> None:
    """Blocks space in table name."""
    with pytest.raises(ValueError):
      Query().upsert('user name', {'name': 'x'})

  def test_special_chars_in_table_name_blocked(self) -> None:
    """Blocks special characters in table name."""
    with pytest.raises(ValueError):
      Query().upsert("user'--", {'name': 'x'})

  def test_digit_start_in_table_name_blocked(self) -> None:
    """Blocks table name starting with a digit."""
    with pytest.raises(ValueError):
      Query().upsert('1user', {'name': 'x'})

  def test_semicolon_in_field_name_blocked(self) -> None:
    """Blocks semicolon in field name."""
    with pytest.raises(ValueError):
      Query().upsert('user:alice', {'field; DROP TABLE user': 'val'})

  def test_free_function_invalid_field_blocked(self) -> None:
    """Free upsert function also validates field names."""
    with pytest.raises(ValueError):
      upsert('user', {'bad field!': 'val'})
