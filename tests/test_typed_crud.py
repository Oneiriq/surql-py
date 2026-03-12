"""Tests for typed CRUD operations module.

Validates that the typed wrappers correctly delegate to lower-level CRUD
functions and return validated Pydantic model instances.
"""

from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, ValidationError

from reverie.connection.client import DatabaseClient
from reverie.query.typed import (
  create_typed,
  get_typed,
  query_typed,
  update_typed,
  upsert_typed,
)

# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------


class User(BaseModel):
  """Test user model."""

  name: str
  email: str
  age: int


# ---------------------------------------------------------------------------
# create_typed
# ---------------------------------------------------------------------------


class TestCreateTyped:
  """Test suite for create_typed."""

  @pytest.mark.anyio
  async def test_returns_validated_model(self, mock_db_client: DatabaseClient) -> None:
    """create_typed should return a validated model instance."""
    mock_db_client.create = AsyncMock(
      return_value={'id': 'user:1', 'name': 'Alice', 'email': 'a@b.com', 'age': 30}
    )
    user = User(name='Alice', email='a@b.com', age=30)

    result = await create_typed('user', user, client=mock_db_client)

    assert isinstance(result, User)
    assert result.name == 'Alice'
    assert result.email == 'a@b.com'
    assert result.age == 30

  @pytest.mark.anyio
  async def test_passes_data_to_create_record(self, mock_db_client: DatabaseClient) -> None:
    """create_typed should delegate to create_record with model data."""
    mock_db_client.create = AsyncMock(
      return_value={'name': 'Bob', 'email': 'bob@test.com', 'age': 25}
    )
    user = User(name='Bob', email='bob@test.com', age=25)

    await create_typed('user', user, client=mock_db_client)

    mock_db_client.create.assert_called_once_with(
      'user', {'name': 'Bob', 'email': 'bob@test.com', 'age': 25}
    )

  @pytest.mark.anyio
  async def test_validation_error_for_invalid_response(
    self, mock_db_client: DatabaseClient
  ) -> None:
    """create_typed should raise ValidationError when response is invalid."""
    mock_db_client.create = AsyncMock(
      return_value={'name': 'Alice', 'email': 'a@b.com'}  # missing 'age'
    )
    user = User(name='Alice', email='a@b.com', age=30)

    with pytest.raises(ValidationError):
      await create_typed('user', user, client=mock_db_client)


# ---------------------------------------------------------------------------
# get_typed
# ---------------------------------------------------------------------------


class TestGetTyped:
  """Test suite for get_typed."""

  @pytest.mark.anyio
  async def test_returns_model_for_existing_record(self, mock_db_client: DatabaseClient) -> None:
    """get_typed should return a model when the record exists."""
    mock_db_client.select = AsyncMock(
      return_value=[{'id': 'user:alice', 'name': 'Alice', 'email': 'a@b.com', 'age': 30}]
    )

    result = await get_typed('user', 'alice', User, client=mock_db_client)

    assert result is not None
    assert isinstance(result, User)
    assert result.name == 'Alice'

  @pytest.mark.anyio
  async def test_returns_none_for_missing_record(self, mock_db_client: DatabaseClient) -> None:
    """get_typed should return None when the record does not exist."""
    mock_db_client.select = AsyncMock(return_value=None)

    result = await get_typed('user', 'missing', User, client=mock_db_client)

    assert result is None

  @pytest.mark.anyio
  async def test_returns_none_for_empty_list(self, mock_db_client: DatabaseClient) -> None:
    """get_typed should return None when select returns an empty list."""
    mock_db_client.select = AsyncMock(return_value=[])

    result = await get_typed('user', 'gone', User, client=mock_db_client)

    assert result is None


# ---------------------------------------------------------------------------
# query_typed
# ---------------------------------------------------------------------------


class TestQueryTyped:
  """Test suite for query_typed."""

  @pytest.mark.anyio
  async def test_returns_list_of_models(self, mock_db_client: DatabaseClient) -> None:
    """query_typed should return a list of validated models."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:1', 'name': 'Alice', 'email': 'a@b.com', 'age': 30},
            {'id': 'user:2', 'name': 'Bob', 'email': 'b@c.com', 'age': 25},
          ]
        }
      ]
    )

    results = await query_typed('SELECT * FROM user', User, client=mock_db_client)

    assert len(results) == 2
    assert all(isinstance(u, User) for u in results)
    assert results[0].name == 'Alice'
    assert results[1].name == 'Bob'

  @pytest.mark.anyio
  async def test_returns_empty_list_when_no_results(self, mock_db_client: DatabaseClient) -> None:
    """query_typed should return an empty list when there are no results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    results = await query_typed('SELECT * FROM user WHERE age > 100', User, client=mock_db_client)

    assert results == []

  @pytest.mark.anyio
  async def test_passes_params_to_execute(self, mock_db_client: DatabaseClient) -> None:
    """query_typed should forward params to execute_raw."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    await query_typed(
      'SELECT * FROM user WHERE age > $min',
      User,
      client=mock_db_client,
      params={'min': 18},
    )

    mock_db_client.execute.assert_called_once_with(
      'SELECT * FROM user WHERE age > $min', {'min': 18}
    )

  @pytest.mark.anyio
  async def test_validation_error_for_invalid_row(self, mock_db_client: DatabaseClient) -> None:
    """query_typed should raise ValidationError when a row is invalid."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Alice'}]}]  # missing email, age
    )

    with pytest.raises(ValidationError):
      await query_typed('SELECT * FROM user', User, client=mock_db_client)


# ---------------------------------------------------------------------------
# update_typed
# ---------------------------------------------------------------------------


class TestUpdateTyped:
  """Test suite for update_typed."""

  @pytest.mark.anyio
  async def test_returns_updated_model(self, mock_db_client: DatabaseClient) -> None:
    """update_typed should return a validated model after updating."""
    mock_db_client.update = AsyncMock(
      return_value={'id': 'user:alice', 'name': 'Alice New', 'email': 'a@b.com', 'age': 31}
    )
    user = User(name='Alice New', email='a@b.com', age=31)

    result = await update_typed('user', 'alice', user, client=mock_db_client)

    assert isinstance(result, User)
    assert result.name == 'Alice New'
    assert result.age == 31

  @pytest.mark.anyio
  async def test_delegates_to_update_record(self, mock_db_client: DatabaseClient) -> None:
    """update_typed should call update_record with the correct target."""
    mock_db_client.update = AsyncMock(
      return_value={'name': 'Bob', 'email': 'bob@test.com', 'age': 40}
    )
    user = User(name='Bob', email='bob@test.com', age=40)

    await update_typed('user', 'bob', user, client=mock_db_client)

    mock_db_client.update.assert_called_once_with(
      'user:bob', {'name': 'Bob', 'email': 'bob@test.com', 'age': 40}
    )


# ---------------------------------------------------------------------------
# upsert_typed
# ---------------------------------------------------------------------------


class TestUpsertTyped:
  """Test suite for upsert_typed."""

  @pytest.mark.anyio
  async def test_returns_upserted_model(self, mock_db_client: DatabaseClient) -> None:
    """upsert_typed should return a validated model after upsert."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {'result': [{'id': 'user:alice', 'name': 'Alice', 'email': 'a@b.com', 'age': 30}]}
      ]
    )
    user = User(name='Alice', email='a@b.com', age=30)

    result = await upsert_typed('user', 'alice', user, client=mock_db_client)

    assert isinstance(result, User)
    assert result.name == 'Alice'
    assert result.age == 30

  @pytest.mark.anyio
  async def test_upsert_with_flat_dict_response(self, mock_db_client: DatabaseClient) -> None:
    """upsert_typed should handle flat dict responses."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'id': 'user:bob', 'name': 'Bob', 'email': 'b@c.com', 'age': 22}]
    )
    user = User(name='Bob', email='b@c.com', age=22)

    result = await upsert_typed('user', 'bob', user, client=mock_db_client)

    assert isinstance(result, User)
    assert result.name == 'Bob'
