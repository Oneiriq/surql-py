"""Typed CRUD operations returning validated Pydantic model instances.

This module wraps the lower-level CRUD functions from reverie.query.crud
with generic type annotations so callers receive fully validated Pydantic
models instead of raw dictionaries.
"""

from __future__ import annotations

from typing import Any, TypeVar

import structlog
from pydantic import BaseModel

from reverie.connection.client import DatabaseClient
from reverie.query.crud import (
  create_record,
  get_record,
  update_record,
  upsert_record,
)
from reverie.query.executor import execute_raw
from reverie.query.results import extract_one, extract_result

logger = structlog.get_logger(__name__)

T = TypeVar('T', bound=BaseModel)


async def create_typed(
  table: str,
  data: T,
  client: DatabaseClient | None = None,
) -> T:
  """Create a record from a Pydantic model and return a validated instance.

  Args:
    table: Table name to insert into
    data: Pydantic model instance with the record data
    client: Database client. If None, uses context client.

  Returns:
    Validated model instance of the same type as *data*

  Raises:
    QueryError: If creation fails
    ValidationError: If the response cannot be validated against the model

  Examples:
    >>> user = User(name='Alice', email='alice@example.com', age=30)
    >>> created = await create_typed('user', user)
    >>> assert isinstance(created, User)
  """
  result = await create_record(table, data, client=client)
  record_data = _normalise_single(result)
  return data.model_validate(record_data)


async def get_typed(
  table: str,
  record_id: str,
  model_class: type[T],
  client: DatabaseClient | None = None,
) -> T | None:
  """Fetch a single record and validate it into the given model type.

  Args:
    table: Table name
    record_id: Record identifier (without the table prefix)
    model_class: Pydantic model class for validation
    client: Database client. If None, uses context client.

  Returns:
    Validated model instance, or None if the record does not exist

  Raises:
    QueryError: If the fetch fails
    ValidationError: If the response cannot be validated against the model

  Examples:
    >>> user = await get_typed('user', 'alice', User)
    >>> if user is not None:
    ...     print(user.name)
  """
  return await get_record(table, record_id, model_class, client=client)


async def query_typed(
  surql: str,
  model_class: type[T],
  client: DatabaseClient | None = None,
  params: dict[str, Any] | None = None,
) -> list[T]:
  """Execute a raw SurrealQL query and validate each result row.

  Args:
    surql: SurrealQL query string
    model_class: Pydantic model class for validation
    client: Database client. If None, uses context client.
    params: Optional query parameters

  Returns:
    List of validated model instances

  Raises:
    QueryError: If query execution fails
    ValidationError: If any result row cannot be validated

  Examples:
    >>> users = await query_typed(
    ...     'SELECT * FROM user WHERE age > $min_age',
    ...     User,
    ...     params={'min_age': 18},
    ... )
  """
  raw = await execute_raw(surql, params, client)
  rows = extract_result(raw)
  return [model_class.model_validate(row) for row in rows]


async def update_typed(
  table: str,
  record_id: str,
  data: T,
  client: DatabaseClient | None = None,
) -> T:
  """Update a record from a Pydantic model and return the validated result.

  Args:
    table: Table name
    record_id: Record identifier (without the table prefix)
    data: Pydantic model instance with the new record data
    client: Database client. If None, uses context client.

  Returns:
    Validated model instance of the same type as *data*

  Raises:
    QueryError: If the update fails
    ValidationError: If the response cannot be validated against the model

  Examples:
    >>> user = User(name='Alice Updated', email='alice@new.com', age=31)
    >>> updated = await update_typed('user', 'alice', user)
  """
  result = await update_record(table, record_id, data, client=client)
  record_data = _normalise_single(result)
  return data.model_validate(record_data)


async def upsert_typed(
  table: str,
  record_id: str,
  data: T,
  client: DatabaseClient | None = None,
) -> T:
  """Upsert a record from a Pydantic model and return the validated result.

  Creates the record if it does not exist, or replaces it if it does.

  Args:
    table: Table name
    record_id: Record identifier (without the table prefix)
    data: Pydantic model instance with the record data
    client: Database client. If None, uses context client.

  Returns:
    Validated model instance of the same type as *data*

  Raises:
    QueryError: If the upsert fails
    ValidationError: If the response cannot be validated against the model

  Examples:
    >>> user = User(name='Alice', email='alice@example.com', age=30)
    >>> result = await upsert_typed('user', 'alice', user)
  """
  result = await upsert_record(table, record_id, data, client=client)
  record_data = _normalise_single(result)
  return data.model_validate(record_data)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalise_single(result: Any) -> dict[str, Any]:
  """Normalise a SurrealDB response into a single record dict.

  Handles both flat dicts and nested list-of-dicts formats returned by the
  various SurrealDB client methods.

  Args:
    result: Raw response from a SurrealDB operation

  Returns:
    A single record dictionary
  """
  if isinstance(result, dict):
    return result

  extracted = extract_one(result)
  if extracted is not None:
    return extracted

  return result  # type: ignore[no-any-return]
