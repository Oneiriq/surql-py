"""Typed CRUD operations returning validated Pydantic model instances.

This module wraps the lower-level CRUD functions from ``surql.query.crud``
with generic type annotations so callers receive fully validated Pydantic
models instead of raw dictionaries.

Typed vs untyped — when to use which
====================================

The untyped helpers in :mod:`surql.query.crud` (``create_record``,
``update_record``, ``upsert_record``) accept any ``BaseModel`` or
``dict`` and return ``dict[str, Any]`` — the raw SurrealDB response
shape after normalisation. They are appropriate when callers want to
inspect the raw envelope, build their own ad-hoc validation, or work
across heterogeneous record types.

The typed helpers here (``create_typed``, ``update_typed``,
``upsert_typed``) take a concrete :class:`~pydantic.BaseModel`
instance, perform the same CRUD, then ``model_validate`` the response
back into the same model type. They are the right call when:

- the call site already knows the concrete model type,
- callers want the typed return value (``T``, not ``dict``) without
  re-validating themselves, and
- the strict-mypy gates on the consumer want a typed handle, not a
  dict-of-Any.

Lightweight asymmetries (intentional, documented here):

- :func:`get_typed` is a thin alias around
  :func:`~surql.query.crud.get_record` — the untyped helper already
  returns ``T | None`` (it takes a model class), so the typed variant
  exists only for naming symmetry with the rest of the typed surface.
  Both behave identically.
- :func:`query_typed` runs a raw SurrealQL string and validates each
  row. It is NOT a typed variant of
  :func:`~surql.query.crud.query_records` — the latter uses the
  :class:`~surql.query.builder.Query` builder with ``conditions=``,
  ``order_by=``, ``limit=``, etc. Use ``query_records`` when you want
  the builder; use ``query_typed`` when you want to hand-write the
  SurrealQL.
"""

from __future__ import annotations

from typing import Any, TypeVar

import structlog
from pydantic import BaseModel

from surql.connection.client import DatabaseClient
from surql.query.crud import (
  create_record,
  get_record,
  update_record,
  upsert_record,
)
from surql.query.executor import execute_raw
from surql.query.results import extract_one, extract_result

logger = structlog.get_logger(__name__)

T = TypeVar('T', bound=BaseModel)


async def create_typed(
  table: str,
  data: T,
  client: DatabaseClient | None = None,
) -> T:
  """Create a record from a Pydantic model and return a validated instance.

  Differs from :func:`~surql.query.crud.create_record` in the return
  type only: ``create_record`` returns ``dict[str, Any]`` (the raw
  normalised response), while this helper revalidates the response
  through ``data.__class__`` and returns a typed instance. Prefer
  this helper when the call site already has the concrete model type
  in hand; prefer ``create_record`` when working across heterogeneous
  rows or when you want the raw dict shape.

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

  Thin alias around :func:`~surql.query.crud.get_record`. The untyped
  helper already takes a ``model: type[T]`` argument and returns
  ``T | None``; this wrapper exists for naming symmetry with the rest
  of the typed surface. Behaviour is identical — either is fine,
  prefer ``get_record`` if you want to stay inside ``surql.query.crud``
  for all CRUD calls.

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

  NOT a typed variant of :func:`~surql.query.crud.query_records` —
  the two helpers are intentionally different:

  - ``query_records`` uses the :class:`~surql.query.builder.Query`
    builder; pass ``conditions=``, ``order_by=``, ``limit=``, etc.
    and SurrealQL is generated for you. Returns typed instances.
  - ``query_typed`` (here) takes a hand-written SurrealQL string
    plus optional positional ``$``-prefixed parameters and validates
    each returned row against ``model_class``. Returns typed instances.

  Use the builder when the query shape fits the builder API; reach
  for ``query_typed`` when you need a SurrealQL construct the builder
  doesn't model (graph paths inside subqueries, custom function
  calls, INFO queries, etc.).

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

  Differs from :func:`~surql.query.crud.update_record` in the return
  type only: ``update_record`` returns ``dict[str, Any]``; this
  helper revalidates the response into ``data.__class__``. Prefer
  this when the call site has the concrete model type; prefer
  ``update_record`` for the raw dict shape.

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
  Differs from :func:`~surql.query.crud.upsert_record` in the return
  type only: ``upsert_record`` returns ``dict[str, Any]``; this
  helper revalidates the response into ``data.__class__``. Prefer
  this when the call site has the concrete model type; prefer
  ``upsert_record`` for the raw dict shape.

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
