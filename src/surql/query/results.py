"""Result wrapper classes for query execution.

This module provides generic result containers for different query types,
with support for pagination and metadata, plus utilities for extracting
data from raw SurrealDB responses.
"""

from collections.abc import Iterator
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar('T', bound=BaseModel)


class QueryResult[T: BaseModel](BaseModel):
  """Generic result container for query execution.

  Wraps query results with metadata about execution.

  Examples:
    >>> result = QueryResult(data=[user1, user2], time='123ms', status='OK')
  """

  data: Any
  time: str | None = None
  status: str = 'OK'

  model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class RecordResult[T: BaseModel](BaseModel):
  """Single record result wrapper.

  Used for operations that return a single record (e.g., get by ID, create).

  Examples:
    >>> result = RecordResult(record=user, exists=True)
  """

  record: T | None
  exists: bool = True

  model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

  def unwrap(self) -> T:
    """Unwrap the record value.

    Returns:
      The record instance

    Raises:
      ValueError: If record is None
    """
    if self.record is None:
      raise ValueError('Cannot unwrap None record')
    return self.record

  def unwrap_or(self, default: T) -> T:
    """Unwrap the record or return default.

    Args:
      default: Default value if record is None

    Returns:
      The record or default value
    """
    return self.record if self.record is not None else default


class ListResult[T: BaseModel](BaseModel):
  """Multiple records result wrapper.

  Used for operations that return multiple records with optional pagination.

  Examples:
    >>> result = ListResult(records=[user1, user2], total=100, limit=10, offset=0)
  """

  records: list[T] = Field(default_factory=list)
  total: int | None = None
  limit: int | None = None
  offset: int | None = None
  has_more: bool = False

  model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

  def __len__(self) -> int:
    """Return number of records.

    Returns:
      Number of records in result
    """
    return len(self.records)

  def __iter__(self) -> Iterator[T]:  # type: ignore[override]
    """Iterate over records.

    Yields:
      Record instances
    """
    return iter(self.records)

  def __getitem__(self, index: int) -> T:
    """Get record by index.

    Args:
      index: Record index

    Returns:
      Record at index
    """
    return self.records[index]

  def is_empty(self) -> bool:
    """Check if result is empty.

    Returns:
      True if no records, False otherwise
    """
    return len(self.records) == 0

  def first(self) -> T | None:
    """Get first record or None.

    Returns:
      First record or None if empty
    """
    return self.records[0] if self.records else None

  def last(self) -> T | None:
    """Get last record or None.

    Returns:
      Last record or None if empty
    """
    return self.records[-1] if self.records else None


class CountResult(BaseModel):
  """Aggregation result wrapper for count operations.

  Used for COUNT and other aggregate operations.

  Examples:
    >>> result = CountResult(count=42)
  """

  count: int

  model_config = ConfigDict(frozen=True)


class AggregateResult(BaseModel):
  """Generic aggregation result wrapper.

  Used for aggregation operations like SUM, AVG, MIN, MAX.

  Examples:
    >>> result = AggregateResult(value=42.5, operation='AVG', field='age')
  """

  value: Any
  operation: str | None = None
  field: str | None = None

  model_config = ConfigDict(frozen=True)


class PageInfo(BaseModel):
  """Pagination metadata.

  Contains information about pagination state.

  Examples:
    >>> page_info = PageInfo(current_page=1, page_size=10, total_pages=10, total_items=100)
  """

  current_page: int
  page_size: int
  total_pages: int
  total_items: int
  has_previous: bool = False
  has_next: bool = False

  model_config = ConfigDict(frozen=True)


class PaginatedResult[T: BaseModel](BaseModel):
  """Paginated result wrapper with page metadata.

  Used for paginated queries with full pagination information.

  Examples:
    >>> result = PaginatedResult(
    ...   items=[user1, user2],
    ...   page_info=PageInfo(current_page=1, page_size=10, total_pages=10, total_items=100)
    ... )
  """

  items: list[T] = Field(default_factory=list)
  page_info: PageInfo

  model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

  def __len__(self) -> int:
    """Return number of items in current page.

    Returns:
      Number of items
    """
    return len(self.items)

  def __iter__(self) -> Iterator[T]:  # type: ignore[override]
    """Iterate over items.

    Yields:
      Item instances
    """
    return iter(self.items)

  def __getitem__(self, index: int) -> T:
    """Get item by index.

    Args:
      index: Item index

    Returns:
      Item at index
    """
    return self.items[index]


# Helper functions for creating results


def success(data: Any, time: str | None = None) -> 'QueryResult[Any]':
  """Create successful query result.

  Args:
    data: Result data
    time: Execution time

  Returns:
    QueryResult instance
  """
  return QueryResult(data=data, time=time, status='OK')


def record[T: BaseModel](rec: T | None, exists: bool = True) -> RecordResult[T]:
  """Create single record result.

  Args:
    rec: Record instance or None
    exists: Whether record exists

  Returns:
    RecordResult instance
  """
  return RecordResult(record=rec, exists=exists)


def records[T: BaseModel](
  items: list[T],
  total: int | None = None,
  limit: int | None = None,
  offset: int | None = None,
) -> ListResult[T]:
  """Create list result.

  Args:
    items: List of records
    total: Total count (if known)
    limit: Limit used in query
    offset: Offset used in query

  Returns:
    ListResult instance
  """
  has_more = False
  if total is not None and limit is not None and offset is not None:
    has_more = (offset + limit) < total
  elif limit is not None:
    has_more = len(items) == limit

  return ListResult(
    records=items,
    total=total,
    limit=limit,
    offset=offset,
    has_more=has_more,
  )


def count_result(value: int) -> CountResult:
  """Create count result.

  Args:
    value: Count value

  Returns:
    CountResult instance
  """
  return CountResult(count=value)


def aggregate(
  value: Any, operation: str | None = None, field: str | None = None
) -> AggregateResult:
  """Create aggregate result.

  Args:
    value: Aggregated value
    operation: Aggregation operation
    field: Field aggregated

  Returns:
    AggregateResult instance
  """
  return AggregateResult(value=value, operation=operation, field=field)


def paginated[T: BaseModel](
  items: list[T],
  page: int,
  page_size: int,
  total: int,
) -> PaginatedResult[T]:
  """Create paginated result.

  Args:
    items: List of items for current page
    page: Current page number (1-indexed)
    page_size: Number of items per page
    total: Total number of items

  Returns:
    PaginatedResult instance
  """
  total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0

  page_info = PageInfo(
    current_page=page,
    page_size=page_size,
    total_pages=total_pages,
    total_items=total,
    has_previous=page > 1,
    has_next=page < total_pages,
  )

  return PaginatedResult(items=items, page_info=page_info)


# Raw result extraction utilities


def extract_result(result: Any) -> list[dict[str, Any]]:
  """Extract result data from raw SurrealDB response.

  Handles both flat and nested result formats returned by SurrealDB:
  - Flat format (from db.select): [{"id": "...", ...}]
  - Nested format (from db.query): [{"result": [{"id": "...", ...}]}]

  This eliminates the need for custom workarounds when working with
  different SurrealDB client methods.

  Args:
    result: Raw SurrealDB query/select response

  Returns:
    List of record dictionaries, empty list if no results

  Examples:
    >>> # Nested format from db.query()
    >>> result = [{"result": [{"id": "user:123", "name": "Alice"}]}]
    >>> extract_result(result)
    [{"id": "user:123", "name": "Alice"}]

    >>> # Flat format from db.select()
    >>> result = [{"id": "user:123", "name": "Alice"}]
    >>> extract_result(result)
    [{"id": "user:123", "name": "Alice"}]

    >>> # Empty result
    >>> extract_result([])
    []

    >>> # Aggregate results (flat format)
    >>> result = [{"count": 42}]
    >>> extract_result(result)
    [{"count": 42}]
  """
  if result is None:
    return []

  # Handle list results
  if isinstance(result, list) and len(result) > 0:
    # Check if it's nested format with 'result' key
    if isinstance(result[0], dict) and 'result' in result[0]:
      # Extract all results from nested format
      extracted = []
      for item in result:
        if isinstance(item, dict) and 'result' in item:
          res = item['result']
          if isinstance(res, list):
            extracted.extend(res)
          elif res is not None:
            extracted.append(res)
      return extracted if extracted else []
    else:
      # Already flat format (includes records with 'id' and aggregate results)
      return result

  # Handle single result object with 'result' key
  if isinstance(result, dict) and 'result' in result:
    res = result['result']
    if isinstance(res, list):
      return res
    return [res] if res is not None else []

  # Empty or unrecognized format
  return []


def extract_one(result: Any) -> dict[str, Any] | None:
  """Extract a single record from raw SurrealDB result.

  Useful for queries expected to return a single record.

  Args:
    result: Raw SurrealDB query/select response

  Returns:
    First record dict if found, None otherwise

  Examples:
    >>> result = [{"result": [{"id": "user:123", "name": "Alice"}]}]
    >>> extract_one(result)
    {"id": "user:123", "name": "Alice"}

    >>> extract_one([])
    None

    >>> # Works with flat format too
    >>> result = [{"id": "user:123", "name": "Alice"}]
    >>> extract_one(result)
    {"id": "user:123", "name": "Alice"}
  """
  records = extract_result(result)
  return records[0] if records else None


def extract_scalar(result: Any, key: str, default: Any = 0) -> Any:
  """Extract a scalar value from aggregate query result.

  Useful for COUNT, SUM, AVG, MIN, MAX and other aggregate operations
  that return a single value in a named field.

  Args:
    result: Raw SurrealDB query result
    key: Key name to extract from first record
    default: Default value if key not found or result is empty

  Returns:
    Scalar value from the specified key, or default if not found

  Examples:
    >>> # COUNT query
    >>> result = [{"result": [{"count": 42}]}]
    >>> extract_scalar(result, 'count')
    42

    >>> # AVG query
    >>> result = [{"result": [{"avg": 25.5}]}]
    >>> extract_scalar(result, 'avg')
    25.5

    >>> # Empty result
    >>> extract_scalar([], 'count', default=0)
    0

    >>> # Missing key
    >>> result = [{"id": "user:123"}]
    >>> extract_scalar(result, 'total', default=0)
    0
  """
  record = extract_one(result)
  return record.get(key, default) if record else default


def has_results(result: Any) -> bool:
  """Check if SurrealDB result contains any records.

  Args:
    result: Raw SurrealDB query/select response

  Returns:
    True if result has one or more records, False otherwise

  Examples:
    >>> has_results([{"result": [{"id": "user:123"}]}])
    True

    >>> has_results([])
    False

    >>> # Works with flat format
    >>> has_results([{"id": "user:123"}])
    True

    >>> # Empty nested result
    >>> has_results([{"result": []}])
    False
  """
  return len(extract_result(result)) > 0


# ---------------------------------------------------------------------------
# 1.5.0 aliases (issue #47 / #4)
# ---------------------------------------------------------------------------


def extract_many(result: Any) -> list[dict[str, Any]]:
  """Alias for :func:`extract_result`.

  Provides a name that reads naturally next to :func:`extract_one` and
  :func:`extract_scalar`. Behaviour is identical to :func:`extract_result`
  and covers the three response shapes documented in issue #4:

  - Direct records: ``[{"id": "...", ...}, ...]``
  - Wrapped envelope: ``[{"result": [...], "time": "..."}]``
  - Scalar aggregates: ``[{"count": 5}]``

  Args:
    result: Raw SurrealDB query/select response.

  Returns:
    List of record dictionaries (empty list for empty / unrecognised input).
  """
  return extract_result(result)


def has_result(result: Any) -> bool:
  """Alias for :func:`has_results`.

  Mirrors the naming used in the issue description (``has_result`` /
  singular). Behaviour is identical to :func:`has_results`.

  Args:
    result: Raw SurrealDB query/select response.

  Returns:
    ``True`` if the response contains at least one record or scalar row.
  """
  return has_results(result)
