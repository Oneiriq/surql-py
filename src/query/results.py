"""Result wrapper classes for query execution.

This module provides generic result containers for different query types,
with support for pagination and metadata.
"""

from typing import Any, Generic, TypeVar
from pydantic import BaseModel, Field

T = TypeVar('T', bound=BaseModel)


class QueryResult(BaseModel, Generic[T]):
  """Generic result container for query execution.
  
  Wraps query results with metadata about execution.
  
  Examples:
    >>> result = QueryResult(data=[user1, user2], time='123ms', status='OK')
  """
  
  data: Any
  time: str | None = None
  status: str = 'OK'
  
  class Config:
    """Pydantic configuration."""
    frozen = True
    arbitrary_types_allowed = True


class RecordResult(BaseModel, Generic[T]):
  """Single record result wrapper.
  
  Used for operations that return a single record (e.g., get by ID, create).
  
  Examples:
    >>> result = RecordResult(record=user, exists=True)
  """
  
  record: T | None
  exists: bool = True
  
  class Config:
    """Pydantic configuration."""
    frozen = True
    arbitrary_types_allowed = True
  
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


class ListResult(BaseModel, Generic[T]):
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
  
  class Config:
    """Pydantic configuration."""
    frozen = True
    arbitrary_types_allowed = True
  
  def __len__(self) -> int:
    """Return number of records.
    
    Returns:
      Number of records in result
    """
    return len(self.records)
  
  def __iter__(self):  # type: ignore[no-untyped-def]
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
  
  class Config:
    """Pydantic configuration."""
    frozen = True


class AggregateResult(BaseModel):
  """Generic aggregation result wrapper.
  
  Used for aggregation operations like SUM, AVG, MIN, MAX.
  
  Examples:
    >>> result = AggregateResult(value=42.5, operation='AVG', field='age')
  """
  
  value: Any
  operation: str | None = None
  field: str | None = None
  
  class Config:
    """Pydantic configuration."""
    frozen = True


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
  
  class Config:
    """Pydantic configuration."""
    frozen = True


class PaginatedResult(BaseModel, Generic[T]):
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
  
  class Config:
    """Pydantic configuration."""
    frozen = True
    arbitrary_types_allowed = True
  
  def __len__(self) -> int:
    """Return number of items in current page.
    
    Returns:
      Number of items
    """
    return len(self.items)
  
  def __iter__(self):  # type: ignore[no-untyped-def]
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


def record(rec: T | None, exists: bool = True) -> RecordResult[T]:
  """Create single record result.
  
  Args:
    rec: Record instance or None
    exists: Whether record exists
    
  Returns:
    RecordResult instance
  """
  return RecordResult(record=rec, exists=exists)


def records(
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


def aggregate(value: Any, operation: str | None = None, field: str | None = None) -> AggregateResult:
  """Create aggregate result.
  
  Args:
    value: Aggregated value
    operation: Aggregation operation
    field: Field aggregated
    
  Returns:
    AggregateResult instance
  """
  return AggregateResult(value=value, operation=operation, field=field)


def paginated(
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
