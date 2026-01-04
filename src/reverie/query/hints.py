"""Query optimization hints for SurrealDB.

This module provides a type-safe system for adding optimization hints to queries,
allowing developers to guide query execution for better performance.
"""

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class HintType(Enum):
  """Types of query optimization hints supported."""

  INDEX = 'index'
  PARALLEL = 'parallel'
  TIMEOUT = 'timeout'
  FETCH = 'fetch'
  EXPLAIN = 'explain'


class IndexHint(BaseModel):
  """Hint to force or suggest specific index usage.

  Guides the query planner to use a particular index, either as a suggestion
  or as a forced requirement.

  Examples:
      >>> hint = IndexHint(table='user', index='email_idx')
      >>> hint.to_surql()
      '/* USE INDEX user.email_idx */'

      >>> force_hint = IndexHint(table='user', index='email_idx', force=True)
      >>> force_hint.to_surql()
      '/* FORCE INDEX user.email_idx */'
  """

  type: Literal[HintType.INDEX] = HintType.INDEX
  table: str = Field(..., description='Table name')
  index: str = Field(..., description='Index name to use')
  force: bool = Field(default=False, description='Force index even if suboptimal')

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Render to SurrealQL hint comment.

    Returns:
        SurrealQL comment string with index hint
    """
    force_str = 'FORCE' if self.force else 'USE'
    return f'/* {force_str} INDEX {self.table}.{self.index} */'


class ParallelHint(BaseModel):
  """Hint for parallel query execution.

  Controls whether the query should be executed in parallel and optionally
  specifies the maximum number of parallel workers.

  Examples:
      >>> hint = ParallelHint(enabled=True, max_workers=4)
      >>> hint.to_surql()
      '/* PARALLEL 4 */'

      >>> hint = ParallelHint(enabled=False)
      >>> hint.to_surql()
      '/* PARALLEL OFF */'
  """

  type: Literal[HintType.PARALLEL] = HintType.PARALLEL
  enabled: bool = Field(default=True, description='Enable parallel execution')
  max_workers: int | None = Field(
    default=None,
    ge=1,
    le=32,
    description='Maximum parallel workers',
  )

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Render to SurrealQL hint comment.

    Returns:
        SurrealQL comment string with parallel hint
    """
    if not self.enabled:
      return '/* PARALLEL OFF */'
    if self.max_workers:
      return f'/* PARALLEL {self.max_workers} */'
    return '/* PARALLEL ON */'


class TimeoutHint(BaseModel):
  """Query timeout override hint.

  Specifies a custom timeout for the query execution, overriding the
  default database timeout settings.

  Examples:
      >>> hint = TimeoutHint(seconds=30.0)
      >>> hint.to_surql()
      '/* TIMEOUT 30.0s */'
  """

  type: Literal[HintType.TIMEOUT] = HintType.TIMEOUT
  seconds: float = Field(..., gt=0, description='Timeout in seconds')

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Render to SurrealQL hint comment.

    Returns:
        SurrealQL comment string with timeout hint
    """
    return f'/* TIMEOUT {self.seconds}s */'


class FetchHint(BaseModel):
  """Record fetch strategy hint.

  Specifies how records should be fetched from the database, controlling
  the balance between memory usage and query latency.

  Examples:
      >>> hint = FetchHint(strategy='eager')
      >>> hint.to_surql()
      '/* FETCH EAGER */'

      >>> hint = FetchHint(strategy='batch', batch_size=100)
      >>> hint.to_surql()
      '/* FETCH BATCH 100 */'
  """

  type: Literal[HintType.FETCH] = HintType.FETCH
  strategy: Literal['eager', 'lazy', 'batch'] = Field(
    default='batch',
    description='Fetch strategy',
  )
  batch_size: int | None = Field(
    default=None,
    ge=1,
    le=10000,
    description='Batch size for batch strategy',
  )

  model_config = ConfigDict(frozen=True)

  @model_validator(mode='after')
  def validate_batch_strategy(self) -> 'FetchHint':
    """Validate batch_size is set when strategy is batch.

    Returns:
      Validated model instance

    Raises:
      ValueError: If batch strategy is used without batch_size
    """
    if self.strategy == 'batch' and self.batch_size is None:
      raise ValueError('batch_size required when strategy is batch')
    return self

  def to_surql(self) -> str:
    """Render to SurrealQL hint comment.

    Returns:
        SurrealQL comment string with fetch hint
    """
    if self.strategy == 'batch' and self.batch_size:
      return f'/* FETCH BATCH {self.batch_size} */'
    return f'/* FETCH {self.strategy.upper()} */'


class ExplainHint(BaseModel):
  """Include query execution plan hint.

  Requests that the database include the query execution plan in the results,
  useful for performance analysis and optimization.

  Examples:
      >>> hint = ExplainHint()
      >>> hint.to_surql()
      '/* EXPLAIN */'

      >>> hint = ExplainHint(full=True)
      >>> hint.to_surql()
      '/* EXPLAIN FULL */'
  """

  type: Literal[HintType.EXPLAIN] = HintType.EXPLAIN
  full: bool = Field(default=False, description='Include full execution plan')

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Render to SurrealQL hint comment.

    Returns:
        SurrealQL comment string with explain hint
    """
    return '/* EXPLAIN FULL */' if self.full else '/* EXPLAIN */'


QueryHint = IndexHint | ParallelHint | TimeoutHint | FetchHint | ExplainHint


def validate_hint(hint: QueryHint, table: str | None = None) -> list[str]:
  """Validate hint is applicable to query context.

  Checks that hints are compatible with the query they're being applied to,
  catching common errors like index hints for the wrong table.

  Args:
      hint: Query hint to validate
      table: Table name from query context

  Returns:
      List of validation error messages (empty if valid)

  Examples:
      >>> hint = IndexHint(table='user', index='email_idx')
      >>> errors = validate_hint(hint, table='user')
      >>> assert len(errors) == 0

      >>> errors = validate_hint(hint, table='post')
      >>> assert len(errors) == 1
  """
  errors: list[str] = []

  if isinstance(hint, IndexHint) and table and hint.table != table:
    errors.append(f'Index hint table "{hint.table}" does not match query table "{table}"')

  return errors


def merge_hints(hints: list[QueryHint]) -> list[QueryHint]:
  """Merge multiple hints, resolving conflicts.

  Later hints override earlier hints of the same type, allowing queries
  to be composed with hints that can be overridden as needed.

  Args:
      hints: List of hints to merge

  Returns:
      Merged list of hints with duplicates resolved

  Examples:
      >>> hints = [TimeoutHint(seconds=10), TimeoutHint(seconds=20)]
      >>> merged = merge_hints(hints)
      >>> assert len(merged) == 1
      >>> assert merged[0].seconds == 20

      >>> hints = [
      ...     TimeoutHint(seconds=30),
      ...     ParallelHint(enabled=True),
      ...     TimeoutHint(seconds=60),
      ... ]
      >>> merged = merge_hints(hints)
      >>> assert len(merged) == 2
      >>> timeout = next(h for h in merged if isinstance(h, TimeoutHint))
      >>> assert timeout.seconds == 60
  """
  hint_map: dict[HintType, QueryHint] = {}

  for hint in hints:
    hint_map[hint.type] = hint

  return list(hint_map.values())


def render_hints(hints: list[QueryHint]) -> str:
  """Render hints to SurrealQL comment string.

  Combines multiple hints into a single string of SQL comments that can
  be prepended to a query.

  Args:
      hints: List of hints to render

  Returns:
      SurrealQL hint comments string

  Examples:
      >>> hints = [TimeoutHint(seconds=30), ParallelHint(enabled=True)]
      >>> sql = render_hints(hints)
      >>> assert '/* TIMEOUT 30.0s */' in sql
      >>> assert '/* PARALLEL ON */' in sql

      >>> assert render_hints([]) == ''
  """
  if not hints:
    return ''

  merged = merge_hints(hints)
  hint_strs = [hint.to_surql() for hint in merged]
  return ' '.join(hint_strs)


class HintRenderer:
  """Renderer for converting hints to SurrealQL comments.

  Provides a stateful interface for rendering hints, though the
  functional render_hints() function is preferred for most use cases.

  Examples:
      >>> renderer = HintRenderer()
      >>> hints = [TimeoutHint(seconds=30), ParallelHint(max_workers=4)]
      >>> sql = renderer.render_hints(hints)
      >>> assert '/* TIMEOUT 30.0s */' in sql
  """

  def render_hints(self, hints: list[QueryHint]) -> str:
    """Render hints to SurrealQL comment string.

    Args:
        hints: List of hints to render

    Returns:
        SurrealQL hint comments string
    """
    return render_hints(hints)
