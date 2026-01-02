"""Query builder and ORM layer for SurrealDB.

This module provides a complete query building and ORM system with:
- Immutable query builder with fluent API
- Type-safe query expressions
- Async query execution with result deserialization
- High-level CRUD operations
- Graph traversal utilities
- Result wrapper classes
"""

# Query builder
from src.query.builder import (
  Query,
  delete,
  from_table,
  insert,
  limit,
  offset,
  order_by,
  relate,
  select,
  update,
  where,
)

# CRUD operations
from src.query.crud import (
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

# Query executor
from src.query.executor import (
  execute_query,
  execute_raw,
  execute_raw_typed,
  fetch_all,
  fetch_many,
  fetch_one,
  fetch_record,
  fetch_records,
)

# Query expressions
from src.query.expressions import (
  Expression,
  FieldExpression,
  FunctionExpression,
  ValueExpression,
  abs_,
  array_contains,
  array_length,
  as_,
  avg,
  cast,
  ceil,
  concat,
  count,
  field,
  floor,
  func,
  lower,
  max_,
  min_,
  raw,
  round_,
  sum_,
  time_format,
  time_now,
  type_is,
  upper,
  value,
)

# Graph operations
from src.query.graph import (
  count_related,
  get_incoming_edges,
  get_outgoing_edges,
  get_related_records,
  shortest_path,
  traverse,
  traverse_with_depth,
)
from src.query.graph import (
  relate as create_relation,
)
from src.query.graph import (
  unrelate as remove_relation,
)

# Result wrappers
from src.query.results import (
  AggregateResult,
  CountResult,
  ListResult,
  PageInfo,
  PaginatedResult,
  QueryResult,
  RecordResult,
  aggregate,
  count_result,
  paginated,
  record,
  records,
  success,
)

__all__ = [
  # Query builder
  'Query',
  'select',
  'from_table',
  'where',
  'order_by',
  'limit',
  'offset',
  'insert',
  'update',
  'delete',
  'relate',
  # Query expressions
  'Expression',
  'FieldExpression',
  'ValueExpression',
  'FunctionExpression',
  'field',
  'value',
  'func',
  'count',
  'sum_',
  'avg',
  'min_',
  'max_',
  'upper',
  'lower',
  'concat',
  'array_length',
  'array_contains',
  'abs_',
  'ceil',
  'floor',
  'round_',
  'time_now',
  'time_format',
  'type_is',
  'cast',
  'as_',
  'raw',
  # Query executor
  'execute_query',
  'fetch_one',
  'fetch_all',
  'fetch_many',
  'fetch_record',
  'fetch_records',
  'execute_raw',
  'execute_raw_typed',
  # CRUD operations
  'create_record',
  'create_records',
  'get_record',
  'update_record',
  'merge_record',
  'delete_record',
  'delete_records',
  'query_records',
  'query_records_wrapped',
  'count_records',
  'exists',
  'first',
  'last',
  # Graph operations
  'traverse',
  'traverse_with_depth',
  'create_relation',
  'remove_relation',
  'get_outgoing_edges',
  'get_incoming_edges',
  'get_related_records',
  'count_related',
  'shortest_path',
  # Result wrappers
  'QueryResult',
  'RecordResult',
  'ListResult',
  'CountResult',
  'AggregateResult',
  'PageInfo',
  'PaginatedResult',
  'success',
  'record',
  'records',
  'count_result',
  'aggregate',
  'paginated',
]
