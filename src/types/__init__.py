"""Type definitions and utilities for ethereal ORM.

This package provides type-safe wrappers and operators for building SurrealDB queries.
"""

from src.types.record_id import RecordID
from src.types.operators import (
  Operator,
  # Comparison operators
  Eq,
  Ne,
  Gt,
  Gte,
  Lt,
  Lte,
  Contains,
  ContainsNot,
  ContainsAll,
  ContainsAny,
  Inside,
  NotInside,
  IsNull,
  IsNotNull,
  # Logical operators
  And,
  Or,
  Not,
  # Helper functions
  eq,
  ne,
  gt,
  gte,
  lt,
  lte,
  contains,
  contains_not,
  contains_all,
  contains_any,
  inside,
  not_inside,
  is_null,
  is_not_null,
  and_,
  or_,
  not_,
)

__all__ = [
  # RecordID
  'RecordID',
  # Operator base
  'Operator',
  # Comparison operator classes
  'Eq',
  'Ne',
  'Gt',
  'Gte',
  'Lt',
  'Lte',
  'Contains',
  'ContainsNot',
  'ContainsAll',
  'ContainsAny',
  'Inside',
  'NotInside',
  'IsNull',
  'IsNotNull',
  # Logical operator classes
  'And',
  'Or',
  'Not',
  # Helper functions
  'eq',
  'ne',
  'gt',
  'gte',
  'lt',
  'lte',
  'contains',
  'contains_not',
  'contains_all',
  'contains_any',
  'inside',
  'not_inside',
  'is_null',
  'is_not_null',
  'and_',
  'or_',
  'not_',
]
