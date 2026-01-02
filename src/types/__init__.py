"""Type definitions and utilities for ethereal ORM.

This package provides type-safe wrappers and operators for building SurrealDB queries.
"""

from src.types.operators import (
  # Logical operators
  And,
  Contains,
  ContainsAll,
  ContainsAny,
  ContainsNot,
  # Comparison operators
  Eq,
  Gt,
  Gte,
  Inside,
  IsNotNull,
  IsNull,
  Lt,
  Lte,
  Ne,
  Not,
  NotInside,
  Operator,
  Or,
  and_,
  contains,
  contains_all,
  contains_any,
  contains_not,
  # Helper functions
  eq,
  gt,
  gte,
  inside,
  is_not_null,
  is_null,
  lt,
  lte,
  ne,
  not_,
  not_inside,
  or_,
)
from src.types.record_id import RecordID

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
