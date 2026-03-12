"""Type definitions and utilities for reverie ORM.

This package provides type-safe wrappers and operators for building SurrealDB queries.
"""

from reverie.types.coerce import coerce_datetime, coerce_record_datetimes
from reverie.types.operators import (
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
from reverie.types.record_id import RecordID
from reverie.types.reserved import SURREAL_RESERVED_WORDS, check_reserved_word

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
  # Coercion utilities
  'coerce_datetime',
  'coerce_record_datetimes',
  # Reserved word validation
  'SURREAL_RESERVED_WORDS',
  'check_reserved_word',
]
