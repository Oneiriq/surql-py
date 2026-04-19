"""Pre-built SurrealQL function factories that return ``SurrealFn``.

These factories wrap common SurrealDB built-ins so they can be composed
directly into ``.set()``, ``.where()``, and ``.select()`` call sites in the
query builder without dropping to raw strings.

Each factory returns a :class:`~surql.types.surreal_fn.SurrealFn` which is
recognised by the builder's value-quoting pipeline and rendered verbatim,
exactly like :func:`~surql.types.surreal_fn.surql_fn`.

The names here intentionally do not collide with the older
``surql.query.expressions`` helpers that return ``FunctionExpression``;
both surfaces remain supported so existing callers continue to work.
"""

from __future__ import annotations

from typing import Any

from surql.types.surreal_fn import SurrealFn, surql_fn

__all__ = [
  'count_if',
  'math_abs_fn',
  'math_ceil_fn',
  'math_floor_fn',
  'math_max_fn',
  'math_mean_fn',
  'math_min_fn',
  'math_round_fn',
  'math_sum_fn',
  'string_concat',
  'string_len',
  'string_lower',
  'string_upper',
  'time_now_fn',
]


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------


def time_now_fn() -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``time::now()``.

  Examples:
    >>> time_now_fn().to_surql()
    'time::now()'
  """
  return surql_fn('time::now')


# ---------------------------------------------------------------------------
# Math
# ---------------------------------------------------------------------------


def math_mean_fn(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``math::mean(<field>)``.

  Args:
    field_name: Field (or sub-expression) to average.
  """
  return surql_fn('math::mean', field_name)


def math_sum_fn(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``math::sum(<field>)``.

  Args:
    field_name: Field (or sub-expression) to sum.
  """
  return surql_fn('math::sum', field_name)


def math_min_fn(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``math::min(<field>)``.

  Args:
    field_name: Field (or sub-expression) to take the minimum of.
  """
  return surql_fn('math::min', field_name)


def math_max_fn(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``math::max(<field>)``.

  Args:
    field_name: Field (or sub-expression) to take the maximum of.
  """
  return surql_fn('math::max', field_name)


def math_ceil_fn(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``math::ceil(<field>)``.

  Args:
    field_name: Numeric field (or sub-expression) to ceil.
  """
  return surql_fn('math::ceil', field_name)


def math_floor_fn(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``math::floor(<field>)``.

  Args:
    field_name: Numeric field (or sub-expression) to floor.
  """
  return surql_fn('math::floor', field_name)


def math_round_fn(field_name: str, precision: int | None = None) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``math::round(<field>[, precision])``.

  Args:
    field_name: Numeric field (or sub-expression) to round.
    precision: Optional decimal-places argument. When ``None`` the SurrealQL
      call is rendered without a precision argument.
  """
  if precision is None:
    return surql_fn('math::round', field_name)
  return surql_fn('math::round', field_name, precision)


def math_abs_fn(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``math::abs(<field>)``.

  Args:
    field_name: Numeric field (or sub-expression) to take the absolute value of.
  """
  return surql_fn('math::abs', field_name)


# ---------------------------------------------------------------------------
# Strings
# ---------------------------------------------------------------------------


def string_len(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``string::len(<field>)``.

  Args:
    field_name: String field (or sub-expression) to measure.
  """
  return surql_fn('string::len', field_name)


def string_concat(*parts: Any) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``string::concat(...)``.

  Args are passed through verbatim into the function call so callers can mix
  field references (``'first_name'``), pre-quoted string literals (``"' '"``)
  and other ``SurrealFn`` instances without double-escaping.

  Args:
    parts: Arguments to ``string::concat``.
  """
  return surql_fn('string::concat', *parts)


def string_lower(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``string::lowercase(<field>)``.

  Args:
    field_name: Field to lowercase.
  """
  return surql_fn('string::lowercase', field_name)


def string_upper(field_name: str) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping ``string::uppercase(<field>)``.

  Args:
    field_name: Field to uppercase.
  """
  return surql_fn('string::uppercase', field_name)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def count_if(predicate: str | None = None) -> SurrealFn:
  """Return a ``SurrealFn`` wrapping SurrealDB's ``count`` aggregate.

  SurrealDB's ``count`` function takes an optional predicate expression to
  count rows matching a condition (``count(status = 'active')``). When the
  predicate is omitted (or ``None`` / ``'*'``) the call renders bare
  ``count()``, which counts every row in the current group -- v3 rejects
  ``count(*)``.

  Args:
    predicate: Optional SurrealQL predicate expression. Passing ``None`` or
      ``'*'`` produces ``count()``.
  """
  if predicate is None or predicate == '*':
    return surql_fn('count')
  return surql_fn('count', predicate)
