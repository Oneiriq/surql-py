"""SurrealDB function value wrapper for raw function calls in queries.

This module provides a wrapper that renders as raw SurrealQL function calls
when used as field values in CREATE, UPDATE, and UPSERT operations instead
of being parameterized as strings.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict


class SurrealFn(BaseModel):
  """Wrapper for SurrealDB function calls that render as raw SurrealQL.

  When used as a value in CREATE/UPDATE/UPSERT operations, the function
  call is emitted verbatim rather than being quoted as a string.

  Examples:
    >>> fn = SurrealFn(expression='time::now()')
    >>> fn.to_surql()
    'time::now()'

    >>> fn = SurrealFn(expression='math::mean(scores)')
    >>> fn.to_surql()
    'math::mean(scores)'
  """

  expression: str

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Render the function call as raw SurrealQL.

    Returns:
      Raw SurrealQL function expression
    """
    return self.expression

  def __str__(self) -> str:
    """Return string representation.

    Returns:
      The raw SurrealQL expression
    """
    return self.expression


def surql_fn(name: str, *args: Any) -> SurrealFn:
  """Create a SurrealDB function call value.

  When used as a field value in CREATE/UPDATE operations, the function
  call renders as raw SurrealQL (not as a quoted string).

  Args:
    name: Fully qualified function name (e.g., 'time::now', 'math::mean')
    args: Function arguments (converted to strings)

  Returns:
    SurrealFn instance

  Examples:
    >>> surql_fn('time::now')
    SurrealFn(expression='time::now()')

    >>> surql_fn('time::format', 'created_at', '%Y-%m-%d')
    SurrealFn(expression="time::format(created_at, %Y-%m-%d)")

    >>> surql_fn('math::sum', 'scores')
    SurrealFn(expression='math::sum(scores)')
  """
  if args:
    args_str = ', '.join(str(a) for a in args)
    return SurrealFn(expression=f'{name}({args_str})')
  return SurrealFn(expression=f'{name}()')
