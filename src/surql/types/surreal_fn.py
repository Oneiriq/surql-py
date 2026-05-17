"""SurrealDB function value wrapper for raw function calls in queries.

This module provides a wrapper that renders as raw SurrealQL function calls
when used as field values in CREATE, UPDATE, and UPSERT operations instead
of being parameterized as strings.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from surql.types.record_id import RecordID


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


def _render_record_id_arg(record_id: Any) -> str:
  """Render a record_id argument for ``type::record`` calls.

  - ``RecordID`` instances render via their ``to_surql()`` form.
  - ``SurrealFn`` values render verbatim (so nested function calls compose).
  - Integers render unquoted.
  - Strings are single-quoted with standard escaping.
  - Everything else falls back to string quoting.

  Args:
    record_id: The record identifier argument.

  Returns:
    SurrealQL-safe rendering of the argument.
  """
  if isinstance(record_id, RecordID):
    return record_id.to_surql()
  if isinstance(record_id, SurrealFn):
    return record_id.to_surql()
  if isinstance(record_id, bool):
    return 'true' if record_id else 'false'
  if isinstance(record_id, int):
    return str(record_id)
  if isinstance(record_id, float):
    return str(record_id)
  if isinstance(record_id, str):
    escaped = record_id.replace('\\', '\\\\').replace("'", "\\'")
    return f"'{escaped}'"
  return f"'{str(record_id)}'"


def type_record(table: str, record_id: Any) -> SurrealFn:
  """Build a ``type::record('table', id)`` SurrealDB function call.

  ``type::record(table, id)`` is the record-id constructor in SurrealDB v3
  (verified against v3.0.4): given a table name and an id, it returns a
  record id of that table.

  Earlier surql-py versions emitted ``type::thing('table', id)`` based on
  a comment claiming ``type::record(value, type)`` was coercion-only. That
  reasoning was outdated — ``type::thing`` was removed in v3 (calling it
  raises ``Invalid function/constant path, did you maybe mean `type::record```),
  so this now emits ``type::record(...)`` and :func:`type_thing` is kept
  only as a deprecated alias that also emits the v3-correct form.

  Produces a :class:`SurrealFn` that renders as raw SurrealQL, so it
  composes with query builders and raw queries just like :func:`surql_fn`.

  Args:
    table: Target table name.
    record_id: Record identifier. ``str``/``int`` values are quoted
      appropriately; :class:`~surql.types.record_id.RecordID` and
      :class:`SurrealFn` values render verbatim.

  Returns:
    ``SurrealFn`` wrapping a ``type::record('table', id)`` expression.

  Examples:
    >>> type_record('task', 'abc').to_surql()
    "type::record('task', 'abc')"

    >>> type_record('post', 42).to_surql()
    "type::record('post', 42)"
  """
  arg = _render_record_id_arg(record_id)
  return SurrealFn(expression=f"type::record('{table}', {arg})")


def type_thing(table: str, record_id: Any) -> SurrealFn:
  """Deprecated alias for :func:`type_record` — emits ``type::record(...)``.

  Kept for source compatibility only. The literal SurrealQL function
  ``type::thing`` no longer exists in v3 (parse error), so this helper
  now emits ``type::record(table, id)`` exactly like :func:`type_record`.
  New code should prefer :func:`type_record` directly.

  Args:
    table: Target table name.
    record_id: Record identifier. ``str``/``int`` values are quoted
      appropriately; :class:`~surql.types.record_id.RecordID` and
      :class:`SurrealFn` values render verbatim.

  Returns:
    ``SurrealFn`` wrapping a ``type::record('table', id)`` expression
    (identical output to :func:`type_record`).

  Examples:
    >>> type_thing('user', 'alice').to_surql()
    "type::record('user', 'alice')"

    >>> type_thing('order', 7).to_surql()
    "type::record('order', 7)"
  """
  arg = _render_record_id_arg(record_id)
  return SurrealFn(expression=f"type::record('{table}', {arg})")
