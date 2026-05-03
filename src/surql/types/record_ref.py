"""Record reference helper for SurrealDB ``type::thing()`` calls.

This module provides a wrapper that generates ``type::thing()`` expressions
for referencing records by table and ID in parameterized queries.

Note: previously this rendered ``type::record('table', 'id')``. In SurrealDB
v3 the two-arg form of ``type::record(value, type)`` is a *type coercion*
(cast ``value`` into ``record<type>``), NOT a table+id constructor -- so the
old rendering produced ``Expected a record<id> but cannot convert 'table'
into a record<id>`` errors. The correct constructor is
``type::thing(table, id)`` and that's what we emit now.
"""

from pydantic import BaseModel, ConfigDict


class RecordRef(BaseModel):
  """Reference to a SurrealDB record via ``type::thing()``.

  Generates a ``type::thing()`` call that resolves to a record ID at query
  time. When used as a field value in CREATE/UPDATE/UPSERT operations, the
  expression renders as raw SurrealQL rather than a quoted string.

  Examples:
    >>> ref = RecordRef(table='user', record_id='alice')
    >>> ref.to_surql()
    "type::thing('user', 'alice')"

    >>> ref = RecordRef(table='post', record_id=123)
    >>> ref.to_surql()
    "type::thing('post', 123)"
  """

  table: str
  record_id: str | int

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Render as a ``type::thing()`` SurrealQL expression.

    Returns:
      SurrealQL ``type::thing()`` expression
    """
    if isinstance(self.record_id, int):
      return f"type::thing('{self.table}', {self.record_id})"
    # Escape single quotes in the record_id string
    escaped_id = self.record_id.replace('\\', '\\\\').replace("'", "\\'")
    return f"type::thing('{self.table}', '{escaped_id}')"

  def __str__(self) -> str:
    """Return string representation.

    Returns:
      SurrealQL ``type::thing()`` expression
    """
    return self.to_surql()


def record_ref(table: str, record_id: str | int) -> RecordRef:
  """Create a SurrealDB ``type::thing()`` reference.

  Generates a ``type::thing()`` expression that can be used as a field value
  in CREATE/UPDATE/UPSERT operations. The expression is emitted as raw
  SurrealQL rather than a quoted string.

  Args:
    table: Target table name
    record_id: Record identifier (string or integer)

  Returns:
    RecordRef instance

  Examples:
    >>> record_ref('user', 'alice').to_surql()
    "type::thing('user', 'alice')"

    >>> record_ref('post', 123).to_surql()
    "type::thing('post', 123)"
  """
  return RecordRef(table=table, record_id=record_id)
