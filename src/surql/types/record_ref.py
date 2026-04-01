"""Record reference helper for SurrealDB type::record() calls.

This module provides a wrapper that generates type::record() expressions
for referencing records by table and ID in parameterized queries.
"""

from pydantic import BaseModel, ConfigDict


class RecordRef(BaseModel):
  """Reference to a SurrealDB record via type::record().

  Generates a type::record() call that resolves to a record ID at query time.
  When used as a field value in CREATE/UPDATE/UPSERT operations, the
  expression renders as raw SurrealQL rather than a quoted string.

  Examples:
    >>> ref = RecordRef(table='user', record_id='alice')
    >>> ref.to_surql()
    "type::record('user', 'alice')"

    >>> ref = RecordRef(table='post', record_id=123)
    >>> ref.to_surql()
    "type::record('post', 123)"
  """

  table: str
  record_id: str | int

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Render as a type::record() SurrealQL expression.

    Returns:
      SurrealQL type::record() expression
    """
    if isinstance(self.record_id, int):
      return f"type::record('{self.table}', {self.record_id})"
    # Escape single quotes in the record_id string
    escaped_id = self.record_id.replace('\\', '\\\\').replace("'", "\\'")
    return f"type::record('{self.table}', '{escaped_id}')"

  def __str__(self) -> str:
    """Return string representation.

    Returns:
      SurrealQL type::record() expression
    """
    return self.to_surql()


def record_ref(table: str, record_id: str | int) -> RecordRef:
  """Create a SurrealDB type::record() reference.

  Generates a type::record() expression that can be used as a field value
  in CREATE/UPDATE/UPSERT operations. The expression is emitted as raw
  SurrealQL rather than a quoted string.

  Args:
    table: Target table name
    record_id: Record identifier (string or integer)

  Returns:
    RecordRef instance

  Examples:
    >>> record_ref('user', 'alice').to_surql()
    "type::record('user', 'alice')"

    >>> record_ref('post', 123).to_surql()
    "type::record('post', 123)"
  """
  return RecordRef(table=table, record_id=record_id)
