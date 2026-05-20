"""RecordID type wrapper for SurrealDB record identifiers.

This module provides a type-safe wrapper for SurrealDB record IDs (table:id format).
Supports angle bracket syntax for complex IDs containing special characters.
"""

import re
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, field_validator

T = TypeVar('T')


class RecordID[T](BaseModel):
  """Type-safe RecordID wrapper for SurrealDB record identifiers.

  Represents a SurrealDB record ID in the format table:id.
  Supports generic typing for table types to enable type safety.
  Automatically uses angle bracket syntax for IDs with special characters.

  Examples:
    Basic usage:
    >>> record_id = RecordID(table='user', id='alice')
    >>> str(record_id)
    'user:alice'

    Complex IDs with unicode angle brackets (SurrealDB v3 syntax):
    >>> record_id = RecordID(table='outlet', id='alaskabeacon.com')
    >>> str(record_id)
    'outlet:⟨alaskabeacon.com⟩'

    Parse from string:
    >>> record_id = RecordID.parse('user:123')
    >>> record_id.table
    'user'
    >>> record_id.id
    '123'

    Parse angle bracket syntax:
    >>> record_id = RecordID.parse('outlet:<alaskabeacon.com>')
    >>> record_id.id
    'alaskabeacon.com'

    Type-safe with generics:
    >>> UserID = RecordID[User]
    >>> user_id: UserID = RecordID(table='user', id='alice')
  """

  table: str
  id: str | int

  @field_validator('table')
  @classmethod
  def validate_table(cls, v: str) -> str:
    """Validate table name follows SurrealDB naming rules.

    Args:
      v: The table name to validate

    Returns:
      The validated table name

    Raises:
      ValueError: If table name is invalid
    """
    if not v:
      raise ValueError('Table name cannot be empty')

    # Check if name contains only alphanumeric and underscore characters
    if not v.replace('_', '').isalnum():
      raise ValueError(
        f'Invalid table name: {v}. Must contain only alphanumeric characters and underscores'
      )

    return v

  @staticmethod
  def _needs_angle_brackets(id_value: str | int) -> bool:
    """Check if an ID requires angle bracket syntax.

    SurrealDB v3 parses ``table:<id>`` left-to-right and expects the
    portion after the colon to be a valid record-id key: either an
    integer literal, or an identifier matching
    ``[a-zA-Z_][a-zA-Z0-9_]*``. Any other shape — dots, hyphens,
    colons, leading digits mixed with letters (``1abc``), etc. — has
    to be wrapped in unicode angle brackets ``⟨ … ⟩`` so the parser
    treats it as an opaque key rather than tokenising it as a number
    followed by garbage (the v3 server error in that case is
    ``Unexpected token`` at the first non-numeric character).

    Args:
      id_value: The ID value to check

    Returns:
      True if angle brackets are needed, False otherwise
    """
    # Integers never need angle brackets
    if isinstance(id_value, int):
      return False

    # Empty strings short-circuit so the identifier regex below doesn't
    # accidentally accept them.
    if not id_value:
      return True

    # Two safe shapes are emitted bare:
    #   1. Identifier-shaped: ``[a-zA-Z_][a-zA-Z0-9_]*`` (``user_42``).
    #   2. Pure-digit strings: ``[0-9]+`` (``123``). SurrealDB happily
    #      parses these as the integer-key shape and round-trips them
    #      as integers, so no brackets are needed.
    # Anything else — leading digit mixed with letters (``1abc``),
    # hyphens, dots, colons, etc. — must be bracketed so the v3
    # parser treats the id as an opaque key. Without this escape the
    # lexer otherwise tokenises ``1abc`` as ``<number> <ident>`` and
    # rejects the record id with a parse error.
    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', id_value):
      return False
    return not id_value.isdigit()

  def __str__(self) -> str:
    r"""Return string representation in table:id format.

    Automatically wraps complex IDs in **unicode** angle brackets (U+27E8 /
    U+27E9) which are the SurrealDB v3 record-id escape syntax. ASCII `<` /
    `>` are rejected by the v3 parser with `Unexpected token \`<\`, expected
    a record-id key` (issue #87).

    Returns:
      String in format `'table:id'` or `'table:⟨id⟩'`.
    """
    id_str = str(self.id)
    if self._needs_angle_brackets(self.id):
      return f'{self.table}:⟨{id_str}⟩'
    return f'{self.table}:{id_str}'

  def __repr__(self) -> str:
    """Return detailed representation.

    Returns:
      String representation for debugging
    """
    return f"RecordID(table='{self.table}', id={self.id!r})"

  @staticmethod
  def strip_brackets(value: str | None) -> str | None:
    """Strip SurrealDB v3 wire-format angle brackets from a record-id string.

    SurrealDB v3 returns record-id field values with unicode angle
    brackets when the id portion contains special characters
    (``'org_node:⟨BFS:corp-1⟩'``, ``'plan_chunk:⟨demo-plan-ff3d5981⟩'``).
    Downstream callers that want the bare ``table:id`` shape — for use
    in API responses, log lines, or string-keyed lookups — previously
    had to call ``value.replace('⟨', '').replace('⟩', '')`` themselves
    at every boundary. This helper centralises that strip.

    Both forms are accepted on input: the v3 unicode brackets
    (``⟨ … ⟩``, U+27E8 / U+27E9) and the legacy ASCII brackets
    (``< … >``). Non-string and bracket-less inputs are returned
    untouched so the helper is safe to apply unconditionally.

    Args:
      value: A record-id-shaped string (e.g. ``'outlet:⟨alaska.com⟩'``).

    Returns:
      The same string with any wire-format brackets removed
      (``'outlet:alaska.com'``). ``None`` returns ``None``; other
      non-string values are coerced via ``str()`` first.

    Examples:
      >>> RecordID.strip_brackets('outlet:⟨alaska.com⟩')
      'outlet:alaska.com'

      >>> RecordID.strip_brackets('plan_chunk:⟨demo-plan-ff3d5981⟩')
      'plan_chunk:demo-plan-ff3d5981'

      >>> RecordID.strip_brackets('user:alice')  # untouched
      'user:alice'

      >>> RecordID.strip_brackets('outlet:<legacy.com>')  # ASCII form
      'outlet:legacy.com'
    """
    if value is None:
      return None
    s = value if isinstance(value, str) else str(value)
    # Unicode brackets first (the v3 norm), then ASCII as a fallback.
    return s.replace('⟨', '').replace('⟩', '').replace('<', '').replace('>', '')

  @classmethod
  def parse(cls, record_id: str) -> 'RecordID[Any]':
    """Parse RecordID from string format.

    Supports both simple and angle bracket syntax. Wire-format
    brackets — unicode ``⟨ … ⟩`` (the SurrealDB v3 escape) or legacy
    ASCII ``< … >`` — are stripped from the id portion so the
    resulting ``.id`` attribute carries the clean key value.
    Round-tripping a bracketed wire string through
    ``parse(s) → str(...)`` reproduces the same bracketed wire form
    (the serialization rule in ``__str__`` re-applies brackets
    whenever the id contains special characters), so callers can
    safely drop their own ``.replace('⟨', '').replace('⟩', '')`` calls.

    Args:
      record_id: String in format 'table:id', 'table:<id>', or
        'table:⟨id⟩'. The id portion may also itself contain colons
        (composite ids) — only the first colon is used as the
        table separator.

    Returns:
      RecordID instance with brackets stripped from ``.id``.

    Raises:
      ValueError: If string format is invalid

    Examples:
      >>> RecordID.parse('user:alice')
      RecordID(table='user', id='alice')

      >>> RecordID.parse('post:123')
      RecordID(table='post', id=123)

      >>> RecordID.parse('outlet:<alaskabeacon.com>')
      RecordID(table='outlet', id='alaskabeacon.com')

      >>> RecordID.parse('outlet:⟨alaskabeacon.com⟩')
      RecordID(table='outlet', id='alaskabeacon.com')

      >>> # Hyphenated id round-trips through bracket form on output.
      >>> rid = RecordID.parse('plan_chunk:⟨demo-plan-ff3d5981⟩')
      >>> rid.id
      'demo-plan-ff3d5981'
      >>> str(rid)
      'plan_chunk:⟨demo-plan-ff3d5981⟩'
    """
    if ':' not in record_id:
      raise ValueError(f'Invalid record ID format: {record_id}. Expected format: table:id')

    parts = record_id.split(':', 1)
    if len(parts) != 2:
      raise ValueError(f'Invalid record ID format: {record_id}. Expected format: table:id')

    table, id_str = parts

    if not table or not table.strip():
      raise ValueError(f'Invalid record ID: table name cannot be empty in {record_id!r}')
    if not id_str or not id_str.strip():
      raise ValueError(f'Invalid record ID: id cannot be empty in {record_id!r}')

    # Strip angle brackets if present — accept BOTH ASCII `<>` (legacy / older
    # serialization) and unicode `⟨⟩` (current SurrealDB v3 syntax).
    if (
      id_str.startswith('<')
      and id_str.endswith('>')
      or id_str.startswith('⟨')
      and id_str.endswith('⟩')
    ):
      id_str = id_str[1:-1]

    # Try to parse as int, otherwise keep as string
    try:
      id_value: str | int = int(id_str)
    except ValueError:
      id_value = id_str

    return cls(table=table, id=id_value)

  def to_surql(self) -> str:
    """Convert to SurrealQL record ID format.

    Returns:
      String in SurrealQL format suitable for queries
    """
    return str(self)

  model_config = ConfigDict(frozen=True)
