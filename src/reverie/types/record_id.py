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

    Complex IDs with angle brackets:
    >>> record_id = RecordID(table='outlet', id='alaskabeacon.com')
    >>> str(record_id)
    'outlet:<alaskabeacon.com>'

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

    Args:
      id_value: The ID value to check

    Returns:
      True if angle brackets are needed, False otherwise
    """
    # Integers never need angle brackets
    if isinstance(id_value, int):
      return False

    # Simple alphanumeric IDs with underscores don't need brackets
    # Pattern: only alphanumeric and underscores
    # Everything else needs angle brackets (dots, hyphens, colons, etc.)
    return not re.match(r'^[a-zA-Z0-9_]+$', id_value)

  def __str__(self) -> str:
    """Return string representation in table:id format.

    Automatically adds angle brackets for complex IDs.

    Returns:
      String in format 'table:id' or 'table:<id>'
    """
    id_str = str(self.id)
    if self._needs_angle_brackets(self.id):
      return f'{self.table}:<{id_str}>'
    return f'{self.table}:{id_str}'

  def __repr__(self) -> str:
    """Return detailed representation.

    Returns:
      String representation for debugging
    """
    return f"RecordID(table='{self.table}', id={self.id!r})"

  @classmethod
  def parse(cls, record_id: str) -> 'RecordID[Any]':
    """Parse RecordID from string format.

    Supports both simple and angle bracket syntax.

    Args:
      record_id: String in format 'table:id' or 'table:<id>'

    Returns:
      RecordID instance

    Raises:
      ValueError: If string format is invalid

    Examples:
      >>> RecordID.parse('user:alice')
      RecordID(table='user', id='alice')

      >>> RecordID.parse('post:123')
      RecordID(table='post', id=123)

      >>> RecordID.parse('outlet:<alaskabeacon.com>')
      RecordID(table='outlet', id='alaskabeacon.com')
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

    # Strip angle brackets if present
    if id_str.startswith('<') and id_str.endswith('>'):
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
