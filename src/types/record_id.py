"""RecordID type wrapper for SurrealDB record identifiers.

This module provides a type-safe wrapper for SurrealDB record IDs (table:id format).
"""

from typing import Any, Generic, TypeVar, Union
from pydantic import BaseModel, field_validator


T = TypeVar('T')


class RecordID(BaseModel, Generic[T]):
  """Type-safe RecordID wrapper for SurrealDB record identifiers.
  
  Represents a SurrealDB record ID in the format table:id.
  Supports generic typing for table types to enable type safety.
  
  Examples:
    Basic usage:
    >>> record_id = RecordID(table='user', id='alice')
    >>> str(record_id)
    'user:alice'
    
    Parse from string:
    >>> record_id = RecordID.parse('user:123')
    >>> record_id.table
    'user'
    >>> record_id.id
    '123'
    
    Type-safe with generics:
    >>> UserID = RecordID[User]
    >>> user_id: UserID = RecordID(table='user', id='alice')
  """
  
  table: str
  id: Union[str, int]
  
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
  
  def __str__(self) -> str:
    """Return string representation in table:id format.
    
    Returns:
      String in format 'table:id'
    """
    return f'{self.table}:{self.id}'
  
  def __repr__(self) -> str:
    """Return detailed representation.
    
    Returns:
      String representation for debugging
    """
    return f"RecordID(table='{self.table}', id={self.id!r})"
  
  @classmethod
  def parse(cls, record_id: str) -> 'RecordID[Any]':
    """Parse RecordID from string format.
    
    Args:
      record_id: String in format 'table:id'
      
    Returns:
      RecordID instance
      
    Raises:
      ValueError: If string format is invalid
      
    Examples:
      >>> RecordID.parse('user:alice')
      RecordID(table='user', id='alice')
      
      >>> RecordID.parse('post:123')
      RecordID(table='post', id=123)
    """
    if ':' not in record_id:
      raise ValueError(
        f'Invalid record ID format: {record_id}. Expected format: table:id'
      )
    
    parts = record_id.split(':', 1)
    if len(parts) != 2:
      raise ValueError(
        f'Invalid record ID format: {record_id}. Expected format: table:id'
      )
    
    table, id_str = parts
    
    # Try to parse as int, otherwise keep as string
    try:
      id_value: Union[str, int] = int(id_str)
    except ValueError:
      id_value = id_str
    
    return cls(table=table, id=id_value)
  
  def to_surql(self) -> str:
    """Convert to SurrealQL record ID format.
    
    Returns:
      String in SurrealQL format suitable for queries
    """
    return str(self)
  
  class Config:
    """Pydantic configuration."""
    frozen = True  # Make immutable
