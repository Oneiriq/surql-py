"""Query operators for building type-safe SurrealDB queries.

This module provides immutable dataclasses for comparison, logical, and array operators.
"""

from dataclasses import dataclass
from typing import Any, Union


@dataclass(frozen=True)
class Operator:
  """Base operator class for query expressions.
  
  All operators are immutable and generate SurrealQL expressions.
  """
  
  def to_surql(self) -> str:
    """Convert operator to SurrealQL expression.
    
    Returns:
      SurrealQL string representation
    """
    raise NotImplementedError('Subclasses must implement to_surql()')


# Comparison Operators

@dataclass(frozen=True)
class Eq(Operator):
  """Equality operator (=).
  
  Examples:
    >>> Eq('name', 'Alice').to_surql()
    "name = 'Alice'"
  """
  
  field: str
  value: Any
  
  def to_surql(self) -> str:
    """Convert to SurrealQL equality expression."""
    return f'{self.field} = {_quote_value(self.value)}'


@dataclass(frozen=True)
class Ne(Operator):
  """Inequality operator (!=).
  
  Examples:
    >>> Ne('status', 'deleted').to_surql()
    "status != 'deleted'"
  """
  
  field: str
  value: Any
  
  def to_surql(self) -> str:
    """Convert to SurrealQL inequality expression."""
    return f'{self.field} != {_quote_value(self.value)}'


@dataclass(frozen=True)
class Gt(Operator):
  """Greater than operator (>).
  
  Examples:
    >>> Gt('age', 18).to_surql()
    'age > 18'
  """
  
  field: str
  value: Any
  
  def to_surql(self) -> str:
    """Convert to SurrealQL greater than expression."""
    return f'{self.field} > {_quote_value(self.value)}'


@dataclass(frozen=True)
class Gte(Operator):
  """Greater than or equal operator (>=).
  
  Examples:
    >>> Gte('score', 100).to_surql()
    'score >= 100'
  """
  
  field: str
  value: Any
  
  def to_surql(self) -> str:
    """Convert to SurrealQL greater than or equal expression."""
    return f'{self.field} >= {_quote_value(self.value)}'


@dataclass(frozen=True)
class Lt(Operator):
  """Less than operator (<).
  
  Examples:
    >>> Lt('price', 50.0).to_surql()
    'price < 50.0'
  """
  
  field: str
  value: Any
  
  def to_surql(self) -> str:
    """Convert to SurrealQL less than expression."""
    return f'{self.field} < {_quote_value(self.value)}'


@dataclass(frozen=True)
class Lte(Operator):
  """Less than or equal operator (<=).
  
  Examples:
    >>> Lte('quantity', 10).to_surql()
    'quantity <= 10'
  """
  
  field: str
  value: Any
  
  def to_surql(self) -> str:
    """Convert to SurrealQL less than or equal expression."""
    return f'{self.field} <= {_quote_value(self.value)}'


@dataclass(frozen=True)
class Contains(Operator):
  """Contains operator for string/array containment checks.
  
  Examples:
    >>> Contains('email', '@example.com').to_surql()
    "email CONTAINS '@example.com'"
  """
  
  field: str
  value: Any
  
  def to_surql(self) -> str:
    """Convert to SurrealQL CONTAINS expression."""
    return f'{self.field} CONTAINS {_quote_value(self.value)}'


@dataclass(frozen=True)
class ContainsNot(Operator):
  """Does not contain operator.
  
  Examples:
    >>> ContainsNot('tags', 'spam').to_surql()
    "tags CONTAINSNOT 'spam'"
  """
  
  field: str
  value: Any
  
  def to_surql(self) -> str:
    """Convert to SurrealQL CONTAINSNOT expression."""
    return f'{self.field} CONTAINSNOT {_quote_value(self.value)}'


@dataclass(frozen=True)
class ContainsAll(Operator):
  """Contains all operator for array checks.
  
  Examples:
    >>> ContainsAll('tags', ['python', 'database']).to_surql()
    "tags CONTAINSALL ['python', 'database']"
  """
  
  field: str
  values: list[Any]
  
  def to_surql(self) -> str:
    """Convert to SurrealQL CONTAINSALL expression."""
    values_str = '[' + ', '.join(_quote_value(v) for v in self.values) + ']'
    return f'{self.field} CONTAINSALL {values_str}'


@dataclass(frozen=True)
class ContainsAny(Operator):
  """Contains any operator for array checks.
  
  Examples:
    >>> ContainsAny('tags', ['python', 'javascript']).to_surql()
    "tags CONTAINSANY ['python', 'javascript']"
  """
  
  field: str
  values: list[Any]
  
  def to_surql(self) -> str:
    """Convert to SurrealQL CONTAINSANY expression."""
    values_str = '[' + ', '.join(_quote_value(v) for v in self.values) + ']'
    return f'{self.field} CONTAINSANY {values_str}'


@dataclass(frozen=True)
class Inside(Operator):
  """Inside operator to check if value is in array.
  
  Examples:
    >>> Inside('status', ['active', 'pending']).to_surql()
    "status INSIDE ['active', 'pending']"
  """
  
  field: str
  values: list[Any]
  
  def to_surql(self) -> str:
    """Convert to SurrealQL INSIDE expression."""
    values_str = '[' + ', '.join(_quote_value(v) for v in self.values) + ']'
    return f'{self.field} INSIDE {values_str}'


@dataclass(frozen=True)
class NotInside(Operator):
  """Not inside operator.
  
  Examples:
    >>> NotInside('status', ['deleted', 'archived']).to_surql()
    "status NOTINSIDE ['deleted', 'archived']"
  """
  
  field: str
  values: list[Any]
  
  def to_surql(self) -> str:
    """Convert to SurrealQL NOTINSIDE expression."""
    values_str = '[' + ', '.join(_quote_value(v) for v in self.values) + ']'
    return f'{self.field} NOTINSIDE {values_str}'


@dataclass(frozen=True)
class IsNull(Operator):
  """IS NULL operator.
  
  Examples:
    >>> IsNull('deleted_at').to_surql()
    'deleted_at IS NULL'
  """
  
  field: str
  
  def to_surql(self) -> str:
    """Convert to SurrealQL IS NULL expression."""
    return f'{self.field} IS NULL'


@dataclass(frozen=True)
class IsNotNull(Operator):
  """IS NOT NULL operator.
  
  Examples:
    >>> IsNotNull('created_at').to_surql()
    'created_at IS NOT NULL'
  """
  
  field: str
  
  def to_surql(self) -> str:
    """Convert to SurrealQL IS NOT NULL expression."""
    return f'{self.field} IS NOT NULL'


# Logical Operators

@dataclass(frozen=True)
class And(Operator):
  """Logical AND operator.
  
  Examples:
    >>> And(Gt('age', 18), Eq('status', 'active')).to_surql()
    "(age > 18) AND (status = 'active')"
  """
  
  left: Operator
  right: Operator
  
  def to_surql(self) -> str:
    """Convert to SurrealQL AND expression."""
    return f'({self.left.to_surql()}) AND ({self.right.to_surql()})'


@dataclass(frozen=True)
class Or(Operator):
  """Logical OR operator.
  
  Examples:
    >>> Or(Eq('type', 'admin'), Eq('type', 'moderator')).to_surql()
    "(type = 'admin') OR (type = 'moderator')"
  """
  
  left: Operator
  right: Operator
  
  def to_surql(self) -> str:
    """Convert to SurrealQL OR expression."""
    return f'({self.left.to_surql()}) OR ({self.right.to_surql()})'


@dataclass(frozen=True)
class Not(Operator):
  """Logical NOT operator.
  
  Examples:
    >>> Not(Eq('status', 'deleted')).to_surql()
    "NOT (status = 'deleted')"
  """
  
  operand: Operator
  
  def to_surql(self) -> str:
    """Convert to SurrealQL NOT expression."""
    return f'NOT ({self.operand.to_surql()})'


# Helper functions for functional composition

def eq(field: str, value: Any) -> Eq:
  """Create equality operator.
  
  Args:
    field: Field name
    value: Value to compare
    
  Returns:
    Eq operator instance
  """
  return Eq(field, value)


def ne(field: str, value: Any) -> Ne:
  """Create inequality operator.
  
  Args:
    field: Field name
    value: Value to compare
    
  Returns:
    Ne operator instance
  """
  return Ne(field, value)


def gt(field: str, value: Any) -> Gt:
  """Create greater than operator.
  
  Args:
    field: Field name
    value: Value to compare
    
  Returns:
    Gt operator instance
  """
  return Gt(field, value)


def gte(field: str, value: Any) -> Gte:
  """Create greater than or equal operator.
  
  Args:
    field: Field name
    value: Value to compare
    
  Returns:
    Gte operator instance
  """
  return Gte(field, value)


def lt(field: str, value: Any) -> Lt:
  """Create less than operator.
  
  Args:
    field: Field name
    value: Value to compare
    
  Returns:
    Lt operator instance
  """
  return Lt(field, value)


def lte(field: str, value: Any) -> Lte:
  """Create less than or equal operator.
  
  Args:
    field: Field name
    value: Value to compare
    
  Returns:
    Lte operator instance
  """
  return Lte(field, value)


def contains(field: str, value: Any) -> Contains:
  """Create contains operator.
  
  Args:
    field: Field name
    value: Value to check for containment
    
  Returns:
    Contains operator instance
  """
  return Contains(field, value)


def contains_not(field: str, value: Any) -> ContainsNot:
  """Create contains not operator.
  
  Args:
    field: Field name
    value: Value to check for non-containment
    
  Returns:
    ContainsNot operator instance
  """
  return ContainsNot(field, value)


def contains_all(field: str, values: list[Any]) -> ContainsAll:
  """Create contains all operator.
  
  Args:
    field: Field name
    values: List of values to check
    
  Returns:
    ContainsAll operator instance
  """
  return ContainsAll(field, values)


def contains_any(field: str, values: list[Any]) -> ContainsAny:
  """Create contains any operator.
  
  Args:
    field: Field name
    values: List of values to check
    
  Returns:
    ContainsAny operator instance
  """
  return ContainsAny(field, values)


def inside(field: str, values: list[Any]) -> Inside:
  """Create inside operator.
  
  Args:
    field: Field name
    values: List of values
    
  Returns:
    Inside operator instance
  """
  return Inside(field, values)


def not_inside(field: str, values: list[Any]) -> NotInside:
  """Create not inside operator.
  
  Args:
    field: Field name
    values: List of values
    
  Returns:
    NotInside operator instance
  """
  return NotInside(field, values)


def is_null(field: str) -> IsNull:
  """Create IS NULL operator.
  
  Args:
    field: Field name
    
  Returns:
    IsNull operator instance
  """
  return IsNull(field)


def is_not_null(field: str) -> IsNotNull:
  """Create IS NOT NULL operator.
  
  Args:
    field: Field name
    
  Returns:
    IsNotNull operator instance
  """
  return IsNotNull(field)


def and_(left: Operator, right: Operator) -> And:
  """Create AND operator.
  
  Args:
    left: Left operand
    right: Right operand
    
  Returns:
    And operator instance
  """
  return And(left, right)


def or_(left: Operator, right: Operator) -> Or:
  """Create OR operator.
  
  Args:
    left: Left operand
    right: Right operand
    
  Returns:
    Or operator instance
  """
  return Or(left, right)


def not_(operand: Operator) -> Not:
  """Create NOT operator.
  
  Args:
    operand: Operand to negate
    
  Returns:
    Not operator instance
  """
  return Not(operand)


# Private helper functions

def _quote_value(value: Any) -> str:
  """Quote value for SurrealQL.
  
  Args:
    value: Value to quote
    
  Returns:
    Quoted string representation
  """
  if value is None:
    return 'NULL'
  elif isinstance(value, bool):
    return 'true' if value else 'false'
  elif isinstance(value, (int, float)):
    return str(value)
  elif isinstance(value, str):
    # Escape single quotes in string
    escaped = value.replace("'", "\\'")
    return f"'{escaped}'"
  else:
    # For other types, convert to string and quote
    return f"'{str(value)}'"
