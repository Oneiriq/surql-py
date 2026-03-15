"""Query expression builders for type-safe SurrealDB queries.

This module provides functional builders for field references, literal values,
function calls, and aggregate operations.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict

from surql.types.operators import _quote_value


class Expression(BaseModel):
  """Base expression type for query components.

  All expressions can be converted to SurrealQL strings.

  Examples:
    >>> expr = Expression(sql='name')
    >>> expr.to_surql()
    'name'
  """

  sql: str

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Convert expression to SurrealQL string.

    Returns:
      SurrealQL string representation
    """
    return self.sql


class FieldExpression(Expression):
  """Field reference expression.

  Examples:
    >>> FieldExpression(sql='user.name')
    >>> FieldExpression(sql='age')
  """

  pass


class ValueExpression(Expression):
  """Literal value expression.

  Examples:
    >>> ValueExpression(sql="'Alice'")
    >>> ValueExpression(sql='42')
  """

  pass


class FunctionExpression(Expression):
  """Function call expression.

  Examples:
    >>> FunctionExpression(sql='COUNT(*)')
    >>> FunctionExpression(sql='AVG(age)')
  """

  pass


# Field reference builders


def field(name: str) -> FieldExpression:
  """Create a field reference expression.

  Args:
    name: Field name (supports nested fields with dot notation)

  Returns:
    FieldExpression instance

  Examples:
    >>> field('name')
    >>> field('user.email')
    >>> field('address.city')
  """
  return FieldExpression(sql=name)


def value(val: Any) -> ValueExpression:
  """Create a literal value expression.

  Args:
    val: Value to wrap (will be properly quoted)

  Returns:
    ValueExpression instance

  Examples:
    >>> value('Alice')
    >>> value(42)
    >>> value(True)
  """
  return ValueExpression(sql=_quote_value(val))


# Function call builders


def func(name: str, *args: str | Expression) -> FunctionExpression:
  """Create a function call expression.

  Args:
    name: Function name
    args: Function arguments

  Returns:
    FunctionExpression instance

  Examples:
    >>> func('COUNT', '*')
    >>> func('UPPER', field('name'))
    >>> func('CONCAT', field('first_name'), value(' '), field('last_name'))
  """
  arg_strs = []
  for arg in args:
    if isinstance(arg, Expression):
      arg_strs.append(arg.to_surql())
    else:
      arg_strs.append(str(arg))

  args_str = ', '.join(arg_strs)
  return FunctionExpression(sql=f'{name}({args_str})')


# Aggregate functions


def count(field_name: str | None = None) -> FunctionExpression:
  """Create COUNT aggregate function.

  Args:
    field_name: Optional field name. If None, counts all records.

  Returns:
    FunctionExpression instance

  Examples:
    >>> count()  # COUNT(*)
    >>> count('id')  # COUNT(id)
  """
  arg = field_name if field_name else '*'
  return FunctionExpression(sql=f'COUNT({arg})')


def sum_(field_name: str) -> FunctionExpression:
  """Create SUM aggregate function.

  Args:
    field_name: Field name to sum

  Returns:
    FunctionExpression instance

  Examples:
    >>> sum_('price')
    >>> sum_('quantity')
  """
  return FunctionExpression(sql=f'SUM({field_name})')


def avg(field_name: str) -> FunctionExpression:
  """Create AVG aggregate function.

  Args:
    field_name: Field name to average

  Returns:
    FunctionExpression instance

  Examples:
    >>> avg('age')
    >>> avg('score')
  """
  return FunctionExpression(sql=f'AVG({field_name})')


def min_(field_name: str) -> FunctionExpression:
  """Create MIN aggregate function.

  Args:
    field_name: Field name to find minimum

  Returns:
    FunctionExpression instance

  Examples:
    >>> min_('price')
    >>> min_('created_at')
  """
  return FunctionExpression(sql=f'MIN({field_name})')


def max_(field_name: str) -> FunctionExpression:
  """Create MAX aggregate function.

  Args:
    field_name: Field name to find maximum

  Returns:
    FunctionExpression instance

  Examples:
    >>> max_('price')
    >>> max_('updated_at')
  """
  return FunctionExpression(sql=f'MAX({field_name})')


# String functions


def upper(field_name: str) -> FunctionExpression:
  """Create UPPER string function.

  Args:
    field_name: Field name to uppercase

  Returns:
    FunctionExpression instance

  Examples:
    >>> upper('name')
  """
  return FunctionExpression(sql=f'string::uppercase({field_name})')


def lower(field_name: str) -> FunctionExpression:
  """Create LOWER string function.

  Args:
    field_name: Field name to lowercase

  Returns:
    FunctionExpression instance

  Examples:
    >>> lower('email')
  """
  return FunctionExpression(sql=f'string::lowercase({field_name})')


def concat(*fields: str | Expression) -> FunctionExpression:
  """Create CONCAT string function.

  Args:
    fields: Field names or expressions to concatenate

  Returns:
    FunctionExpression instance

  Examples:
    >>> concat(field('first_name'), value(' '), field('last_name'))
  """
  field_strs = []
  for f in fields:
    if isinstance(f, Expression):
      field_strs.append(f.to_surql())
    else:
      field_strs.append(f)

  fields_str = ', '.join(field_strs)
  return FunctionExpression(sql=f'string::concat({fields_str})')


# Array functions


def array_length(field_name: str) -> FunctionExpression:
  """Create array length function.

  Args:
    field_name: Array field name

  Returns:
    FunctionExpression instance

  Examples:
    >>> array_length('tags')
  """
  return FunctionExpression(sql=f'array::len({field_name})')


def array_contains(field_name: str, val: Any) -> FunctionExpression:
  """Create array contains check function.

  Args:
    field_name: Array field name
    val: Value to check for

  Returns:
    FunctionExpression instance

  Examples:
    >>> array_contains('tags', 'python')
  """
  quoted = _quote_value(val)
  return FunctionExpression(sql=f'array::includes({field_name}, {quoted})')


# Math functions


def abs_(field_name: str) -> FunctionExpression:
  """Create ABS math function.

  Args:
    field_name: Field name

  Returns:
    FunctionExpression instance

  Examples:
    >>> abs_('temperature')
  """
  return FunctionExpression(sql=f'math::abs({field_name})')


def ceil(field_name: str) -> FunctionExpression:
  """Create CEIL math function.

  Args:
    field_name: Field name

  Returns:
    FunctionExpression instance

  Examples:
    >>> ceil('price')
  """
  return FunctionExpression(sql=f'math::ceil({field_name})')


def floor(field_name: str) -> FunctionExpression:
  """Create FLOOR math function.

  Args:
    field_name: Field name

  Returns:
    FunctionExpression instance

  Examples:
    >>> floor('price')
  """
  return FunctionExpression(sql=f'math::floor({field_name})')


def round_(field_name: str, precision: int = 0) -> FunctionExpression:
  """Create ROUND math function.

  Args:
    field_name: Field name
    precision: Number of decimal places

  Returns:
    FunctionExpression instance

  Examples:
    >>> round_('price', 2)
  """
  return FunctionExpression(sql=f'math::round({field_name}, {precision})')


# Time functions


def time_now() -> FunctionExpression:
  """Create time::now() function.

  Returns:
    FunctionExpression instance

  Examples:
    >>> time_now()
  """
  return FunctionExpression(sql='time::now()')


def time_format(field_name: str, format_str: str) -> FunctionExpression:
  """Create time format function.

  Args:
    field_name: Datetime field name
    format_str: Format string

  Returns:
    FunctionExpression instance

  Examples:
    >>> time_format('created_at', '%Y-%m-%d')
  """
  quoted_format = _quote_value(format_str)
  return FunctionExpression(sql=f'time::format({field_name}, {quoted_format})')


# Type functions


def type_is(field_name: str, type_name: str) -> FunctionExpression:
  """Create type::is::* check function.

  Args:
    field_name: Field name
    type_name: Type to check (e.g., 'string', 'number', 'bool')

  Returns:
    FunctionExpression instance

  Examples:
    >>> type_is('value', 'string')
  """
  return FunctionExpression(sql=f'type::is::{type_name}({field_name})')


def cast(field_name: str, target_type: str) -> FunctionExpression:
  """Create type cast function.

  Args:
    field_name: Field name
    target_type: Target type

  Returns:
    FunctionExpression instance

  Examples:
    >>> cast('id', 'string')
    >>> cast('count', 'int')
  """
  return FunctionExpression(sql=f'<{target_type}>{field_name}')


# Expression composition helpers


def as_(expr: Expression, alias: str) -> Expression:
  """Create expression with alias.

  Args:
    expr: Expression to alias
    alias: Alias name

  Returns:
    New Expression instance with alias

  Examples:
    >>> as_(count(), 'total_count')
    >>> as_(concat(field('first'), field('last')), 'full_name')
  """
  return Expression(sql=f'{expr.to_surql()} AS {alias}')


def raw(sql: str) -> Expression:
  """Create raw SQL expression.

  Use with caution - no validation or escaping is performed.

  Args:
    sql: Raw SurrealQL string

  Returns:
    Expression instance

  Examples:
    >>> raw('time::now()')
    >>> raw('meta::id(id)')
  """
  return Expression(sql=sql)
