"""Tests for query expression builders."""

import pytest
from pydantic import ValidationError

from reverie.query.expressions import (
    Expression,
    FieldExpression,
    FunctionExpression,
    ValueExpression,
    abs_,
    array_contains,
    array_length,
    as_,
    avg,
    cast,
    ceil,
    concat,
    count,
    field,
    floor,
    func,
    lower,
    max_,
    min_,
    raw,
    round_,
    sum_,
    time_format,
    time_now,
    type_is,
    upper,
    value,
)


class TestExpressionBase:
    """Test Expression base class."""

    def test_create_expression(self):
        """Test creating a basic expression."""
        expr = Expression(sql='test_field')
        assert expr.sql == 'test_field'

    def test_to_surql(self):
        """Test converting expression to SurrealQL."""
        expr = Expression(sql='name')
        assert expr.to_surql() == 'name'

    def test_expression_immutable(self):
        """Test that Expression is frozen."""
        expr = Expression(sql='name')
        with pytest.raises((ValidationError, AttributeError)):
            expr.sql = 'modified'

    def test_expression_equality(self):
        """Test expression equality comparison."""
        expr1 = Expression(sql='field')
        expr2 = Expression(sql='field')
        expr3 = Expression(sql='other')
        assert expr1 == expr2
        assert expr1 != expr3


class TestFieldExpression:
    """Test FieldExpression class."""

    def test_create_field_expression(self):
        """Test creating field expression."""
        expr = FieldExpression(sql='user.name')
        assert isinstance(expr, Expression)
        assert expr.sql == 'user.name'

    def test_field_expression_to_surql(self):
        """Test field expression to SurrealQL."""
        expr = FieldExpression(sql='email')
        assert expr.to_surql() == 'email'


class TestValueExpression:
    """Test ValueExpression class."""

    def test_create_value_expression(self):
        """Test creating value expression."""
        expr = ValueExpression(sql="'Alice'")
        assert isinstance(expr, Expression)
        assert expr.sql == "'Alice'"

    def test_value_expression_to_surql(self):
        """Test value expression to SurrealQL."""
        expr = ValueExpression(sql='42')
        assert expr.to_surql() == '42'


class TestFunctionExpression:
    """Test FunctionExpression class."""

    def test_create_function_expression(self):
        """Test creating function expression."""
        expr = FunctionExpression(sql='COUNT(*)')
        assert isinstance(expr, Expression)
        assert expr.sql == 'COUNT(*)'

    def test_function_expression_to_surql(self):
        """Test function expression to SurrealQL."""
        expr = FunctionExpression(sql='AVG(age)')
        assert expr.to_surql() == 'AVG(age)'


class TestFieldBuilder:
    """Test field reference builder."""

    def test_field_simple(self):
        """Test simple field reference."""
        expr = field('name')
        assert isinstance(expr, FieldExpression)
        assert expr.to_surql() == 'name'

    def test_field_nested(self):
        """Test nested field reference."""
        expr = field('user.email')
        assert expr.to_surql() == 'user.email'

    def test_field_deep_nested(self):
        """Test deeply nested field reference."""
        expr = field('address.city.zipcode')
        assert expr.to_surql() == 'address.city.zipcode'


class TestValueBuilder:
    """Test literal value builder."""

    def test_value_string(self):
        """Test string value."""
        expr = value('Alice')
        assert isinstance(expr, ValueExpression)
        assert "'Alice'" in expr.to_surql()

    def test_value_number(self):
        """Test numeric value."""
        expr = value(42)
        assert expr.to_surql() == '42'

    def test_value_boolean_true(self):
        """Test boolean true value."""
        expr = value(True)
        assert expr.to_surql() == 'true'

    def test_value_boolean_false(self):
        """Test boolean false value."""
        expr = value(False)
        assert expr.to_surql() == 'false'

    def test_value_none(self):
        """Test None value."""
        expr = value(None)
        assert expr.to_surql() == 'NULL'


class TestFunctionBuilder:
    """Test generic function builder."""

    def test_func_no_args(self):
        """Test function with no arguments."""
        expr = func('NOW')
        assert isinstance(expr, FunctionExpression)
        assert expr.to_surql() == 'NOW()'

    def test_func_string_arg(self):
        """Test function with string argument."""
        expr = func('COUNT', '*')
        assert expr.to_surql() == 'COUNT(*)'

    def test_func_expression_arg(self):
        """Test function with expression argument."""
        expr = func('UPPER', field('name'))
        assert expr.to_surql() == 'UPPER(name)'

    def test_func_multiple_args(self):
        """Test function with multiple arguments."""
        expr = func('CONCAT', field('first'), value(' '), field('last'))
        result = expr.to_surql()
        assert 'CONCAT' in result
        assert 'first' in result
        assert 'last' in result

    def test_func_mixed_args(self):
        """Test function with mixed argument types."""
        expr = func('TEST', field('x'), 'literal', value(42))
        result = expr.to_surql()
        assert 'TEST(' in result
        assert 'x' in result
        assert 'literal' in result


class TestAggregates:
    """Test aggregate function builders."""

    def test_count_all(self):
        """Test COUNT(*) aggregate."""
        expr = count()
        assert expr.to_surql() == 'COUNT(*)'

    def test_count_field(self):
        """Test COUNT(field) aggregate."""
        expr = count('id')
        assert expr.to_surql() == 'COUNT(id)'

    def test_sum(self):
        """Test SUM aggregate."""
        expr = sum_('price')
        assert expr.to_surql() == 'SUM(price)'

    def test_avg(self):
        """Test AVG aggregate."""
        expr = avg('age')
        assert expr.to_surql() == 'AVG(age)'

    def test_min(self):
        """Test MIN aggregate."""
        expr = min_('price')
        assert expr.to_surql() == 'MIN(price)'

    def test_max(self):
        """Test MAX aggregate."""
        expr = max_('updated_at')
        assert expr.to_surql() == 'MAX(updated_at)'


class TestStringFunctions:
    """Test string function builders."""

    def test_upper(self):
        """Test UPPER function."""
        expr = upper('name')
        assert expr.to_surql() == 'string::uppercase(name)'

    def test_lower(self):
        """Test LOWER function."""
        expr = lower('email')
        assert expr.to_surql() == 'string::lowercase(email)'

    def test_concat_strings(self):
        """Test CONCAT with string arguments."""
        expr = concat('first', 'last')
        assert expr.to_surql() == 'string::concat(first, last)'

    def test_concat_expressions(self):
        """Test CONCAT with expression arguments."""
        expr = concat(field('first'), value(' '), field('last'))
        result = expr.to_surql()
        assert 'string::concat' in result
        assert 'first' in result
        assert 'last' in result

    def test_concat_mixed(self):
        """Test CONCAT with mixed arguments."""
        expr = concat(field('title'), ': ', field('subtitle'))
        result = expr.to_surql()
        assert 'string::concat' in result


class TestArrayFunctions:
    """Test array function builders."""

    def test_array_length(self):
        """Test array length function."""
        expr = array_length('tags')
        assert expr.to_surql() == 'array::len(tags)'

    def test_array_contains_string(self):
        """Test array contains with string value."""
        expr = array_contains('tags', 'python')
        result = expr.to_surql()
        assert 'array::includes' in result
        assert 'tags' in result
        assert 'python' in result

    def test_array_contains_number(self):
        """Test array contains with numeric value."""
        expr = array_contains('numbers', 42)
        result = expr.to_surql()
        assert 'array::includes' in result
        assert 'numbers' in result
        assert '42' in result


class TestMathFunctions:
    """Test math function builders."""

    def test_abs(self):
        """Test ABS function."""
        expr = abs_('temperature')
        assert expr.to_surql() == 'math::abs(temperature)'

    def test_ceil(self):
        """Test CEIL function."""
        expr = ceil('price')
        assert expr.to_surql() == 'math::ceil(price)'

    def test_floor(self):
        """Test FLOOR function."""
        expr = floor('price')
        assert expr.to_surql() == 'math::floor(price)'

    def test_round_default_precision(self):
        """Test ROUND with default precision."""
        expr = round_('price')
        assert expr.to_surql() == 'math::round(price, 0)'

    def test_round_with_precision(self):
        """Test ROUND with custom precision."""
        expr = round_('price', 2)
        assert expr.to_surql() == 'math::round(price, 2)'

    def test_round_negative_precision(self):
        """Test ROUND with negative precision."""
        expr = round_('value', -1)
        assert expr.to_surql() == 'math::round(value, -1)'


class TestTimeFunctions:
    """Test time function builders."""

    def test_time_now(self):
        """Test time::now() function."""
        expr = time_now()
        assert expr.to_surql() == 'time::now()'

    def test_time_format(self):
        """Test time format function."""
        expr = time_format('created_at', '%Y-%m-%d')
        result = expr.to_surql()
        assert 'time::format' in result
        assert 'created_at' in result
        assert '%Y-%m-%d' in result

    def test_time_format_different_format(self):
        """Test time format with different format string."""
        expr = time_format('updated_at', '%H:%M:%S')
        result = expr.to_surql()
        assert 'time::format' in result
        assert 'updated_at' in result
        assert '%H:%M:%S' in result


class TestTypeFunctions:
    """Test type checking and casting functions."""

    def test_type_is_string(self):
        """Test type::is::string check."""
        expr = type_is('value', 'string')
        assert expr.to_surql() == 'type::is::string(value)'

    def test_type_is_number(self):
        """Test type::is::number check."""
        expr = type_is('count', 'number')
        assert expr.to_surql() == 'type::is::number(count)'

    def test_type_is_bool(self):
        """Test type::is::bool check."""
        expr = type_is('flag', 'bool')
        assert expr.to_surql() == 'type::is::bool(flag)'

    def test_cast_to_string(self):
        """Test casting to string."""
        expr = cast('id', 'string')
        assert expr.to_surql() == '<string>id'

    def test_cast_to_int(self):
        """Test casting to int."""
        expr = cast('count', 'int')
        assert expr.to_surql() == '<int>count'

    def test_cast_to_float(self):
        """Test casting to float."""
        expr = cast('value', 'float')
        assert expr.to_surql() == '<float>value'


class TestCompositionHelpers:
    """Test expression composition helpers."""

    def test_as_with_field(self):
        """Test aliasing a field expression."""
        expr = as_(field('name'), 'user_name')
        assert expr.to_surql() == 'name AS user_name'

    def test_as_with_function(self):
        """Test aliasing a function expression."""
        expr = as_(count(), 'total')
        assert expr.to_surql() == 'COUNT(*) AS total'

    def test_as_with_aggregate(self):
        """Test aliasing an aggregate expression."""
        expr = as_(avg('age'), 'average_age')
        assert expr.to_surql() == 'AVG(age) AS average_age'

    def test_as_with_concat(self):
        """Test aliasing a concat expression."""
        expr = as_(concat(field('first'), field('last')), 'full_name')
        result = expr.to_surql()
        assert 'AS full_name' in result
        assert 'string::concat' in result

    def test_raw_simple(self):
        """Test raw SQL expression."""
        expr = raw('time::now()')
        assert expr.to_surql() == 'time::now()'

    def test_raw_complex(self):
        """Test raw SQL with complex expression."""
        expr = raw('meta::id(id)')
        assert expr.to_surql() == 'meta::id(id)'

    def test_raw_custom_function(self):
        """Test raw SQL with custom function."""
        expr = raw('custom::function(arg1, arg2)')
        assert expr.to_surql() == 'custom::function(arg1, arg2)'


class TestExpressionComposition:
    """Test composing multiple expressions together."""

    def test_nested_functions(self):
        """Test nesting function calls."""
        inner = field('name')
        outer = func('UPPER', inner)
        assert 'UPPER' in outer.to_surql()
        assert 'name' in outer.to_surql()

    def test_complex_concat(self):
        """Test complex concatenation with multiple expression types."""
        expr = concat(
            field('first_name'),
            value(' '),
            field('last_name'),
            value(' - '),
            field('email'),
        )
        result = expr.to_surql()
        assert 'string::concat' in result
        assert 'first_name' in result
        assert 'last_name' in result
        assert 'email' in result

    def test_aliased_aggregate(self):
        """Test aliased aggregate function."""
        expr = as_(sum_('total_sales'), 'revenue')
        result = expr.to_surql()
        assert 'SUM(total_sales)' in result
        assert 'AS revenue' in result

    def test_function_in_function(self):
        """Test function as argument to another function."""
        inner = upper('name')
        outer = func('LENGTH', inner)
        result = outer.to_surql()
        assert 'LENGTH' in result
        assert 'string::uppercase' in result


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_empty_field_name(self):
        """Test field with empty name."""
        expr = field('')
        assert expr.to_surql() == ''

    def test_field_with_special_chars(self):
        """Test field with special characters."""
        expr = field('user.address.street_name')
        assert expr.to_surql() == 'user.address.street_name'

    def test_value_with_quotes(self):
        """Test value containing quotes."""
        expr = value("It's working")
        result = expr.to_surql()
        assert "It's working" in result or 'It' in result

    def test_func_no_name(self):
        """Test function with empty name."""
        expr = func('')
        assert expr.to_surql() == '()'

    def test_concat_single_field(self):
        """Test concat with single field."""
        expr = concat(field('name'))
        assert 'string::concat(name)' in expr.to_surql()

    def test_round_large_precision(self):
        """Test round with large precision value."""
        expr = round_('value', 10)
        assert expr.to_surql() == 'math::round(value, 10)'

    def test_multiple_aliases(self):
        """Test applying alias to already aliased expression."""
        expr1 = as_(field('name'), 'alias1')
        expr2 = as_(expr1, 'alias2')
        result = expr2.to_surql()
        assert 'AS alias2' in result
        assert 'AS alias1' in result
