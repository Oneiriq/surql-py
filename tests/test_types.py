"""Tests for the types module (RecordID and operators)."""

import pytest
from pydantic import ValidationError

from reverie.types.operators import (
  And,
  Contains,
  ContainsAll,
  ContainsAny,
  ContainsNot,
  Eq,
  Gt,
  Gte,
  Inside,
  IsNotNull,
  IsNull,
  Lt,
  Lte,
  Ne,
  Not,
  NotInside,
  Operator,
  Or,
  _quote_value,
  and_,
  contains,
  contains_all,
  contains_any,
  contains_not,
  eq,
  gt,
  gte,
  inside,
  is_not_null,
  is_null,
  lt,
  lte,
  ne,
  not_,
  not_inside,
  or_,
)
from reverie.types.record_id import RecordID


class TestRecordID:
  """Test suite for RecordID class."""

  def test_record_id_creation(self) -> None:
    """Test basic RecordID creation."""
    record_id = RecordID(table='user', id='alice')

    assert record_id.table == 'user'
    assert record_id.id == 'alice'

  def test_record_id_with_int_id(self) -> None:
    """Test RecordID with integer ID."""
    record_id = RecordID(table='post', id=123)

    assert record_id.table == 'post'
    assert record_id.id == 123

  def test_record_id_string_representation(self) -> None:
    """Test string representation of RecordID."""
    record_id = RecordID(table='user', id='alice')

    assert str(record_id) == 'user:alice'

  def test_record_id_repr(self) -> None:
    """Test repr of RecordID."""
    record_id = RecordID(table='user', id='alice')

    assert repr(record_id) == "RecordID(table='user', id='alice')"

  def test_record_id_parse_string_id(self) -> None:
    """Test parsing RecordID from string with string ID."""
    record_id = RecordID.parse('user:alice')

    assert record_id.table == 'user'
    assert record_id.id == 'alice'

  def test_record_id_parse_int_id(self) -> None:
    """Test parsing RecordID from string with integer ID."""
    record_id = RecordID.parse('post:123')

    assert record_id.table == 'post'
    assert record_id.id == 123

  def test_record_id_parse_invalid_format_no_colon(self) -> None:
    """Test parsing invalid RecordID format without colon."""
    with pytest.raises(ValueError) as exc_info:
      RecordID.parse('useralice')

    assert 'Invalid record ID format' in str(exc_info.value)
    assert 'Expected format: table:id' in str(exc_info.value)

  def test_record_id_parse_invalid_format_multiple_parts(self) -> None:
    """Test parsing RecordID with extra colons."""
    # Should only split on first colon
    record_id = RecordID.parse('user:alice:extra')
    assert record_id.table == 'user'
    assert record_id.id == 'alice:extra'

  def test_record_id_to_surql(self) -> None:
    """Test converting RecordID to SurrealQL format."""
    record_id = RecordID(table='user', id='alice')

    assert record_id.to_surql() == 'user:alice'

  def test_record_id_immutability(self) -> None:
    """Test that RecordID is immutable."""
    record_id = RecordID(table='user', id='alice')

    with pytest.raises((ValidationError, AttributeError)):
      record_id.table = 'post'  # type: ignore[misc]

  def test_record_id_validate_table_empty(self) -> None:
    """Test validation of empty table name."""
    with pytest.raises(ValidationError) as exc_info:
      RecordID(table='', id='alice')

    assert 'Table name cannot be empty' in str(exc_info.value)

  def test_record_id_validate_table_invalid_chars(self) -> None:
    """Test validation of table name with invalid characters."""
    with pytest.raises(ValidationError) as exc_info:
      RecordID(table='user-table', id='alice')

    assert 'Invalid table name' in str(exc_info.value)

  def test_record_id_validate_table_valid_underscore(self) -> None:
    """Test that underscores are allowed in table names."""
    record_id = RecordID(table='user_profile', id='alice')

    assert record_id.table == 'user_profile'

  def test_record_id_with_domain_id(self) -> None:
    """Test RecordID with domain as ID (requires angle brackets)."""
    record_id = RecordID(table='outlet', id='alaskabeacon.com')

    assert record_id.table == 'outlet'
    assert record_id.id == 'alaskabeacon.com'
    assert str(record_id) == 'outlet:⟨alaskabeacon.com⟩'

  def test_record_id_with_composite_id(self) -> None:
    """Test RecordID with composite ID containing colon."""
    record_id = RecordID(table='document', id='alaskabeacon.com:01JEHE123')

    assert record_id.table == 'document'
    assert record_id.id == 'alaskabeacon.com:01JEHE123'
    assert str(record_id) == 'document:⟨alaskabeacon.com:01JEHE123⟩'

  def test_record_id_with_hyphen_id(self) -> None:
    """Test RecordID with hyphen in ID (requires angle brackets)."""
    record_id = RecordID(table='user', id='john-doe')

    assert str(record_id) == 'user:⟨john-doe⟩'

  def test_record_id_simple_no_brackets(self) -> None:
    """Test that simple alphanumeric IDs don't get angle brackets."""
    record_id = RecordID(table='user', id='alice123')

    assert str(record_id) == 'user:alice123'

  def test_record_id_underscore_no_brackets(self) -> None:
    """Test that IDs with underscores don't need angle brackets."""
    record_id = RecordID(table='user', id='alice_smith')

    assert str(record_id) == 'user:alice_smith'

  def test_record_id_parse_angle_bracket_domain(self) -> None:
    """Test parsing RecordID with angle bracket domain syntax."""
    record_id = RecordID.parse('outlet:⟨alaskabeacon.com⟩')

    assert record_id.table == 'outlet'
    assert record_id.id == 'alaskabeacon.com'
    assert str(record_id) == 'outlet:⟨alaskabeacon.com⟩'

  def test_record_id_parse_angle_bracket_composite(self) -> None:
    """Test parsing RecordID with angle bracket composite ID."""
    record_id = RecordID.parse('document:⟨domain.com:ulid123⟩')

    assert record_id.table == 'document'
    assert record_id.id == 'domain.com:ulid123'
    assert str(record_id) == 'document:⟨domain.com:ulid123⟩'

  def test_record_id_bidirectional_parsing_simple(self) -> None:
    """Test bidirectional parsing for simple IDs."""
    original = RecordID(table='user', id='alice')
    as_string = str(original)
    parsed = RecordID.parse(as_string)

    assert parsed.table == original.table
    assert parsed.id == original.id
    assert str(parsed) == as_string

  def test_record_id_bidirectional_parsing_domain(self) -> None:
    """Test bidirectional parsing for domain IDs."""
    original = RecordID(table='outlet', id='alaskabeacon.com')
    as_string = str(original)
    parsed = RecordID.parse(as_string)

    assert parsed.table == original.table
    assert parsed.id == original.id
    assert str(parsed) == as_string

  def test_record_id_bidirectional_parsing_composite(self) -> None:
    """Test bidirectional parsing for composite IDs."""
    original = RecordID(table='document', id='domain.com:01JEHE123')
    as_string = str(original)
    parsed = RecordID.parse(as_string)

    assert parsed.table == original.table
    assert parsed.id == original.id
    assert str(parsed) == as_string

  def test_record_id_parse_without_brackets(self) -> None:
    """Test parsing domain ID without angle brackets still works."""
    # User provides domain without brackets, parse accepts it
    record_id = RecordID.parse('outlet:alaskabeacon.com')

    assert record_id.table == 'outlet'
    assert record_id.id == 'alaskabeacon.com'
    # When converted to string, brackets are added automatically
    assert str(record_id) == 'outlet:⟨alaskabeacon.com⟩'

  def test_record_id_to_surql_with_angle_brackets(self) -> None:
    """Test converting RecordID with angle brackets to SurrealQL format."""
    record_id = RecordID(table='outlet', id='alaskabeacon.com')

    assert record_id.to_surql() == 'outlet:⟨alaskabeacon.com⟩'

  def test_record_id_integer_no_brackets(self) -> None:
    """Test that integer IDs never get angle brackets."""
    record_id = RecordID(table='post', id=12345)

    assert str(record_id) == 'post:12345'


class TestQuoteValue:
  """Test suite for _quote_value helper function."""

  def test_quote_none(self) -> None:
    """Test quoting None value."""
    assert _quote_value(None) == 'NULL'

  def test_quote_bool_true(self) -> None:
    """Test quoting boolean True."""
    assert _quote_value(True) == 'true'

  def test_quote_bool_false(self) -> None:
    """Test quoting boolean False."""
    assert _quote_value(False) == 'false'

  def test_quote_int(self) -> None:
    """Test quoting integer."""
    assert _quote_value(42) == '42'

  def test_quote_float(self) -> None:
    """Test quoting float."""
    assert _quote_value(3.14) == '3.14'

  def test_quote_string(self) -> None:
    """Test quoting string."""
    assert _quote_value('hello') == "'hello'"

  def test_quote_string_with_single_quote(self) -> None:
    """Test quoting string with single quote (should escape)."""
    assert _quote_value("it's") == "'it\\'s'"

  def test_quote_other_types(self) -> None:
    """Test quoting other types (converts to string)."""
    assert _quote_value([1, 2, 3]) == "'[1, 2, 3]'"


class TestComparisonOperators:
  """Test suite for comparison operators."""

  def test_eq_operator(self) -> None:
    """Test Eq operator."""
    op = Eq('name', 'Alice')
    assert op.to_surql() == "name = 'Alice'"

  def test_eq_helper_function(self) -> None:
    """Test eq helper function."""
    op = eq('name', 'Alice')
    assert isinstance(op, Eq)
    assert op.to_surql() == "name = 'Alice'"

  def test_ne_operator(self) -> None:
    """Test Ne operator."""
    op = Ne('status', 'deleted')
    assert op.to_surql() == "status != 'deleted'"

  def test_ne_helper_function(self) -> None:
    """Test ne helper function."""
    op = ne('status', 'deleted')
    assert isinstance(op, Ne)

  def test_gt_operator(self) -> None:
    """Test Gt operator."""
    op = Gt('age', 18)
    assert op.to_surql() == 'age > 18'

  def test_gt_helper_function(self) -> None:
    """Test gt helper function."""
    op = gt('age', 18)
    assert isinstance(op, Gt)

  def test_gte_operator(self) -> None:
    """Test Gte operator."""
    op = Gte('score', 100)
    assert op.to_surql() == 'score >= 100'

  def test_gte_helper_function(self) -> None:
    """Test gte helper function."""
    op = gte('score', 100)
    assert isinstance(op, Gte)

  def test_lt_operator(self) -> None:
    """Test Lt operator."""
    op = Lt('price', 50.0)
    assert op.to_surql() == 'price < 50.0'

  def test_lt_helper_function(self) -> None:
    """Test lt helper function."""
    op = lt('price', 50.0)
    assert isinstance(op, Lt)

  def test_lte_operator(self) -> None:
    """Test Lte operator."""
    op = Lte('quantity', 10)
    assert op.to_surql() == 'quantity <= 10'

  def test_lte_helper_function(self) -> None:
    """Test lte helper function."""
    op = lte('quantity', 10)
    assert isinstance(op, Lte)


class TestStringArrayOperators:
  """Test suite for string and array operators."""

  def test_contains_operator(self) -> None:
    """Test Contains operator."""
    op = Contains('email', '@example.com')
    assert op.to_surql() == "email CONTAINS '@example.com'"

  def test_contains_helper_function(self) -> None:
    """Test contains helper function."""
    op = contains('email', '@example.com')
    assert isinstance(op, Contains)

  def test_contains_not_operator(self) -> None:
    """Test ContainsNot operator."""
    op = ContainsNot('tags', 'spam')
    assert op.to_surql() == "tags CONTAINSNOT 'spam'"

  def test_contains_not_helper_function(self) -> None:
    """Test contains_not helper function."""
    op = contains_not('tags', 'spam')
    assert isinstance(op, ContainsNot)

  def test_contains_all_operator(self) -> None:
    """Test ContainsAll operator."""
    op = ContainsAll('tags', ['python', 'database'])
    assert op.to_surql() == "tags CONTAINSALL ['python', 'database']"

  def test_contains_all_helper_function(self) -> None:
    """Test contains_all helper function."""
    op = contains_all('tags', ['python', 'database'])
    assert isinstance(op, ContainsAll)

  def test_contains_any_operator(self) -> None:
    """Test ContainsAny operator."""
    op = ContainsAny('tags', ['python', 'javascript'])
    assert op.to_surql() == "tags CONTAINSANY ['python', 'javascript']"

  def test_contains_any_helper_function(self) -> None:
    """Test contains_any helper function."""
    op = contains_any('tags', ['python', 'javascript'])
    assert isinstance(op, ContainsAny)

  def test_inside_operator(self) -> None:
    """Test Inside operator."""
    op = Inside('status', ['active', 'pending'])
    assert op.to_surql() == "status INSIDE ['active', 'pending']"

  def test_inside_helper_function(self) -> None:
    """Test inside helper function."""
    op = inside('status', ['active', 'pending'])
    assert isinstance(op, Inside)

  def test_not_inside_operator(self) -> None:
    """Test NotInside operator."""
    op = NotInside('status', ['deleted', 'archived'])
    assert op.to_surql() == "status NOTINSIDE ['deleted', 'archived']"

  def test_not_inside_helper_function(self) -> None:
    """Test not_inside helper function."""
    op = not_inside('status', ['deleted', 'archived'])
    assert isinstance(op, NotInside)


class TestNullOperators:
  """Test suite for NULL operators."""

  def test_is_null_operator(self) -> None:
    """Test IsNull operator."""
    op = IsNull('deleted_at')
    assert op.to_surql() == 'deleted_at IS NULL'

  def test_is_null_helper_function(self) -> None:
    """Test is_null helper function."""
    op = is_null('deleted_at')
    assert isinstance(op, IsNull)

  def test_is_not_null_operator(self) -> None:
    """Test IsNotNull operator."""
    op = IsNotNull('created_at')
    assert op.to_surql() == 'created_at IS NOT NULL'

  def test_is_not_null_helper_function(self) -> None:
    """Test is_not_null helper function."""
    op = is_not_null('created_at')
    assert isinstance(op, IsNotNull)


class TestLogicalOperators:
  """Test suite for logical operators."""

  def test_and_operator(self) -> None:
    """Test And operator."""
    op = And(Gt('age', 18), Eq('status', 'active'))
    assert op.to_surql() == "(age > 18) AND (status = 'active')"

  def test_and_helper_function(self) -> None:
    """Test and_ helper function."""
    op = and_(gt('age', 18), eq('status', 'active'))
    assert isinstance(op, And)

  def test_or_operator(self) -> None:
    """Test Or operator."""
    op = Or(Eq('type', 'admin'), Eq('type', 'moderator'))
    assert op.to_surql() == "(type = 'admin') OR (type = 'moderator')"

  def test_or_helper_function(self) -> None:
    """Test or_ helper function."""
    op = or_(eq('type', 'admin'), eq('type', 'moderator'))
    assert isinstance(op, Or)

  def test_not_operator(self) -> None:
    """Test Not operator."""
    op = Not(Eq('status', 'deleted'))
    assert op.to_surql() == "NOT (status = 'deleted')"

  def test_not_helper_function(self) -> None:
    """Test not_ helper function."""
    op = not_(eq('status', 'deleted'))
    assert isinstance(op, Not)

  def test_complex_nested_operators(self) -> None:
    """Test complex nested logical operators."""
    # (age > 18 AND status = 'active') OR type = 'admin'
    op = Or(And(Gt('age', 18), Eq('status', 'active')), Eq('type', 'admin'))
    expected = "((age > 18) AND (status = 'active')) OR (type = 'admin')"
    assert op.to_surql() == expected


class TestOperatorImmutability:
  """Test suite for operator immutability."""

  def test_eq_immutability(self) -> None:
    """Test that Eq operator is immutable."""
    op = Eq('name', 'Alice')

    with pytest.raises((AttributeError, TypeError)):
      op.field = 'email'  # type: ignore[misc]

  def test_and_immutability(self) -> None:
    """Test that And operator is immutable."""
    op = And(Gt('age', 18), Eq('status', 'active'))

    with pytest.raises((AttributeError, TypeError)):
      op.left = Gt('age', 21)  # type: ignore[misc]

  def test_contains_all_immutability(self) -> None:
    """Test that ContainsAll operator is immutable."""
    op = ContainsAll('tags', ['python', 'database'])

    with pytest.raises((AttributeError, TypeError)):
      op.values = ['java']  # type: ignore[misc]


class TestOperatorBaseClass:
  """Test suite for Operator base class."""

  def test_operator_base_not_implemented(self) -> None:
    """Test that base Operator class raises NotImplementedError."""
    op = Operator()

    with pytest.raises(NotImplementedError):
      op.to_surql()


class TestOperatorEdgeCases:
  """Test suite for operator edge cases."""

  def test_eq_with_null_value(self) -> None:
    """Test Eq operator with None value."""
    op = Eq('deleted_at', None)
    assert op.to_surql() == 'deleted_at = NULL'

  def test_eq_with_bool_value(self) -> None:
    """Test Eq operator with boolean value."""
    op = Eq('is_active', True)
    assert op.to_surql() == 'is_active = true'

  def test_contains_all_empty_list(self) -> None:
    """Test ContainsAll with empty list."""
    op = ContainsAll('tags', [])
    assert op.to_surql() == 'tags CONTAINSALL []'

  def test_inside_with_integers(self) -> None:
    """Test Inside operator with integer values."""
    op = Inside('status_code', [200, 201, 204])
    assert op.to_surql() == 'status_code INSIDE [200, 201, 204]'

  def test_operator_with_special_characters_in_string(self) -> None:
    """Test operator with special characters in value."""
    op = Eq('description', "It's a test with 'quotes'")
    assert op.to_surql() == "description = 'It\\'s a test with \\'quotes\\''"
