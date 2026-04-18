"""Tests for schema parser clause-extraction helpers.

Regression coverage for the `$value` capture bug (issue #7) where the
terminator lookahead mis-matched the `VALUE` keyword inside `$value`.
"""

from __future__ import annotations

import pytest

from surql.schema.parser import (
  _extract_assertion,
  _extract_default,
  _extract_readonly,
  _extract_value,
)


class TestExtractAssertion:
  """`_extract_assertion` pulls the ASSERT expression out of a DEFINE FIELD."""

  def test_bare_dollar_value_is_preserved(self) -> None:
    assert _extract_assertion('ASSERT $value >= 0') == '$value >= 0'

  def test_combined_dollar_value_expression(self) -> None:
    definition = 'ASSERT $value >= 0 AND $value <= 150'
    assert _extract_assertion(definition) == '$value >= 0 AND $value <= 150'

  def test_function_call_with_dollar_value(self) -> None:
    assert _extract_assertion('ASSERT string::is::email($value)') == ('string::is::email($value)')

  def test_stops_at_default_terminator(self) -> None:
    definition = 'ASSERT string::is::email($value) DEFAULT "a@b.example"'
    assert _extract_assertion(definition) == 'string::is::email($value)'

  def test_stops_at_value_terminator(self) -> None:
    definition = 'ASSERT $value >= 0 VALUE $value'
    assert _extract_assertion(definition) == '$value >= 0'

  def test_stops_at_readonly(self) -> None:
    assert _extract_assertion('ASSERT $value != NONE READONLY') == '$value != NONE'

  def test_missing_assert_returns_none(self) -> None:
    assert _extract_assertion('DEFINE FIELD name ON TABLE user TYPE string') is None

  def test_semicolon_terminator(self) -> None:
    assert _extract_assertion('ASSERT $value >= 0;') == '$value >= 0'


class TestExtractDefault:
  """`_extract_default` pulls the DEFAULT expression out of a DEFINE FIELD."""

  def test_bare_dollar_value_default(self) -> None:
    assert _extract_default('DEFAULT $value + 1') == '$value + 1'

  def test_stops_at_value_terminator(self) -> None:
    assert _extract_default('DEFAULT $value + 1 VALUE 42') == '$value + 1'

  def test_stops_at_assert_terminator(self) -> None:
    assert _extract_default('DEFAULT $value + 1 ASSERT $value > 0') == '$value + 1'

  def test_function_default(self) -> None:
    assert _extract_default('DEFAULT time::now()') == 'time::now()'

  def test_missing_default_returns_none(self) -> None:
    assert _extract_default('ASSERT $value > 0') is None


class TestExtractValue:
  """`_extract_value` pulls the VALUE expression out of a DEFINE FIELD."""

  def test_bare_dollar_value_value(self) -> None:
    assert _extract_value('VALUE $value * 2') == '$value * 2'

  def test_stops_at_readonly(self) -> None:
    assert _extract_value('VALUE $value * 2 READONLY') == '$value * 2'

  def test_stops_at_default_terminator(self) -> None:
    assert _extract_value('VALUE $value + 1 DEFAULT 0') == '$value + 1'

  def test_missing_value_returns_none(self) -> None:
    assert _extract_value('ASSERT $value > 0') is None


class TestExtractReadonly:
  """`_extract_readonly` checks for the READONLY flag."""

  def test_readonly_detected(self) -> None:
    assert _extract_readonly('DEFINE FIELD x ON y TYPE datetime READONLY') is True

  def test_absent_returns_false(self) -> None:
    assert _extract_readonly('DEFINE FIELD x ON y TYPE datetime') is False


@pytest.mark.parametrize(
  'definition,expected',
  [
    # Representative real-world forms that previously failed
    ('ASSERT $value >= 0', '$value >= 0'),
    ('ASSERT $value >= 0 AND $value <= 150', '$value >= 0 AND $value <= 150'),
    ('ASSERT string::is::email($value)', 'string::is::email($value)'),
    (
      'ASSERT $value != NONE AND string::len($value) > 0',
      '$value != NONE AND string::len($value) > 0',
    ),
    ('ASSERT array::len($value) > 0', 'array::len($value) > 0'),
  ],
)
def test_parametrized_assert_with_dollar_value(definition: str, expected: str) -> None:
  """Property-ish coverage: any ASSERT clause with `$value` round-trips."""
  assert _extract_assertion(definition) == expected
