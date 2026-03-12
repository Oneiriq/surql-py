"""Tests for SurrealDB reserved word validation."""

import warnings

import pytest

from reverie.schema.fields import string_field
from reverie.types.reserved import check_reserved_word


class TestCheckReservedWord:
  """Tests for the check_reserved_word function."""

  def test_reserved_word_returns_warning(self) -> None:
    result = check_reserved_word('select')
    assert result is not None
    assert 'select' in result

  def test_safe_name_returns_none(self) -> None:
    result = check_reserved_word('my_field')
    assert result is None

  def test_in_returns_warning_by_default(self) -> None:
    result = check_reserved_word('in')
    assert result is not None
    assert 'in' in result

  def test_in_allowed_with_edge_fields(self) -> None:
    result = check_reserved_word('in', allow_edge_fields=True)
    assert result is None

  def test_out_allowed_with_edge_fields(self) -> None:
    result = check_reserved_word('out', allow_edge_fields=True)
    assert result is None

  def test_case_insensitive_match(self) -> None:
    result = check_reserved_word('SELECT')
    assert result is not None
    assert 'select' in result

  def test_mixed_case_match(self) -> None:
    result = check_reserved_word('Select')
    assert result is not None

  def test_dot_notation_checks_leaf(self) -> None:
    result = check_reserved_word('address.type')
    assert result is not None
    assert 'type' in result

  def test_dot_notation_safe_leaf(self) -> None:
    result = check_reserved_word('address.city')
    assert result is None


class TestFieldReservedWordWarning:
  """Tests that field builder functions emit warnings for reserved words."""

  def test_string_field_reserved_emits_warning(self) -> None:
    with pytest.warns(UserWarning, match='select'):
      string_field('select')

  def test_string_field_safe_no_warning(self) -> None:
    with warnings.catch_warnings():
      warnings.simplefilter('error')
      string_field('name')
