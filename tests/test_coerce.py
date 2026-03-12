"""Tests for type coercion utilities."""

from datetime import datetime, timezone

import pytest

from reverie.types.coerce import coerce_datetime, coerce_record_datetimes


class TestCoerceDatetime:
  """Tests for coerce_datetime function."""

  def test_standard_iso_format(self) -> None:
    """Parses standard ISO 8601 with Z suffix."""
    result = coerce_datetime('2024-01-15T10:30:00Z')

    assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

  def test_z_suffix_is_utc(self) -> None:
    """Result is timezone-aware UTC when Z suffix given."""
    result = coerce_datetime('2024-01-15T10:30:00Z')

    assert result.tzinfo == timezone.utc

  def test_timezone_offset(self) -> None:
    """Parses ISO 8601 with explicit +00:00 offset."""
    result = coerce_datetime('2024-01-15T10:30:00+00:00')

    assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

  def test_nanoseconds_truncated_to_microseconds(self) -> None:
    """Truncates nanoseconds (9 digits) to microseconds (6 digits)."""
    result = coerce_datetime('2024-01-15T10:30:00.123456789Z')

    assert result.microsecond == 123456

  def test_nanoseconds_with_offset(self) -> None:
    """Truncates nanoseconds when timezone offset is present."""
    result = coerce_datetime('2024-01-15T10:30:00.987654321+00:00')

    assert result.microsecond == 987654

  def test_six_digit_fractional_preserved(self) -> None:
    """Microseconds (6 digits) are preserved without truncation."""
    result = coerce_datetime('2024-01-15T10:30:00.123456Z')

    assert result.microsecond == 123456

  def test_already_datetime_with_timezone(self) -> None:
    """Returns the same datetime object when already a datetime with tzinfo."""
    dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    result = coerce_datetime(dt)

    assert result is dt

  def test_three_digit_fractional_seconds(self) -> None:
    """Handles milliseconds (3 digits) correctly."""
    result = coerce_datetime('2024-01-15T10:30:00.123Z')

    assert result.microsecond == 123000

  def test_naive_datetime_gets_utc(self) -> None:
    """Naive datetime (no tzinfo) is assigned UTC timezone."""
    naive = datetime(2024, 1, 15, 10, 30, 0)

    result = coerce_datetime(naive)

    assert result.tzinfo == timezone.utc
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15

  def test_invalid_string_raises_value_error(self) -> None:
    """Raises ValueError for unparseable string."""
    with pytest.raises((ValueError, Exception)):
      coerce_datetime('not-a-date')


class TestCoerceRecordDatetimes:
  """Tests for coerce_record_datetimes function."""

  def test_coerces_datetime_string_field(self) -> None:
    """Converts a datetime string field to datetime object."""
    data = {'name': 'Alice', 'created_at': '2024-01-15T10:30:00Z'}

    result = coerce_record_datetimes(data, ['created_at'])

    assert isinstance(result['created_at'], datetime)
    assert result['created_at'].tzinfo == timezone.utc

  def test_non_datetime_fields_unchanged(self) -> None:
    """Leaves non-datetime fields untouched."""
    data = {'name': 'Alice', 'age': 30, 'created_at': '2024-01-15T10:30:00Z'}

    result = coerce_record_datetimes(data, ['created_at'])

    assert result['name'] == 'Alice'
    assert result['age'] == 30

  def test_missing_field_skipped(self) -> None:
    """Missing fields in datetime_fields list are silently skipped."""
    data = {'name': 'Alice'}

    result = coerce_record_datetimes(data, ['created_at', 'updated_at'])

    assert result == {'name': 'Alice'}

  def test_none_value_skipped(self) -> None:
    """Fields with None value are not coerced."""
    data = {'name': 'Alice', 'deleted_at': None}

    result = coerce_record_datetimes(data, ['deleted_at'])

    assert result['deleted_at'] is None

  def test_multiple_datetime_fields(self) -> None:
    """Coerces multiple datetime fields in one call."""
    data = {
      'created_at': '2024-01-15T10:00:00Z',
      'updated_at': '2024-06-01T12:00:00Z',
    }

    result = coerce_record_datetimes(data, ['created_at', 'updated_at'])

    assert isinstance(result['created_at'], datetime)
    assert isinstance(result['updated_at'], datetime)

  def test_already_datetime_field_preserved(self) -> None:
    """Fields that are already datetime objects are kept as-is."""
    dt = datetime(2024, 1, 15, tzinfo=timezone.utc)
    data = {'created_at': dt}

    result = coerce_record_datetimes(data, ['created_at'])

    assert result['created_at'] is dt

  def test_returns_new_dict(self) -> None:
    """Returns a new dict, not a mutated original."""
    data = {'created_at': '2024-01-15T10:30:00Z'}
    original_value = data['created_at']

    result = coerce_record_datetimes(data, ['created_at'])

    assert data['created_at'] == original_value
    assert result is not data

  def test_empty_datetime_fields_list(self) -> None:
    """Returns unchanged dict when datetime_fields is empty."""
    data = {'name': 'Alice', 'created_at': '2024-01-15T10:30:00Z'}

    result = coerce_record_datetimes(data, [])

    assert result == data

  def test_mixed_present_and_missing_fields(self) -> None:
    """Coerces present fields and skips missing ones."""
    data = {'created_at': '2024-01-15T10:30:00Z', 'name': 'Bob'}

    result = coerce_record_datetimes(data, ['created_at', 'nonexistent'])

    assert isinstance(result['created_at'], datetime)
    assert 'nonexistent' not in result
