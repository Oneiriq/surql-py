"""Type coercion utilities for SurrealDB response data.

Provides functions for converting SurrealDB response values into proper
Python types, particularly datetime strings into datetime objects.
"""

from datetime import datetime, timezone
from typing import Any


def coerce_datetime(value: str | datetime) -> datetime:
  """Convert a SurrealDB ISO datetime string to a Python datetime.

  Handles ISO 8601 format strings including:
  - Standard format: '2024-01-15T10:30:00Z'
  - With timezone offset: '2024-01-15T10:30:00+00:00'
  - With nanoseconds: '2024-01-15T10:30:00.123456789Z'

  Args:
    value: ISO datetime string or datetime instance

  Returns:
    Timezone-aware datetime object in UTC

  Raises:
    ValueError: If the string cannot be parsed as a datetime

  Examples:
    >>> coerce_datetime('2024-01-15T10:30:00Z')
    datetime.datetime(2024, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)

    >>> from datetime import datetime
    >>> dt = datetime.now()
    >>> coerce_datetime(dt) is dt
    True
  """
  if isinstance(value, datetime):
    if value.tzinfo is None:
      return value.replace(tzinfo=timezone.utc)
    return value

  # Normalize Z suffix to +00:00 for fromisoformat compatibility
  normalized = value.replace('Z', '+00:00')

  # Truncate nanoseconds to microseconds (Python supports up to 6 decimal places)
  # SurrealDB can return up to 9 decimal places
  dot_idx = normalized.find('.')
  if dot_idx != -1:
    # Find end of fractional seconds (before + or - for timezone)
    frac_end = dot_idx + 1
    while frac_end < len(normalized) and normalized[frac_end].isdigit():
      frac_end += 1
    frac_part = normalized[dot_idx + 1 : frac_end]
    # Truncate to 6 digits
    if len(frac_part) > 6:
      frac_part = frac_part[:6]
    normalized = normalized[: dot_idx + 1] + frac_part + normalized[frac_end:]

  dt = datetime.fromisoformat(normalized)

  if dt.tzinfo is None:
    dt = dt.replace(tzinfo=timezone.utc)

  return dt


def coerce_record_datetimes(
  data: dict[str, Any],
  datetime_fields: list[str],
) -> dict[str, Any]:
  """Coerce datetime string fields in a record dict to datetime objects.

  Args:
    data: Record dictionary from SurrealDB
    datetime_fields: List of field names expected to contain datetime values

  Returns:
    New dictionary with datetime fields coerced

  Examples:
    >>> record = {'name': 'Alice', 'created_at': '2024-01-15T10:30:00Z'}
    >>> coerced = coerce_record_datetimes(record, ['created_at'])
    >>> isinstance(coerced['created_at'], datetime)
    True
  """
  result = dict(data)
  for field_name in datetime_fields:
    if field_name in result and result[field_name] is not None:
      raw = result[field_name]
      if isinstance(raw, str):
        result[field_name] = coerce_datetime(raw)
  return result
