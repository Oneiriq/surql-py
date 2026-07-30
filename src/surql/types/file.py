"""FileRef value type for SurrealDB v3 file pointers.

A :class:`FileRef` is the runtime value carried by a ``TYPE file`` field — a
pointer into a bucket (see :class:`~surql.schema.bucket.BucketDefinition`). It
serialises to the SurrealQL pointer form ``f"<bucket>:/<key>"`` and round-trips
through the SQON object form ``{"bucket": ..., "key": ...}`` that surfaces in
query responses.
"""

import re
from typing import Any

from pydantic import BaseModel, ConfigDict

# Matches the SurrealQL file-pointer body ``<bucket>:/<key>`` (optionally
# wrapped in the ``f"..."`` literal prefix/suffix). The bucket is an identifier;
# the key is everything after the ``:/`` separator.
_FILE_POINTER_PATTERN = re.compile(r'^(?:f)?"?([a-zA-Z_][a-zA-Z0-9_]*):/(.+?)"?$')


class FileRef(BaseModel):
  """Immutable reference to a file stored in a SurrealDB bucket.

  Represents a SurrealDB ``file`` value: a pointer identifying a ``key`` within
  a named ``bucket``. Stringifies to the SurrealQL pointer form
  ``<bucket>:/<key>`` (the body of the ``f"..."`` literal).

  Examples:
    >>> ref = FileRef(bucket='avatars', key='alice.png')
    >>> str(ref)
    'avatars:/alice.png'

    Round-trip from the SQON object form returned by queries:
    >>> FileRef.from_sqon({'bucket': 'avatars', 'key': 'alice.png'})
    FileRef(bucket='avatars', key='alice.png')

    Parse a pointer string (with or without the ``f"..."`` wrapper):
    >>> FileRef.parse('f"avatars:/alice.png"').key
    'alice.png'
  """

  bucket: str
  key: str

  model_config = ConfigDict(frozen=True)

  def __str__(self) -> str:
    """Return the SurrealQL pointer body ``<bucket>:/<key>``.

    The ``key`` is stored verbatim (SurrealDB reports keys canonically with a
    leading ``/`` via ``file::key``); the pointer body collapses a single
    leading ``/`` so both ``alice.png`` and ``/alice.png`` render as
    ``<bucket>:/alice.png``.
    """
    return f'{self.bucket}:/{self.key.removeprefix("/")}'

  def __repr__(self) -> str:
    """Return a debugging representation."""
    return f'FileRef(bucket={self.bucket!r}, key={self.key!r})'

  @property
  def pointer(self) -> str:
    """Return the full ``f"<bucket>:/<key>"`` SurrealQL file-literal form.

    Use :meth:`__str__` (or ``str(ref)``) for the bare body without the
    ``f"..."`` wrapper.
    """
    return f'f"{self.bucket}:/{self.key.removeprefix("/")}"'

  def to_sqon(self) -> dict[str, str]:
    """Return the SQON object form ``{'bucket': ..., 'key': ...}``.

    This is the shape used when a file value is materialised as an object in a
    query response or passed back as a structured parameter.
    """
    return {'bucket': self.bucket, 'key': self.key}

  @classmethod
  def from_sqon(cls, value: dict[str, Any]) -> 'FileRef':
    """Build a FileRef from the SQON object form ``{'bucket': ..., 'key': ...}``.

    Args:
      value: A mapping with ``bucket`` and ``key`` entries.

    Returns:
      The corresponding FileRef.

    Raises:
      ValueError: If the mapping lacks ``bucket`` or ``key``.
    """
    if 'bucket' not in value or 'key' not in value:
      raise ValueError(f'Invalid file object, expected bucket+key keys: {value!r}')
    return cls(bucket=str(value['bucket']), key=str(value['key']))

  @classmethod
  def parse(cls, pointer: str) -> 'FileRef':
    """Parse a file-pointer string into a FileRef.

    Accepts both the bare body ``<bucket>:/<key>`` and the full ``f"..."``
    literal form.

    Args:
      pointer: A file-pointer string (e.g. ``'avatars:/alice.png'`` or
        ``'f"avatars:/alice.png"'``).

    Returns:
      The parsed FileRef.

    Raises:
      ValueError: If the string is not a valid file pointer.
    """
    match = _FILE_POINTER_PATTERN.match(pointer.strip())
    if not match:
      raise ValueError(f'Invalid file pointer: {pointer!r}. Expected format: bucket:/key')
    return cls(bucket=match.group(1), key=match.group(2))

  @staticmethod
  def is_file_object(value: Any) -> bool:
    """Return True for a SQON file-object dict ``{'bucket': ..., 'key': ...}``.

    Used by the connection layer's response normaliser to recognise a file
    value without misclassifying arbitrary two-key dicts: requires exactly the
    ``bucket`` and ``key`` keys and nothing else.
    """
    return (
      isinstance(value, dict)
      and set(value.keys()) == {'bucket', 'key'}
      and isinstance(value.get('bucket'), str)
      and isinstance(value.get('key'), str)
    )
