"""Async runtime handle for SurrealDB v3 bucket / file operations.

A :class:`Bucket` is obtained from :meth:`DatabaseClient.bucket` and exposes the
file operations (put / get / exists / copy / rename / delete / list / head) for
a single bucket.

CRITICAL SAFETY: every operation constructs the file pointer with the
parameterised ``type::file($bucket, $key)`` constructor and binds the bucket,
key, data, and destination as query parameters. Nothing is f-string
interpolated into the SurrealQL — mirroring the ``type::record($table, $id)``
pattern in :mod:`surql.connection.client`. This keeps bucket names, keys, and
file contents free of injection risk.
"""

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
  from surql.connection.client import DatabaseClient

logger = structlog.get_logger(__name__)


def _coerce_bytes(data: str | bytes) -> str | bytes:
  """Return ``data`` ready to bind as a file-content parameter.

  ``bytes`` pass straight through (the SDK CBOR-encodes them as a byte string,
  which ``.put`` stores verbatim). ``str`` is bound as-is — SurrealDB accepts a
  string argument to ``.put`` and stores its UTF-8 bytes.
  """
  return data


def _unwrap_result(value: Any) -> Any:
  """Unwrap a single-statement ``RETURN`` / ``SELECT`` response to its value.

  ``DatabaseClient.execute`` already normalises SDK types; depending on the
  server/SDK shape the result may be:

  - the bare value (SDK 2.x single-statement unwrap),
  - ``[value]`` (a one-element list),
  - ``[{'result': value, 'status': 'OK'}]`` (statement-envelope form).

  This collapses all of those to the underlying value.
  """
  if isinstance(value, list):
    if len(value) == 0:
      return None
    first = value[0]
    if isinstance(first, dict) and 'result' in first and 'status' in first:
      return first['result']
    if len(value) == 1:
      return first
    return value
  if isinstance(value, dict) and 'result' in value and 'status' in value:
    return value['result']
  return value


class Bucket:
  """Async handle for file operations on a single SurrealDB bucket.

  Obtain one via :meth:`surql.connection.client.DatabaseClient.bucket`. All
  methods are coroutines and use the connection's retry + concurrency controls
  (they delegate to :meth:`DatabaseClient.execute`).

  Method names are kept identical across the surql sibling ports (py / rs / go /
  ts) for API parity.

  Examples:
    ```python
    bucket = client.bucket('avatars')
    await bucket.put('alice.png', image_bytes)
    data = await bucket.get('alice.png')          # -> bytes
    text = await bucket.get_text('note.txt')      # -> str
    if await bucket.exists('alice.png'):
      await bucket.copy('alice.png', 'alice_backup.png')
    for entry in await bucket.list():
      print(entry['key'], entry['size'])
    ```
  """

  def __init__(self, client: 'DatabaseClient', name: str) -> None:
    """Initialise a bucket handle.

    Args:
      client: The owning database client.
      name: Bucket name (as defined via ``DEFINE BUCKET``).
    """
    self._client = client
    self._name = name

  @property
  def name(self) -> str:
    """The bucket name."""
    return self._name

  async def put(self, key: str, data: str | bytes) -> None:
    """Write ``data`` to ``key``, overwriting any existing file.

    SurrealQL: ``RETURN type::file($bucket, $key).put($data)``

    Args:
      key: File key within the bucket.
      data: File content — ``str`` or ``bytes`` (bytes pass through as bytes).
    """
    await self._client.execute(
      'RETURN type::file($bucket, $key).put($data)',
      {'bucket': self._name, 'key': key, 'data': _coerce_bytes(data)},
    )

  async def put_if_not_exists(self, key: str, data: str | bytes) -> None:
    """Write ``data`` to ``key`` only if no file already exists there.

    SurrealQL: ``RETURN type::file($bucket, $key).put_if_not_exists($data)``

    Args:
      key: File key within the bucket.
      data: File content — ``str`` or ``bytes``.
    """
    await self._client.execute(
      'RETURN type::file($bucket, $key).put_if_not_exists($data)',
      {'bucket': self._name, 'key': key, 'data': _coerce_bytes(data)},
    )

  async def get(self, key: str) -> bytes | None:
    """Read the contents of ``key`` as bytes.

    SurrealQL: ``RETURN type::file($bucket, $key).get()``

    Args:
      key: File key within the bucket.

    Returns:
      The file content as ``bytes``, or ``None`` if the file does not exist.
    """
    result = await self._client.execute(
      'RETURN type::file($bucket, $key).get()',
      {'bucket': self._name, 'key': key},
    )
    value = _unwrap_result(result)
    if value is None:
      return None
    if isinstance(value, bytes | bytearray):
      return bytes(value)
    # Some transports surface text content as str; encode back to bytes so the
    # return type is stable.
    if isinstance(value, str):
      return value.encode('utf-8')
    return bytes(value)

  async def get_text(self, key: str) -> str | None:
    """Read the contents of ``key`` as a UTF-8 string.

    SurrealQL: ``RETURN <string>type::file($bucket, $key).get()`` (the server
    casts the bytes to a string).

    Args:
      key: File key within the bucket.

    Returns:
      The file content as ``str``, or ``None`` if the file does not exist.
    """
    result = await self._client.execute(
      'RETURN <string>type::file($bucket, $key).get()',
      {'bucket': self._name, 'key': key},
    )
    value = _unwrap_result(result)
    if value is None:
      return None
    if isinstance(value, bytes | bytearray):
      return bytes(value).decode('utf-8')
    return str(value)

  async def exists(self, key: str) -> bool:
    """Return whether a file exists at ``key``.

    SurrealQL: ``RETURN type::file($bucket, $key).exists()``

    Args:
      key: File key within the bucket.

    Returns:
      ``True`` if the file exists, ``False`` otherwise.
    """
    result = await self._client.execute(
      'RETURN type::file($bucket, $key).exists()',
      {'bucket': self._name, 'key': key},
    )
    return bool(_unwrap_result(result))

  async def head(self, key: str) -> dict[str, Any] | None:
    """Return file metadata for ``key`` (``bucket``, ``key``, ``size``, ``updated``).

    The raw ``.head()`` result embeds the file pointer in its ``file`` field,
    which the installed ``surrealdb`` SDK cannot decode (it ships no decoder for
    the file CBOR tag). This projects the pointer into its ``bucket`` + ``key``
    strings -- mirroring :meth:`list` -- so the wire response stays decodable.

    SurrealQL::

      SELECT file::bucket(file) AS bucket, file::key(file) AS key, size, updated
      FROM type::file($bucket, $key).head()

    Args:
      key: File key within the bucket.

    Returns:
      A ``{'bucket', 'key', 'size', 'updated'}`` dict, or ``None`` if the file
      does not exist.
    """
    result = await self._client.execute(
      'SELECT file::bucket(file) AS bucket, file::key(file) AS key, size, updated '
      'FROM type::file($bucket, $key).head()',
      {'bucket': self._name, 'key': key},
    )
    value = _unwrap_result(result)
    if isinstance(value, list):
      value = value[0] if value else None
    if value is None:
      return None
    if isinstance(value, dict):
      return value
    return {'result': value}

  async def delete(self, key: str) -> None:
    """Delete the file at ``key``.

    SurrealQL: ``RETURN type::file($bucket, $key).delete()``

    Args:
      key: File key within the bucket.
    """
    await self._client.execute(
      'RETURN type::file($bucket, $key).delete()',
      {'bucket': self._name, 'key': key},
    )

  async def copy(self, key: str, dst: str) -> None:
    """Copy the file at ``key`` to ``dst`` (a key in the same bucket).

    SurrealQL: ``RETURN type::file($bucket, $key).copy($dst)``

    Note:
      Per the SurrealDB docs, ``.copy`` / ``.rename`` take a destination *key*
      (e.g. ``"backup.txt"``), not a full ``bucket:/key`` pointer. The
      destination is bound as a parameter. This is verified against a live
      server in the integration tests (see ``tests/integration``).

    Args:
      key: Source file key.
      dst: Destination file key within the same bucket.
    """
    await self._client.execute(
      'RETURN type::file($bucket, $key).copy($dst)',
      {'bucket': self._name, 'key': key, 'dst': dst},
    )

  async def copy_if_not_exists(self, key: str, dst: str) -> None:
    """Copy ``key`` to ``dst`` only if ``dst`` does not already exist.

    SurrealQL: ``RETURN type::file($bucket, $key).copy_if_not_exists($dst)``

    Args:
      key: Source file key.
      dst: Destination file key within the same bucket.
    """
    await self._client.execute(
      'RETURN type::file($bucket, $key).copy_if_not_exists($dst)',
      {'bucket': self._name, 'key': key, 'dst': dst},
    )

  async def rename(self, key: str, dst: str) -> None:
    """Rename the file at ``key`` to ``dst`` (a key in the same bucket).

    SurrealQL: ``RETURN type::file($bucket, $key).rename($dst)``

    Args:
      key: Source file key.
      dst: Destination file key within the same bucket.
    """
    await self._client.execute(
      'RETURN type::file($bucket, $key).rename($dst)',
      {'bucket': self._name, 'key': key, 'dst': dst},
    )

  async def rename_if_not_exists(self, key: str, dst: str) -> None:
    """Rename ``key`` to ``dst`` only if ``dst`` does not already exist.

    SurrealQL: ``RETURN type::file($bucket, $key).rename_if_not_exists($dst)``

    Args:
      key: Source file key.
      dst: Destination file key within the same bucket.
    """
    await self._client.execute(
      'RETURN type::file($bucket, $key).rename_if_not_exists($dst)',
      {'bucket': self._name, 'key': key, 'dst': dst},
    )

  async def list(self) -> list[dict[str, Any]]:
    """List the files in the bucket with their metadata.

    Wraps the contract function ``file::list($bucket)`` in a ``SELECT`` that
    projects the un-decodable raw file pointer into its ``bucket`` + ``key``
    strings (via ``file::bucket`` / ``file::key``). This keeps the wire
    response CBOR-decodable — the installed ``surrealdb`` SDK ships no decoder
    for the file CBOR tag, so returning the bare ``file::list`` rows (which
    embed a ``file`` pointer) would raise a decode error.

    SurrealQL::

      SELECT file::bucket(file) AS bucket, file::key(file) AS key, size, updated
      FROM file::list($bucket)

    Returns:
      A list of ``{'bucket', 'key', 'size', 'updated'}`` dicts (one per file).
    """
    result = await self._client.execute(
      'SELECT file::bucket(file) AS bucket, file::key(file) AS key, size, updated '
      'FROM file::list($bucket)',
      {'bucket': self._name},
    )
    value = _unwrap_result(result)
    if value is None:
      return []
    if isinstance(value, list):
      return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
      return [value]
    return []
