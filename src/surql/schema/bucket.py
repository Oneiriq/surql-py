"""Bucket (object-storage) schema definition functions.

This module provides functions for defining ``DEFINE BUCKET`` schemas for
SurrealDB v3 file/object storage. A bucket is named storage for ``file`` values,
accessed at runtime via file pointers (``f"<bucket>:/<key>"``) and the
:class:`~surql.connection.client.Bucket` handle.

Buckets are configured with a backend: ``'memory'`` (non-persistent),
``'file:/path'`` (a local folder), or an object-store URL such as
``'s3://my-bucket'``.
"""

from pydantic import BaseModel, ConfigDict


class BucketDefinition(BaseModel):
  """Immutable bucket (object-storage) schema definition.

  Represents a ``DEFINE BUCKET`` statement for SurrealDB v3.

  Args:
    name: Bucket name
    backend: Storage backend — e.g. ``'memory'``, ``'file:/path'``, or
      ``'s3://bucket'``
    readonly: When True, the bucket rejects writes (emits ``READONLY``)
    permissions: Optional SurrealQL ``PERMISSIONS`` expression. The bucket
      grammar takes a single expression (``PERMISSIONS FULL`` / ``PERMISSIONS
      NONE`` / ``PERMISSIONS WHERE <rule>``) rather than the per-action
      ``FOR <action>`` form tables use, so this is stored as an action -> rule
      mapping and emitted as ``FOR <action> WHERE <rule>`` clauses (matching the
      table emitter) — pass ``{'put': '...', 'get': '...'}`` keyed by the file
      actions (``put``/``get``/``delete``/etc.).
    comment: Optional human-readable comment

  Examples:
    Memory-backed bucket:
    >>> bucket = BucketDefinition(name='avatars', backend='memory')

    File-backed, read-only:
    >>> bucket = BucketDefinition(
    ...   name='archive',
    ...   backend='file:/srv/data/archive',
    ...   readonly=True,
    ... )
  """

  name: str
  backend: str
  readonly: bool = False
  permissions: dict[str, str] | None = None
  comment: str | None = None

  model_config = ConfigDict(frozen=True)


# Builder functions


def bucket_schema(
  name: str,
  *,
  backend: str,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
  comment: str | None = None,
) -> BucketDefinition:
  """Create a bucket schema definition.

  Pure function to create an immutable bucket definition.

  Args:
    name: Bucket name
    backend: Storage backend (e.g. ``'memory'``, ``'file:/path'``, ``'s3://...'``)
    readonly: If True, the bucket rejects writes
    permissions: Optional action -> rule mapping (see :class:`BucketDefinition`)
    comment: Optional human-readable comment

  Returns:
    Immutable BucketDefinition instance
  """
  return BucketDefinition(
    name=name,
    backend=backend,
    readonly=readonly,
    permissions=permissions,
    comment=comment,
  )


def memory_bucket(
  name: str,
  *,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
  comment: str | None = None,
) -> BucketDefinition:
  """Create a memory-backed (non-persistent) bucket definition.

  Convenience wrapper for ``bucket_schema(name, backend='memory', ...)``.

  Args:
    name: Bucket name
    readonly: If True, the bucket rejects writes
    permissions: Optional action -> rule mapping (see :class:`BucketDefinition`)
    comment: Optional human-readable comment

  Returns:
    Immutable BucketDefinition with the ``memory`` backend
  """
  return bucket_schema(
    name,
    backend='memory',
    readonly=readonly,
    permissions=permissions,
    comment=comment,
  )


def file_bucket(
  name: str,
  path: str,
  *,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
  comment: str | None = None,
) -> BucketDefinition:
  """Create a file-backed (local-folder) bucket definition.

  Convenience wrapper that builds the ``file:<path>`` backend string.

  Args:
    name: Bucket name
    path: Local folder path (e.g. ``'/srv/data/archive'``). A leading ``file:``
      scheme is accepted and not double-prefixed.
    readonly: If True, the bucket rejects writes
    permissions: Optional action -> rule mapping (see :class:`BucketDefinition`)
    comment: Optional human-readable comment

  Returns:
    Immutable BucketDefinition with a ``file:<path>`` backend

  Examples:
    >>> file_bucket('archive', '/srv/data/archive').backend
    'file:/srv/data/archive'
  """
  backend = path if path.startswith('file:') else f'file:{path}'
  return bucket_schema(
    name,
    backend=backend,
    readonly=readonly,
    permissions=permissions,
    comment=comment,
  )
