"""File / object-storage runtime layer for surql.

Exposes the :class:`Bucket` async handle returned by
:meth:`surql.connection.client.DatabaseClient.bucket` for SurrealDB v3 file
operations (put / get / exists / copy / rename / delete / list / head).
"""

from surql.files.bucket import Bucket

__all__ = [
  'Bucket',
]
