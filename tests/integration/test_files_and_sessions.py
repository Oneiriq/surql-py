"""Integration tests for SurrealDB v3 files/buckets and multiple sessions.

File/bucket tests require the server to be started with the experimental
``files`` capability::

    SURREAL_CAPS_ALLOW_EXPERIMENTAL=files surreal start --user root --pass root memory

(``--allow-all`` does NOT enable files, and the ``--allow-experimental files``
flag form swallows the ``memory`` datastore argument; use the env var.)

When buckets are not permitted (capability disabled), the bucket-dependent
tests skip themselves after probing ``DEFINE BUCKET``. Session tests require a
WebSocket connection (the default ``SURREAL_URL``); they skip on non-ws URLs.
Sessions start unauthenticated, so the tests that execute queries sign in with
the configured credentials first (guest access is not assumed).

All tests are additionally skipped when no server is reachable (see
``tests/integration/conftest.py``).
"""

from __future__ import annotations

import contextlib
import uuid

import pytest

from surql.connection.client import ConnectionError, DatabaseClient
from surql.connection.session import Session
from surql.schema.bucket import memory_bucket
from surql.schema.sql import generate_bucket_sql
from surql.types import FileRef


async def _bucket_supported(client: DatabaseClient, bucket_name: str) -> bool:
  """Probe whether DEFINE BUCKET is permitted on this server."""
  try:
    await client.execute(generate_bucket_sql(memory_bucket(bucket_name))[0])
    return True
  except Exception:
    return False


@pytest.fixture
async def bucket_name(integration_client: DatabaseClient) -> str:
  """Define a fresh memory bucket, skipping if files are not enabled."""
  name = f'b_{uuid.uuid4().hex[:10]}'
  if not await _bucket_supported(integration_client, name):
    pytest.skip(
      'SurrealDB files capability not enabled; start the server with '
      '`SURREAL_CAPS_ALLOW_EXPERIMENTAL=files` to run bucket tests.'
    )
  return name


class TestBucketFileOpsLive:
  """End-to-end file operations against a live, files-enabled server."""

  @pytest.mark.anyio
  async def test_put_get_roundtrip_text(
    self, integration_client: DatabaseClient, bucket_name: str
  ) -> None:
    bucket = integration_client.bucket(bucket_name)
    await bucket.put('greeting.txt', 'hello world')
    assert await bucket.get_text('greeting.txt') == 'hello world'

  @pytest.mark.anyio
  async def test_put_get_roundtrip_bytes(
    self, integration_client: DatabaseClient, bucket_name: str
  ) -> None:
    bucket = integration_client.bucket(bucket_name)
    payload = bytes(range(256))
    await bucket.put('blob.bin', payload)
    assert await bucket.get('blob.bin') == payload

  @pytest.mark.anyio
  async def test_exists(self, integration_client: DatabaseClient, bucket_name: str) -> None:
    bucket = integration_client.bucket(bucket_name)
    assert await bucket.exists('missing.txt') is False
    await bucket.put('present.txt', 'x')
    assert await bucket.exists('present.txt') is True

  @pytest.mark.anyio
  async def test_delete(self, integration_client: DatabaseClient, bucket_name: str) -> None:
    bucket = integration_client.bucket(bucket_name)
    await bucket.put('temp.txt', 'x')
    await bucket.delete('temp.txt')
    assert await bucket.exists('temp.txt') is False

  @pytest.mark.anyio
  async def test_head(self, integration_client: DatabaseClient, bucket_name: str) -> None:
    bucket = integration_client.bucket(bucket_name)
    await bucket.put('sized.txt', 'abcde')
    head = await bucket.head('sized.txt')
    assert head is not None
    assert head.get('size') == 5

  @pytest.mark.anyio
  async def test_copy_target_is_same_bucket_key(
    self, integration_client: DatabaseClient, bucket_name: str
  ) -> None:
    # Verifies the copy/rename target semantics against a live server: the
    # destination arg is a bare key in the same bucket, not a full pointer.
    bucket = integration_client.bucket(bucket_name)
    await bucket.put('orig.txt', 'data')
    await bucket.copy('orig.txt', 'copy.txt')
    assert await bucket.get_text('copy.txt') == 'data'
    assert await bucket.get_text('orig.txt') == 'data'

  @pytest.mark.anyio
  async def test_rename_target_is_same_bucket_key(
    self, integration_client: DatabaseClient, bucket_name: str
  ) -> None:
    bucket = integration_client.bucket(bucket_name)
    await bucket.put('before.txt', 'data')
    await bucket.rename('before.txt', 'after.txt')
    assert await bucket.exists('before.txt') is False
    assert await bucket.get_text('after.txt') == 'data'

  @pytest.mark.anyio
  async def test_put_if_not_exists(
    self, integration_client: DatabaseClient, bucket_name: str
  ) -> None:
    bucket = integration_client.bucket(bucket_name)
    await bucket.put('once.txt', 'first')
    # Second put_if_not_exists must not overwrite (server raises; we tolerate).
    with contextlib.suppress(Exception):
      await bucket.put_if_not_exists('once.txt', 'second')
    assert await bucket.get_text('once.txt') == 'first'

  @pytest.mark.anyio
  async def test_list(self, integration_client: DatabaseClient, bucket_name: str) -> None:
    bucket = integration_client.bucket(bucket_name)
    await bucket.put('a.txt', 'aaa')
    await bucket.put('b.txt', 'bbbbbb')
    rows = await bucket.list()
    keys = {row['key'] for row in rows}
    assert {'/a.txt', '/b.txt'} <= keys

  @pytest.mark.anyio
  async def test_file_field_record_roundtrip(
    self, integration_client: DatabaseClient, bucket_name: str
  ) -> None:
    # Store a file, then reference it via a record's file field and read it back.
    bucket = integration_client.bucket(bucket_name)
    await bucket.put('doc.txt', 'content')
    await integration_client.execute('DEFINE TABLE media SCHEMALESS;')
    await integration_client.execute(
      'CREATE media:one SET source = type::file($bucket, $key) RETURN NONE',
      {'bucket': bucket_name, 'key': 'doc.txt'},
    )
    rows = await integration_client.execute(
      'SELECT file::bucket(source) AS bucket, file::key(source) AS key FROM media:one'
    )
    # The projected {bucket, key} row is auto-normalised to a FileRef by the
    # client response normaliser; accept either the FileRef or the raw dict.
    result = (
      rows[0]['result']
      if isinstance(rows, list) and rows and isinstance(rows[0], dict) and 'result' in rows[0]
      else rows
    )
    row = result[0] if isinstance(result, list) else result
    actual = row if isinstance(row, FileRef) else FileRef.from_sqon(row)
    assert actual == FileRef(bucket=bucket_name, key='/doc.txt')


class TestSessionsLive:
  """Multiple-session support against a live WebSocket connection."""

  @pytest.mark.anyio
  async def test_new_session_executes(self, integration_client: DatabaseClient) -> None:
    if not integration_client._config.url.startswith(('ws://', 'wss://')):
      pytest.skip('Sessions require a WebSocket connection.')
    session = await integration_client.new_session()
    try:
      await session.signin(
        {
          'username': integration_client._config.username,
          'password': integration_client._config.password,
        }
      )
      await session.use(integration_client._config.namespace, integration_client._config.database)
      result = await session.execute('RETURN 42')
      value = (
        result[0]['result']
        if isinstance(result, list) and result and 'result' in result[0]
        else result
      )
      assert value == 42 or value == [42]
    finally:
      await session.close_session()

  @pytest.mark.anyio
  async def test_session_isolated_crud(self, integration_client: DatabaseClient) -> None:
    if not integration_client._config.url.startswith(('ws://', 'wss://')):
      pytest.skip('Sessions require a WebSocket connection.')
    async with await integration_client.new_session() as session:
      await session.signin(
        {
          'username': integration_client._config.username,
          'password': integration_client._config.password,
        }
      )
      await session.use(integration_client._config.namespace, integration_client._config.database)
      created = await session.create('widget', {'name': 'gadget'})
      assert created is not None

  @pytest.mark.anyio
  async def test_session_context_manager_closes(self, integration_client: DatabaseClient) -> None:
    if not integration_client._config.url.startswith(('ws://', 'wss://')):
      pytest.skip('Sessions require a WebSocket connection.')
    session: Session
    async with await integration_client.new_session() as session:
      await session.use(integration_client._config.namespace, integration_client._config.database)
    # After exit the session is closed; further ops raise.
    with pytest.raises(ConnectionError):
      await session.execute('RETURN 1')


if __name__ == '__main__':
  pytest.main([__file__, '-v'])
