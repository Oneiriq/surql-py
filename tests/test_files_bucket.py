"""Unit tests for the Bucket async handle (file operations).

These exercise the exact SurrealQL each operation emits and assert the
parameterised ``type::file($bucket, $key)`` safety contract (bucket / key /
data / dst are always bound params, never f-string interpolated). No live
server is required — the client's ``execute`` is mocked.
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from surql.files.bucket import Bucket, _unwrap_result


class _FakeClient:
  """Minimal client stand-in recording execute() calls and returning a value."""

  def __init__(self, return_value: Any = None) -> None:
    self.execute = AsyncMock(return_value=return_value)

  @property
  def last_query(self) -> str:
    return self.execute.call_args.args[0]

  @property
  def last_params(self) -> dict[str, Any]:
    return self.execute.call_args.args[1]


@pytest.fixture
def fake_client() -> _FakeClient:
  return _FakeClient()


class TestBucketQueries:
  """Each method emits the contract SurrealQL with bound params only."""

  @pytest.mark.anyio
  async def test_put(self, fake_client: _FakeClient) -> None:
    await Bucket(fake_client, 'avatars').put('alice.png', b'data')
    assert fake_client.last_query == 'RETURN type::file($bucket, $key).put($data)'
    assert fake_client.last_params == {'bucket': 'avatars', 'key': 'alice.png', 'data': b'data'}

  @pytest.mark.anyio
  async def test_put_str(self, fake_client: _FakeClient) -> None:
    await Bucket(fake_client, 'b').put('k', 'hello')
    assert fake_client.last_params['data'] == 'hello'

  @pytest.mark.anyio
  async def test_put_if_not_exists(self, fake_client: _FakeClient) -> None:
    await Bucket(fake_client, 'b').put_if_not_exists('k', b'x')
    assert fake_client.last_query == 'RETURN type::file($bucket, $key).put_if_not_exists($data)'

  @pytest.mark.anyio
  async def test_get(self) -> None:
    client = _FakeClient(return_value=b'bytes')
    out = await Bucket(client, 'b').get('k')
    assert client.last_query == 'RETURN type::file($bucket, $key).get()'
    assert client.last_params == {'bucket': 'b', 'key': 'k'}
    assert out == b'bytes'

  @pytest.mark.anyio
  async def test_get_text_casts(self) -> None:
    client = _FakeClient(return_value='hello')
    out = await Bucket(client, 'b').get_text('k')
    assert client.last_query == 'RETURN <string>type::file($bucket, $key).get()'
    assert out == 'hello'

  @pytest.mark.anyio
  async def test_get_decodes_bytes_to_text(self) -> None:
    client = _FakeClient(return_value=b'hi')
    assert await Bucket(client, 'b').get_text('k') == 'hi'

  @pytest.mark.anyio
  async def test_get_missing_returns_none(self) -> None:
    client = _FakeClient(return_value=None)
    assert await Bucket(client, 'b').get('k') is None

  @pytest.mark.anyio
  async def test_get_encodes_str_to_bytes(self) -> None:
    client = _FakeClient(return_value='hi')
    assert await Bucket(client, 'b').get('k') == b'hi'

  @pytest.mark.anyio
  async def test_exists_true(self) -> None:
    client = _FakeClient(return_value=True)
    out = await Bucket(client, 'b').exists('k')
    assert client.last_query == 'RETURN type::file($bucket, $key).exists()'
    assert out is True

  @pytest.mark.anyio
  async def test_exists_false(self) -> None:
    client = _FakeClient(return_value=False)
    assert await Bucket(client, 'b').exists('k') is False

  @pytest.mark.anyio
  async def test_head(self) -> None:
    # head() projects the (undecodable) file pointer into bucket/key strings,
    # mirroring list(), and returns the key canonically (leading slash kept).
    client = _FakeClient(return_value=[{'bucket': 'b', 'key': '/k', 'size': 3}])
    out = await Bucket(client, 'b').head('k')
    assert client.last_query == (
      'SELECT file::bucket(file) AS bucket, file::key(file) AS key, size, updated '
      'FROM type::file($bucket, $key).head()'
    )
    assert out == {'bucket': 'b', 'key': '/k', 'size': 3}

  @pytest.mark.anyio
  async def test_head_missing(self) -> None:
    client = _FakeClient(return_value=None)
    assert await Bucket(client, 'b').head('k') is None

  @pytest.mark.anyio
  async def test_delete(self, fake_client: _FakeClient) -> None:
    await Bucket(fake_client, 'b').delete('k')
    assert fake_client.last_query == 'RETURN type::file($bucket, $key).delete()'

  @pytest.mark.anyio
  async def test_copy(self, fake_client: _FakeClient) -> None:
    await Bucket(fake_client, 'b').copy('k', 'k2')
    assert fake_client.last_query == 'RETURN type::file($bucket, $key).copy($dst)'
    assert fake_client.last_params == {'bucket': 'b', 'key': 'k', 'dst': 'k2'}

  @pytest.mark.anyio
  async def test_copy_if_not_exists(self, fake_client: _FakeClient) -> None:
    await Bucket(fake_client, 'b').copy_if_not_exists('k', 'k2')
    assert fake_client.last_query == 'RETURN type::file($bucket, $key).copy_if_not_exists($dst)'

  @pytest.mark.anyio
  async def test_rename(self, fake_client: _FakeClient) -> None:
    await Bucket(fake_client, 'b').rename('k', 'k2')
    assert fake_client.last_query == 'RETURN type::file($bucket, $key).rename($dst)'
    assert fake_client.last_params == {'bucket': 'b', 'key': 'k', 'dst': 'k2'}

  @pytest.mark.anyio
  async def test_rename_if_not_exists(self, fake_client: _FakeClient) -> None:
    await Bucket(fake_client, 'b').rename_if_not_exists('k', 'k2')
    assert fake_client.last_query == 'RETURN type::file($bucket, $key).rename_if_not_exists($dst)'

  @pytest.mark.anyio
  async def test_list_projects_pointer(self) -> None:
    client = _FakeClient(return_value=[{'bucket': 'b', 'key': 'k', 'size': 3, 'updated': 'd'}])
    out = await Bucket(client, 'b').list()
    assert 'file::list($bucket)' in client.last_query
    assert 'file::bucket(file)' in client.last_query
    assert 'file::key(file)' in client.last_query
    assert client.last_params == {'bucket': 'b'}
    assert out == [{'bucket': 'b', 'key': 'k', 'size': 3, 'updated': 'd'}]

  @pytest.mark.anyio
  async def test_list_empty(self) -> None:
    client = _FakeClient(return_value=None)
    assert await Bucket(client, 'b').list() == []


class TestBucketSafety:
  """No bucket/key/data/dst value is ever interpolated into the query text."""

  @pytest.mark.anyio
  async def test_injection_in_key_is_a_bound_param(self, fake_client: _FakeClient) -> None:
    nasty = 'x").delete(); DEFINE TABLE evil; --'
    await Bucket(fake_client, 'b').get(nasty)
    # The dangerous string never appears in the query text; it's a bound param.
    assert nasty not in fake_client.last_query
    assert fake_client.last_params['key'] == nasty

  @pytest.mark.anyio
  async def test_injection_in_bucket_is_a_bound_param(self, fake_client: _FakeClient) -> None:
    nasty = 'b"; REMOVE DATABASE; --'
    await Bucket(fake_client, nasty).exists('k')
    assert nasty not in fake_client.last_query
    assert fake_client.last_params['bucket'] == nasty

  def test_name_property(self, fake_client: _FakeClient) -> None:
    assert Bucket(fake_client, 'avatars').name == 'avatars'


class TestUnwrapResult:
  """_unwrap_result collapses the various single-statement response shapes."""

  def test_bare_value(self) -> None:
    assert _unwrap_result('x') == 'x'

  def test_one_element_list(self) -> None:
    assert _unwrap_result(['x']) == 'x'

  def test_statement_envelope(self) -> None:
    assert _unwrap_result([{'result': 'x', 'status': 'OK'}]) == 'x'

  def test_dict_envelope(self) -> None:
    assert _unwrap_result({'result': 'x', 'status': 'OK'}) == 'x'

  def test_empty_list(self) -> None:
    assert _unwrap_result([]) is None

  def test_multi_element_list_passes_through(self) -> None:
    assert _unwrap_result([1, 2]) == [1, 2]


if __name__ == '__main__':
  pytest.main([__file__, '-v'])
