"""Tests for the `surql bucket` CLI command group.

The DB-touching paths are exercised with a mocked ``get_client`` async context
manager so no live server is needed; the assertions focus on the SurrealQL /
bucket-handle calls each command makes.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, patch

import pytest
from typer.testing import CliRunner

from surql.cli.bucket import app as bucket_app
from surql.connection.client import ConnectionError as DBConnectionError


@pytest.fixture
def runner() -> CliRunner:
  return CliRunner()


def _patch_client(client: Mock):
  """Patch surql.cli.bucket.get_client to yield ``client``."""

  @asynccontextmanager
  async def _fake_get_client(_config):
    yield client

  return patch('surql.cli.bucket.get_client', _fake_get_client)


class TestBucketCliHelp:
  """Every subcommand is wired and shows help."""

  @pytest.mark.parametrize(
    'cmd', ['define', 'list', 'rm', 'put', 'get', 'delete', 'exists', 'files']
  )
  def test_subcommand_help(self, runner: CliRunner, cmd: str) -> None:
    result = runner.invoke(bucket_app, [cmd, '--help'])
    assert result.exit_code == 0

  def test_group_help(self, runner: CliRunner) -> None:
    result = runner.invoke(bucket_app, ['--help'])
    assert result.exit_code == 0
    assert 'define' in result.stdout
    assert 'files' in result.stdout


class TestBucketCliDefine:
  """`bucket define` emits the right DEFINE BUCKET statement."""

  def test_define_memory(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock()
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['define', 'avatars'])
    assert result.exit_code == 0
    client.execute.assert_awaited_once_with('DEFINE BUCKET avatars BACKEND "memory";')

  def test_define_file_path_readonly(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock()
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['define', 'arc', '--path', '/srv/data', '--readonly'])
    assert result.exit_code == 0
    client.execute.assert_awaited_once_with('DEFINE BUCKET arc BACKEND "file:/srv/data" READONLY;')

  def test_define_if_not_exists(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock()
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['define', 'b', '--if-not-exists'])
    assert result.exit_code == 0
    client.execute.assert_awaited_once_with('DEFINE BUCKET IF NOT EXISTS b BACKEND "memory";')


class TestBucketCliRemove:
  """`bucket rm` emits REMOVE BUCKET."""

  def test_rm(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock()
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['rm', 'avatars'])
    assert result.exit_code == 0
    client.execute.assert_awaited_once_with('REMOVE BUCKET avatars;')

  def test_rm_if_exists(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock()
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['rm', 'avatars', '--if-exists'])
    assert result.exit_code == 0
    client.execute.assert_awaited_once_with('REMOVE BUCKET IF EXISTS avatars;')


class TestBucketCliFileOps:
  """File op commands delegate to the Bucket handle."""

  def test_put_text(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.put = AsyncMock()
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['put', 'notes', 'hello.txt', '--text', 'hi'])
    assert result.exit_code == 0
    client.bucket.assert_called_once_with('notes')
    bucket.put.assert_awaited_once_with('hello.txt', 'hi')

  def test_put_requires_content(self, runner: CliRunner) -> None:
    client = Mock()
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['put', 'notes', 'hello.txt'])
    assert result.exit_code == 1

  def test_put_file(self, runner: CliRunner, tmp_path) -> None:
    src = tmp_path / 'data.bin'
    src.write_bytes(b'\x00\x01\x02')
    bucket = Mock()
    bucket.put = AsyncMock()
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['put', 'b', 'k', '--file', str(src)])
    assert result.exit_code == 0
    bucket.put.assert_awaited_once_with('k', b'\x00\x01\x02')

  def test_get_text_output(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.get = AsyncMock(return_value=b'hello world')
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['get', 'notes', 'hello.txt'])
    assert result.exit_code == 0
    assert 'hello world' in result.stdout

  def test_get_to_file(self, runner: CliRunner, tmp_path) -> None:
    out = tmp_path / 'out.bin'
    bucket = Mock()
    bucket.get = AsyncMock(return_value=b'\x00\x01')
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['get', 'b', 'k', '--output', str(out)])
    assert result.exit_code == 0
    assert out.read_bytes() == b'\x00\x01'

  def test_get_missing_exits_1(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.get = AsyncMock(return_value=None)
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['get', 'b', 'missing'])
    assert result.exit_code == 1

  def test_delete(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.delete = AsyncMock()
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['delete', 'b', 'k'])
    assert result.exit_code == 0
    bucket.delete.assert_awaited_once_with('k')

  def test_exists_true(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.exists = AsyncMock(return_value=True)
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['exists', 'b', 'k'])
    assert result.exit_code == 0

  def test_exists_false_exits_1(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.exists = AsyncMock(return_value=False)
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['exists', 'b', 'k'])
    assert result.exit_code == 1

  def test_files_list(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.list = AsyncMock(return_value=[{'bucket': 'b', 'key': 'k', 'size': 3, 'updated': 'd'}])
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['files', 'b', '--format', 'json'])
    assert result.exit_code == 0
    bucket.list.assert_awaited_once()

  def test_files_list_empty(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.list = AsyncMock(return_value=[])
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['files', 'empty', '--verbose'])
    assert result.exit_code == 0

  def test_put_rejects_both_inputs(self, runner: CliRunner, tmp_path) -> None:
    src = tmp_path / 'f.bin'
    src.write_bytes(b'x')
    client = Mock()
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['put', 'b', 'k', '--file', str(src), '--text', 'y'])
    assert result.exit_code == 1


class TestBucketCliList:
  """`bucket list` parses INFO FOR DB into a bucket table."""

  def test_list_buckets(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock(
      return_value=[
        {
          'result': {
            'buckets': {
              'avatars': 'DEFINE BUCKET avatars BACKEND "memory"',
              'archive': 'DEFINE BUCKET archive BACKEND "file:/x" READONLY',
            }
          }
        }
      ]
    )
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['list', '--format', 'json', '--verbose'])
    assert result.exit_code == 0
    client.execute.assert_awaited_once_with('INFO FOR DB;')

  def test_list_buckets_empty(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock(return_value=[{'result': {'buckets': {}}}])
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['list'])
    assert result.exit_code == 0


class TestBucketCliErrors:
  """Error branches surface connection / generic failures with exit code 1."""

  def test_define_connection_error(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock(side_effect=DBConnectionError('refused'))
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['define', 'b'])
    assert result.exit_code == 1

  def test_define_generic_error(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock(side_effect=RuntimeError('boom'))
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['define', 'b'])
    assert result.exit_code == 1

  def test_rm_connection_error(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock(side_effect=DBConnectionError('refused'))
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['rm', 'b'])
    assert result.exit_code == 1

  def test_list_connection_error(self, runner: CliRunner) -> None:
    client = Mock()
    client.execute = AsyncMock(side_effect=DBConnectionError('refused'))
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['list'])
    assert result.exit_code == 1

  def test_put_connection_error(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.put = AsyncMock(side_effect=DBConnectionError('refused'))
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['put', 'b', 'k', '--text', 'x'])
    assert result.exit_code == 1

  def test_get_connection_error(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.get = AsyncMock(side_effect=DBConnectionError('refused'))
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['get', 'b', 'k'])
    assert result.exit_code == 1

  def test_delete_connection_error(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.delete = AsyncMock(side_effect=DBConnectionError('refused'))
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['delete', 'b', 'k'])
    assert result.exit_code == 1

  def test_exists_connection_error(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.exists = AsyncMock(side_effect=DBConnectionError('refused'))
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['exists', 'b', 'k'])
    assert result.exit_code == 1

  def test_files_connection_error(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.list = AsyncMock(side_effect=DBConnectionError('refused'))
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['files', 'b'])
    assert result.exit_code == 1

  def test_put_generic_error(self, runner: CliRunner) -> None:
    bucket = Mock()
    bucket.put = AsyncMock(side_effect=RuntimeError('boom'))
    client = Mock()
    client.bucket = Mock(return_value=bucket)
    with _patch_client(client):
      result = runner.invoke(bucket_app, ['put', 'b', 'k', '--text', 'x'])
    assert result.exit_code == 1


if __name__ == '__main__':
  pytest.main([__file__, '-v'])
