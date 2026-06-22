"""Tests for the multiplexed Session wrapper and DatabaseClient.new_session()."""

from unittest.mock import AsyncMock, Mock

import pytest
from surrealdb.errors import UnsupportedFeatureError

from surql.connection.client import ConnectionError, DatabaseClient, QueryError
from surql.connection.config import ConnectionConfig
from surql.connection.session import Session
from surql.types import FileRef, RecordID


def _make_sdk_session() -> Mock:
  """A mock SDK session exposing the methods Session delegates to."""
  session = Mock()
  session.query = AsyncMock(return_value=[{'result': [], 'status': 'OK'}])
  session.select = AsyncMock(return_value=[])
  session.create = AsyncMock(return_value={'id': 'user:1'})
  session.update = AsyncMock(return_value={'id': 'user:1'})
  session.merge = AsyncMock(return_value={'id': 'user:1'})
  session.delete = AsyncMock(return_value=None)
  session.use = AsyncMock()
  session.signin = AsyncMock(return_value='token')
  session.invalidate = AsyncMock()
  session.live = AsyncMock(return_value='live-uuid')
  session.close_session = AsyncMock()
  return session


@pytest.fixture
def sdk_session() -> Mock:
  return _make_sdk_session()


@pytest.fixture
def session(mock_db_client: DatabaseClient, sdk_session: Mock) -> Session:
  return Session(mock_db_client, sdk_session)


class TestNewSessionGating:
  """new_session() is async and gates on transport support."""

  @pytest.mark.anyio
  async def test_returns_session_on_ws(
    self, mock_db_client: DatabaseClient, sdk_session: Mock
  ) -> None:
    mock_db_client._client.new_session = AsyncMock(return_value=sdk_session)
    result = await mock_db_client.new_session()
    assert isinstance(result, Session)

  @pytest.mark.anyio
  async def test_raises_on_unsupported_feature(self, mock_db_client: DatabaseClient) -> None:
    # HTTP / embedded engines raise UnsupportedFeatureError in the SDK.
    mock_db_client._client.new_session = AsyncMock(side_effect=UnsupportedFeatureError('ws only'))
    with pytest.raises(ConnectionError, match='Sessions are not supported'):
      await mock_db_client.new_session()

  @pytest.mark.anyio
  async def test_raises_on_none_return(self, mock_db_client: DatabaseClient) -> None:
    mock_db_client._client.new_session = AsyncMock(return_value=None)
    with pytest.raises(ConnectionError, match='Sessions are not supported'):
      await mock_db_client.new_session()

  @pytest.mark.anyio
  async def test_raises_when_disconnected(self, db_config: ConnectionConfig) -> None:
    client = DatabaseClient(db_config)
    with pytest.raises(ConnectionError, match='not connected'):
      await client.new_session()


class TestSessionOperations:
  """Session mirrors the client query surface with normalize/denormalize."""

  @pytest.mark.anyio
  async def test_execute_denormalizes_params(self, session: Session, sdk_session: Mock) -> None:
    await session.execute(
      'SELECT * FROM user WHERE id = $id', {'id': RecordID(table='user', id='a')}
    )
    args = sdk_session.query.call_args.args
    assert args[0] == 'SELECT * FROM user WHERE id = $id'
    # The surql RecordID is converted to an SDK RecordID before sending.
    assert type(args[1]['id']).__name__ == 'RecordID'

  @pytest.mark.anyio
  async def test_execute_denormalizes_fileref(self, session: Session, sdk_session: Mock) -> None:
    await session.execute('CREATE doc SET f = $f', {'f': FileRef(bucket='b', key='k')})
    assert sdk_session.query.call_args.args[1]['f'] == {'bucket': 'b', 'key': 'k'}

  @pytest.mark.anyio
  async def test_select_table(self, session: Session, sdk_session: Mock) -> None:
    await session.select('user')
    sdk_session.select.assert_awaited_once_with('user')

  @pytest.mark.anyio
  async def test_select_record_id_uses_type_record(
    self, session: Session, sdk_session: Mock
  ) -> None:
    sdk_session.query = AsyncMock(return_value=[{'result': [{'id': 'user:alice'}], 'status': 'OK'}])
    await session.select('user:alice')
    args = sdk_session.query.call_args.args
    assert args[0] == 'SELECT * FROM type::record($table, $id)'
    assert args[1] == {'table': 'user', 'id': 'alice'}

  @pytest.mark.anyio
  async def test_create(self, session: Session, sdk_session: Mock) -> None:
    await session.create('user', {'name': 'alice'})
    sdk_session.create.assert_awaited_once()

  @pytest.mark.anyio
  async def test_update(self, session: Session, sdk_session: Mock) -> None:
    await session.update('user:alice', {'name': 'bob'})
    sdk_session.update.assert_awaited_once()

  @pytest.mark.anyio
  async def test_merge(self, session: Session, sdk_session: Mock) -> None:
    await session.merge('user:alice', {'name': 'bob'})
    sdk_session.merge.assert_awaited_once()

  @pytest.mark.anyio
  async def test_delete(self, session: Session, sdk_session: Mock) -> None:
    await session.delete('user:alice')
    sdk_session.delete.assert_awaited_once()

  @pytest.mark.anyio
  async def test_use(self, session: Session, sdk_session: Mock) -> None:
    await session.use('ns', 'db')
    sdk_session.use.assert_awaited_once_with('ns', 'db')

  @pytest.mark.anyio
  async def test_signin(self, session: Session, sdk_session: Mock) -> None:
    out = await session.signin({'username': 'root', 'password': 'root'})
    assert out == 'token'
    sdk_session.signin.assert_awaited_once_with({'username': 'root', 'password': 'root'})

  @pytest.mark.anyio
  async def test_invalidate(self, session: Session, sdk_session: Mock) -> None:
    await session.invalidate()
    sdk_session.invalidate.assert_awaited_once()

  @pytest.mark.anyio
  async def test_live(self, session: Session, sdk_session: Mock) -> None:
    out = await session.live('reading', diff=True)
    sdk_session.live.assert_awaited_once_with('reading', True)
    assert out == 'live-uuid'


class TestSessionLifecycle:
  """Session close / context-manager behaviour."""

  @pytest.mark.anyio
  async def test_close_session(self, session: Session, sdk_session: Mock) -> None:
    await session.close_session()
    sdk_session.close_session.assert_awaited_once()

  @pytest.mark.anyio
  async def test_close_is_idempotent(self, session: Session, sdk_session: Mock) -> None:
    await session.close_session()
    await session.close_session()
    sdk_session.close_session.assert_awaited_once()

  @pytest.mark.anyio
  async def test_aclose_alias(self, session: Session, sdk_session: Mock) -> None:
    await session.aclose()
    sdk_session.close_session.assert_awaited_once()

  @pytest.mark.anyio
  async def test_operations_after_close_raise(self, session: Session) -> None:
    await session.close_session()
    with pytest.raises(ConnectionError, match='Session is closed'):
      await session.execute('RETURN 1')

  @pytest.mark.anyio
  async def test_context_manager_closes(
    self, mock_db_client: DatabaseClient, sdk_session: Mock
  ) -> None:
    async with Session(mock_db_client, sdk_session) as s:
      await s.execute('RETURN 1')
    sdk_session.close_session.assert_awaited_once()

  @pytest.mark.anyio
  async def test_query_error_wraps(self, session: Session, sdk_session: Mock) -> None:
    sdk_session.query = AsyncMock(side_effect=RuntimeError('boom'))
    with pytest.raises(QueryError, match='Query execution failed'):
      await session.execute('RETURN 1')


if __name__ == '__main__':
  pytest.main([__file__, '-v'])
