"""Tests for the query graph module."""

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from surql.query.graph import (
  GraphQuery,
  compute_degree,
  count_related,
  find_mutual_connections,
  find_shortest_path,
  get_incoming_edges,
  get_neighbors,
  get_outgoing_edges,
  get_related_records,
  relate,
  shortest_path,
  traverse,
  traverse_with_depth,
  unrelate,
)
from surql.types.record_id import RecordID


# Test models
class User(BaseModel):
  """Test user model."""

  name: str
  email: str


class Post(BaseModel):
  """Test post model."""

  title: str
  content: str


class Edge(BaseModel):
  """Test edge model."""

  id: str
  weight: int | None = None


class TestTraverse:
  """Test suite for traverse function."""

  @pytest.mark.anyio
  async def test_traverse_basic(self, mock_db_client):
    """Test basic graph traversal."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'title': 'Post 1', 'content': 'Content 1'},
            {'title': 'Post 2', 'content': 'Content 2'},
          ]
        }
      ]
    )

    posts = await traverse('user:alice', '->likes->post', Post, client=mock_db_client)

    assert len(posts) == 2
    assert all(isinstance(p, Post) for p in posts)
    assert posts[0].title == 'Post 1'

  @pytest.mark.anyio
  async def test_traverse_with_record_id(self, mock_db_client):
    """Test traversal with RecordID."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Bob', 'email': 'bob@example.com'}]}]
    )

    start_id = RecordID(table='user', id='alice')
    users = await traverse(start_id, '<-follows<-user', User, client=mock_db_client)

    assert len(users) == 1
    assert users[0].name == 'Bob'

  @pytest.mark.anyio
  async def test_traverse_inbound(self, mock_db_client):
    """Test inbound graph traversal."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Bob', 'email': 'bob@example.com'},
          ]
        }
      ]
    )

    followers = await traverse('user:charlie', '<-follows<-user', User, client=mock_db_client)

    assert len(followers) == 2

  @pytest.mark.anyio
  async def test_traverse_multi_hop(self, mock_db_client):
    """Test multi-hop traversal (friends of friends)."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Charlie', 'email': 'charlie@example.com'},
          ]
        }
      ]
    )

    fof = await traverse(
      'user:alice', '<-follows<-user<-follows<-user', User, client=mock_db_client
    )

    assert len(fof) == 1
    assert fof[0].name == 'Charlie'

  @pytest.mark.anyio
  async def test_traverse_empty_result(self, mock_db_client):
    """Test traversal with no results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    posts = await traverse('user:alice', '->likes->post', Post, client=mock_db_client)

    assert posts == []

  @pytest.mark.anyio
  async def test_traverse_with_context(self, mock_db_client):
    """Test traverse using context client."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'title': 'Post 1', 'content': 'Content 1'}]}]
    )

    with patch('surql.query.executor.get_db', return_value=mock_db_client):
      posts = await traverse('user:alice', '->likes->post', Post)

    assert len(posts) == 1


class TestTraverseWithDepth:
  """Test suite for traverse_with_depth function."""

  @pytest.mark.anyio
  async def test_traverse_with_depth_out(self, mock_db_client):
    """Test outbound traversal with depth."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'title': 'Post 1', 'content': 'Content 1'},
          ]
        }
      ]
    )

    posts = await traverse_with_depth(
      'user:alice', 'likes', 'post', direction='out', depth=1, model=Post, client=mock_db_client
    )

    assert len(posts) == 1
    assert isinstance(posts[0], Post)

  @pytest.mark.anyio
  async def test_traverse_with_depth_in(self, mock_db_client):
    """Test inbound traversal with depth."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
          ]
        }
      ]
    )

    users = await traverse_with_depth(
      'post:123', 'likes', 'user', direction='in', depth=1, model=User, client=mock_db_client
    )

    assert len(users) == 1
    assert users[0].name == 'Alice'

  @pytest.mark.anyio
  async def test_traverse_with_depth_both(self, mock_db_client):
    """Test bidirectional traversal."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Bob', 'email': 'bob@example.com'},
          ]
        }
      ]
    )

    users = await traverse_with_depth(
      'user:charlie',
      'follows',
      'user',
      direction='both',
      depth=1,
      model=User,
      client=mock_db_client,
    )

    assert len(users) == 2

  @pytest.mark.anyio
  async def test_traverse_with_depth_unlimited(self, mock_db_client):
    """Test traversal with unlimited depth."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
          ]
        }
      ]
    )

    users = await traverse_with_depth(
      'user:alice',
      'follows',
      'user',
      direction='both',
      depth=None,
      model=User,
      client=mock_db_client,
    )

    assert len(users) == 1

  @pytest.mark.anyio
  async def test_traverse_with_depth_no_model(self, mock_db_client):
    """Test traversal without model returns raw dicts."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
          ]
        }
      ]
    )

    results = await traverse_with_depth(
      'user:alice', 'follows', 'user', direction='out', depth=1, model=None, client=mock_db_client
    )

    assert len(results) == 1
    assert isinstance(results[0], dict)
    assert results[0]['name'] == 'Alice'

  @pytest.mark.anyio
  async def test_traverse_with_depth_invalid_direction(self, mock_db_client):
    """Test traverse_with_depth with invalid direction."""
    with pytest.raises(ValueError) as exc_info:
      await traverse_with_depth(
        'user:alice', 'follows', 'user', direction='invalid', client=mock_db_client
      )

    assert 'Invalid direction' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_traverse_with_depth_record_id(self, mock_db_client):
    """Test traverse_with_depth with RecordID."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'title': 'Post 1', 'content': 'Content 1'}]}]
    )

    start_id = RecordID(table='user', id='alice')
    posts = await traverse_with_depth(
      start_id, 'likes', 'post', direction='out', depth=1, model=Post, client=mock_db_client
    )

    assert len(posts) == 1


class TestRelate:
  """Test suite for relate function."""

  @pytest.mark.anyio
  async def test_relate_basic(self, mock_db_client):
    """Test creating basic edge relationship."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': {'id': 'likes:123', 'in': 'user:alice', 'out': 'post:456'}}]
    )

    edge = await relate('likes', 'user:alice', 'post:456', client=mock_db_client)

    assert edge['id'] == 'likes:123'
    mock_db_client.execute.assert_called_once()

  @pytest.mark.anyio
  async def test_relate_with_data(self, mock_db_client):
    """Test creating edge with properties."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': {
            'id': 'follows:123',
            'in': 'user:alice',
            'out': 'user:bob',
            'since': '2024-01-01',
            'weight': 1,
          }
        }
      ]
    )

    edge = await relate(
      'follows',
      'user:alice',
      'user:bob',
      data={'since': '2024-01-01', 'weight': 1},
      client=mock_db_client,
    )

    assert edge['since'] == '2024-01-01'
    assert edge['weight'] == 1

  @pytest.mark.anyio
  async def test_relate_with_record_ids(self, mock_db_client):
    """Test relate with RecordID instances."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': {'id': 'likes:123'}}])

    from_id = RecordID(table='user', id='alice')
    to_id = RecordID(table='post', id=456)

    edge = await relate('likes', from_id, to_id, client=mock_db_client)

    assert 'id' in edge

  @pytest.mark.anyio
  async def test_relate_with_context(self, mock_db_client):
    """Test relate using context client."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': {'id': 'likes:123'}}])

    with patch('surql.query.graph.get_db', return_value=mock_db_client):
      edge = await relate('likes', 'user:alice', 'post:456')

    assert 'id' in edge

  @pytest.mark.anyio
  async def test_relate_flat_result(self, mock_db_client):
    """Test relate with flat result format."""
    mock_db_client.execute = AsyncMock(
      return_value={'id': 'likes:123', 'in': 'user:alice', 'out': 'post:456'}
    )

    edge = await relate('likes', 'user:alice', 'post:456', client=mock_db_client)

    assert edge['id'] == 'likes:123'


class TestUnrelate:
  """Test suite for unrelate function."""

  @pytest.mark.anyio
  async def test_unrelate_basic(self, mock_db_client):
    """Test removing edge relationship."""
    mock_db_client.execute = AsyncMock(return_value=None)

    await unrelate('likes', 'user:alice', 'post:456', client=mock_db_client)

    mock_db_client.execute.assert_called_once()
    call_args = mock_db_client.execute.call_args[0][0]
    assert 'DELETE' in call_args
    assert 'user:alice->likes->post:456' in call_args

  @pytest.mark.anyio
  async def test_unrelate_with_record_ids(self, mock_db_client):
    """Test unrelate with RecordID instances."""
    mock_db_client.execute = AsyncMock(return_value=None)

    from_id = RecordID(table='user', id='alice')
    to_id = RecordID(table='post', id=456)

    await unrelate('likes', from_id, to_id, client=mock_db_client)

    mock_db_client.execute.assert_called_once()

  @pytest.mark.anyio
  async def test_unrelate_with_context(self, mock_db_client):
    """Test unrelate using context client."""
    mock_db_client.execute = AsyncMock(return_value=None)

    with patch('surql.query.graph.get_db', return_value=mock_db_client):
      await unrelate('likes', 'user:alice', 'post:456')

    mock_db_client.execute.assert_called_once()


class TestGetOutgoingEdges:
  """Test suite for get_outgoing_edges function."""

  @pytest.mark.anyio
  async def test_get_outgoing_edges_basic(self, mock_db_client):
    """Test getting outgoing edges."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'likes:1', 'in': 'user:alice', 'out': 'post:123'},
            {'id': 'likes:2', 'in': 'user:alice', 'out': 'post:456'},
          ]
        }
      ]
    )

    edges = await get_outgoing_edges('user:alice', 'likes', client=mock_db_client)

    assert len(edges) == 2
    assert all(isinstance(e, dict) for e in edges)

  @pytest.mark.anyio
  async def test_get_outgoing_edges_with_model(self, mock_db_client):
    """Test getting outgoing edges with model."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'likes:1', 'weight': 5},
          ]
        }
      ]
    )

    edges = await get_outgoing_edges('user:alice', 'likes', model=Edge, client=mock_db_client)

    assert len(edges) == 1
    assert isinstance(edges[0], Edge)
    assert edges[0].weight == 5

  @pytest.mark.anyio
  async def test_get_outgoing_edges_empty(self, mock_db_client):
    """Test getting outgoing edges with no results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    edges = await get_outgoing_edges('user:alice', 'likes', client=mock_db_client)

    assert edges == []

  @pytest.mark.anyio
  async def test_get_outgoing_edges_with_record_id(self, mock_db_client):
    """Test get_outgoing_edges with RecordID."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'id': 'likes:1'}]}])

    record_id = RecordID(table='user', id='alice')
    edges = await get_outgoing_edges(record_id, 'likes', client=mock_db_client)

    assert len(edges) == 1


class TestGetIncomingEdges:
  """Test suite for get_incoming_edges function."""

  @pytest.mark.anyio
  async def test_get_incoming_edges_basic(self, mock_db_client):
    """Test getting incoming edges."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'follows:1', 'in': 'user:alice', 'out': 'user:bob'},
            {'id': 'follows:2', 'in': 'user:alice', 'out': 'user:charlie'},
          ]
        }
      ]
    )

    edges = await get_incoming_edges('user:alice', 'follows', client=mock_db_client)

    assert len(edges) == 2

  @pytest.mark.anyio
  async def test_get_incoming_edges_with_model(self, mock_db_client):
    """Test getting incoming edges with model."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'follows:1', 'weight': 3}]}]
    )

    edges = await get_incoming_edges('user:alice', 'follows', model=Edge, client=mock_db_client)

    assert len(edges) == 1
    assert isinstance(edges[0], Edge)

  @pytest.mark.anyio
  async def test_get_incoming_edges_empty(self, mock_db_client):
    """Test getting incoming edges with no results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    edges = await get_incoming_edges('user:alice', 'follows', client=mock_db_client)

    assert edges == []


class TestGetRelatedRecords:
  """Test suite for get_related_records function."""

  @pytest.mark.anyio
  async def test_get_related_records_out(self, mock_db_client):
    """Test getting related records in outbound direction."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'title': 'Post 1', 'content': 'Content 1'},
          ]
        }
      ]
    )

    posts = await get_related_records(
      'user:alice', 'likes', 'post', direction='out', model=Post, client=mock_db_client
    )

    assert len(posts) == 1
    assert isinstance(posts[0], Post)

  @pytest.mark.anyio
  async def test_get_related_records_in(self, mock_db_client):
    """Test getting related records in inbound direction."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Bob', 'email': 'bob@example.com'},
          ]
        }
      ]
    )

    users = await get_related_records(
      'post:123', 'likes', 'user', direction='in', model=User, client=mock_db_client
    )

    assert len(users) == 2

  @pytest.mark.anyio
  async def test_get_related_records_no_model(self, mock_db_client):
    """Test getting related records without model."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'title': 'Post 1', 'content': 'Content 1'}]}]
    )

    posts = await get_related_records(
      'user:alice', 'likes', 'post', direction='out', model=None, client=mock_db_client
    )

    assert len(posts) == 1
    assert isinstance(posts[0], dict)

  @pytest.mark.anyio
  async def test_get_related_records_invalid_direction(self, mock_db_client):
    """Test get_related_records with invalid direction."""
    with pytest.raises(ValueError) as exc_info:
      await get_related_records(
        'user:alice', 'likes', 'post', direction='invalid', client=mock_db_client
      )

    assert 'Invalid direction' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_get_related_records_with_record_id(self, mock_db_client):
    """Test get_related_records with RecordID."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'title': 'Post 1', 'content': 'Content 1'}]}]
    )

    record_id = RecordID(table='user', id='alice')
    posts = await get_related_records(
      record_id, 'likes', 'post', direction='out', model=Post, client=mock_db_client
    )

    assert len(posts) == 1

  @pytest.mark.anyio
  async def test_get_related_records_flat_result(self, mock_db_client):
    """Test get_related_records with flat result format."""
    mock_db_client.execute = AsyncMock(return_value=[{'title': 'Post 1', 'content': 'Content 1'}])

    posts = await get_related_records(
      'user:alice', 'likes', 'post', direction='out', model=None, client=mock_db_client
    )

    assert len(posts) == 1

  @pytest.mark.anyio
  async def test_get_related_records_empty_result(self, mock_db_client):
    """Test get_related_records with empty result."""
    mock_db_client.execute = AsyncMock(return_value=[])

    posts = await get_related_records(
      'user:alice', 'likes', 'post', direction='out', model=None, client=mock_db_client
    )

    assert posts == []


class TestCountRelated:
  """Test suite for count_related function."""

  @pytest.mark.anyio
  async def test_count_related_out(self, mock_db_client):
    """Test counting outbound related records."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 42}]}])

    count = await count_related('user:alice', 'likes', direction='out', client=mock_db_client)

    assert count == 42

  @pytest.mark.anyio
  async def test_count_related_in(self, mock_db_client):
    """Test counting inbound related records."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 15}]}])

    count = await count_related('user:alice', 'follows', direction='in', client=mock_db_client)

    assert count == 15

  @pytest.mark.anyio
  async def test_count_related_zero(self, mock_db_client):
    """Test counting with zero results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 0}]}])

    count = await count_related('user:alice', 'likes', direction='out', client=mock_db_client)

    assert count == 0

  @pytest.mark.anyio
  async def test_count_related_invalid_direction(self, mock_db_client):
    """Test count_related with invalid direction."""
    with pytest.raises(ValueError) as exc_info:
      await count_related('user:alice', 'likes', direction='invalid', client=mock_db_client)

    assert 'Invalid direction' in str(exc_info.value)

  @pytest.mark.anyio
  async def test_count_related_with_record_id(self, mock_db_client):
    """Test count_related with RecordID."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 10}]}])

    record_id = RecordID(table='user', id='alice')
    count = await count_related(record_id, 'likes', direction='out', client=mock_db_client)

    assert count == 10

  @pytest.mark.anyio
  async def test_count_related_empty_result(self, mock_db_client):
    """Test count_related with empty result returns 0."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    count = await count_related('user:alice', 'likes', direction='out', client=mock_db_client)

    assert count == 0

  @pytest.mark.anyio
  async def test_count_related_with_context(self, mock_db_client):
    """Test count_related using context client."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 5}]}])

    with patch('surql.query.graph.get_db', return_value=mock_db_client):
      count = await count_related('user:alice', 'likes', direction='out')

    assert count == 5


class TestShortestPath:
  """Test suite for shortest_path function."""

  @pytest.mark.anyio
  async def test_shortest_path_found(self, mock_db_client):
    """Test finding shortest path between records."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {'name': 'Alice', 'email': 'alice@example.com'},
        {'name': 'Bob', 'email': 'bob@example.com'},
        {'name': 'Charlie', 'email': 'charlie@example.com'},
      ]
    )

    path = await shortest_path(
      'user:alice', 'user:charlie', 'follows', max_depth=5, client=mock_db_client
    )

    assert len(path) == 3

  @pytest.mark.anyio
  async def test_shortest_path_not_found(self, mock_db_client):
    """Test shortest path when no path exists."""
    mock_db_client.execute = AsyncMock(return_value=None)

    path = await shortest_path(
      'user:alice', 'user:isolated', 'follows', max_depth=3, client=mock_db_client
    )

    assert path == []

  @pytest.mark.anyio
  async def test_shortest_path_with_record_ids(self, mock_db_client):
    """Test shortest_path with RecordID instances."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {'name': 'Alice', 'email': 'alice@example.com'},
      ]
    )

    from_id = RecordID(table='user', id='alice')
    to_id = RecordID(table='user', id='bob')

    path = await shortest_path(from_id, to_id, 'follows', max_depth=5, client=mock_db_client)

    assert len(path) >= 1

  @pytest.mark.anyio
  async def test_shortest_path_max_depth(self, mock_db_client):
    """Test shortest path respects max_depth."""
    mock_db_client.execute = AsyncMock(return_value=None)

    path = await shortest_path(
      'user:alice', 'user:charlie', 'follows', max_depth=1, client=mock_db_client
    )

    # Should try depth 1 and return empty if not found
    assert path == []
    # Verify it was only called once (max_depth=1 means range(1, 2))
    assert mock_db_client.execute.call_count == 1

  @pytest.mark.anyio
  async def test_shortest_path_with_context(self, mock_db_client):
    """Test shortest_path using context client."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'name': 'Alice', 'email': 'alice@example.com'}]
    )

    with patch('surql.query.graph.get_db', return_value=mock_db_client):
      path = await shortest_path('user:alice', 'user:bob', 'follows')

    assert len(path) >= 1

  @pytest.mark.anyio
  async def test_shortest_path_iterative_search(self, mock_db_client):
    """Test shortest path iterative depth search."""
    # First call returns None (depth 1), second returns result (depth 2)
    mock_db_client.execute = AsyncMock(
      side_effect=[
        None,  # Depth 1: not found
        [{'name': 'Alice'}, {'name': 'Bob'}],  # Depth 2: found
      ]
    )

    path = await shortest_path(
      'user:alice', 'user:bob', 'follows', max_depth=5, client=mock_db_client
    )

    assert len(path) == 2
    # Verify it tried depth 1, then depth 2 and stopped
    assert mock_db_client.execute.call_count == 2


# =============================================================================
# GraphQuery Builder Class Tests
# =============================================================================


class TestGraphQueryInitialization:
  """Test suite for GraphQuery builder initialization."""

  def test_init_with_string_id(self):
    """Test GraphQuery initialization with string record ID."""
    query = GraphQuery('user:alice')

    assert query._start == 'user:alice'
    assert query._path == []
    assert query._conditions == []
    assert query._limit is None
    assert query._fields == []
    assert query._target_table is None

  def test_init_with_record_id(self):
    """Test GraphQuery initialization with RecordID instance."""
    record_id = RecordID(table='user', id='alice')
    query = GraphQuery(record_id)

    assert query._start == 'user:alice'

  def test_init_with_integer_record_id(self):
    """Test GraphQuery initialization with RecordID having integer ID."""
    record_id = RecordID(table='post', id=123)
    query = GraphQuery(record_id)

    assert query._start == 'post:123'


class TestGraphQueryTraversalMethods:
  """Test suite for GraphQuery traversal methods (out, in_, both)."""

  def test_out_without_depth(self):
    """Test out() method without specifying depth."""
    query = GraphQuery('user:alice').out('follows')

    assert '->follows' in query._path

  def test_out_with_depth(self):
    """Test out() method with specific depth.

    Post Oneiriq/surql-py#34 a depth suffix emits the grouped
    ``(->edge->?){depth}`` form rather than the v2-only ``->edge{depth}``
    trailing-arrow form (rejected by SurrealDB v3).
    """
    query = GraphQuery('user:alice').out('follows', depth=2)

    assert '(->follows->?){2}' in query._path

  def test_in_without_depth(self):
    """Test in_() method without specifying depth."""
    query = GraphQuery('user:alice').in_('follows')

    assert '<-follows' in query._path

  def test_in_with_depth(self):
    """Test in_() method with specific depth."""
    query = GraphQuery('user:alice').in_('follows', depth=3)

    assert '(<-follows<-?){3}' in query._path

  def test_both_without_depth(self):
    """Test both() method without specifying depth."""
    query = GraphQuery('user:alice').both('knows')

    assert '<->knows' in query._path

  def test_both_with_depth(self):
    """Test both() method with specific depth."""
    query = GraphQuery('user:alice').both('knows', depth=2)

    assert '(<->knows<->?){2}' in query._path

  def test_method_chaining_traversals(self):
    """Test chaining multiple traversal methods."""
    query = GraphQuery('user:alice').out('follows').out('follows')

    assert len(query._path) == 2
    assert query._path == ['->follows', '->follows']


class TestGraphQueryModifierMethods:
  """Test suite for GraphQuery modifier methods (to, where, select, limit)."""

  def test_to_method(self):
    """Test to() method sets target table."""
    query = GraphQuery('user:alice').out('likes').to('post')

    assert query._target_table == 'post'

  def test_where_single_condition(self):
    """Test where() method with single condition."""
    query = GraphQuery('user:alice').out('follows').where('age > 18')

    assert 'age > 18' in query._conditions

  def test_where_multiple_conditions(self):
    """Test where() method with multiple conditions."""
    query = GraphQuery('user:alice').out('follows').where('age > 18').where('active = true')

    assert len(query._conditions) == 2
    assert 'age > 18' in query._conditions
    assert 'active = true' in query._conditions

  def test_select_single_field(self):
    """Test select() method with single field."""
    query = GraphQuery('user:alice').out('follows').select('name')

    assert query._fields == ['name']

  def test_select_multiple_fields(self):
    """Test select() method with multiple fields."""
    query = GraphQuery('user:alice').out('follows').select('id', 'name', 'email')

    assert query._fields == ['id', 'name', 'email']

  def test_limit_valid_value(self):
    """Test limit() method with valid value."""
    query = GraphQuery('user:alice').out('follows').limit(10)

    assert query._limit == 10

  def test_limit_zero(self):
    """Test limit() method with zero value."""
    query = GraphQuery('user:alice').out('follows').limit(0)

    assert query._limit == 0

  def test_limit_negative_raises_error(self):
    """Test limit() method with negative value raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
      GraphQuery('user:alice').out('follows').limit(-1)

    assert 'non-negative' in str(exc_info.value)

  def test_complex_method_chaining(self):
    """Test complex method chaining with all modifiers."""
    query = (
      GraphQuery('user:alice')
      .out('follows')
      .out('follows')
      .to('user')
      .where('id != user:alice')
      .where('active = true')
      .select('id', 'name')
      .limit(100)
    )

    assert len(query._path) == 2
    assert query._target_table == 'user'
    assert len(query._conditions) == 2
    assert query._fields == ['id', 'name']
    assert query._limit == 100


class TestGraphQueryBuild:
  """Test suite for GraphQuery build() method."""

  def test_build_simple_out(self):
    """Test build() generates correct SurrealQL for simple out traversal."""
    query = GraphQuery('user:alice').out('follows').build()

    assert query == 'SELECT * FROM user:alice->follows'

  def test_build_simple_in(self):
    """Test build() generates correct SurrealQL for simple in traversal."""
    query = GraphQuery('user:alice').in_('follows').build()

    assert query == 'SELECT * FROM user:alice<-follows'

  def test_build_simple_both(self):
    """Test build() generates correct SurrealQL for simple both traversal."""
    query = GraphQuery('user:alice').both('knows').build()

    assert query == 'SELECT * FROM user:alice<->knows'

  def test_build_with_depth(self):
    """Test build() generates correct SurrealQL with depth specifier."""
    query = GraphQuery('user:alice').out('follows', depth=2).build()

    assert query == 'SELECT * FROM user:alice(->follows->?){2}'

  def test_build_with_target_table(self):
    """Test build() generates correct SurrealQL with target table."""
    query = GraphQuery('user:alice').out('likes').to('post').build()

    assert query == 'SELECT * FROM user:alice->likes->post'

  def test_build_with_where_condition(self):
    """Test build() generates correct SurrealQL with WHERE clause."""
    query = GraphQuery('user:alice').out('follows').where('age > 18').build()

    assert query == 'SELECT * FROM user:alice->follows WHERE (age > 18)'

  def test_build_with_multiple_conditions(self):
    """Test build() generates correct SurrealQL with multiple WHERE conditions."""
    query = GraphQuery('user:alice').out('follows').where('age > 18').where('active = true').build()

    assert 'WHERE (age > 18) AND (active = true)' in query

  def test_build_with_select_fields(self):
    """Test build() generates correct SurrealQL with SELECT fields."""
    query = GraphQuery('user:alice').out('follows').select('id', 'name').build()

    assert query == 'SELECT id, name FROM user:alice->follows'

  def test_build_with_limit(self):
    """Test build() generates correct SurrealQL with LIMIT clause."""
    query = GraphQuery('user:alice').out('follows').limit(10).build()

    assert query == 'SELECT * FROM user:alice->follows LIMIT 10'

  def test_build_complex_query(self):
    """Test build() generates correct SurrealQL for complex query."""
    query = (
      GraphQuery('user:alice')
      .out('follows')
      .out('follows')
      .to('user')
      .where('id != user:alice')
      .select('id', 'name')
      .limit(50)
      .build()
    )

    assert 'SELECT id, name FROM user:alice->follows->follows->user' in query
    assert 'WHERE (id != user:alice)' in query
    assert 'LIMIT 50' in query

  def test_build_empty_path_raises_error(self):
    """Test build() raises ValueError when no traversal path specified."""
    with pytest.raises(ValueError) as exc_info:
      GraphQuery('user:alice').build()

    assert 'At least one traversal step' in str(exc_info.value)

  def test_build_multi_hop_traversal(self):
    """Test build() handles multi-hop traversal correctly."""
    query = GraphQuery('user:alice').out('follows').in_('knows').both('connected').build()

    assert 'user:alice->follows<-knows<->connected' in query


class TestGraphQueryFetch:
  """Test suite for GraphQuery fetch() method."""

  @pytest.mark.anyio
  async def test_fetch_with_model(self, mock_db_client):
    """Test fetch() returns typed model instances."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Bob', 'email': 'bob@example.com'},
            {'name': 'Charlie', 'email': 'charlie@example.com'},
          ]
        }
      ]
    )

    users = await GraphQuery('user:alice').out('follows').fetch(User, client=mock_db_client)

    assert len(users) == 2
    assert all(isinstance(u, User) for u in users)
    assert users[0].name == 'Bob'

  @pytest.mark.anyio
  async def test_fetch_without_model_returns_dicts(self, mock_db_client):
    """Test fetch() without model returns dictionaries."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:bob', 'name': 'Bob'}]}]
    )

    results = await GraphQuery('user:alice').out('follows').fetch(client=mock_db_client)

    assert len(results) == 1
    assert isinstance(results[0], dict)
    assert results[0]['name'] == 'Bob'

  @pytest.mark.anyio
  async def test_fetch_empty_result(self, mock_db_client):
    """Test fetch() returns empty list when no results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    results = await GraphQuery('user:alice').out('follows').fetch(client=mock_db_client)

    assert results == []

  @pytest.mark.anyio
  async def test_fetch_uses_context_client(self, mock_db_client):
    """Test fetch() uses context client when no client provided."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Bob', 'email': 'bob@example.com'}]}]
    )

    with patch('surql.query.graph_query.get_db', return_value=mock_db_client):
      users = await GraphQuery('user:alice').out('follows').fetch(User)

    assert len(users) == 1
    mock_db_client.execute.assert_called_once()


class TestGraphQueryCount:
  """Test suite for GraphQuery count() method."""

  @pytest.mark.anyio
  async def test_count_returns_integer(self, mock_db_client):
    """Test count() returns integer count."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 42}]}])

    count = await GraphQuery('user:alice').out('follows').count(client=mock_db_client)

    assert count == 42
    assert isinstance(count, int)

  @pytest.mark.anyio
  async def test_count_returns_zero_for_empty(self, mock_db_client):
    """Test count() returns 0 when no results."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    count = await GraphQuery('user:alice').out('follows').count(client=mock_db_client)

    assert count == 0

  @pytest.mark.anyio
  async def test_count_with_conditions(self, mock_db_client):
    """Test count() applies WHERE conditions."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 10}]}])

    count = (
      await GraphQuery('user:alice').out('follows').where('age > 18').count(client=mock_db_client)
    )

    assert count == 10
    call_args = mock_db_client.execute.call_args[0][0]
    assert 'WHERE (age > 18)' in call_args

  @pytest.mark.anyio
  async def test_count_generates_correct_sql(self, mock_db_client):
    """Test count() generates correct SQL with GROUP ALL."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 5}]}])

    await GraphQuery('user:alice').out('follows').count(client=mock_db_client)

    call_args = mock_db_client.execute.call_args[0][0]
    assert 'SELECT count()' in call_args
    assert 'GROUP ALL' in call_args

  @pytest.mark.anyio
  async def test_count_empty_path_raises_error(self, mock_db_client):
    """Test count() raises ValueError when no traversal path."""
    with pytest.raises(ValueError) as exc_info:
      await GraphQuery('user:alice').count(client=mock_db_client)

    assert 'At least one traversal step' in str(exc_info.value)


class TestGraphQueryExists:
  """Test suite for GraphQuery exists() method."""

  @pytest.mark.anyio
  async def test_exists_returns_true_when_found(self, mock_db_client):
    """Test exists() returns True when records exist."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 1}]}])

    result = await GraphQuery('user:alice').out('follows').exists(client=mock_db_client)

    assert result is True

  @pytest.mark.anyio
  async def test_exists_returns_false_when_empty(self, mock_db_client):
    """Test exists() returns False when no records exist."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 0}]}])

    result = await GraphQuery('user:alice').out('follows').exists(client=mock_db_client)

    assert result is False

  @pytest.mark.anyio
  async def test_exists_preserves_original_limit(self, mock_db_client):
    """Test exists() preserves original limit after execution."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 1}]}])

    query = GraphQuery('user:alice').out('follows').limit(100)
    original_limit = query._limit

    await query.exists(client=mock_db_client)

    assert query._limit == original_limit

  @pytest.mark.anyio
  async def test_exists_with_where_condition(self, mock_db_client):
    """Test exists() applies WHERE conditions."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': [{'count': 1}]}])

    result = (
      await GraphQuery('user:alice')
      .out('follows')
      .where('active = true')
      .exists(client=mock_db_client)
    )

    assert result is True
    call_args = mock_db_client.execute.call_args[0][0]
    assert 'active = true' in call_args


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestFindMutualConnections:
  """Test suite for find_mutual_connections helper function."""

  @pytest.mark.anyio
  async def test_find_mutual_connections_with_model(self, mock_db_client):
    """Test find_mutual_connections returns typed model instances."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'name': 'Bob', 'email': 'bob@example.com'},
            {'name': 'Charlie', 'email': 'charlie@example.com'},
          ]
        }
      ]
    )

    mutuals = await find_mutual_connections(
      'user:alice', 'follows', model=User, client=mock_db_client
    )

    assert len(mutuals) == 2
    assert all(isinstance(u, User) for u in mutuals)
    assert mutuals[0].name == 'Bob'

  @pytest.mark.anyio
  async def test_find_mutual_connections_without_model(self, mock_db_client):
    """Test find_mutual_connections returns dicts when no model provided."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:bob', 'name': 'Bob'}]}]
    )

    mutuals = await find_mutual_connections('user:alice', 'follows', client=mock_db_client)

    assert len(mutuals) == 1
    assert isinstance(mutuals[0], dict)

  @pytest.mark.anyio
  async def test_find_mutual_connections_empty_result(self, mock_db_client):
    """Test find_mutual_connections returns empty list when no mutual connections."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    mutuals = await find_mutual_connections(
      'user:alice', 'follows', model=User, client=mock_db_client
    )

    assert mutuals == []

  @pytest.mark.anyio
  async def test_find_mutual_connections_with_record_id(self, mock_db_client):
    """Test find_mutual_connections with RecordID instance."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'name': 'Bob', 'email': 'bob@example.com'}]}]
    )

    record_id = RecordID(table='user', id='alice')
    mutuals = await find_mutual_connections(record_id, 'follows', model=User, client=mock_db_client)

    assert len(mutuals) == 1

  @pytest.mark.anyio
  async def test_find_mutual_connections_uses_context_client(self, mock_db_client):
    """Test find_mutual_connections uses context client when not provided."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    with patch('surql.query.graph.get_db', return_value=mock_db_client):
      await find_mutual_connections('user:alice', 'follows')

    mock_db_client.execute.assert_called_once()


class TestFindShortestPath:
  """Test suite for find_shortest_path helper function."""

  @pytest.mark.anyio
  async def test_find_shortest_path_direct_connection(self, mock_db_client):
    """Test find_shortest_path when direct path exists (depth 1)."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        # Direct path query finds result
        [{'result': [{'id': 'user:bob'}]}],
        # Source record query
        [{'result': [{'id': 'user:alice', 'name': 'Alice'}]}],
        # Target record query
        [{'result': [{'id': 'user:bob', 'name': 'Bob'}]}],
      ]
    )

    path = await find_shortest_path(
      'user:alice', 'user:bob', 'follows', max_depth=5, client=mock_db_client
    )

    assert len(path) == 2
    assert path[0]['name'] == 'Alice'
    assert path[1]['name'] == 'Bob'

  @pytest.mark.anyio
  async def test_find_shortest_path_no_path_exists(self, mock_db_client):
    """Test find_shortest_path when no path exists."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    path = await find_shortest_path(
      'user:alice', 'user:isolated', 'follows', max_depth=3, client=mock_db_client
    )

    assert path == []

  @pytest.mark.anyio
  async def test_find_shortest_path_same_record(self, mock_db_client):
    """Test find_shortest_path when source and target are the same."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:alice', 'name': 'Alice'}]}]
    )

    path = await find_shortest_path(
      'user:alice', 'user:alice', 'follows', max_depth=5, client=mock_db_client
    )

    assert len(path) == 1
    assert path[0]['name'] == 'Alice'

  @pytest.mark.anyio
  async def test_find_shortest_path_max_depth_respected(self, mock_db_client):
    """Test find_shortest_path respects max_depth parameter."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    path = await find_shortest_path(
      'user:alice', 'user:charlie', 'follows', max_depth=2, client=mock_db_client
    )

    assert path == []
    # Should be called twice (depth 1 and 2)
    assert mock_db_client.execute.call_count == 2

  @pytest.mark.anyio
  async def test_find_shortest_path_with_record_ids(self, mock_db_client):
    """Test find_shortest_path with RecordID instances."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    from_id = RecordID(table='user', id='alice')
    to_id = RecordID(table='user', id='bob')

    await find_shortest_path(from_id, to_id, 'follows', max_depth=2, client=mock_db_client)

    # Verify execution happened
    assert mock_db_client.execute.called


class TestGetNeighbors:
  """Test suite for get_neighbors helper function."""

  @pytest.mark.anyio
  async def test_get_neighbors_direction_out(self, mock_db_client):
    """Test get_neighbors with direction='out'."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:bob', 'name': 'Bob'},
            {'id': 'user:charlie', 'name': 'Charlie'},
          ]
        }
      ]
    )

    neighbors = await get_neighbors(
      'user:alice', 'follows', depth=1, direction='out', client=mock_db_client
    )

    assert len(neighbors) == 2
    call_args = mock_db_client.execute.call_args[0][0]
    assert '(->follows->?){1}' in call_args

  @pytest.mark.anyio
  async def test_get_neighbors_direction_in(self, mock_db_client):
    """Test get_neighbors with direction='in'."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:bob', 'name': 'Bob'}]}]
    )

    neighbors = await get_neighbors(
      'user:alice', 'follows', depth=1, direction='in', client=mock_db_client
    )

    assert len(neighbors) == 1
    call_args = mock_db_client.execute.call_args[0][0]
    assert '(<-follows<-?){1}' in call_args

  @pytest.mark.anyio
  async def test_get_neighbors_direction_both(self, mock_db_client):
    """Test get_neighbors with direction='both'."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:bob', 'name': 'Bob'},
            {'id': 'user:charlie', 'name': 'Charlie'},
          ]
        }
      ]
    )

    neighbors = await get_neighbors(
      'user:alice', 'follows', depth=1, direction='both', client=mock_db_client
    )

    assert len(neighbors) == 2
    call_args = mock_db_client.execute.call_args[0][0]
    assert '(<->follows<->?){1}' in call_args

  @pytest.mark.anyio
  async def test_get_neighbors_multiple_depths(self, mock_db_client):
    """Test get_neighbors with depth > 1 collects from all depths."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        # Depth 1 results
        [{'result': [{'id': 'user:bob', 'name': 'Bob'}]}],
        # Depth 2 results
        [{'result': [{'id': 'user:charlie', 'name': 'Charlie'}]}],
      ]
    )

    neighbors = await get_neighbors(
      'user:alice', 'follows', depth=2, direction='out', client=mock_db_client
    )

    assert len(neighbors) == 2
    assert mock_db_client.execute.call_count == 2

  @pytest.mark.anyio
  async def test_get_neighbors_deduplicates_results(self, mock_db_client):
    """Test get_neighbors removes duplicate IDs."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        # Depth 1 results
        [{'result': [{'id': 'user:bob', 'name': 'Bob'}]}],
        # Depth 2 results - includes same user
        [
          {'result': [{'id': 'user:bob', 'name': 'Bob'}, {'id': 'user:charlie', 'name': 'Charlie'}]}
        ],
      ]
    )

    neighbors = await get_neighbors(
      'user:alice', 'follows', depth=2, direction='out', client=mock_db_client
    )

    # Should only include unique neighbors (bob once, charlie once)
    assert len(neighbors) == 2

  @pytest.mark.anyio
  async def test_get_neighbors_excludes_start_record(self, mock_db_client):
    """Test get_neighbors excludes the starting record from results."""
    mock_db_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {'id': 'user:alice', 'name': 'Alice'},  # Self - should be excluded
            {'id': 'user:bob', 'name': 'Bob'},
          ]
        }
      ]
    )

    neighbors = await get_neighbors(
      'user:alice', 'knows', depth=1, direction='both', client=mock_db_client
    )

    assert len(neighbors) == 1
    assert neighbors[0]['name'] == 'Bob'

  @pytest.mark.anyio
  async def test_get_neighbors_empty_result(self, mock_db_client):
    """Test get_neighbors returns empty list when no neighbors."""
    mock_db_client.execute = AsyncMock(return_value=[{'result': []}])

    neighbors = await get_neighbors(
      'user:alice', 'follows', depth=1, direction='out', client=mock_db_client
    )

    assert neighbors == []

  @pytest.mark.anyio
  async def test_get_neighbors_with_record_id(self, mock_db_client):
    """Test get_neighbors with RecordID instance."""
    mock_db_client.execute = AsyncMock(
      return_value=[{'result': [{'id': 'user:bob', 'name': 'Bob'}]}]
    )

    record_id = RecordID(table='user', id='alice')
    neighbors = await get_neighbors(
      record_id, 'follows', depth=1, direction='out', client=mock_db_client
    )

    assert len(neighbors) == 1


class TestComputeDegree:
  """Test suite for compute_degree helper function."""

  @pytest.mark.anyio
  async def test_compute_degree_returns_all_counts(self, mock_db_client):
    """Test compute_degree returns in_degree, out_degree, and total."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        # Out degree query
        [{'result': [{'count': 5}]}],
        # In degree query
        [{'result': [{'count': 3}]}],
      ]
    )

    degree = await compute_degree('user:alice', 'follows', client=mock_db_client)

    assert degree['out_degree'] == 5
    assert degree['in_degree'] == 3
    assert degree['total'] == 8

  @pytest.mark.anyio
  async def test_compute_degree_zero_connections(self, mock_db_client):
    """Test compute_degree with zero connections."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        # Out degree query - empty
        [{'result': []}],
        # In degree query - empty
        [{'result': []}],
      ]
    )

    degree = await compute_degree('user:isolated', 'follows', client=mock_db_client)

    assert degree['in_degree'] == 0
    assert degree['out_degree'] == 0
    assert degree['total'] == 0

  @pytest.mark.anyio
  async def test_compute_degree_only_outgoing(self, mock_db_client):
    """Test compute_degree with only outgoing connections."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        # Out degree query
        [{'result': [{'count': 10}]}],
        # In degree query - empty
        [{'result': []}],
      ]
    )

    degree = await compute_degree('user:alice', 'follows', client=mock_db_client)

    assert degree['out_degree'] == 10
    assert degree['in_degree'] == 0
    assert degree['total'] == 10

  @pytest.mark.anyio
  async def test_compute_degree_only_incoming(self, mock_db_client):
    """Test compute_degree with only incoming connections."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        # Out degree query - empty
        [{'result': []}],
        # In degree query
        [{'result': [{'count': 7}]}],
      ]
    )

    degree = await compute_degree('user:alice', 'follows', client=mock_db_client)

    assert degree['out_degree'] == 0
    assert degree['in_degree'] == 7
    assert degree['total'] == 7

  @pytest.mark.anyio
  async def test_compute_degree_with_record_id(self, mock_db_client):
    """Test compute_degree with RecordID instance."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        [{'result': [{'count': 2}]}],
        [{'result': [{'count': 3}]}],
      ]
    )

    record_id = RecordID(table='user', id='alice')
    degree = await compute_degree(record_id, 'follows', client=mock_db_client)

    assert degree['total'] == 5

  @pytest.mark.anyio
  async def test_compute_degree_uses_context_client(self, mock_db_client):
    """Test compute_degree uses context client when not provided."""
    mock_db_client.execute = AsyncMock(
      side_effect=[
        [{'result': [{'count': 1}]}],
        [{'result': [{'count': 1}]}],
      ]
    )

    with patch('surql.query.graph.get_db', return_value=mock_db_client):
      degree = await compute_degree('user:alice', 'follows')

    assert degree['total'] == 2
    assert mock_db_client.execute.call_count == 2
