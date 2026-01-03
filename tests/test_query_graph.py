"""Tests for the query graph module."""

import pytest
from pydantic import BaseModel, ValidationError
from unittest.mock import AsyncMock, patch

from reverie.query.graph import (
    count_related,
    get_incoming_edges,
    get_outgoing_edges,
    get_related_records,
    relate,
    shortest_path,
    traverse,
    traverse_with_depth,
    unrelate,
)
from reverie.types.record_id import RecordID


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
            return_value=[{
                'result': [
                    {'title': 'Post 1', 'content': 'Content 1'},
                    {'title': 'Post 2', 'content': 'Content 2'},
                ]
            }]
        )
        
        posts = await traverse('user:alice', '->likes->post', Post, client=mock_db_client)
        
        assert len(posts) == 2
        assert all(isinstance(p, Post) for p in posts)
        assert posts[0].title == 'Post 1'

    @pytest.mark.anyio
    async def test_traverse_with_record_id(self, mock_db_client):
        """Test traversal with RecordID."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [{'name': 'Bob', 'email': 'bob@example.com'}]
            }]
        )
        
        start_id = RecordID(table='user', id='alice')
        users = await traverse(start_id, '<-follows<-user', User, client=mock_db_client)
        
        assert len(users) == 1
        assert users[0].name == 'Bob'

    @pytest.mark.anyio
    async def test_traverse_inbound(self, mock_db_client):
        """Test inbound graph traversal."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'name': 'Alice', 'email': 'alice@example.com'},
                    {'name': 'Bob', 'email': 'bob@example.com'},
                ]
            }]
        )
        
        followers = await traverse('user:charlie', '<-follows<-user', User, client=mock_db_client)
        
        assert len(followers) == 2

    @pytest.mark.anyio
    async def test_traverse_multi_hop(self, mock_db_client):
        """Test multi-hop traversal (friends of friends)."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'name': 'Charlie', 'email': 'charlie@example.com'},
                ]
            }]
        )
        
        fof = await traverse(
            'user:alice',
            '<-follows<-user<-follows<-user',
            User,
            client=mock_db_client
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
        
        with patch('reverie.query.executor.get_db', return_value=mock_db_client):
            posts = await traverse('user:alice', '->likes->post', Post)
        
        assert len(posts) == 1


class TestTraverseWithDepth:
    """Test suite for traverse_with_depth function."""

    @pytest.mark.anyio
    async def test_traverse_with_depth_out(self, mock_db_client):
        """Test outbound traversal with depth."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'title': 'Post 1', 'content': 'Content 1'},
                ]
            }]
        )
        
        posts = await traverse_with_depth(
            'user:alice',
            'likes',
            'post',
            direction='out',
            depth=1,
            model=Post,
            client=mock_db_client
        )
        
        assert len(posts) == 1
        assert isinstance(posts[0], Post)

    @pytest.mark.anyio
    async def test_traverse_with_depth_in(self, mock_db_client):
        """Test inbound traversal with depth."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'name': 'Alice', 'email': 'alice@example.com'},
                ]
            }]
        )
        
        users = await traverse_with_depth(
            'post:123',
            'likes',
            'user',
            direction='in',
            depth=1,
            model=User,
            client=mock_db_client
        )
        
        assert len(users) == 1
        assert users[0].name == 'Alice'

    @pytest.mark.anyio
    async def test_traverse_with_depth_both(self, mock_db_client):
        """Test bidirectional traversal."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'name': 'Alice', 'email': 'alice@example.com'},
                    {'name': 'Bob', 'email': 'bob@example.com'},
                ]
            }]
        )
        
        users = await traverse_with_depth(
            'user:charlie',
            'follows',
            'user',
            direction='both',
            depth=1,
            model=User,
            client=mock_db_client
        )
        
        assert len(users) == 2

    @pytest.mark.anyio
    async def test_traverse_with_depth_unlimited(self, mock_db_client):
        """Test traversal with unlimited depth."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'name': 'Alice', 'email': 'alice@example.com'},
                ]
            }]
        )
        
        users = await traverse_with_depth(
            'user:alice',
            'follows',
            'user',
            direction='both',
            depth=None,
            model=User,
            client=mock_db_client
        )
        
        assert len(users) == 1

    @pytest.mark.anyio
    async def test_traverse_with_depth_no_model(self, mock_db_client):
        """Test traversal without model returns raw dicts."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'name': 'Alice', 'email': 'alice@example.com'},
                ]
            }]
        )
        
        results = await traverse_with_depth(
            'user:alice',
            'follows',
            'user',
            direction='out',
            depth=1,
            model=None,
            client=mock_db_client
        )
        
        assert len(results) == 1
        assert isinstance(results[0], dict)
        assert results[0]['name'] == 'Alice'

    @pytest.mark.anyio
    async def test_traverse_with_depth_invalid_direction(self, mock_db_client):
        """Test traverse_with_depth with invalid direction."""
        with pytest.raises(ValueError) as exc_info:
            await traverse_with_depth(
                'user:alice',
                'follows',
                'user',
                direction='invalid',
                client=mock_db_client
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
            start_id,
            'likes',
            'post',
            direction='out',
            depth=1,
            model=Post,
            client=mock_db_client
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
            return_value=[{
                'result': {
                    'id': 'follows:123',
                    'in': 'user:alice',
                    'out': 'user:bob',
                    'since': '2024-01-01',
                    'weight': 1
                }
            }]
        )
        
        edge = await relate(
            'follows',
            'user:alice',
            'user:bob',
            data={'since': '2024-01-01', 'weight': 1},
            client=mock_db_client
        )
        
        assert edge['since'] == '2024-01-01'
        assert edge['weight'] == 1

    @pytest.mark.anyio
    async def test_relate_with_record_ids(self, mock_db_client):
        """Test relate with RecordID instances."""
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': {'id': 'likes:123'}}]
        )
        
        from_id = RecordID(table='user', id='alice')
        to_id = RecordID(table='post', id=456)
        
        edge = await relate('likes', from_id, to_id, client=mock_db_client)
        
        assert 'id' in edge

    @pytest.mark.anyio
    async def test_relate_with_context(self, mock_db_client):
        """Test relate using context client."""
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': {'id': 'likes:123'}}]
        )
        
        with patch('reverie.query.graph.get_db', return_value=mock_db_client):
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
        
        with patch('reverie.query.graph.get_db', return_value=mock_db_client):
            await unrelate('likes', 'user:alice', 'post:456')
        
        mock_db_client.execute.assert_called_once()


class TestGetOutgoingEdges:
    """Test suite for get_outgoing_edges function."""

    @pytest.mark.anyio
    async def test_get_outgoing_edges_basic(self, mock_db_client):
        """Test getting outgoing edges."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'id': 'likes:1', 'in': 'user:alice', 'out': 'post:123'},
                    {'id': 'likes:2', 'in': 'user:alice', 'out': 'post:456'},
                ]
            }]
        )
        
        edges = await get_outgoing_edges('user:alice', 'likes', client=mock_db_client)
        
        assert len(edges) == 2
        assert all(isinstance(e, dict) for e in edges)

    @pytest.mark.anyio
    async def test_get_outgoing_edges_with_model(self, mock_db_client):
        """Test getting outgoing edges with model."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'id': 'likes:1', 'weight': 5},
                ]
            }]
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
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': [{'id': 'likes:1'}]}]
        )
        
        record_id = RecordID(table='user', id='alice')
        edges = await get_outgoing_edges(record_id, 'likes', client=mock_db_client)
        
        assert len(edges) == 1


class TestGetIncomingEdges:
    """Test suite for get_incoming_edges function."""

    @pytest.mark.anyio
    async def test_get_incoming_edges_basic(self, mock_db_client):
        """Test getting incoming edges."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'id': 'follows:1', 'in': 'user:alice', 'out': 'user:bob'},
                    {'id': 'follows:2', 'in': 'user:alice', 'out': 'user:charlie'},
                ]
            }]
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
            return_value=[{
                'result': [
                    {'title': 'Post 1', 'content': 'Content 1'},
                ]
            }]
        )
        
        posts = await get_related_records(
            'user:alice',
            'likes',
            'post',
            direction='out',
            model=Post,
            client=mock_db_client
        )
        
        assert len(posts) == 1
        assert isinstance(posts[0], Post)

    @pytest.mark.anyio
    async def test_get_related_records_in(self, mock_db_client):
        """Test getting related records in inbound direction."""
        mock_db_client.execute = AsyncMock(
            return_value=[{
                'result': [
                    {'name': 'Alice', 'email': 'alice@example.com'},
                    {'name': 'Bob', 'email': 'bob@example.com'},
                ]
            }]
        )
        
        users = await get_related_records(
            'post:123',
            'likes',
            'user',
            direction='in',
            model=User,
            client=mock_db_client
        )
        
        assert len(users) == 2

    @pytest.mark.anyio
    async def test_get_related_records_no_model(self, mock_db_client):
        """Test getting related records without model."""
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': [{'title': 'Post 1', 'content': 'Content 1'}]}]
        )
        
        posts = await get_related_records(
            'user:alice',
            'likes',
            'post',
            direction='out',
            model=None,
            client=mock_db_client
        )
        
        assert len(posts) == 1
        assert isinstance(posts[0], dict)

    @pytest.mark.anyio
    async def test_get_related_records_invalid_direction(self, mock_db_client):
        """Test get_related_records with invalid direction."""
        with pytest.raises(ValueError) as exc_info:
            await get_related_records(
                'user:alice',
                'likes',
                'post',
                direction='invalid',
                client=mock_db_client
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
            record_id,
            'likes',
            'post',
            direction='out',
            model=Post,
            client=mock_db_client
        )
        
        assert len(posts) == 1

    @pytest.mark.anyio
    async def test_get_related_records_flat_result(self, mock_db_client):
        """Test get_related_records with flat result format."""
        mock_db_client.execute = AsyncMock(
            return_value=[{'title': 'Post 1', 'content': 'Content 1'}]
        )
        
        posts = await get_related_records(
            'user:alice',
            'likes',
            'post',
            direction='out',
            model=None,
            client=mock_db_client
        )
        
        assert len(posts) == 1

    @pytest.mark.anyio
    async def test_get_related_records_empty_result(self, mock_db_client):
        """Test get_related_records with empty result."""
        mock_db_client.execute = AsyncMock(return_value=[])
        
        posts = await get_related_records(
            'user:alice',
            'likes',
            'post',
            direction='out',
            model=None,
            client=mock_db_client
        )
        
        assert posts == []


class TestCountRelated:
    """Test suite for count_related function."""

    @pytest.mark.anyio
    async def test_count_related_out(self, mock_db_client):
        """Test counting outbound related records."""
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': [{'count': 42}]}]
        )
        
        count = await count_related('user:alice', 'likes', direction='out', client=mock_db_client)
        
        assert count == 42

    @pytest.mark.anyio
    async def test_count_related_in(self, mock_db_client):
        """Test counting inbound related records."""
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': [{'count': 15}]}]
        )
        
        count = await count_related('user:alice', 'follows', direction='in', client=mock_db_client)
        
        assert count == 15

    @pytest.mark.anyio
    async def test_count_related_zero(self, mock_db_client):
        """Test counting with zero results."""
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': [{'count': 0}]}]
        )
        
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
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': [{'count': 10}]}]
        )
        
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
        mock_db_client.execute = AsyncMock(
            return_value=[{'result': [{'count': 5}]}]
        )
        
        with patch('reverie.query.graph.get_db', return_value=mock_db_client):
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
            'user:alice',
            'user:charlie',
            'follows',
            max_depth=5,
            client=mock_db_client
        )
        
        assert len(path) == 3

    @pytest.mark.anyio
    async def test_shortest_path_not_found(self, mock_db_client):
        """Test shortest path when no path exists."""
        mock_db_client.execute = AsyncMock(return_value=None)
        
        path = await shortest_path(
            'user:alice',
            'user:isolated',
            'follows',
            max_depth=3,
            client=mock_db_client
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
        
        path = await shortest_path(
            from_id,
            to_id,
            'follows',
            max_depth=5,
            client=mock_db_client
        )
        
        assert len(path) >= 1

    @pytest.mark.anyio
    async def test_shortest_path_max_depth(self, mock_db_client):
        """Test shortest path respects max_depth."""
        mock_db_client.execute = AsyncMock(return_value=None)
        
        path = await shortest_path(
            'user:alice',
            'user:charlie',
            'follows',
            max_depth=1,
            client=mock_db_client
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
        
        with patch('reverie.query.graph.get_db', return_value=mock_db_client):
            path = await shortest_path('user:alice', 'user:bob', 'follows')
        
        assert len(path) >= 1

    @pytest.mark.anyio
    async def test_shortest_path_iterative_search(self, mock_db_client):
        """Test shortest path iterative depth search."""
        # First call returns None (depth 1), second returns result (depth 2)
        mock_db_client.execute = AsyncMock(side_effect=[
            None,  # Depth 1: not found
            [{'name': 'Alice'}, {'name': 'Bob'}],  # Depth 2: found
        ])
        
        path = await shortest_path(
            'user:alice',
            'user:bob',
            'follows',
            max_depth=5,
            client=mock_db_client
        )
        
        assert len(path) == 2
        # Verify it tried depth 1, then depth 2 and stopped
        assert mock_db_client.execute.call_count == 2
