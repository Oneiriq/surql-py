"""Tests for migration versioning and snapshot functionality."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from surql.migration.models import Migration
from surql.migration.versioning import (
  SchemaSnapshot,
  VersionGraph,
  VersionNode,
  compare_snapshots,
  create_snapshot,
  list_snapshots,
  load_snapshot,
  store_snapshot,
)


@pytest.fixture
def sample_migration() -> Migration:
  """Create a sample migration for testing."""
  return Migration(
    version='20260109_120000',
    description='Test migration',
    path=Path('migrations/20260109_120000_test.py'),
    up=lambda: ['CREATE TABLE test;'],
    down=lambda: ['DROP TABLE test;'],
    checksum='test_checksum',
  )


@pytest.fixture
def sample_snapshot() -> SchemaSnapshot:
  """Create a sample schema snapshot."""
  return SchemaSnapshot(
    version='20260109_120000',
    created_at=datetime.now(UTC),
    tables={'user': {'definition': 'DEFINE TABLE user;'}},
    edges={'follows': {'definition': 'DEFINE TABLE follows;'}},
    indexes={'user': [{'name': 'email_idx', 'definition': 'DEFINE INDEX email_idx;'}]},
    checksum='abc123def456',
    migration_count=5,
  )


class TestSchemaSnapshot:
  """Tests for SchemaSnapshot model."""

  def test_snapshot_creation(self, sample_snapshot: SchemaSnapshot) -> None:
    """Test creating a schema snapshot."""
    assert sample_snapshot.version == '20260109_120000'
    assert sample_snapshot.migration_count == 5
    assert 'user' in sample_snapshot.tables
    assert sample_snapshot.checksum == 'abc123def456'

  def test_snapshot_immutable(self, sample_snapshot: SchemaSnapshot) -> None:
    """Test that snapshot is immutable."""
    with pytest.raises(ValidationError):
      sample_snapshot.version = 'new_version'


class TestVersionNode:
  """Tests for VersionNode model."""

  def test_version_node_creation(self, sample_migration: Migration) -> None:
    """Test creating a version node."""
    node = VersionNode(
      version='20260109_120000',
      parent='20260108_120000',
      migration=sample_migration,
    )

    assert node.version == '20260109_120000'
    assert node.parent == '20260108_120000'
    assert node.migration == sample_migration
    assert len(node.children) == 0


class TestVersionGraph:
  """Tests for VersionGraph."""

  def test_empty_graph(self) -> None:
    """Test creating an empty version graph."""
    graph = VersionGraph()
    assert len(graph.get_all_versions()) == 0

  def test_add_version(self, sample_migration: Migration) -> None:
    """Test adding a version to the graph."""
    graph = VersionGraph()
    graph.add_version(sample_migration, parent=None)

    assert '20260109_120000' in graph.get_all_versions()
    node = graph.get_node('20260109_120000')
    assert node is not None
    assert node.version == '20260109_120000'

  def test_add_version_with_parent(self) -> None:
    """Test adding version with parent relationship."""
    graph = VersionGraph()

    migration1 = Migration(
      version='20260108_120000',
      description='First migration',
      path=Path('migrations/20260108_120000_first.py'),
      up=lambda: ['SQL1'],
      down=lambda: ['SQL1_DOWN'],
    )

    migration2 = Migration(
      version='20260109_120000',
      description='Second migration',
      path=Path('migrations/20260109_120000_second.py'),
      up=lambda: ['SQL2'],
      down=lambda: ['SQL2_DOWN'],
    )

    graph.add_version(migration1, parent=None)
    graph.add_version(migration2, parent='20260108_120000')

    node1 = graph.get_node('20260108_120000')
    node2 = graph.get_node('20260109_120000')

    assert node2 is not None
    assert node2.parent == '20260108_120000'
    assert node1 is not None
    assert '20260109_120000' in node1.children

  def test_get_ancestors(self) -> None:
    """Test getting ancestor versions."""
    graph = VersionGraph()

    for i in range(1, 4):
      migration = Migration(
        version=f'2026010{i}_120000',
        description=f'Migration {i}',
        path=Path(f'migrations/2026010{i}_120000_test.py'),
        up=lambda: ['SQL'],
        down=lambda: ['SQL_DOWN'],
      )
      parent = f'2026010{i - 1}_120000' if i > 1 else None
      graph.add_version(migration, parent=parent)

    ancestors = graph.get_ancestors('20260103_120000')
    assert ancestors == ['20260101_120000', '20260102_120000']

  def test_get_descendants(self) -> None:
    """Test getting descendant versions."""
    graph = VersionGraph()

    for i in range(1, 4):
      migration = Migration(
        version=f'2026010{i}_120000',
        description=f'Migration {i}',
        path=Path(f'migrations/2026010{i}_120000_test.py'),
        up=lambda: ['SQL'],
        down=lambda: ['SQL_DOWN'],
      )
      parent = f'2026010{i - 1}_120000' if i > 1 else None
      graph.add_version(migration, parent=parent)

    descendants = graph.get_descendants('20260101_120000')
    assert '20260102_120000' in descendants
    assert '20260103_120000' in descendants

  def test_get_path(self) -> None:
    """Test finding path between versions."""
    graph = VersionGraph()

    for i in range(1, 4):
      migration = Migration(
        version=f'2026010{i}_120000',
        description=f'Migration {i}',
        path=Path(f'migrations/2026010{i}_120000_test.py'),
        up=lambda: ['SQL'],
        down=lambda: ['SQL_DOWN'],
      )
      parent = f'2026010{i - 1}_120000' if i > 1 else None
      graph.add_version(migration, parent=parent)

    path = graph.get_path('20260101_120000', '20260103_120000')
    assert path is not None
    assert path[0] == '20260101_120000'
    assert path[-1] == '20260103_120000'


class TestCreateSnapshot:
  """Tests for create_snapshot function."""

  @pytest.mark.anyio
  async def test_create_snapshot(self) -> None:
    """Test creating a schema snapshot."""
    mock_client = AsyncMock()
    mock_client.execute = AsyncMock(
      return_value=[
        {
          'result': {
            'tb': {
              'user': {'fields': {}},
              'post': {'fields': {}},
            }
          }
        }
      ]
    )

    snapshot = await create_snapshot(mock_client, '20260109_120000', 5)

    assert snapshot.version == '20260109_120000'
    assert snapshot.migration_count == 5
    assert snapshot.checksum is not None
    mock_client.execute.assert_called_once_with('INFO FOR DB')


class TestStoreAndLoadSnapshot:
  """Tests for store_snapshot and load_snapshot functions."""

  @pytest.mark.anyio
  async def test_store_snapshot(self, sample_snapshot: SchemaSnapshot) -> None:
    """Test storing a snapshot."""
    mock_client = AsyncMock()
    mock_client.execute = AsyncMock()
    mock_client.create = AsyncMock()

    await store_snapshot(mock_client, sample_snapshot)

    mock_client.create.assert_called_once()
    args = mock_client.create.call_args
    assert args[0][0] == '_schema_snapshot'
    assert args[0][1]['version'] == sample_snapshot.version

  @pytest.mark.anyio
  async def test_load_snapshot_found(self, sample_snapshot: SchemaSnapshot) -> None:
    """Test loading a snapshot that exists."""
    mock_client = AsyncMock()
    mock_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'version': sample_snapshot.version,
              'created_at': sample_snapshot.created_at.isoformat(),
              'tables': sample_snapshot.tables,
              'edges': sample_snapshot.edges,
              'indexes': sample_snapshot.indexes,
              'checksum': sample_snapshot.checksum,
              'migration_count': sample_snapshot.migration_count,
            }
          ]
        }
      ]
    )

    loaded = await load_snapshot(mock_client, sample_snapshot.version)

    assert loaded is not None
    assert loaded.version == sample_snapshot.version
    assert loaded.checksum == sample_snapshot.checksum

  @pytest.mark.anyio
  async def test_load_snapshot_not_found(self) -> None:
    """Test loading a snapshot that doesn't exist."""
    mock_client = AsyncMock()
    mock_client.execute = AsyncMock(return_value=[{'result': []}])

    loaded = await load_snapshot(mock_client, '20260109_120000')

    assert loaded is None


class TestListSnapshots:
  """Tests for list_snapshots function."""

  @pytest.mark.anyio
  async def test_list_snapshots_empty(self) -> None:
    """Test listing snapshots when none exist."""
    mock_client = AsyncMock()
    mock_client.execute = AsyncMock(return_value=[{'result': []}])

    snapshots = await list_snapshots(mock_client)

    assert len(snapshots) == 0

  @pytest.mark.anyio
  async def test_list_snapshots_multiple(self) -> None:
    """Test listing multiple snapshots."""
    mock_client = AsyncMock()
    mock_client.execute = AsyncMock(
      return_value=[
        {
          'result': [
            {
              'version': '20260108_120000',
              'created_at': datetime.now(UTC).isoformat(),
              'tables': {},
              'edges': {},
              'indexes': {},
              'checksum': 'abc123',
              'migration_count': 4,
            },
            {
              'version': '20260109_120000',
              'created_at': datetime.now(UTC).isoformat(),
              'tables': {},
              'edges': {},
              'indexes': {},
              'checksum': 'def456',
              'migration_count': 5,
            },
          ]
        }
      ]
    )

    snapshots = await list_snapshots(mock_client)

    assert len(snapshots) == 2
    assert snapshots[0].version == '20260108_120000'
    assert snapshots[1].version == '20260109_120000'


class TestCompareSnapshots:
  """Tests for compare_snapshots function."""

  def test_compare_identical_snapshots(self, sample_snapshot: SchemaSnapshot) -> None:
    """Test comparing identical snapshots."""
    diff = compare_snapshots(sample_snapshot, sample_snapshot)

    assert diff['checksum_match'] is True
    assert len(diff['tables_added']) == 0
    assert len(diff['tables_removed']) == 0

  def test_compare_different_snapshots(self) -> None:
    """Test comparing different snapshots."""
    snapshot1 = SchemaSnapshot(
      version='20260108_120000',
      created_at=datetime.now(UTC),
      tables={'user': {'def': 'table'}},
      edges={},
      indexes={},
      checksum='abc123',
      migration_count=4,
    )

    snapshot2 = SchemaSnapshot(
      version='20260109_120000',
      created_at=datetime.now(UTC),
      tables={'user': {'def': 'table'}, 'post': {'def': 'table'}},
      edges={'follows': {'def': 'edge'}},
      indexes={},
      checksum='def456',
      migration_count=5,
    )

    diff = compare_snapshots(snapshot1, snapshot2)

    assert diff['checksum_match'] is False
    assert 'post' in diff['tables_added']
    assert 'follows' in diff['edges_added']
    assert len(diff['tables_removed']) == 0

  def test_compare_removed_tables(self) -> None:
    """Test detecting removed tables."""
    snapshot1 = SchemaSnapshot(
      version='20260108_120000',
      created_at=datetime.now(UTC),
      tables={'user': {'def': 'table'}, 'post': {'def': 'table'}},
      edges={},
      indexes={},
      checksum='abc123',
      migration_count=4,
    )

    snapshot2 = SchemaSnapshot(
      version='20260109_120000',
      created_at=datetime.now(UTC),
      tables={'user': {'def': 'table'}},
      edges={},
      indexes={},
      checksum='def456',
      migration_count=5,
    )

    diff = compare_snapshots(snapshot1, snapshot2)

    assert 'post' in diff['tables_removed']
    assert len(diff['tables_added']) == 0
