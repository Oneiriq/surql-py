"""Schema versioning and snapshot management.

This module provides comprehensive version tracking and snapshot functionality
for database schema evolution, enabling version history and safe rollbacks.
"""

import hashlib
import json
from collections import deque
from datetime import UTC, datetime
from typing import Any

import structlog
from pydantic import BaseModel, ConfigDict, Field

from surql.connection.client import DatabaseClient
from surql.migration.models import Migration

logger = structlog.get_logger(__name__)


class SchemaSnapshot(BaseModel):
  """Point-in-time snapshot of database schema.

  Captures complete schema state including tables, edges, indexes, and fields
  for version comparison and rollback operations.

  Examples:
    >>> snapshot = SchemaSnapshot(
    ...   version='20260109_120000',
    ...   created_at=datetime.now(UTC),
    ...   tables={'user': user_table},
    ...   edges={'likes': likes_edge},
    ... )
  """

  version: str = Field(..., description='Migration version')
  created_at: datetime = Field(..., description='Snapshot creation time')
  tables: dict[str, dict[str, Any]] = Field(
    default_factory=dict,
    description='Table definitions at this version',
  )
  edges: dict[str, dict[str, Any]] = Field(
    default_factory=dict,
    description='Edge definitions at this version',
  )
  indexes: dict[str, list[dict[str, Any]]] = Field(
    default_factory=dict,
    description='Indexes per table',
  )
  checksum: str = Field(..., description='Schema content checksum')
  migration_count: int = Field(default=0, description='Total migrations at this point')

  model_config = ConfigDict(frozen=True)


class VersionNode(BaseModel):
  """Node in the version graph representing a schema version.

  Examples:
    >>> node = VersionNode(
    ...   version='20260109_120000',
    ...   parent='20260108_120000',
    ...   migration=migration,
    ... )
  """

  version: str = Field(..., description='Version identifier')
  parent: str | None = Field(None, description='Parent version')
  migration: Migration = Field(..., description='Migration at this version')
  snapshot: SchemaSnapshot | None = Field(None, description='Schema snapshot')
  children: list[str] = Field(default_factory=list, description='Child versions')

  model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class VersionGraph:
  """Graph of schema versions showing evolution history.

  Tracks the complete migration history as a directed graph, enabling
  path-finding for rollbacks and version comparison.

  Examples:
    >>> graph = VersionGraph()
    >>> graph.add_version(migration1)
    >>> graph.add_version(migration2, parent='20260108_120000')
  """

  def __init__(self) -> None:
    """Initialize empty version graph."""
    self._nodes: dict[str, VersionNode] = {}
    self._root: str | None = None

  def add_version(
    self,
    migration: Migration,
    parent: str | None = None,
    snapshot: SchemaSnapshot | None = None,
  ) -> None:
    """Add version to graph.

    Args:
      migration: Migration for this version
      parent: Parent version identifier
      snapshot: Optional schema snapshot
    """
    version = migration.version

    node = VersionNode(
      version=version,
      parent=parent,
      migration=migration,
      snapshot=snapshot,
    )

    self._nodes[version] = node

    # Update parent's children list
    if parent and parent in self._nodes:
      parent_node = self._nodes[parent]
      parent_node = parent_node.model_copy(update={'children': [*parent_node.children, version]})
      self._nodes[parent] = parent_node

    # Set root if no parent
    if not parent and self._root is None:
      self._root = version

    logger.info('version_added_to_graph', version=version, parent=parent)

  def get_node(self, version: str) -> VersionNode | None:
    """Get version node by version identifier.

    Args:
      version: Version identifier

    Returns:
      VersionNode or None if not found
    """
    return self._nodes.get(version)

  def get_path(self, from_version: str, to_version: str) -> list[str] | None:
    """Get path between two versions using BFS.

    Args:
      from_version: Starting version
      to_version: Target version

    Returns:
      List of versions forming path, or None if no path exists

    Examples:
      >>> path = graph.get_path('20260109_120000', '20260108_120000')
      >>> print(path)
      ['20260109_120000', '20260108_120000']
    """
    queue: deque[tuple[str, list[str]]] = deque([(from_version, [from_version])])
    visited: set[str] = {from_version}

    while queue:
      current, path = queue.popleft()

      if current == to_version:
        return path

      node = self._nodes.get(current)
      if not node:
        continue

      # Check children
      for child in node.children:
        if child not in visited:
          visited.add(child)
          queue.append((child, path + [child]))

      # Check parent
      if node.parent and node.parent not in visited:
        visited.add(node.parent)
        queue.append((node.parent, path + [node.parent]))

    return None

  def get_ancestors(self, version: str) -> list[str]:
    """Get all ancestor versions from root to version.

    Args:
      version: Version to get ancestors for

    Returns:
      List of ancestor versions in order from root to parent

    Examples:
      >>> ancestors = graph.get_ancestors('20260109_120000')
      >>> print(ancestors)
      ['20260107_120000', '20260108_120000']
    """
    ancestors: list[str] = []
    current = version

    while current:
      node = self._nodes.get(current)
      if not node or not node.parent:
        break
      ancestors.insert(0, node.parent)
      current = node.parent

    return ancestors

  def get_descendants(self, version: str) -> list[str]:
    """Get all descendant versions from version forward.

    Args:
      version: Version to get descendants for

    Returns:
      List of descendant versions

    Examples:
      >>> descendants = graph.get_descendants('20260108_120000')
      >>> print(descendants)
      ['20260109_120000', '20260110_120000']
    """
    descendants: list[str] = []
    node = self._nodes.get(version)

    if not node:
      return descendants

    # BFS to collect all descendants
    queue: deque[str] = deque(node.children)
    visited: set[str] = set(node.children)

    while queue:
      current = queue.popleft()
      descendants.append(current)

      child_node = self._nodes.get(current)
      if child_node:
        for child in child_node.children:
          if child not in visited:
            visited.add(child)
            queue.append(child)

    return descendants

  def get_all_versions(self) -> list[str]:
    """Get all versions in the graph.

    Returns:
      List of all version identifiers
    """
    return list(self._nodes.keys())


async def create_snapshot(
  client: DatabaseClient,
  version: str,
  migration_count: int,
) -> SchemaSnapshot:
  """Create schema snapshot at current database state.

  Captures complete schema information including tables, edges, and indexes
  by querying the database INFO.

  Args:
    client: Database client
    version: Version identifier
    migration_count: Number of migrations applied

  Returns:
    Schema snapshot

  Examples:
    >>> snapshot = await create_snapshot(client, '20260109_120000', 5)
  """
  logger.info('creating_schema_snapshot', version=version)

  try:
    # Fetch schema information from database
    info_result = await client.execute('INFO FOR DB')

    # Parse schema data
    tables_data = _parse_tables_from_info(info_result)
    edges_data = _parse_edges_from_info(info_result)
    indexes_data = _parse_indexes_from_info(info_result)

    # Calculate checksum
    schema_str = json.dumps(
      {
        'tables': tables_data,
        'edges': edges_data,
        'indexes': indexes_data,
      },
      sort_keys=True,
    )

    checksum = hashlib.sha256(schema_str.encode()).hexdigest()

    snapshot = SchemaSnapshot(
      version=version,
      created_at=datetime.now(UTC),
      tables=tables_data,
      edges=edges_data,
      indexes=indexes_data,
      checksum=checksum,
      migration_count=migration_count,
    )

    logger.info('schema_snapshot_created', version=version, checksum=checksum)
    return snapshot

  except Exception as e:
    logger.error('failed_to_create_snapshot', version=version, error=str(e))
    raise


async def store_snapshot(
  client: DatabaseClient,
  snapshot: SchemaSnapshot,
) -> None:
  """Store schema snapshot in database.

  Args:
    client: Database client
    snapshot: Snapshot to store

  Raises:
    Exception: If storage fails

  Examples:
    >>> await store_snapshot(client, snapshot)
  """
  logger.info('storing_snapshot', version=snapshot.version)

  try:
    # Ensure snapshot table exists
    await _ensure_snapshot_table(client)

    # Store snapshot
    data = {
      'version': snapshot.version,
      'created_at': snapshot.created_at.isoformat(),
      'tables': snapshot.tables,
      'edges': snapshot.edges,
      'indexes': snapshot.indexes,
      'checksum': snapshot.checksum,
      'migration_count': snapshot.migration_count,
    }

    await client.create('_schema_snapshot', data)

    logger.info('snapshot_stored', version=snapshot.version)

  except Exception as e:
    logger.error('failed_to_store_snapshot', version=snapshot.version, error=str(e))
    raise


async def load_snapshot(
  client: DatabaseClient,
  version: str,
) -> SchemaSnapshot | None:
  """Load schema snapshot from database.

  Args:
    client: Database client
    version: Version identifier

  Returns:
    Schema snapshot or None if not found

  Examples:
    >>> snapshot = await load_snapshot(client, '20260109_120000')
  """
  logger.info('loading_snapshot', version=version)

  try:
    query = 'SELECT * FROM _schema_snapshot WHERE version = $version'
    result = await client.execute(query, {'version': version})

    records = _extract_records(result)

    if not records:
      logger.warning('snapshot_not_found', version=version)
      return None

    record = records[0]

    snapshot = SchemaSnapshot(
      version=record['version'],
      created_at=_parse_datetime(record['created_at']),
      tables=record.get('tables', {}),
      edges=record.get('edges', {}),
      indexes=record.get('indexes', {}),
      checksum=record['checksum'],
      migration_count=record.get('migration_count', 0),
    )

    logger.info('snapshot_loaded', version=version)
    return snapshot

  except Exception as e:
    logger.error('failed_to_load_snapshot', version=version, error=str(e))
    return None


async def list_snapshots(client: DatabaseClient) -> list[SchemaSnapshot]:
  """List all stored snapshots.

  Args:
    client: Database client

  Returns:
    List of schema snapshots ordered by creation time

  Examples:
    >>> snapshots = await list_snapshots(client)
    >>> for snapshot in snapshots:
    ...   print(snapshot.version, snapshot.created_at)
  """
  logger.info('listing_snapshots')

  try:
    query = 'SELECT * FROM _schema_snapshot ORDER BY created_at ASC'
    result = await client.execute(query)

    records = _extract_records(result)

    snapshots: list[SchemaSnapshot] = []
    for record in records:
      try:
        snapshot = SchemaSnapshot(
          version=record['version'],
          created_at=_parse_datetime(record['created_at']),
          tables=record.get('tables', {}),
          edges=record.get('edges', {}),
          indexes=record.get('indexes', {}),
          checksum=record['checksum'],
          migration_count=record.get('migration_count', 0),
        )
        snapshots.append(snapshot)
      except Exception as e:
        logger.warning('skipping_invalid_snapshot', record=record, error=str(e))

    logger.info('snapshots_listed', count=len(snapshots))
    return snapshots

  except Exception as e:
    logger.error('failed_to_list_snapshots', error=str(e))
    return []


def compare_snapshots(
  snapshot1: SchemaSnapshot,
  snapshot2: SchemaSnapshot,
) -> dict[str, Any]:
  """Compare two schema snapshots.

  Args:
    snapshot1: First snapshot
    snapshot2: Second snapshot

  Returns:
    Dictionary describing differences

  Examples:
    >>> diff = compare_snapshots(old_snapshot, new_snapshot)
    >>> print(diff['tables_added'])
    ['new_table']
  """
  differences: dict[str, Any] = {
    'tables_added': [],
    'tables_removed': [],
    'tables_modified': [],
    'edges_added': [],
    'edges_removed': [],
    'indexes_added': [],
    'indexes_removed': [],
    'checksum_match': snapshot1.checksum == snapshot2.checksum,
  }

  # Compare tables
  tables1 = set(snapshot1.tables.keys())
  tables2 = set(snapshot2.tables.keys())

  differences['tables_added'] = list(tables2 - tables1)
  differences['tables_removed'] = list(tables1 - tables2)

  # Check for modified tables
  for table in tables1 & tables2:
    if snapshot1.tables[table] != snapshot2.tables[table]:
      differences['tables_modified'].append(table)

  # Compare edges
  edges1 = set(snapshot1.edges.keys())
  edges2 = set(snapshot2.edges.keys())

  differences['edges_added'] = list(edges2 - edges1)
  differences['edges_removed'] = list(edges1 - edges2)

  # Compare indexes
  indexes1 = set(snapshot1.indexes.keys())
  indexes2 = set(snapshot2.indexes.keys())

  differences['indexes_added'] = list(indexes2 - indexes1)
  differences['indexes_removed'] = list(indexes1 - indexes2)

  return differences


async def _ensure_snapshot_table(client: DatabaseClient) -> None:
  """Ensure snapshot storage table exists.

  Args:
    client: Database client
  """
  try:
    # Try to query the table
    await client.execute('SELECT * FROM _schema_snapshot LIMIT 1')
  except (RuntimeError, ConnectionError):
    # Create table
    statements = [
      'DEFINE TABLE _schema_snapshot SCHEMAFULL;',
      'DEFINE FIELD version ON TABLE _schema_snapshot TYPE string;',
      'DEFINE FIELD created_at ON TABLE _schema_snapshot TYPE datetime;',
      'DEFINE FIELD tables ON TABLE _schema_snapshot TYPE object;',
      'DEFINE FIELD edges ON TABLE _schema_snapshot TYPE object;',
      'DEFINE FIELD indexes ON TABLE _schema_snapshot TYPE object;',
      'DEFINE FIELD checksum ON TABLE _schema_snapshot TYPE string;',
      'DEFINE FIELD migration_count ON TABLE _schema_snapshot TYPE int;',
      'DEFINE INDEX version_idx ON TABLE _schema_snapshot COLUMNS version UNIQUE;',
    ]

    for statement in statements:
      await client.execute(statement)


def _parse_tables_from_info(info: Any) -> dict[str, dict[str, Any]]:
  """Parse table definitions from INFO response.

  Args:
    info: INFO FOR DB result

  Returns:
    Dictionary of table definitions
  """
  tables: dict[str, dict[str, Any]] = {}

  # SurrealDB INFO returns a complex structure
  # This is a simplified parser - actual implementation would need
  # to handle the full INFO response structure
  if isinstance(info, list) and len(info) > 0:
    result = info[0] if isinstance(info[0], dict) else {}
    if 'result' in result:
      info_data = result['result']
      if isinstance(info_data, dict) and 'tb' in info_data:
        # Extract table information
        for table_name, table_info in info_data.get('tb', {}).items():
          tables[table_name] = {'definition': str(table_info)}

  return tables


def _parse_edges_from_info(info: Any) -> dict[str, dict[str, Any]]:
  """Parse edge definitions from INFO response.

  Args:
    info: INFO FOR DB result

  Returns:
    Dictionary of edge definitions
  """
  edges: dict[str, dict[str, Any]] = {}

  # Edge tables are typically identified by their relation fields
  # This is simplified - actual implementation would parse edge tables
  if isinstance(info, list) and len(info) > 0:
    result = info[0] if isinstance(info[0], dict) else {}
    if 'result' in result:
      info_data = result['result']
      if isinstance(info_data, dict) and 'tb' in info_data:
        # Look for edge-like tables
        for table_name, table_info in info_data.get('tb', {}).items():
          # Simple heuristic: tables with 'in' and 'out' fields are edges
          table_str = str(table_info)
          if 'in' in table_str and 'out' in table_str:
            edges[table_name] = {'definition': table_str}

  return edges


def _parse_indexes_from_info(info: Any) -> dict[str, list[dict[str, Any]]]:
  """Parse index definitions from INFO response.

  Args:
    info: INFO FOR DB result

  Returns:
    Dictionary of indexes per table
  """
  indexes: dict[str, list[dict[str, Any]]] = {}

  # Parse index information from INFO result
  if isinstance(info, list) and len(info) > 0:
    result = info[0] if isinstance(info[0], dict) else {}
    if 'result' in result:
      info_data = result['result']
      if isinstance(info_data, dict):
        # Indexes might be under tb -> table_name -> ix
        for table_name, table_info in info_data.get('tb', {}).items():
          if isinstance(table_info, dict) and 'ix' in table_info:
            table_indexes = []
            for idx_name, idx_info in table_info['ix'].items():
              table_indexes.append(
                {
                  'name': idx_name,
                  'definition': str(idx_info),
                }
              )
            if table_indexes:
              indexes[table_name] = table_indexes

  return indexes


def _extract_records(result: Any) -> list[dict[str, Any]]:
  """Extract records from SurrealDB query result.

  Args:
    result: Raw query result

  Returns:
    List of record dictionaries
  """
  if isinstance(result, list):
    if len(result) > 0 and isinstance(result[0], dict):
      if 'result' in result[0]:
        return result[0]['result'] or []
      return result
    return result
  elif isinstance(result, dict):
    if 'result' in result:
      return result['result'] or []
    return [result]

  return []


def _parse_datetime(value: Any) -> datetime:
  """Parse datetime from various formats.

  Args:
    value: Datetime value (string, datetime, or other)

  Returns:
    datetime object
  """
  if isinstance(value, datetime):
    return value
  elif isinstance(value, str):
    try:
      return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, TypeError):
      return datetime.now(UTC)
  else:
    return datetime.now(UTC)
