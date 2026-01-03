"""Schema registry for code-defined schemas.

This module provides a global registry to track code-defined table and edge schemas.
Schemas registered here can be compared against the database schema to detect drift.
"""

from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from reverie.schema.table import TableDefinition

if TYPE_CHECKING:
  from reverie.schema.edge import EdgeDefinition

logger = structlog.get_logger(__name__)


class SchemaRegistry:
  """Global registry for code-defined schemas.

  A singleton-pattern registry that stores table and edge definitions
  for comparison with database schemas.

  Examples:
    Register a schema:
    >>> registry = SchemaRegistry()
    >>> registry.register_table(user_schema)

    Get all registered tables:
    >>> tables = registry.get_tables()
  """

  _instance: 'SchemaRegistry | None' = None
  _tables: dict[str, TableDefinition]
  _edges: dict[str, 'EdgeDefinition']
  _schema_files: list[Path]

  def __new__(cls) -> 'SchemaRegistry':
    """Create or return singleton instance."""
    if cls._instance is None:
      cls._instance = super().__new__(cls)
      cls._instance._tables = {}
      cls._instance._edges = {}
      cls._instance._schema_files = []
    return cls._instance

  def register_table(self, table: TableDefinition) -> None:
    """Register a table schema.

    Args:
      table: TableDefinition to register

    Examples:
      >>> registry.register_table(user_schema)
    """
    logger.debug('registering_table', table=table.name)
    self._tables[table.name] = table

  def register_edge(self, edge: 'EdgeDefinition') -> None:
    """Register an edge schema.

    Args:
      edge: EdgeDefinition to register

    Examples:
      >>> registry.register_edge(follows_edge)
    """
    logger.debug('registering_edge', edge=edge.name)
    self._edges[edge.name] = edge

  def get_table(self, name: str) -> TableDefinition | None:
    """Get a registered table by name.

    Args:
      name: Table name

    Returns:
      TableDefinition if found, None otherwise
    """
    return self._tables.get(name)

  def get_edge(self, name: str) -> 'EdgeDefinition | None':
    """Get a registered edge by name.

    Args:
      name: Edge name

    Returns:
      EdgeDefinition if found, None otherwise
    """
    return self._edges.get(name)

  def get_tables(self) -> dict[str, TableDefinition]:
    """Get all registered table schemas.

    Returns:
      Dictionary of table name to TableDefinition
    """
    return dict(self._tables)

  def get_edges(self) -> dict[str, 'EdgeDefinition']:
    """Get all registered edge schemas.

    Returns:
      Dictionary of edge name to EdgeDefinition
    """
    return dict(self._edges)

  def get_table_names(self) -> list[str]:
    """Get names of all registered tables.

    Returns:
      List of table names
    """
    return list(self._tables.keys())

  def get_edge_names(self) -> list[str]:
    """Get names of all registered edges.

    Returns:
      List of edge names
    """
    return list(self._edges.keys())

  def clear(self) -> None:
    """Clear all registered schemas.

    Useful for testing and resetting the registry.
    """
    logger.debug('clearing_registry')
    self._tables.clear()
    self._edges.clear()
    self._schema_files.clear()

  def add_schema_file(self, path: Path) -> None:
    """Track a schema file that has been loaded.

    Args:
      path: Path to the schema file
    """
    if path not in self._schema_files:
      self._schema_files.append(path)

  def get_schema_files(self) -> list[Path]:
    """Get all tracked schema files.

    Returns:
      List of paths to schema files
    """
    return list(self._schema_files)

  @property
  def table_count(self) -> int:
    """Get number of registered tables."""
    return len(self._tables)

  @property
  def edge_count(self) -> int:
    """Get number of registered edges."""
    return len(self._edges)


# Global singleton instance
_registry = SchemaRegistry()


def get_registry() -> SchemaRegistry:
  """Get the global schema registry instance.

  Returns:
    The singleton SchemaRegistry instance

  Examples:
    >>> registry = get_registry()
    >>> registry.register_table(user_schema)
  """
  return _registry


def register_table(table: TableDefinition) -> TableDefinition:
  """Register a table schema and return it.

  This is a convenience function that also returns the table,
  making it suitable for use as a decorator or inline registration.

  Args:
    table: TableDefinition to register

  Returns:
    The same TableDefinition (for chaining)

  Examples:
    >>> user_schema = register_table(table_schema('user', fields=[...]))
  """
  get_registry().register_table(table)
  return table


def register_edge(edge: 'EdgeDefinition') -> 'EdgeDefinition':
  """Register an edge schema and return it.

  This is a convenience function that also returns the edge,
  making it suitable for use as a decorator or inline registration.

  Args:
    edge: EdgeDefinition to register

  Returns:
    The same EdgeDefinition (for chaining)

  Examples:
    >>> follows_edge = register_edge(edge_schema('follows', ...))
  """
  get_registry().register_edge(edge)
  return edge


def clear_registry() -> None:
  """Clear all registered schemas.

  Convenience function to reset the global registry.
  """
  get_registry().clear()


def get_registered_tables() -> dict[str, TableDefinition]:
  """Get all registered table schemas.

  Convenience function to get tables from the global registry.

  Returns:
    Dictionary of table name to TableDefinition
  """
  return get_registry().get_tables()


def get_registered_edges() -> dict[str, 'EdgeDefinition']:
  """Get all registered edge schemas.

  Convenience function to get edges from the global registry.

  Returns:
    Dictionary of edge name to EdgeDefinition
  """
  return get_registry().get_edges()
