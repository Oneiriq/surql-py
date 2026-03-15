"""Connection registry for managing multiple database connections."""

import asyncio
from typing import Optional

import structlog

from surql.connection.client import DatabaseClient
from surql.connection.config import ConnectionConfig

logger = structlog.get_logger(__name__)


class RegistryError(Exception):
  """Raised when connection registry operations fail."""

  pass


class ConnectionRegistry:
  """Registry for managing multiple named database connections."""

  _instance: Optional['ConnectionRegistry'] = None
  _lock = asyncio.Lock()
  _initialized: bool

  def __new__(cls) -> 'ConnectionRegistry':
    """Singleton pattern - ensure only one registry exists."""
    if cls._instance is None:
      cls._instance = super().__new__(cls)
      cls._instance._initialized = False
    return cls._instance

  def __init__(self) -> None:
    """Initialize the connection registry."""
    if self._initialized:
      return

    self._connections: dict[str, DatabaseClient] = {}
    self._configs: dict[str, ConnectionConfig] = {}
    self._default_name: str | None = None
    self._initialized = True
    logger.info('connection_registry_initialized')

  async def register(
    self,
    name: str,
    config: ConnectionConfig,
    connect: bool = True,
    set_default: bool = False,
  ) -> DatabaseClient:
    """Register a new named connection.

    Args:
      name: Connection name (must be unique)
      config: Connection configuration
      connect: Whether to connect immediately
      set_default: Whether to set as default connection

    Returns:
      Registered database client

    Raises:
      RegistryError: If connection name already exists
    """
    async with self._lock:
      if name in self._connections:
        raise RegistryError(f'Connection "{name}" already registered')

      client = DatabaseClient(config)

      if connect:
        await client.connect()

      self._connections[name] = client
      self._configs[name] = config

      if set_default or self._default_name is None:
        self._default_name = name

      logger.info(
        'connection_registered',
        name=name,
        is_default=name == self._default_name,
        connected=client.is_connected,
      )

      return client

  async def unregister(self, name: str, disconnect: bool = True) -> None:
    """Unregister a named connection.

    Args:
      name: Connection name to unregister
      disconnect: Whether to disconnect before unregistering

    Raises:
      RegistryError: If connection name doesn't exist
    """
    async with self._lock:
      if name not in self._connections:
        raise RegistryError(f'Connection "{name}" not found')

      client = self._connections[name]

      if disconnect and client.is_connected:
        await client.disconnect()

      del self._connections[name]
      del self._configs[name]

      if self._default_name == name:
        self._default_name = next(iter(self._connections.keys())) if self._connections else None

      logger.info('connection_unregistered', name=name)

  def get(self, name: str | None = None) -> DatabaseClient:
    """Get a named connection or the default connection.

    Args:
      name: Connection name (uses default if None)

    Returns:
      Database client

    Raises:
      RegistryError: If connection doesn't exist
    """
    if name is None:
      if self._default_name is None:
        raise RegistryError('No default connection set')
      name = self._default_name

    if name not in self._connections:
      raise RegistryError(f'Connection "{name}" not found')

    return self._connections[name]

  def get_config(self, name: str | None = None) -> ConnectionConfig:
    """Get connection configuration.

    Args:
      name: Connection name (uses default if None)

    Returns:
      Connection configuration

    Raises:
      RegistryError: If connection doesn't exist
    """
    if name is None:
      if self._default_name is None:
        raise RegistryError('No default connection set')
      name = self._default_name

    if name not in self._configs:
      raise RegistryError(f'Connection "{name}" not found')

    return self._configs[name]

  def set_default(self, name: str) -> None:
    """Set the default connection.

    Args:
      name: Connection name to set as default

    Raises:
      RegistryError: If connection doesn't exist
    """
    if name not in self._connections:
      raise RegistryError(f'Connection "{name}" not found')

    self._default_name = name
    logger.info('default_connection_set', name=name)

  def list_connections(self) -> list[str]:
    """List all registered connection names.

    Returns:
      List of connection names
    """
    return list(self._connections.keys())

  @property
  def default_name(self) -> str | None:
    """Get the default connection name."""
    return self._default_name

  async def disconnect_all(self) -> None:
    """Disconnect all registered connections."""
    async with self._lock:
      for name, client in self._connections.items():
        if client.is_connected:
          await client.disconnect()
          logger.info('connection_disconnected', name=name)

  async def clear(self) -> None:
    """Clear all connections from registry."""
    await self.disconnect_all()
    async with self._lock:
      self._connections.clear()
      self._configs.clear()
      self._default_name = None
      logger.info('connection_registry_cleared')


# Global registry instance
_registry = ConnectionRegistry()


def get_registry() -> ConnectionRegistry:
  """Get the global connection registry.

  Returns:
    Connection registry instance
  """
  return _registry
