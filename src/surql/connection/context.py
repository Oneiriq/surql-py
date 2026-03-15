"""Connection context management for scoped database access."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar

import structlog

from surql.connection.client import DatabaseClient, get_client
from surql.connection.config import ConnectionConfig

logger = structlog.get_logger(__name__)

_current_client: ContextVar[DatabaseClient | None] = ContextVar(
  'current_client',
  default=None,
)


class ContextError(Exception):
  """Raised when context operations fail."""

  pass


def get_db() -> DatabaseClient:
  """Get the current database client from context.

  Returns:
    Active database client

  Raises:
    ContextError: If no active database connection exists in context

  Example:
    ```python
    async with connection_scope(config):
      client = get_db()
      await client.execute('SELECT * FROM user')
    ```
  """
  client = _current_client.get()
  if client is None:
    raise ContextError('No active database connection. Use connection_scope() or set_db() first.')
  return client


def set_db(client: DatabaseClient) -> None:
  """Set the database client in the current context.

  Args:
    client: Database client to set in context

  Example:
    ```python
    client = DatabaseClient(config)
    await client.connect()
    set_db(client)
    ```
  """
  _current_client.set(client)
  logger.debug('database_client_set_in_context')


def clear_db() -> None:
  """Clear the database client from the current context.

  Example:
    ```python
    clear_db()
    ```
  """
  _current_client.set(None)
  logger.debug('database_client_cleared_from_context')


def has_db() -> bool:
  """Check if a database client exists in the current context.

  Returns:
    True if a client is set, False otherwise

  Example:
    ```python
    if has_db():
      client = get_db()
    ```
  """
  return _current_client.get() is not None


@asynccontextmanager
async def connection_scope(config: ConnectionConfig) -> AsyncIterator[DatabaseClient]:
  """Create a scoped connection context with automatic lifecycle management.

  This context manager:
  - Creates and connects a database client
  - Sets it in the context for the scope duration
  - Automatically disconnects and clears context on exit

  Args:
    config: Connection configuration

  Yields:
    Connected database client

  Example:
    ```python
    async with connection_scope(config) as client:
      # Client is automatically available via get_db()
      await client.execute('SELECT * FROM user')

      # Or use get_db() in nested functions
      await some_function()  # Can call get_db() inside
    ```
  """
  async with get_client(config) as client:
    set_db(client)
    try:
      logger.info('connection_scope_started')
      yield client
    finally:
      clear_db()
      logger.info('connection_scope_closed')


@asynccontextmanager
async def connection_override(client: DatabaseClient) -> AsyncIterator[DatabaseClient]:
  """Temporarily override the current database client in context.

  Useful for testing or when you need to use a different client temporarily.

  Args:
    client: Database client to use temporarily

  Yields:
    The provided database client

  Example:
    ```python
    async with connection_override(test_client):
      # All get_db() calls will return test_client
      await some_function()
    # Original client (if any) is restored
    ```
  """
  previous_client = _current_client.get()
  set_db(client)
  try:
    logger.debug('connection_override_started')
    yield client
  finally:
    _current_client.set(previous_client)
    logger.debug('connection_override_restored')
