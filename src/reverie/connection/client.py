"""Async SurrealDB client wrapper with connection pooling and retry logic."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import structlog
from surrealdb import AsyncSurreal
from tenacity import (
  AsyncRetrying,
  RetryError,
  retry_if_exception_type,
  stop_after_attempt,
  wait_exponential,
)

from reverie.connection.config import ConnectionConfig

logger = structlog.get_logger(__name__)


class DatabaseError(Exception):
  """Base exception for database operations."""

  pass


class ConnectionError(DatabaseError):
  """Raised when connection to database fails."""

  pass


class QueryError(DatabaseError):
  """Raised when query execution fails."""

  pass


class DatabaseClient:
  """Async wrapper around SurrealDB client with connection pooling and retry logic."""

  def __init__(self, config: ConnectionConfig) -> None:
    """Initialize database client with configuration.

    Args:
      config: Connection configuration
    """
    self._config = config
    self._client: Any = None
    self._connected = False
    self._semaphore = asyncio.Semaphore(config.max_connections)
    self._log = logger.bind(
      namespace=config.namespace,
      database=config.database,
      url=config.url,
    )

  @property
  def is_connected(self) -> bool:
    """Check if client is currently connected."""
    return self._connected and self._client is not None

  async def connect(self) -> None:
    """Establish connection to the database with retry logic.

    Raises:
      ConnectionError: If connection fails after all retries
    """
    if self._connected:
      self._log.warning('client_already_connected')
      return

    try:
      async for attempt in AsyncRetrying(
        retry=retry_if_exception_type((Exception,)),
        stop=stop_after_attempt(self._config.retry_max_attempts),
        wait=wait_exponential(
          multiplier=self._config.retry_multiplier,
          min=self._config.retry_min_wait,
          max=self._config.retry_max_wait,
        ),
        reraise=True,
      ):
        with attempt:
          self._log.info('connecting_to_database', attempt=attempt.retry_state.attempt_number)

          self._client = AsyncSurreal(self._config.url)
          await self._client.connect()

          if self._config.username and self._config.password:
            await self._client.signin(
              {
                'username': self._config.username,
                'password': self._config.password,
              }
            )
            self._log.debug('authenticated_successfully')

          await self._client.use(self._config.namespace, self._config.database)
          self._connected = True

          self._log.info('connected_to_database')

    except RetryError as e:
      self._log.error('connection_failed_after_retries', error=str(e))
      raise ConnectionError(
        f'Failed to connect after {self._config.retry_max_attempts} attempts'
      ) from e
    except Exception as e:
      self._log.error('unexpected_connection_error', error=str(e), error_type=type(e).__name__)
      raise ConnectionError(f'Connection failed: {e}') from e

  async def disconnect(self) -> None:
    """Close the database connection.

    Raises:
      DatabaseError: If disconnect operation fails
    """
    if not self._connected:
      self._log.debug('client_not_connected')
      return

    try:
      if self._client:
        await self._client.close()
        self._log.info('disconnected_from_database')
    except Exception as e:
      self._log.error('disconnect_failed', error=str(e))
      raise DatabaseError(f'Failed to disconnect: {e}') from e
    finally:
      self._client = None
      self._connected = False

  async def execute(
    self,
    query: str,
    params: dict[str, Any] | None = None,
  ) -> Any:
    """Execute raw SurrealQL query with retry logic.

    Args:
      query: SurrealQL query string
      params: Optional query parameters

    Returns:
      Query results

    Raises:
      ConnectionError: If client is not connected
      QueryError: If query execution fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_query', query=query, params=params)

        result = await self._client.query(query, params or {})

        self._log.debug('query_executed_successfully', result_type=type(result).__name__)
        return result

      except Exception as e:
        self._log.error(
          'query_execution_failed',
          error=str(e),
          error_type=type(e).__name__,
          query=query,
        )
        raise QueryError(f'Query execution failed: {e}') from e

  async def select(self, target: str) -> Any:
    """Execute SELECT operation.

    Args:
      target: Target table or record ID

    Returns:
      Selected records

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_select', target=target)
        result = await self._client.select(target)
        return result
      except Exception as e:
        self._log.error('select_failed', error=str(e), target=target)
        raise QueryError(f'SELECT operation failed: {e}') from e

  async def create(self, table: str, data: dict[str, Any]) -> Any:
    """Execute CREATE operation.

    Args:
      table: Target table name
      data: Record data

    Returns:
      Created record

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_create', table=table, data=data)
        result = await self._client.create(table, data)
        return result
      except Exception as e:
        self._log.error('create_failed', error=str(e), table=table)
        raise QueryError(f'CREATE operation failed: {e}') from e

  async def update(self, target: str, data: dict[str, Any]) -> Any:
    """Execute UPDATE operation.

    Args:
      target: Target table or record ID
      data: Update data

    Returns:
      Updated record

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_update', target=target, data=data)
        result = await self._client.update(target, data)
        return result
      except Exception as e:
        self._log.error('update_failed', error=str(e), target=target)
        raise QueryError(f'UPDATE operation failed: {e}') from e

  async def merge(self, target: str, data: dict[str, Any]) -> Any:
    """Execute MERGE operation.

    Args:
      target: Target table or record ID
      data: Data to merge

    Returns:
      Merged record

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_merge', target=target, data=data)
        result = await self._client.merge(target, data)
        return result
      except Exception as e:
        self._log.error('merge_failed', error=str(e), target=target)
        raise QueryError(f'MERGE operation failed: {e}') from e

  async def delete(self, target: str) -> Any:
    """Execute DELETE operation.

    Args:
      target: Target table or record ID

    Returns:
      Deletion result

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_delete', target=target)
        result = await self._client.delete(target)
        return result
      except Exception as e:
        self._log.error('delete_failed', error=str(e), target=target)
        raise QueryError(f'DELETE operation failed: {e}') from e

  async def insert_relation(self, table: str, data: dict[str, Any]) -> Any:
    """Execute INSERT RELATION operation for edges.

    Args:
      table: Edge table name
      data: Relation data with 'in' and 'out' fields

    Returns:
      Created relation

    Raises:
      ConnectionError: If client is not connected
      QueryError: If operation fails
    """
    if not self.is_connected or self._client is None:
      raise ConnectionError('Client is not connected to database')

    async with self._semaphore:
      try:
        self._log.debug('executing_insert_relation', table=table, data=data)
        result = await self._client.insert_relation(table, data)
        return result
      except Exception as e:
        self._log.error('insert_relation_failed', error=str(e), table=table)
        raise QueryError(f'INSERT RELATION operation failed: {e}') from e

  async def __aenter__(self) -> 'DatabaseClient':
    """Async context manager entry."""
    await self.connect()
    return self

  async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    """Async context manager exit."""
    await self.disconnect()


@asynccontextmanager
async def get_client(config: ConnectionConfig) -> AsyncIterator[DatabaseClient]:
  """Context manager for database client lifecycle.

  Args:
    config: Connection configuration

  Yields:
    Connected database client

  Example:
    ```python
    async with get_client(config) as client:
      results = await client.execute('SELECT * FROM user')
    ```
  """
  client = DatabaseClient(config)
  await client.connect()
  try:
    yield client
  finally:
    await client.disconnect()
