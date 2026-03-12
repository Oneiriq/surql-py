"""Transaction support for database operations."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from enum import Enum
from typing import Any

import structlog

from reverie.connection.client import DatabaseClient

logger = structlog.get_logger(__name__)

_active_transaction: ContextVar[bool] = ContextVar('_active_transaction', default=False)


class TransactionState(Enum):
  """Transaction state enumeration."""

  PENDING = 'pending'
  ACTIVE = 'active'
  COMMITTED = 'committed'
  CANCELLED = 'cancelled'


class TransactionError(Exception):
  """Raised when transaction operation fails."""

  pass


class Transaction:
  """Transaction context manager for SurrealDB operations.

  Provides ACID transaction support using SurrealDB's BEGIN/COMMIT/CANCEL statements.
  """

  def __init__(self, client: DatabaseClient) -> None:
    """Initialize transaction with database client.

    Args:
      client: Connected database client
    """
    self._client = client
    self._state = TransactionState.PENDING
    self._log = logger.bind(transaction_id=id(self))

  @property
  def state(self) -> TransactionState:
    """Get current transaction state."""
    return self._state

  @property
  def is_active(self) -> bool:
    """Check if transaction is currently active."""
    return self._state == TransactionState.ACTIVE

  async def begin(self) -> None:
    """Begin the transaction.

    Raises:
      TransactionError: If transaction is already active, nested, or cannot be started
    """
    if self._state != TransactionState.PENDING:
      raise TransactionError(f'Cannot begin transaction in {self._state.value} state')

    if _active_transaction.get():
      raise TransactionError('Nested transactions are not supported by SurrealDB')

    try:
      self._log.info('beginning_transaction')
      await self._client.execute('BEGIN TRANSACTION;')
      self._state = TransactionState.ACTIVE
      _active_transaction.set(True)
      self._log.info('transaction_started')
    except Exception as e:
      self._log.error('transaction_begin_failed', error=str(e))
      raise TransactionError(f'Failed to begin transaction: {e}') from e

  async def commit(self) -> None:
    """Commit the transaction.

    Raises:
      TransactionError: If transaction is not active or commit fails
    """
    if self._state != TransactionState.ACTIVE:
      raise TransactionError(f'Cannot commit transaction in {self._state.value} state')

    try:
      self._log.info('committing_transaction')
      await self._client.execute('COMMIT TRANSACTION;')
      self._state = TransactionState.COMMITTED
      _active_transaction.set(False)
      self._log.info('transaction_committed')
    except Exception as e:
      self._log.error('transaction_commit_failed', error=str(e))
      self._state = TransactionState.CANCELLED
      _active_transaction.set(False)
      raise TransactionError(f'Failed to commit transaction: {e}') from e

  async def cancel(self) -> None:
    """Cancel/rollback the transaction.

    Raises:
      TransactionError: If transaction cannot be cancelled
    """
    if self._state not in (TransactionState.ACTIVE, TransactionState.PENDING):
      self._log.warning(
        'transaction_already_finalized',
        state=self._state.value,
      )
      return

    try:
      self._log.info('cancelling_transaction')
      if self._state == TransactionState.ACTIVE:
        await self._client.execute('CANCEL TRANSACTION;')
      self._state = TransactionState.CANCELLED
      _active_transaction.set(False)
      self._log.info('transaction_cancelled')
    except Exception as e:
      self._log.error('transaction_cancel_failed', error=str(e))
      self._state = TransactionState.CANCELLED
      _active_transaction.set(False)
      raise TransactionError(f'Failed to cancel transaction: {e}') from e

  async def execute(self, query: str, params: dict[str, Any] | None = None) -> Any:
    """Execute a query within the transaction context.

    Args:
      query: SurrealQL query string
      params: Optional query parameters

    Returns:
      Query results

    Raises:
      TransactionError: If transaction is not active
      QueryError: If query execution fails
    """
    if not self.is_active:
      raise TransactionError(f'Cannot execute query in {self._state.value} state')

    self._log.debug('executing_query_in_transaction', query=query)
    return await self._client.execute(query, params)

  async def __aenter__(self) -> 'Transaction':
    """Async context manager entry."""
    await self.begin()
    return self

  async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    """Async context manager exit.

    Automatically commits on success or cancels on exception.
    """
    if exc_type is not None:
      self._log.warning(
        'transaction_aborting_due_to_exception',
        exception_type=exc_type.__name__,
        exception=str(exc_val),
      )
      await self.cancel()
    elif self.is_active:
      await self.commit()


@asynccontextmanager
async def transaction(client: DatabaseClient) -> AsyncIterator[Transaction]:
  """Create a transaction context manager.

  Args:
    client: Connected database client

  Yields:
    Active transaction instance

  Example:
    ```python
    async with transaction(client) as txn:
      await txn.execute('CREATE user:alice SET name = "Alice"')
      await txn.execute('CREATE user:bob SET name = "Bob"')
      # Automatically commits on success, cancels on exception
    ```
  """
  txn = Transaction(client)
  await txn.begin()
  try:
    yield txn
    if txn.is_active:
      await txn.commit()
  except BaseException:
    if txn.is_active:
      await txn.cancel()
    raise
