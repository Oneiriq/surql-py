"""Transaction support for database operations.

Implementation decision (bug #13, option b)
==========================================

SurrealDB v3 treats each ``query()`` RPC as a standalone request, so
sending ``BEGIN TRANSACTION`` / ``COMMIT TRANSACTION`` /
``CANCEL TRANSACTION`` as three separate ``execute()`` calls fails on
v3 servers with "no transaction is currently open" when the bare
``COMMIT`` lands. The ``surrealdb`` Python SDK at the pinned version
(``2.0.0a1``) does not yet expose an interactive ``begin()`` /
``commit(txn_id)`` API (option a in the original audit), so this class
buffers queued statements client-side and flushes them as a single
atomic
``BEGIN TRANSACTION; <stmts>; COMMIT TRANSACTION;`` request when
[`Transaction.commit`] is invoked. [`Transaction.cancel`] simply drops
the buffered statements without contacting the server. This mirrors
the rs port's approach (see ``surql-rs/src/connection/transaction.rs``)
and keeps cross-port semantics identical.

Consequences of option (b):

- Results from individual in-transaction statements are not available
  until ``commit`` returns. ``Transaction.execute`` therefore returns
  ``None`` at call time; the aggregate response can be obtained via
  ``commit``'s return value if callers need it.
- Parameter names across queued statements share one flat namespace
  (the SDK exposes a single ``vars`` dict per request). Callers must
  choose unique parameter keys across their queued statements inside
  the same transaction block.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from enum import Enum
from typing import Any

import structlog

from surql.connection.client import DatabaseClient

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

  Buffers queued statements client-side and flushes them atomically on
  ``commit``. See module docstring for why the single-flush strategy
  is required on SurrealDB v3.
  """

  def __init__(self, client: DatabaseClient) -> None:
    """Initialize transaction with database client.

    Args:
      client: Connected database client
    """
    self._client = client
    self._state = TransactionState.PENDING
    self._statements: list[str] = []
    self._params: dict[str, Any] = {}
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

    Does not contact the server; simply flips the state machine so
    subsequent [`execute`][surql.connection.transaction.Transaction.execute]
    calls can queue statements.

    Raises:
      TransactionError: If transaction is already active, nested, or cannot be started
    """
    if self._state != TransactionState.PENDING:
      raise TransactionError(f'Cannot begin transaction in {self._state.value} state')

    if _active_transaction.get():
      raise TransactionError('Nested transactions are not supported by SurrealDB')

    self._log.info('beginning_transaction')
    self._state = TransactionState.ACTIVE
    _active_transaction.set(True)
    self._log.info('transaction_started')

  async def commit(self) -> Any:
    """Commit the transaction.

    Flushes buffered statements as a single
    ``BEGIN TRANSACTION; ...; COMMIT TRANSACTION;`` request.

    Returns:
      The aggregate result of the batched query, or ``None`` when no
      statements were queued.

    Raises:
      TransactionError: If transaction is not active or commit fails
    """
    if self._state != TransactionState.ACTIVE:
      raise TransactionError(f'Cannot commit transaction in {self._state.value} state')

    if not self._statements:
      # Nothing to flush; treat as a successful no-op commit.
      self._log.info('committing_empty_transaction')
      self._state = TransactionState.COMMITTED
      _active_transaction.set(False)
      return None

    batched = 'BEGIN TRANSACTION;\n'
    for stmt in self._statements:
      batched += stmt.rstrip(';') + ';\n'
    batched += 'COMMIT TRANSACTION;'

    try:
      self._log.info('committing_transaction', statement_count=len(self._statements))
      result = await self._client.execute(batched, self._params or None)
      self._state = TransactionState.COMMITTED
      _active_transaction.set(False)
      self._log.info('transaction_committed')
      return result
    except Exception as e:
      self._log.error('transaction_commit_failed', error=str(e))
      self._state = TransactionState.CANCELLED
      _active_transaction.set(False)
      raise TransactionError(f'Failed to commit transaction: {e}') from e

  async def cancel(self) -> None:
    """Cancel/rollback the transaction.

    Buffered statements are discarded client-side; no server request
    is made (nothing was sent yet).

    Raises:
      TransactionError: If transaction cannot be cancelled
    """
    if self._state not in (TransactionState.ACTIVE, TransactionState.PENDING):
      self._log.warning(
        'transaction_already_finalized',
        state=self._state.value,
      )
      return

    self._log.info('cancelling_transaction')
    self._statements.clear()
    self._params.clear()
    self._state = TransactionState.CANCELLED
    _active_transaction.set(False)
    self._log.info('transaction_cancelled')

  async def execute(self, query: str, params: dict[str, Any] | None = None) -> None:
    """Queue a statement for execution inside the transaction.

    The statement is **not** executed until
    [`commit`][surql.connection.transaction.Transaction.commit] flushes
    the buffered batch. Returns ``None`` at call time; the aggregate
    response is available from ``commit``'s return value.

    Args:
      query: SurrealQL query string
      params: Optional query parameters. These are merged into a
        shared namespace across all queued statements in this
        transaction; duplicate keys are rejected.

    Raises:
      TransactionError: If transaction is not active, or if ``params``
        overwrite an already-queued parameter key.
    """
    if not self.is_active:
      raise TransactionError(f'Cannot execute query in {self._state.value} state')

    if params:
      overlap = set(params) & set(self._params)
      if overlap:
        raise TransactionError(
          f'Duplicate transaction parameter names: {sorted(overlap)}. '
          'All queued statements share one `vars` namespace; rename to disambiguate.'
        )
      self._params.update(params)

    self._statements.append(query)
    self._log.debug(
      'queued_query_in_transaction',
      query=query,
      queued_count=len(self._statements),
    )
    return None

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
