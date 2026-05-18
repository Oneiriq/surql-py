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

Mid-batch error detection (1.6.0)
=================================

SurrealDB v3 collapses the SDK ``query`` method's return value to
``None`` for batched ``BEGIN TRANSACTION; ...; COMMIT TRANSACTION;``
requests regardless of whether the batch succeeded or any statement
inside it rolled back the transaction. Prior to 1.6.0, the commit
helper read that ``None`` as a success — silently swallowing
mid-batch failures (type mismatches, assertion failures, FK violations,
etc.). To restore observability without dropping the single-RPC
strategy, ``commit`` now:

1. Injects a sentinel ``RETURN '__txn_ok__';`` statement immediately
   before ``COMMIT TRANSACTION`` so the server emits a recognisable
   marker in the response envelope on success.
2. Sends the batched statement via the SDK's ``query_raw`` method
   instead of ``query``. ``query_raw`` preserves the per-statement
   ``{status, result}`` envelope; ``query`` collapses it.
3. Inspects the returned envelope: if any statement has
   ``status == 'ERR'`` OR the sentinel value is absent from the result
   set, the batch is considered failed, the state moves to
   ``CANCELLED`` (not ``COMMITTED``), and a ``TransactionError`` is
   raised with a message pointing at the per-statement error and
   server-log inspection guidance.

The sentinel and ``query_raw`` behaviours were verified against
SurrealDB v3.0.5 (see ``CHANGES`` 1.6.0 ``### Verified``).
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from enum import Enum
from typing import Any

import structlog

from surql.connection.client import DatabaseClient, _denormalize_params

logger = structlog.get_logger(__name__)

_active_transaction: ContextVar[bool] = ContextVar('_active_transaction', default=False)

# Sentinel emitted via ``RETURN '<SENTINEL>'`` immediately before the
# ``COMMIT TRANSACTION`` line. On success SurrealDB v3 surfaces it as
# ``{'result': '__txn_ok__', 'status': 'OK'}`` in the per-statement
# envelope; on rollback no statement returns this value (the COMMIT
# entry's status is ``'ERR'`` with ``details.kind == 'Cancelled'``).
_TRANSACTION_SENTINEL: str = '__txn_ok__'


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
    ``BEGIN TRANSACTION; ...; <sentinel>; COMMIT TRANSACTION;`` request.

    The sentinel (``RETURN '__txn_ok__';``) is injected immediately
    before the ``COMMIT TRANSACTION`` line so the response envelope
    carries a recognisable success marker. On rollback the server
    returns per-statement ``status == 'ERR'`` entries and the sentinel
    is absent, allowing this method to raise instead of silently
    swallowing mid-batch failures (the pre-1.6.0 regression — see
    module docstring).

    Returns:
      The list of user-statement results extracted from the batched
      response envelope (the ``BEGIN TRANSACTION``, sentinel, and
      ``COMMIT TRANSACTION`` entries are stripped), or ``None`` when
      no statements were queued.

    Raises:
      TransactionError: If transaction is not active, the commit RPC
        raised, or the batch was rolled back server-side.
    """
    if self._state != TransactionState.ACTIVE:
      raise TransactionError(f'Cannot commit transaction in {self._state.value} state')

    if not self._statements:
      # Nothing to flush; treat as a successful no-op commit.
      self._log.info('committing_empty_transaction')
      self._state = TransactionState.COMMITTED
      _active_transaction.set(False)
      return None

    queued_count = len(self._statements)
    batched = 'BEGIN TRANSACTION;\n'
    for stmt in self._statements:
      batched += stmt.rstrip(';') + ';\n'
    # Sentinel must precede COMMIT — it is the marker we look for in
    # the response envelope to confirm success. See module docstring.
    batched += f"RETURN '{_TRANSACTION_SENTINEL}';\n"
    batched += 'COMMIT TRANSACTION;'

    try:
      self._log.info('committing_transaction', statement_count=queued_count)
      envelope, envelope_inspectable = await self._raw_query(batched, self._params or None)
    except Exception as e:
      self._log.error('transaction_commit_failed', error=str(e))
      self._state = TransactionState.CANCELLED
      _active_transaction.set(False)
      raise TransactionError(f'Failed to commit transaction: {e}') from e

    if not envelope_inspectable:
      # Fallback path: SDK lacks ``query_raw`` so sentinel detection
      # is impossible. Preserve the pre-1.6.0 silent-success behaviour
      # rather than break the commit on SDK versions we cannot probe.
      # The module docstring + CHANGES 1.6.0 ``### Verified`` warn
      # operators that this mode is observationally weaker.
      self._state = TransactionState.COMMITTED
      _active_transaction.set(False)
      self._log.warning(
        'transaction_committed_without_sentinel_inspection',
        statement_count=queued_count,
        reason='SDK ``query_raw`` unavailable; mid-batch failures cannot be detected',
      )
      return envelope

    statements = _extract_envelope_statements(envelope)
    error_messages = _collect_envelope_errors(statements)
    sentinel_present = any(_is_sentinel_entry(entry) for entry in statements)

    if error_messages or not sentinel_present:
      self._state = TransactionState.CANCELLED
      _active_transaction.set(False)
      detail = '; '.join(error_messages) if error_messages else 'sentinel marker absent'
      self._log.error(
        'transaction_rolled_back',
        error_count=len(error_messages),
        sentinel_present=sentinel_present,
        first_error=error_messages[0] if error_messages else None,
      )
      raise TransactionError(
        'Transaction failed — SurrealDB rolled back the batch; one or more '
        'statements rejected. SurrealDB v3 batched transactions do not surface '
        "per-statement errors via the SDK's ``query`` method, so the server "
        'envelope was inspected directly. '
        f'Detail: {detail}. Check server logs for the full statement trace.'
      )

    self._state = TransactionState.COMMITTED
    _active_transaction.set(False)
    self._log.info('transaction_committed', statement_count=queued_count)
    # Strip the BEGIN (always first), sentinel, and COMMIT (always last)
    # framing entries before returning, so callers see only their own
    # statements' results.
    return _strip_framing(statements)

  async def _raw_query(self, batched: str, params: dict[str, Any] | None) -> tuple[Any, bool]:
    """Send the batched statement via the SDK and return the raw envelope.

    The SDK's ``query`` method collapses batched-transaction responses
    to ``None`` regardless of success or failure (verified against
    SurrealDB v3.0.5 + ``surrealdb==2.0.0a1``), so we route through
    ``query_raw``, which preserves the per-statement
    ``{status, result}`` envelope required for sentinel inspection.
    Falls back to the public ``DatabaseClient.execute`` path when the
    underlying SDK does not expose ``query_raw`` — preserves the prior
    silent-swallow behaviour on unsupported SDK versions rather than
    breaking the commit path.

    Params are routed through :func:`_denormalize_params` before being
    handed to the SDK so ``surql.types.record_id.RecordID`` values
    (surql-py's Pydantic wrapper) are converted to ``surrealdb.RecordID``
    (the SDK's native CBOR-encodable class) — symmetry with
    :meth:`DatabaseClient.execute`, which already normalises its own
    params. Without this conversion the SDK's CBOR encoder raises
    ``no encoder for type <class 'surql.types.record_id.RecordID'>``
    when callers queue bound-param RecordIDs via ``txn.execute``.
    The fallback ``execute`` branch double-normalises (``execute``
    runs ``_denormalize_params`` again internally) — that is a no-op
    because ``_denormalize_params`` is idempotent: an already-converted
    :class:`surrealdb.RecordID` is neither a ``SurqlRecordID``, nor a
    string matching the record-id regex, nor a dict/list, so it falls
    through to the final ``return value``.

    Args:
      batched: The full ``BEGIN ... <stmts> ... RETURN '<sentinel>'; COMMIT;``
        string.
      params: Optional flat ``vars`` dict shared across all queued
        statements.

    Returns:
      ``(envelope, inspectable)`` tuple. ``inspectable=True`` means
      the envelope came from ``query_raw`` and carries the per-statement
      ``{status, result}`` rows the sentinel logic needs.
      ``inspectable=False`` means we used the legacy ``execute`` path;
      callers must skip sentinel inspection in that case (the SDK has
      already collapsed the envelope and there is nothing to probe).
    """
    resolved_params = _denormalize_params(params) if params else None
    sdk_client = getattr(self._client, '_client', None)
    query_raw = getattr(sdk_client, 'query_raw', None) if sdk_client is not None else None
    if query_raw is None:
      self._log.debug('query_raw_unavailable_falling_back_to_execute')
      return await self._client.execute(batched, resolved_params), False
    return await query_raw(batched, resolved_params or {}), True

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


# ---------------------------------------------------------------------------
# Envelope inspection helpers (Item 2 — 1.6.0)
# ---------------------------------------------------------------------------


def _extract_envelope_statements(envelope: Any) -> list[Any]:
  """Normalise the SDK ``query_raw`` envelope into a flat statement list.

  SurrealDB v3 returns the result envelope in one of two shapes:

  - ``{'id': <req-id>, 'result': [<per-stmt>, ...]}`` — the standard
    WebSocket-RPC shape returned by ``surrealdb.AsyncSurreal.query_raw``.
  - ``[<per-stmt>, ...]`` — the HTTP ``/sql`` shape (used by some
    mocks and older SDK paths).

  Returns an empty list when the envelope is anything else (e.g.
  ``None`` from the SDK-fallback path on an unsupported SDK version).
  Empty list disables sentinel detection and matches the pre-1.6.0
  behaviour for that edge case.
  """
  if isinstance(envelope, dict) and isinstance(envelope.get('result'), list):
    return list(envelope['result'])
  if isinstance(envelope, list):
    return list(envelope)
  return []


def _collect_envelope_errors(statements: list[Any]) -> list[str]:
  """Return human-readable error messages for any ``status == 'ERR'`` entries.

  Each error entry is expected to be a dict with a ``'result'`` field
  carrying the server-supplied error string (e.g.
  ``"Couldn't coerce value for field `age` of `bar:2`..."``). The
  ``COMMIT TRANSACTION`` entry's ``Cancelled`` status is intentionally
  surfaced here as well — it tells the caller the rollback fired.
  """
  errors: list[str] = []
  for entry in statements:
    if not isinstance(entry, dict):
      continue
    if entry.get('status') != 'ERR':
      continue
    message = entry.get('result')
    if isinstance(message, str) and message:
      errors.append(message)
    else:
      errors.append(repr(entry))
  return errors


def _is_sentinel_entry(entry: Any) -> bool:
  """Check whether a per-statement envelope entry carries the sentinel value."""
  if not isinstance(entry, dict):
    return False
  if entry.get('status') != 'OK':
    return False
  return entry.get('result') == _TRANSACTION_SENTINEL


def _strip_framing(statements: list[Any]) -> list[Any]:
  """Strip the BEGIN, sentinel, and COMMIT framing entries from the envelope.

  The leading entry is always the ``BEGIN TRANSACTION`` ack, the
  trailing entry is the ``COMMIT TRANSACTION`` ack, and the
  second-to-last entry is the sentinel ``RETURN '__txn_ok__'`` result.
  Callers should see only the results of their own queued statements.
  Defensively handles short envelopes (mocks may return a shorter list).
  """
  filtered = [entry for entry in statements if not _is_sentinel_entry(entry)]
  # Drop a leading no-op (BEGIN) entry: ``{'result': None, 'status': 'OK'}``
  # or any None-result OK row at index 0. Same for the trailing COMMIT ack.
  if filtered and _is_framing_ack(filtered[0]):
    filtered = filtered[1:]
  if filtered and _is_framing_ack(filtered[-1]):
    filtered = filtered[:-1]
  return filtered


def _is_framing_ack(entry: Any) -> bool:
  """An ack row is ``{'status': 'OK', 'result': None}``."""
  return isinstance(entry, dict) and entry.get('status') == 'OK' and entry.get('result') is None
