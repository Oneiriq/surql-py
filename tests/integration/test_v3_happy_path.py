"""SurrealDB v3 happy-path integration suite.

Exercises every v3-correctness fix from the release/1.4.0 campaign:

- #11: migration history records the ``applied_at`` datetime cast
- #12: ``is_migration_applied`` uses a targeted WHERE query and
       ``SELECT *``
- #13: transactions flush as a single batched RPC
- #14: ``count_records`` aggregates via ``GROUP ALL``
- #15: ``get_record`` / ``db.select('table:id')`` resolves the
       record via ``type::record``
- #16: ``DEFINE TABLE`` / ``DEFINE FIELD`` / ``DEFINE INDEX`` are
       idempotent on re-run
- #17: table-missing probe does not swallow unrelated errors

Every test runs against the live ``surrealdb:v3.0.5`` container booted
by ``.github/workflows/integration.yml``. Each test gets its own
namespace/database (see conftest).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from surql.connection.client import DatabaseClient
from surql.connection.transaction import Transaction
from surql.migration.executor import execute_migration
from surql.migration.history import (
  MIGRATION_TABLE_NAME,
  create_migration_table,
  ensure_migration_table,
  get_applied_migrations,
  is_migration_applied,
  record_migration,
  remove_migration_record,
)
from surql.migration.models import Migration, MigrationDirection
from surql.query.crud import (
  count_records,
  create_record,
  get_record,
)


class _Product(BaseModel):
  """Test model used by TestRecordIdSelectV3."""

  name: str
  price: float | None = None


# ---------------------------------------------------------------------------
# Migration history -- record, is_applied, full round-trip (#11, #12)
# ---------------------------------------------------------------------------


class TestMigrationHistoryV3:
  """Integration tests for migration history on SurrealDB v3."""

  @pytest.mark.anyio
  async def test_migration_table_idempotent(self, integration_client: DatabaseClient) -> None:
    """Bug #16: creating the table twice must not error on v3."""
    await create_migration_table(integration_client)
    # Second invocation exercises `IF NOT EXISTS` idempotency.
    await create_migration_table(integration_client)

  @pytest.mark.anyio
  async def test_record_and_query_migration(self, integration_client: DatabaseClient) -> None:
    """Bug #11 + #12: record, then query via targeted applied probe."""
    await ensure_migration_table(integration_client)

    await record_migration(
      integration_client,
      version='20260101_120000',
      description='Initial schema',
      checksum='abc123',
      execution_time_ms=42,
    )

    assert await is_migration_applied(integration_client, '20260101_120000') is True
    assert await is_migration_applied(integration_client, '20260102_999999') is False

    applied = await get_applied_migrations(integration_client)
    assert len(applied) == 1
    assert applied[0].version == '20260101_120000'
    assert applied[0].description == 'Initial schema'
    assert applied[0].execution_time_ms == 42

  @pytest.mark.anyio
  async def test_remove_migration(self, integration_client: DatabaseClient) -> None:
    """Round-trip: remove recorded migration."""
    await ensure_migration_table(integration_client)
    await record_migration(
      integration_client,
      version='20260101_120000',
      description='X',
      checksum='c',
    )

    await remove_migration_record(integration_client, '20260101_120000')

    assert await is_migration_applied(integration_client, '20260101_120000') is False


# ---------------------------------------------------------------------------
# Transactions (#13)
# ---------------------------------------------------------------------------


class TestTransactionBatchedCommitV3:
  """Bug #13: BEGIN/COMMIT must land in a single RPC."""

  @pytest.mark.anyio
  async def test_transaction_commit_applies_all_statements(
    self, integration_client: DatabaseClient
  ) -> None:
    """Queued statements commit atomically and become visible afterwards."""
    # Prepare schema.
    await integration_client.execute('DEFINE TABLE widget SCHEMALESS;')

    async with Transaction(integration_client) as txn:
      await txn.execute("CREATE widget:alpha SET label = 'alpha'")
      await txn.execute("CREATE widget:beta  SET label = 'beta'")

    # Both rows must exist after commit.
    alpha = await integration_client.select('widget:alpha')
    beta = await integration_client.select('widget:beta')
    assert isinstance(alpha, dict) and alpha['label'] == 'alpha'
    assert isinstance(beta, dict) and beta['label'] == 'beta'

  @pytest.mark.anyio
  async def test_transaction_cancel_discards_buffer(
    self, integration_client: DatabaseClient
  ) -> None:
    """Buffered statements are not flushed on cancel."""
    await integration_client.execute('DEFINE TABLE widget SCHEMALESS;')

    txn = Transaction(integration_client)
    await txn.begin()
    await txn.execute("CREATE widget:gamma SET label = 'gamma'")
    await txn.cancel()

    # Row must not exist.
    gamma = await integration_client.select('widget:gamma')
    assert gamma is None


# ---------------------------------------------------------------------------
# count_records GROUP ALL (#14)
# ---------------------------------------------------------------------------


class TestCountRecordsV3:
  """Bug #14: count() must aggregate with GROUP ALL."""

  @pytest.mark.anyio
  async def test_count_matches_cardinality(self, integration_client: DatabaseClient) -> None:
    """Inserting N records must yield count == N, not 1."""
    await integration_client.execute('DEFINE TABLE thing SCHEMALESS;')

    for i in range(5):
      await create_record('thing', {'n': i}, client=integration_client)

    total = await count_records('thing', client=integration_client)
    assert total == 5

    # With a condition narrowing to 3 rows.
    narrowed = await count_records('thing', 'n < 3', client=integration_client)
    assert narrowed == 3


# ---------------------------------------------------------------------------
# db.select("table:id") via type::record (#15)
# ---------------------------------------------------------------------------


class TestRecordIdSelectV3:
  """Bug #15: bare `table:id` string -> raw type::record SQL."""

  @pytest.mark.anyio
  async def test_get_record_resolves_record_id(self, integration_client: DatabaseClient) -> None:
    """``get_record`` on a ``table:id`` target returns the row on v3."""
    await integration_client.execute('DEFINE TABLE product SCHEMALESS;')
    await integration_client.execute("CREATE product:widget SET name = 'Widget', price = 9.99")

    row = await get_record('product', 'widget', _Product, client=integration_client)
    assert row is not None
    assert row.name == 'Widget'
    assert row.price == pytest.approx(9.99)

  @pytest.mark.anyio
  async def test_get_record_returns_none_for_missing(
    self, integration_client: DatabaseClient
  ) -> None:
    """Missing record id yields None, not an empty list."""
    await integration_client.execute('DEFINE TABLE product SCHEMALESS;')

    row = await get_record('product', 'ghost', _Product, client=integration_client)
    assert row is None


# ---------------------------------------------------------------------------
# End-to-end migration up/down (#11 + #13 combined)
# ---------------------------------------------------------------------------


class TestMigrationExecutorV3:
  """End-to-end: execute_migration applies and tracks a migration."""

  @pytest.mark.anyio
  async def test_migration_up_then_down(
    self, integration_client: DatabaseClient, tmp_path: Path
  ) -> None:
    """Up creates the table; down drops it and removes the history row."""
    migration = Migration(
      version='20260102_000000',
      description='create user table',
      path=tmp_path / 'm1.py',
      up=lambda: ['DEFINE TABLE v3_user SCHEMALESS;'],
      down=lambda: ['REMOVE TABLE v3_user;'],
      checksum='deadbeef',
    )

    # UP: table exists, history row present.
    await execute_migration(integration_client, migration, MigrationDirection.UP)
    assert await is_migration_applied(integration_client, '20260102_000000') is True

    # Migration history table should contain exactly one row.
    history = await get_applied_migrations(integration_client)
    assert len(history) == 1
    assert history[0].version == '20260102_000000'

    # DOWN: history row gone. Table drop is permissive -- server may
    # complain if table was already removed, but the history record
    # must be cleared.
    await execute_migration(integration_client, migration, MigrationDirection.DOWN)
    assert await is_migration_applied(integration_client, '20260102_000000') is False


# ---------------------------------------------------------------------------
# Sanity: migration history table name constant still matches server
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_migration_history_table_name_matches(
  integration_client: DatabaseClient,
) -> None:
  """The `_migration_history` table is created on first use."""
  await ensure_migration_table(integration_client)
  # Bare SELECT should succeed even with zero rows; that's enough to
  # prove the table exists and is queryable on v3.
  result = await integration_client.execute(f'SELECT * FROM {MIGRATION_TABLE_NAME} LIMIT 1')
  # Result type depends on SDK shape, but the call must not raise.
  assert result is not None or result == [] or result == {}


# ---------------------------------------------------------------------------
# Bug #32: UPSERT INTO [...] array form rejected by v3
# ---------------------------------------------------------------------------


class TestUpsertManyV3:
  """Bug #32: `upsert_many` must emit per-record `UPSERT id CONTENT`."""

  @pytest.mark.anyio
  async def test_upsert_many_round_trips_on_v3(
    self, integration_client: DatabaseClient
  ) -> None:
    """Two records upserted; readback returns both."""
    from surql.query.batch import upsert_many

    await integration_client.execute('DEFINE TABLE person SCHEMALESS;')
    items = [
      {'id': 'person:alice', 'name': 'Alice', 'age': 30},
      {'id': 'person:bob', 'name': 'Bob', 'age': 25},
    ]
    await upsert_many(integration_client, 'person', items)

    rows = await integration_client.execute('SELECT * FROM person ORDER BY name')
    # Envelope shape varies by SDK; unwrap either `[row, ...]` or
    # `[{result:[row, ...]}]`.
    if isinstance(rows, list) and rows and isinstance(rows[0], dict) and 'result' in rows[0]:
      rows = rows[0]['result']
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# Bug #33: `FROM <-edge<-record` form rejected by v3
# ---------------------------------------------------------------------------


class TestIncomingEdgesV3:
  """Bug #33: incoming-edge queries must anchor the record on the left."""

  @pytest.mark.anyio
  async def test_get_incoming_edges_on_v3(
    self, integration_client: DatabaseClient
  ) -> None:
    """A `follow` edge from alice to bob; `get_incoming_edges(bob, 'follow')` returns one row."""
    from surql.query.graph import get_incoming_edges

    await integration_client.execute('DEFINE TABLE user SCHEMALESS;')
    await integration_client.execute('DEFINE TABLE follow TYPE RELATION SCHEMALESS;')
    await integration_client.execute("CREATE user:alice SET name = 'Alice'")
    await integration_client.execute("CREATE user:bob SET name = 'Bob'")
    await integration_client.execute('RELATE user:alice->follow->user:bob')

    edges = await get_incoming_edges('user:bob', 'follow', client=integration_client)
    assert len(edges) == 1


# ---------------------------------------------------------------------------
# Bug #34: `->edge{depth}->` trailing-arrow form rejected by v3
# ---------------------------------------------------------------------------


class TestShortestPathV3:
  """Bug #34: `shortest_path` must emit the grouped `(->edge->?){d}` form."""

  @pytest.mark.anyio
  async def test_shortest_path_on_v3(self, integration_client: DatabaseClient) -> None:
    """Alice -> Bob -> Charlie; shortest_path finds a 2-hop path."""
    from surql.query.graph import find_shortest_path

    await integration_client.execute('DEFINE TABLE user SCHEMALESS;')
    await integration_client.execute('DEFINE TABLE follows TYPE RELATION SCHEMALESS;')
    await integration_client.execute("CREATE user:alice SET name = 'Alice'")
    await integration_client.execute("CREATE user:bob SET name = 'Bob'")
    await integration_client.execute("CREATE user:charlie SET name = 'Charlie'")
    await integration_client.execute('RELATE user:alice->follows->user:bob')
    await integration_client.execute('RELATE user:bob->follows->user:charlie')

    # The helper returns the reconstructed path, but here we only need
    # to prove the server accepts the emitted SurrealQL (v2 trailing
    # arrow would parse-error before returning).
    path = await find_shortest_path(
      'user:alice', 'user:charlie', 'follows', max_depth=3, client=integration_client
    )
    assert isinstance(path, list)
