"""Integration tests for the 1.5.0 query UX additions (issue #47).

Exercises behavior that only matters against a live ``surrealdb:v3.0.5``
container:

- ``type::record(...)`` rendered through the builder still round-trips
  against v3 (sub-feature #1).
- ``aggregate_records`` executes GROUP BY / GROUP ALL queries and unwraps
  the response envelope (sub-feature #4).
- ``Query.set()`` combined with ``time_now_fn`` persists a server-side
  timestamp (sub-features #2 + #3).
"""

from __future__ import annotations

import pytest

from surql.connection.client import DatabaseClient
from surql.query.builder import Query
from surql.query.crud import aggregate_records, create_record
from surql.query.functions import count_if, math_mean_fn, math_sum_fn, time_now_fn
from surql.query.results import extract_many, extract_one
from surql.types.surreal_fn import type_record


class TestTypeRecordV3:
  """type::record(...) must still round-trip through the builder on v3."""

  @pytest.mark.anyio
  async def test_insert_with_type_record_reference(
    self, integration_client: DatabaseClient
  ) -> None:
    """Insert a comment that references a user via ``type::record()``."""
    await integration_client.execute('DEFINE TABLE ux_user SCHEMALESS;')
    await integration_client.execute('DEFINE TABLE ux_comment SCHEMALESS;')
    await integration_client.execute("CREATE ux_user:alice SET name = 'Alice'")

    author_ref = type_record('ux_user', 'alice')
    query = Query().insert(
      'ux_comment',
      {'body': 'hello', 'author': author_ref},
    )
    await integration_client.execute(query.to_surql())

    rows = await integration_client.execute('SELECT * FROM ux_comment')
    records = extract_many(rows)
    assert len(records) == 1
    # author resolves to the user record id on SurrealDB v3.
    assert str(records[0]['author']) == 'ux_user:alice'


class TestAggregateRecordsV3:
  """``aggregate_records`` must execute against the live v3 server."""

  @pytest.mark.anyio
  async def test_group_all_counts_table(self, integration_client: DatabaseClient) -> None:
    """GROUP ALL aggregates every row into a single result."""
    await integration_client.execute('DEFINE TABLE ux_memory SCHEMALESS;')
    for i in range(5):
      await create_record(
        'ux_memory',
        {'network': 'n1', 'strength': float(i)},
        client=integration_client,
      )

    rows = await aggregate_records(
      table='ux_memory',
      select={
        'count': count_if('*'),
        'total_strength': math_sum_fn('strength'),
        'avg_strength': math_mean_fn('strength'),
      },
      group_all=True,
      client=integration_client,
    )

    assert len(rows) == 1
    assert rows[0]['count'] == 5
    assert rows[0]['total_strength'] == pytest.approx(0 + 1 + 2 + 3 + 4)
    assert rows[0]['avg_strength'] == pytest.approx(2.0)

  @pytest.mark.anyio
  async def test_group_by_field(self, integration_client: DatabaseClient) -> None:
    """GROUP BY <field> returns one row per distinct value."""
    await integration_client.execute('DEFINE TABLE ux_memory SCHEMALESS;')
    seed = [
      ('n1', 1.0),
      ('n1', 2.0),
      ('n2', 4.0),
      ('n2', 8.0),
      ('n2', 16.0),
    ]
    for network, strength in seed:
      await create_record(
        'ux_memory',
        {'network': network, 'strength': strength},
        client=integration_client,
      )

    rows = await aggregate_records(
      table='ux_memory',
      select={
        'count': count_if('*'),
        'total_strength': math_sum_fn('strength'),
      },
      group_by=['network'],
      client=integration_client,
    )

    assert len(rows) == 2
    by_network = {row['network']: row for row in rows}
    assert by_network['n1']['count'] == 2
    assert by_network['n1']['total_strength'] == pytest.approx(3.0)
    assert by_network['n2']['count'] == 3
    assert by_network['n2']['total_strength'] == pytest.approx(28.0)

  @pytest.mark.anyio
  async def test_group_all_with_where_filter(self, integration_client: DatabaseClient) -> None:
    """Optional WHERE narrows the aggregation."""
    await integration_client.execute('DEFINE TABLE ux_memory SCHEMALESS;')
    for i in range(6):
      await create_record(
        'ux_memory',
        {'strength': float(i)},
        client=integration_client,
      )

    rows = await aggregate_records(
      table='ux_memory',
      select={'count': count_if('*')},
      group_all=True,
      where='strength >= 3',
      client=integration_client,
    )

    assert len(rows) == 1
    assert rows[0]['count'] == 3


class TestSetWithTimeNowFnV3:
  """``Query.update(...).set(...)`` with ``time_now_fn`` persists a timestamp."""

  @pytest.mark.anyio
  async def test_set_writes_server_timestamp(self, integration_client: DatabaseClient) -> None:
    """An UPDATE built via ``.set(...)`` with ``time_now_fn`` stamps the row."""
    await integration_client.execute('DEFINE TABLE ux_user SCHEMALESS;')
    await integration_client.execute("CREATE ux_user:alice SET name = 'Alice'")

    query = Query().update('ux_user:alice').set(last_login=time_now_fn())
    await integration_client.execute(query.to_surql())

    rows = await integration_client.execute('SELECT last_login FROM ux_user:alice')
    record = extract_one(rows)
    assert record is not None
    # SurrealDB returns a datetime-like value; we only assert non-empty.
    assert record.get('last_login') is not None
