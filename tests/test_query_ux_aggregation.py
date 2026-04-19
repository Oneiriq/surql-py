"""Sub-feature #1 (issue #47): ``aggregate_records`` typed CRUD helper."""

from __future__ import annotations

import pytest

from surql.query.crud import _build_aggregate_query, aggregate_records
from surql.query.functions import count_if, math_sum_fn


class TestBuildAggregateQuery:
  """Unit tests for the pure SQL-builder half of ``aggregate_records``."""

  def test_group_all_builds_expected_sql(self) -> None:
    sql = _build_aggregate_query(
      table='memory_entry',
      select={'count': count_if(), 'total': math_sum_fn('strength')},
      group_by=None,
      group_all=True,
    )
    assert sql.startswith('SELECT ')
    assert 'count() AS count' in sql
    assert 'math::sum(strength) AS total' in sql
    assert 'FROM memory_entry' in sql
    assert sql.endswith('GROUP ALL')

  def test_group_by_single_field(self) -> None:
    sql = _build_aggregate_query(
      table='memory_entry',
      select={'total': math_sum_fn('strength')},
      group_by=['network'],
      group_all=False,
    )
    assert 'network' in sql
    assert 'math::sum(strength) AS total' in sql
    assert 'GROUP BY network' in sql

  def test_group_by_multiple_fields(self) -> None:
    sql = _build_aggregate_query(
      table='event',
      select={'cnt': count_if()},
      group_by=['type', 'source'],
      group_all=False,
    )
    assert 'GROUP BY type, source' in sql

  def test_select_accepts_string_values(self) -> None:
    sql = _build_aggregate_query(
      table='memory_entry',
      select={'count': 'count()'},
      group_by=None,
      group_all=True,
    )
    assert 'count() AS count' in sql

  def test_where_clause_is_rendered(self) -> None:
    sql = _build_aggregate_query(
      table='memory_entry',
      select={'count': count_if()},
      group_by=None,
      group_all=True,
      where='strength >= 3',
    )
    assert 'WHERE (strength >= 3)' in sql
    assert sql.endswith('GROUP ALL')

  def test_requires_group_spec(self) -> None:
    with pytest.raises(ValueError):
      _build_aggregate_query(
        table='t',
        select={'count': count_if()},
        group_by=None,
        group_all=False,
      )

  def test_requires_select(self) -> None:
    with pytest.raises(ValueError):
      _build_aggregate_query(
        table='t',
        select={},
        group_by=None,
        group_all=True,
      )


class TestAggregateRecordsPublicApi:
  """Aggregation helper should be importable from ``surql.query`` and ``surql``."""

  def test_query_submodule_exports(self) -> None:
    from surql.query import aggregate_records as ar

    assert callable(ar)

  def test_top_level_export(self) -> None:
    import surql

    assert hasattr(surql, 'aggregate_records')

  def test_callable_alias_exists(self) -> None:
    assert callable(aggregate_records)


class TestAggregateRecordsValidation:
  """``aggregate_records`` argument validation runs before hitting the DB."""

  @pytest.mark.anyio
  async def test_both_group_all_and_group_by_rejected(self) -> None:
    with pytest.raises(ValueError):
      await aggregate_records(
        table='t',
        select={'count': count_if()},
        group_by=['network'],
        group_all=True,
      )
