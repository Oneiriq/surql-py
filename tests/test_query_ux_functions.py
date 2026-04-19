"""Sub-feature #3 (issue #47): SurrealQL function factories."""

from __future__ import annotations

from surql.query.builder import Query
from surql.query.expressions import FunctionExpression
from surql.query.functions import (
  count_if,
  math_abs_fn,
  math_ceil_fn,
  math_floor_fn,
  math_max_fn,
  math_mean_fn,
  math_min_fn,
  math_round_fn,
  math_sum_fn,
  string_concat,
  string_len,
  string_lower,
  string_upper,
  time_now_fn,
)
from surql.types.surreal_fn import SurrealFn, type_record


class TestSurrealFunctionFactories:
  """Tests for pre-built function factories that return ``SurrealFn``."""

  def test_time_now_fn(self) -> None:
    fn = time_now_fn()
    assert isinstance(fn, SurrealFn)
    assert fn.to_surql() == 'time::now()'

  def test_math_mean_fn(self) -> None:
    fn = math_mean_fn('score')
    assert isinstance(fn, SurrealFn)
    assert fn.to_surql() == 'math::mean(score)'

  def test_math_sum_fn(self) -> None:
    fn = math_sum_fn('amount')
    assert fn.to_surql() == 'math::sum(amount)'

  def test_math_min_fn(self) -> None:
    fn = math_min_fn('price')
    assert fn.to_surql() == 'math::min(price)'

  def test_math_max_fn(self) -> None:
    fn = math_max_fn('price')
    assert fn.to_surql() == 'math::max(price)'

  def test_math_ceil_fn(self) -> None:
    fn = math_ceil_fn('value')
    assert fn.to_surql() == 'math::ceil(value)'

  def test_math_floor_fn(self) -> None:
    fn = math_floor_fn('value')
    assert fn.to_surql() == 'math::floor(value)'

  def test_math_round_fn_default(self) -> None:
    fn = math_round_fn('value')
    assert fn.to_surql() == 'math::round(value)'

  def test_math_round_fn_with_precision(self) -> None:
    fn = math_round_fn('value', precision=2)
    assert fn.to_surql() == 'math::round(value, 2)'

  def test_math_abs_fn(self) -> None:
    fn = math_abs_fn('delta')
    assert fn.to_surql() == 'math::abs(delta)'

  def test_string_len(self) -> None:
    fn = string_len('name')
    assert fn.to_surql() == 'string::len(name)'

  def test_string_lower(self) -> None:
    fn = string_lower('email')
    assert fn.to_surql() == 'string::lowercase(email)'

  def test_string_upper(self) -> None:
    fn = string_upper('name')
    assert fn.to_surql() == 'string::uppercase(name)'

  def test_string_concat_fields(self) -> None:
    fn = string_concat('first_name', "' '", 'last_name')
    assert fn.to_surql() == "string::concat(first_name, ' ', last_name)"

  def test_count_if_with_predicate(self) -> None:
    fn = count_if('status = "active"')
    assert fn.to_surql() == 'count(status = "active")'

  def test_count_if_default_is_bare_count(self) -> None:
    fn = count_if()
    assert fn.to_surql() == 'count()'

  def test_count_if_star_alias_is_bare_count(self) -> None:
    # SurrealDB v3 rejects ``count(*)``; the helper normalizes to ``count()``.
    fn = count_if('*')
    assert fn.to_surql() == 'count()'


class TestFunctionFactoryComposition:
  """Function factories should compose with the existing builder."""

  def test_time_now_fn_in_update_set(self) -> None:
    now = time_now_fn()
    query = Query().update('user:alice', {'updated_at': now})
    sql = query.to_surql()
    assert 'updated_at = time::now()' in sql
    assert "updated_at = 'time::now()'" not in sql

  def test_math_sum_fn_in_upsert(self) -> None:
    total = math_sum_fn('line_items.price')
    query = Query().upsert('cart:abc', {'total': total})
    sql = query.to_surql()
    assert 'total: math::sum(line_items.price)' in sql

  def test_type_record_composes_with_set_payload(self) -> None:
    ref = type_record('user', 'alice')
    now = time_now_fn()
    query = Query().update('post:1', {'author': ref, 'updated_at': now})
    sql = query.to_surql()
    assert "author = type::record('user', 'alice')" in sql
    assert 'updated_at = time::now()' in sql


class TestQueryBuilderSetAndSelect:
  """Builder-side additions that pair with the function factories."""

  def test_select_accepts_function_expressions(self) -> None:
    query = (
      Query().select([count_if(), math_mean_fn('strength')]).from_table('memory_entry').group_all()
    )
    sql = query.to_surql()
    assert 'count()' in sql
    assert 'math::mean(strength)' in sql
    assert sql.endswith('GROUP ALL')

  def test_set_method_populates_update_data(self) -> None:
    query = Query().update('user:alice').set(updated_at=time_now_fn(), status='active')
    sql = query.to_surql()
    assert 'updated_at = time::now()' in sql
    assert "status = 'active'" in sql

  def test_set_merges_with_existing_update_data(self) -> None:
    existing = {'name': 'Alice'}
    query = Query().update('user:alice', existing).set(updated_at=time_now_fn())
    sql = query.to_surql()
    assert "name = 'Alice'" in sql
    assert 'updated_at = time::now()' in sql

  def test_update_with_time_now_via_set_and_where(self) -> None:
    query = Query().update('user:alice').set(updated_at=time_now_fn()).where('id = "alice"')
    sql = query.to_surql()
    assert 'UPDATE user:alice SET updated_at = time::now()' in sql
    assert 'WHERE (id = "alice")' in sql

  def test_existing_select_string_path_unchanged(self) -> None:
    # Regression: prior behavior relies on plain string field lists.
    query = Query().select(['count()']).from_table('user').group_all()
    assert query.to_surql() == 'SELECT count() FROM user GROUP ALL'


class TestFunctionExpressionRawRendering:
  """Existing ``FunctionExpression`` values should render raw in SET values.

  This ensures the pre-1.5 ``count``/``math_mean`` helpers now compose with
  UPDATE/INSERT/UPSERT just like the new ``SurrealFn`` factories.
  """

  def test_function_expression_in_update(self) -> None:
    from surql.query.expressions import math_sum

    query = Query().update('cart:1', {'total': math_sum('line_items.price')})
    sql = query.to_surql()
    assert 'total = math::sum(line_items.price)' in sql
    assert "total = 'math::sum" not in sql

  def test_function_expression_in_insert(self) -> None:
    from surql.query.expressions import time_now

    expr = time_now()
    assert isinstance(expr, FunctionExpression)
    query = Query().insert('event', {'name': 'x', 'created_at': expr})
    sql = query.to_surql()
    assert 'created_at: time::now()' in sql


class TestFunctionFactoryPublicApi:
  """Expose function factories via ``surql.query`` and the top-level package."""

  def test_query_submodule_exports(self) -> None:
    from surql.query import (
      count_if,
      math_abs_fn,
      math_ceil_fn,
      math_floor_fn,
      math_max_fn,
      math_mean_fn,
      math_min_fn,
      math_round_fn,
      math_sum_fn,
      string_concat,
      string_len,
      string_lower,
      string_upper,
      time_now_fn,
    )

    for fn in (
      count_if,
      math_abs_fn,
      math_ceil_fn,
      math_floor_fn,
      math_max_fn,
      math_mean_fn,
      math_min_fn,
      math_round_fn,
      math_sum_fn,
      string_concat,
      string_len,
      string_lower,
      string_upper,
      time_now_fn,
    ):
      assert callable(fn)

  def test_top_level_exports(self) -> None:
    import surql

    for name in (
      'time_now_fn',
      'math_mean_fn',
      'math_sum_fn',
      'math_max_fn',
      'math_min_fn',
      'math_ceil_fn',
      'math_floor_fn',
      'math_round_fn',
      'math_abs_fn',
      'string_len',
      'string_concat',
      'string_lower',
      'string_upper',
      'count_if',
    ):
      assert hasattr(surql, name)
