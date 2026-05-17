"""Tests for features #1-#4: aggregation, record_ref, surql_fn, result extraction.

Issue #1: GROUP BY / GROUP ALL aggregation support
Issue #2: type::record() helper
Issue #3: time::now() / SurrealDB function support
Issue #4: Result extraction helpers
"""

from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from surql.query.builder import Query
from surql.query.expressions import (
  FunctionExpression,
  as_,
  count,
  math_max,
  math_mean,
  math_min,
  math_sum,
)
from surql.query.results import (
  extract_one,
  extract_result,
  extract_scalar,
  has_results,
)
from surql.types.record_ref import RecordRef, record_ref
from surql.types.surreal_fn import SurrealFn, surql_fn

# -- Test models --


class User(BaseModel):
  """Test user model."""

  name: str
  email: str
  age: int | None = None


# =============================================================================
# Issue #1: GROUP BY / GROUP ALL aggregation support
# =============================================================================


class TestMathMean:
  """Tests for math::mean() SurrealQL function."""

  def test_math_mean_basic(self) -> None:
    """Test basic math::mean() generation."""
    expr = math_mean('score')
    assert isinstance(expr, FunctionExpression)
    assert expr.to_surql() == 'math::mean(score)'

  def test_math_mean_nested_field(self) -> None:
    """Test math::mean() with nested field."""
    expr = math_mean('stats.rating')
    assert expr.to_surql() == 'math::mean(stats.rating)'


class TestMathSum:
  """Tests for math::sum() SurrealQL function."""

  def test_math_sum_basic(self) -> None:
    """Test basic math::sum() generation."""
    expr = math_sum('price')
    assert isinstance(expr, FunctionExpression)
    assert expr.to_surql() == 'math::sum(price)'

  def test_math_sum_nested_field(self) -> None:
    """Test math::sum() with nested field."""
    expr = math_sum('order.total')
    assert expr.to_surql() == 'math::sum(order.total)'


class TestMathMax:
  """Tests for math::max() SurrealQL function."""

  def test_math_max_basic(self) -> None:
    """Test basic math::max() generation."""
    expr = math_max('score')
    assert isinstance(expr, FunctionExpression)
    assert expr.to_surql() == 'math::max(score)'

  def test_math_max_nested_field(self) -> None:
    """Test math::max() with nested field."""
    expr = math_max('stats.high')
    assert expr.to_surql() == 'math::max(stats.high)'


class TestMathMin:
  """Tests for math::min() SurrealQL function."""

  def test_math_min_basic(self) -> None:
    """Test basic math::min() generation."""
    expr = math_min('price')
    assert isinstance(expr, FunctionExpression)
    assert expr.to_surql() == 'math::min(price)'

  def test_math_min_nested_field(self) -> None:
    """Test math::min() with nested field."""
    expr = math_min('stats.low')
    assert expr.to_surql() == 'math::min(stats.low)'


class TestGroupAll:
  """Tests for GROUP ALL clause on Query builder."""

  def test_group_all_basic(self) -> None:
    """Test GROUP ALL generates correct SurrealQL."""
    query = Query[User]().select(['count()']).from_table('user').group_all()
    sql = query.to_surql()
    assert 'GROUP ALL' in sql
    assert 'GROUP BY' not in sql

  def test_group_all_with_aggregation(self) -> None:
    """Test GROUP ALL with aggregation functions."""
    query = (
      Query[User]().select(['count()', 'math::mean(age) AS avg_age']).from_table('user').group_all()
    )
    sql = query.to_surql()
    assert sql == 'SELECT count(), math::mean(age) AS avg_age FROM user GROUP ALL'

  def test_group_all_with_where(self) -> None:
    """Test GROUP ALL combined with WHERE clause."""
    query = Query[User]().select(['count()']).from_table('user').where('age > 18').group_all()
    sql = query.to_surql()
    assert 'WHERE (age > 18)' in sql
    assert sql.endswith('GROUP ALL')

  def test_group_all_flag_is_false_by_default(self) -> None:
    """Test that group_all_flag defaults to False."""
    query: Query[User] = Query()
    assert query.group_all_flag is False

  def test_group_all_immutability(self) -> None:
    """Test that group_all() returns a new Query instance."""
    query = Query[User]().select().from_table('user')
    grouped = query.group_all()
    assert query.group_all_flag is False
    assert grouped.group_all_flag is True

  def test_group_all_takes_precedence_over_group_by(self) -> None:
    """Test that group_all takes precedence when both are set."""
    query = (
      Query[User]().select(['status', 'count()']).from_table('user').group_by('status').group_all()
    )
    sql = query.to_surql()
    # group_all_flag is True, so GROUP ALL should appear, not GROUP BY
    assert 'GROUP ALL' in sql
    assert 'GROUP BY' not in sql


class TestGroupByWithAggregation:
  """Tests for GROUP BY with aggregation functions."""

  def test_group_by_with_count(self) -> None:
    """Test GROUP BY with count() function."""
    query = Query[User]().select(['status', 'count()']).from_table('user').group_by('status')
    sql = query.to_surql()
    assert sql == 'SELECT status, count() FROM user GROUP BY status'

  def test_group_by_with_math_mean(self) -> None:
    """Test GROUP BY with math::mean() function."""
    cnt = as_(count(), 'cnt')
    mean = as_(math_mean('score'), 'avg')
    query = (
      Query[User]().select([cnt.to_surql(), mean.to_surql()]).from_table('user').group_by('status')
    )
    sql = query.to_surql()
    assert 'COUNT(*) AS cnt' in sql
    assert 'math::mean(score) AS avg' in sql
    assert 'GROUP BY status' in sql

  def test_group_by_multiple_fields(self) -> None:
    """Test GROUP BY with multiple fields."""
    query = (
      Query[User]()
      .select(['department', 'role', 'count()'])
      .from_table('employee')
      .group_by('department', 'role')
    )
    sql = query.to_surql()
    assert 'GROUP BY department, role' in sql

  def test_group_by_with_alias(self) -> None:
    """Test GROUP BY with aliased aggregation."""
    aliased = as_(math_sum('amount'), 'total')
    query = (
      Query[User]()
      .select(['category', aliased.to_surql()])
      .from_table('order')
      .group_by('category')
    )
    sql = query.to_surql()
    assert 'math::sum(amount) AS total' in sql


class TestAggregationExpressionComposition:
  """Tests for composing aggregation expressions with as_()."""

  def test_as_with_count(self) -> None:
    """Test aliasing count() expression."""
    expr = as_(count(), 'total_count')
    assert expr.to_surql() == 'COUNT(*) AS total_count'

  def test_as_with_math_mean(self) -> None:
    """Test aliasing math::mean() expression."""
    expr = as_(math_mean('score'), 'avg_score')
    assert expr.to_surql() == 'math::mean(score) AS avg_score'

  def test_as_with_math_sum(self) -> None:
    """Test aliasing math::sum() expression."""
    expr = as_(math_sum('price'), 'total_price')
    assert expr.to_surql() == 'math::sum(price) AS total_price'

  def test_as_with_math_max(self) -> None:
    """Test aliasing math::max() expression."""
    expr = as_(math_max('score'), 'highest')
    assert expr.to_surql() == 'math::max(score) AS highest'

  def test_as_with_math_min(self) -> None:
    """Test aliasing math::min() expression."""
    expr = as_(math_min('price'), 'lowest')
    assert expr.to_surql() == 'math::min(price) AS lowest'


# =============================================================================
# Issue #2: type::record() helper
# =============================================================================


class TestRecordRef:
  """Tests for RecordRef type."""

  def test_record_ref_string_id(self) -> None:
    """Test RecordRef with string ID."""
    ref = RecordRef(table='user', record_id='alice')
    assert ref.to_surql() == "type::record('user', 'alice')"

  def test_record_ref_int_id(self) -> None:
    """Test RecordRef with integer ID."""
    ref = RecordRef(table='post', record_id=123)
    assert ref.to_surql() == "type::record('post', 123)"

  def test_record_ref_str_method(self) -> None:
    """Test RecordRef __str__ matches to_surql."""
    ref = RecordRef(table='user', record_id='bob')
    assert str(ref) == ref.to_surql()

  def test_record_ref_immutable(self) -> None:
    """Test that RecordRef is frozen."""
    ref = RecordRef(table='user', record_id='alice')
    with pytest.raises((ValidationError, AttributeError)):
      ref.table = 'modified'

  def test_record_ref_escapes_single_quotes(self) -> None:
    """Test that single quotes in record_id are escaped."""
    ref = RecordRef(table='user', record_id="o'brien")
    sql = ref.to_surql()
    assert "o\\'brien" in sql

  def test_record_ref_escapes_backslashes(self) -> None:
    """Test that backslashes in record_id are escaped."""
    ref = RecordRef(table='file', record_id='path\\to\\file')
    sql = ref.to_surql()
    assert 'path\\\\to\\\\file' in sql


class TestRecordRefFunction:
  """Tests for record_ref() helper function."""

  def test_record_ref_helper_string_id(self) -> None:
    """Test record_ref() helper with string ID."""
    ref = record_ref('user', 'alice')
    assert isinstance(ref, RecordRef)
    assert ref.to_surql() == "type::record('user', 'alice')"

  def test_record_ref_helper_int_id(self) -> None:
    """Test record_ref() helper with integer ID."""
    ref = record_ref('post', 42)
    assert isinstance(ref, RecordRef)
    assert ref.to_surql() == "type::record('post', 42)"


class TestRecordRefInQueries:
  """Tests for using RecordRef in query operations."""

  def test_record_ref_in_insert(self) -> None:
    """Test RecordRef used as a value in INSERT query."""
    ref = record_ref('user', 'alice')
    query = Query().insert('post', {'title': 'Hello', 'author': ref})
    sql = query.to_surql()
    assert "author: type::record('user', 'alice')" in sql

  def test_record_ref_in_update(self) -> None:
    """Test RecordRef used as a value in UPDATE query."""
    ref = record_ref('category', 'tech')
    query = Query().update('post:123', {'category': ref})
    sql = query.to_surql()
    assert "category = type::record('category', 'tech')" in sql

  def test_record_ref_in_upsert(self) -> None:
    """Test RecordRef used as a value in UPSERT query."""
    ref = record_ref('org', 'acme')
    query = Query().upsert('member:alice', {'org': ref})
    sql = query.to_surql()
    assert "org: type::record('org', 'acme')" in sql


# =============================================================================
# Issue #3: time::now() / SurrealDB function support
# =============================================================================


class TestSurrealFn:
  """Tests for SurrealFn type."""

  def test_surreal_fn_basic(self) -> None:
    """Test basic SurrealFn creation."""
    fn = SurrealFn(expression='time::now()')
    assert fn.to_surql() == 'time::now()'

  def test_surreal_fn_str(self) -> None:
    """Test SurrealFn __str__ returns expression."""
    fn = SurrealFn(expression='math::mean(scores)')
    assert str(fn) == 'math::mean(scores)'

  def test_surreal_fn_immutable(self) -> None:
    """Test that SurrealFn is frozen."""
    fn = SurrealFn(expression='time::now()')
    with pytest.raises((ValidationError, AttributeError)):
      fn.expression = 'modified'

  def test_surreal_fn_arbitrary_function(self) -> None:
    """Test SurrealFn with arbitrary function."""
    fn = SurrealFn(expression='type::string(42)')
    assert fn.to_surql() == 'type::string(42)'


class TestSurqlFnHelper:
  """Tests for surql_fn() helper function."""

  def test_surql_fn_no_args(self) -> None:
    """Test surql_fn() with no arguments."""
    fn = surql_fn('time::now')
    assert isinstance(fn, SurrealFn)
    assert fn.to_surql() == 'time::now()'

  def test_surql_fn_with_args(self) -> None:
    """Test surql_fn() with arguments."""
    fn = surql_fn('time::format', 'created_at', '%Y-%m-%d')
    assert fn.to_surql() == 'time::format(created_at, %Y-%m-%d)'

  def test_surql_fn_math_function(self) -> None:
    """Test surql_fn() for math functions."""
    fn = surql_fn('math::sum', 'scores')
    assert fn.to_surql() == 'math::sum(scores)'

  def test_surql_fn_type_function(self) -> None:
    """Test surql_fn() for type functions."""
    fn = surql_fn('type::string', 42)
    assert fn.to_surql() == 'type::string(42)'


class TestSurrealFnInQueries:
  """Tests for SurrealFn used in query operations."""

  def test_surreal_fn_in_insert(self) -> None:
    """Test SurrealFn renders as raw SurrealQL in INSERT."""
    fn = surql_fn('time::now')
    query = Query().insert('user', {'name': 'Alice', 'created_at': fn})
    sql = query.to_surql()
    assert 'created_at: time::now()' in sql
    # Should NOT be quoted as a string
    assert "created_at: 'time::now()'" not in sql

  def test_surreal_fn_in_update(self) -> None:
    """Test SurrealFn renders as raw SurrealQL in UPDATE."""
    fn = surql_fn('time::now')
    query = Query().update('user:alice', {'updated_at': fn})
    sql = query.to_surql()
    assert 'updated_at = time::now()' in sql
    assert "updated_at = 'time::now()'" not in sql

  def test_surreal_fn_in_upsert(self) -> None:
    """Test SurrealFn renders as raw SurrealQL in UPSERT."""
    fn = surql_fn('time::now')
    query = Query().upsert('user:alice', {'updated_at': fn})
    sql = query.to_surql()
    assert 'updated_at: time::now()' in sql
    assert "updated_at: 'time::now()'" not in sql

  def test_surreal_fn_in_relate(self) -> None:
    """Test SurrealFn renders as raw SurrealQL in RELATE."""
    fn = surql_fn('time::now')
    query = Query().relate('likes', 'user:alice', 'post:123', {'at': fn})
    sql = query.to_surql()
    assert 'at: time::now()' in sql
    assert "at: 'time::now()'" not in sql

  def test_surreal_fn_mixed_with_regular_values(self) -> None:
    """Test SurrealFn mixed with regular values in INSERT."""
    fn = surql_fn('time::now')
    query = Query().insert(
      'user',
      {
        'name': 'Alice',
        'age': 30,
        'active': True,
        'created_at': fn,
      },
    )
    sql = query.to_surql()
    assert "name: 'Alice'" in sql
    assert 'age: 30' in sql
    assert 'active: true' in sql
    assert 'created_at: time::now()' in sql


class TestSurrealFnAndRecordRefMixed:
  """Tests for mixing SurrealFn and RecordRef in the same query."""

  def test_insert_with_both(self) -> None:
    """Test INSERT with both SurrealFn and RecordRef values."""
    fn = surql_fn('time::now')
    ref = record_ref('user', 'alice')
    query = Query().insert(
      'post',
      {
        'title': 'Hello',
        'author': ref,
        'created_at': fn,
      },
    )
    sql = query.to_surql()
    assert "author: type::record('user', 'alice')" in sql
    assert 'created_at: time::now()' in sql
    assert "title: 'Hello'" in sql


# =============================================================================
# Issue #4: Result extraction helpers
# =============================================================================


class TestExtractResult:
  """Tests for extract_result() function."""

  def test_extract_result_nested_format(self) -> None:
    """Test extracting from nested result format."""
    result = [{'result': [{'id': 'user:123', 'name': 'Alice'}]}]
    extracted = extract_result(result)
    assert len(extracted) == 1
    assert extracted[0]['name'] == 'Alice'

  def test_extract_result_flat_format(self) -> None:
    """Test extracting from flat result format."""
    result = [{'id': 'user:123', 'name': 'Alice'}]
    extracted = extract_result(result)
    assert len(extracted) == 1
    assert extracted[0]['name'] == 'Alice'

  def test_extract_result_empty(self) -> None:
    """Test extracting from empty result."""
    assert extract_result([]) == []

  def test_extract_result_none(self) -> None:
    """Test extracting from None result."""
    assert extract_result(None) == []

  def test_extract_result_multiple_records(self) -> None:
    """Test extracting multiple records from nested format."""
    result = [
      {
        'result': [
          {'id': 'user:1', 'name': 'Alice'},
          {'id': 'user:2', 'name': 'Bob'},
        ]
      }
    ]
    extracted = extract_result(result)
    assert len(extracted) == 2

  def test_extract_result_aggregate(self) -> None:
    """Test extracting aggregate results."""
    result = [{'count': 42}]
    extracted = extract_result(result)
    assert len(extracted) == 1
    assert extracted[0]['count'] == 42

  def test_extract_result_nested_with_empty(self) -> None:
    """Test extracting from nested format with empty result."""
    result = [{'result': []}]
    extracted = extract_result(result)
    assert extracted == []

  def test_extract_result_dict_with_result_key(self) -> None:
    """Test extracting from single dict with result key."""
    result = {'result': [{'id': 'user:1', 'name': 'Alice'}]}
    extracted = extract_result(result)
    assert len(extracted) == 1
    assert extracted[0]['name'] == 'Alice'

  def test_extract_result_multiple_statement_results(self) -> None:
    """Test extracting from multiple statement results."""
    result = [
      {'result': [{'id': 'user:1', 'name': 'Alice'}]},
      {'result': [{'id': 'user:2', 'name': 'Bob'}]},
    ]
    extracted = extract_result(result)
    assert len(extracted) == 2


class TestExtractOne:
  """Tests for extract_one() function."""

  def test_extract_one_nested(self) -> None:
    """Test extracting single record from nested format."""
    result = [{'result': [{'id': 'user:123', 'name': 'Alice'}]}]
    record = extract_one(result)
    assert record is not None
    assert record['name'] == 'Alice'

  def test_extract_one_flat(self) -> None:
    """Test extracting single record from flat format."""
    result = [{'id': 'user:123', 'name': 'Alice'}]
    record = extract_one(result)
    assert record is not None
    assert record['name'] == 'Alice'

  def test_extract_one_empty(self) -> None:
    """Test extracting from empty result returns None."""
    assert extract_one([]) is None

  def test_extract_one_none(self) -> None:
    """Test extracting from None returns None."""
    assert extract_one(None) is None

  def test_extract_one_multiple_returns_first(self) -> None:
    """Test extracting one from multiple records returns first."""
    result = [
      {
        'result': [
          {'id': 'user:1', 'name': 'Alice'},
          {'id': 'user:2', 'name': 'Bob'},
        ]
      }
    ]
    record = extract_one(result)
    assert record is not None
    assert record['name'] == 'Alice'


class TestExtractScalar:
  """Tests for extract_scalar() function."""

  def test_extract_scalar_count(self) -> None:
    """Test extracting count scalar."""
    result = [{'result': [{'count': 42}]}]
    assert extract_scalar(result, 'count') == 42

  def test_extract_scalar_avg(self) -> None:
    """Test extracting avg scalar."""
    result = [{'result': [{'avg': 25.5}]}]
    assert extract_scalar(result, 'avg') == 25.5

  def test_extract_scalar_default(self) -> None:
    """Test default value when key not found."""
    result = [{'id': 'user:123'}]
    assert extract_scalar(result, 'total', default=0) == 0

  def test_extract_scalar_empty(self) -> None:
    """Test default value for empty result."""
    assert extract_scalar([], 'count', default=0) == 0

  def test_extract_scalar_none_result(self) -> None:
    """Test default value for None result."""
    assert extract_scalar(None, 'count', default=-1) == -1

  def test_extract_scalar_flat_format(self) -> None:
    """Test extracting scalar from flat format."""
    result = [{'total': 100}]
    assert extract_scalar(result, 'total') == 100

  def test_extract_scalar_custom_default(self) -> None:
    """Test custom default value."""
    assert extract_scalar([], 'count', default='N/A') == 'N/A'


class TestHasResults:
  """Tests for has_results() function."""

  def test_has_results_nested_with_data(self) -> None:
    """Test has_results with nested format containing data."""
    result = [{'result': [{'id': 'user:123'}]}]
    assert has_results(result) is True

  def test_has_results_flat_with_data(self) -> None:
    """Test has_results with flat format containing data."""
    result = [{'id': 'user:123'}]
    assert has_results(result) is True

  def test_has_results_empty(self) -> None:
    """Test has_results with empty list."""
    assert has_results([]) is False

  def test_has_results_nested_empty(self) -> None:
    """Test has_results with nested empty result."""
    result = [{'result': []}]
    assert has_results(result) is False

  def test_has_results_none(self) -> None:
    """Test has_results with None."""
    assert has_results(None) is False


# =============================================================================
# Integration tests: SurrealQL generation end-to-end
# =============================================================================


class TestIntegrationAggregationQueries:
  """Integration tests verifying full SurrealQL generation for aggregation."""

  def test_count_group_by_status(self) -> None:
    """Test COUNT with GROUP BY generates correct SurrealQL."""
    cnt = as_(count(), 'cnt')
    query = Query().select(['status', cnt.to_surql()]).from_table('user').group_by('status')
    assert query.to_surql() == 'SELECT status, COUNT(*) AS cnt FROM user GROUP BY status'

  def test_math_mean_group_all(self) -> None:
    """Test math::mean with GROUP ALL generates correct SurrealQL."""
    avg = as_(math_mean('score'), 'avg')
    query = Query().select([avg.to_surql()]).from_table('exam').group_all()
    assert query.to_surql() == 'SELECT math::mean(score) AS avg FROM exam GROUP ALL'

  def test_multiple_aggregations_group_by(self) -> None:
    """Test multiple aggregations with GROUP BY."""
    cnt = as_(count(), 'cnt')
    total = as_(math_sum('amount'), 'total')
    highest = as_(math_max('amount'), 'highest')
    query = (
      Query()
      .select(['department', cnt.to_surql(), total.to_surql(), highest.to_surql()])
      .from_table('expense')
      .group_by('department')
    )
    sql = query.to_surql()
    assert 'department' in sql
    assert 'COUNT(*) AS cnt' in sql
    assert 'math::sum(amount) AS total' in sql
    assert 'math::max(amount) AS highest' in sql
    assert 'GROUP BY department' in sql

  def test_aggregation_with_where_and_group_by(self) -> None:
    """Test aggregation with WHERE clause and GROUP BY."""
    query = (
      Query()
      .select(['status', 'count()'])
      .from_table('task')
      .where('created_at > "2024-01-01"')
      .group_by('status')
      .order_by('status')
    )
    sql = query.to_surql()
    assert 'WHERE' in sql
    assert 'GROUP BY status' in sql
    assert 'ORDER BY status ASC' in sql


class TestIntegrationRecordRefQueries:
  """Integration tests verifying type::record() in full queries."""

  def test_create_with_record_ref(self) -> None:
    """Test CREATE query with type::record() reference."""
    ref = record_ref('user', 'alice')
    query = Query().insert(
      'comment',
      {
        'text': 'Great post!',
        'author': ref,
      },
    )
    sql = query.to_surql()
    expected_parts = [
      'CREATE comment CONTENT',
      "text: 'Great post!'",
      "author: type::record('user', 'alice')",
    ]
    for part in expected_parts:
      assert part in sql

  def test_update_with_record_ref(self) -> None:
    """Test UPDATE query with type::record() reference."""
    ref = record_ref('department', 'engineering')
    query = Query().update('employee:123', {'dept': ref})
    sql = query.to_surql()
    assert "dept = type::record('department', 'engineering')" in sql


class TestIntegrationSurrealFnQueries:
  """Integration tests verifying SurrealFn in full queries."""

  def test_create_with_timestamps(self) -> None:
    """Test CREATE query with time::now() timestamps."""
    now = surql_fn('time::now')
    query = Query().insert(
      'event',
      {
        'name': 'Launch',
        'created_at': now,
        'updated_at': now,
      },
    )
    sql = query.to_surql()
    assert 'created_at: time::now()' in sql
    assert 'updated_at: time::now()' in sql

  def test_update_with_surreal_fn(self) -> None:
    """Test UPDATE query with SurrealDB function."""
    now = surql_fn('time::now')
    query = Query().update('user:alice', {'last_login': now})
    sql = query.to_surql()
    assert 'last_login = time::now()' in sql

  def test_relate_with_surreal_fn_timestamp(self) -> None:
    """Test RELATE query with SurrealDB function timestamp."""
    now = surql_fn('time::now')
    query = Query().relate('follows', 'user:alice', 'user:bob', {'since': now})
    sql = query.to_surql()
    assert 'since: time::now()' in sql


class TestIntegrationResultExtraction:
  """Integration tests for result extraction against realistic SurrealDB responses."""

  def test_extract_count_from_group_all(self) -> None:
    """Test extracting count from GROUP ALL response."""
    response: list[dict[str, Any]] = [{'result': [{'count': 150}]}]
    assert extract_scalar(response, 'count') == 150

  def test_extract_aggregation_results(self) -> None:
    """Test extracting multiple aggregation results."""
    response: list[dict[str, Any]] = [
      {
        'result': [
          {'status': 'active', 'cnt': 100, 'avg_age': 32.5},
          {'status': 'inactive', 'cnt': 50, 'avg_age': 45.0},
        ]
      }
    ]
    records = extract_result(response)
    assert len(records) == 2
    assert records[0]['cnt'] == 100
    assert records[1]['avg_age'] == 45.0

  def test_extract_from_sdk_response_format(self) -> None:
    """Test extracting from native SurrealDB SDK response format."""
    # SDK returns flat list directly
    response: list[dict[str, Any]] = [
      {'id': 'user:1', 'name': 'Alice', 'email': 'alice@example.com'},
      {'id': 'user:2', 'name': 'Bob', 'email': 'bob@example.com'},
    ]
    records = extract_result(response)
    assert len(records) == 2
    one = extract_one(response)
    assert one is not None
    assert one['name'] == 'Alice'

  def test_extract_empty_aggregate(self) -> None:
    """Test extracting from empty aggregate response."""
    response: list[dict[str, Any]] = [{'result': []}]
    assert extract_scalar(response, 'count', default=0) == 0
    assert has_results(response) is False

  def test_chain_extract_operations(self) -> None:
    """Test chaining multiple extract operations on same response."""
    response: list[dict[str, Any]] = [{'result': [{'count': 42, 'avg': 25.5, 'max': 100}]}]
    assert extract_scalar(response, 'count') == 42
    assert extract_scalar(response, 'avg') == 25.5
    assert extract_scalar(response, 'max') == 100
    assert has_results(response) is True
    record = extract_one(response)
    assert record is not None
    assert record['count'] == 42


# =============================================================================
# Public API export tests
# =============================================================================


class TestPublicApiExports:
  """Test that new features are properly exported from public API."""

  def test_aggregation_exports_from_surql(self) -> None:
    """Test aggregation functions exported from top-level surql package."""
    import surql

    assert hasattr(surql, 'count')
    assert hasattr(surql, 'math_mean')
    assert hasattr(surql, 'math_sum')
    assert hasattr(surql, 'math_max')
    assert hasattr(surql, 'math_min')
    assert hasattr(surql, 'as_')

  def test_record_ref_exports_from_surql(self) -> None:
    """Test RecordRef exported from top-level surql package."""
    import surql

    assert hasattr(surql, 'RecordRef')
    assert hasattr(surql, 'record_ref')

  def test_surreal_fn_exports_from_surql(self) -> None:
    """Test SurrealFn exported from top-level surql package."""
    import surql

    assert hasattr(surql, 'SurrealFn')
    assert hasattr(surql, 'surql_fn')

  def test_result_extraction_exports_from_surql(self) -> None:
    """Test result extraction helpers exported from top-level surql package."""
    import surql

    assert hasattr(surql, 'extract_result')
    assert hasattr(surql, 'extract_one')
    assert hasattr(surql, 'extract_scalar')
    assert hasattr(surql, 'has_results')

  def test_aggregation_exports_from_query(self) -> None:
    """Test aggregation functions exported from surql.query."""
    from surql.query import math_max, math_mean, math_min, math_sum

    assert callable(math_mean)
    assert callable(math_sum)
    assert callable(math_max)
    assert callable(math_min)

  def test_types_exports(self) -> None:
    """Test new types exported from surql.types."""
    from surql.types import RecordRef, SurrealFn, record_ref, surql_fn

    assert RecordRef is not None
    assert SurrealFn is not None
    assert callable(record_ref)
    assert callable(surql_fn)
