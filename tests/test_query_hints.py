"""Tests for query optimization hints."""

import pytest
from pydantic import ValidationError

from surql.query.builder import Query
from surql.query.hints import (
  ExplainHint,
  FetchHint,
  HintRenderer,
  IndexHint,
  ParallelHint,
  TimeoutHint,
  merge_hints,
  render_hints,
  validate_hint,
)


class TestIndexHint:
  """Tests for IndexHint."""

  def test_index_hint_creation(self) -> None:
    """Test creating index hint."""
    hint = IndexHint(table='user', index='email_idx')
    assert hint.table == 'user'
    assert hint.index == 'email_idx'
    assert hint.force is False

  def test_index_hint_to_surql_use(self) -> None:
    """Test rendering index hint to SurrealQL (USE)."""
    hint = IndexHint(table='user', index='email_idx', force=False)
    assert hint.to_surql() == '/* USE INDEX user.email_idx */'

  def test_index_hint_to_surql_force(self) -> None:
    """Test rendering index hint to SurrealQL (FORCE)."""
    hint = IndexHint(table='user', index='email_idx', force=True)
    assert hint.to_surql() == '/* FORCE INDEX user.email_idx */'

  def test_index_hint_immutable(self) -> None:
    """Test index hint is immutable."""
    hint = IndexHint(table='user', index='email_idx')
    with pytest.raises(ValidationError):
      hint.table = 'post'  # type: ignore


class TestParallelHint:
  """Tests for ParallelHint."""

  def test_parallel_hint_enabled(self) -> None:
    """Test parallel hint with workers."""
    hint = ParallelHint(enabled=True, max_workers=4)
    assert hint.enabled is True
    assert hint.max_workers == 4
    assert hint.to_surql() == '/* PARALLEL 4 */'

  def test_parallel_hint_enabled_no_workers(self) -> None:
    """Test parallel hint without specific worker count."""
    hint = ParallelHint(enabled=True)
    assert hint.to_surql() == '/* PARALLEL ON */'

  def test_parallel_hint_disabled(self) -> None:
    """Test disabling parallel execution."""
    hint = ParallelHint(enabled=False)
    assert hint.to_surql() == '/* PARALLEL OFF */'

  def test_parallel_hint_max_workers_validation_low(self) -> None:
    """Test max_workers validation (too low)."""
    with pytest.raises(ValidationError):
      ParallelHint(max_workers=0)

  def test_parallel_hint_max_workers_validation_high(self) -> None:
    """Test max_workers validation (too high)."""
    with pytest.raises(ValidationError):
      ParallelHint(max_workers=100)

  def test_parallel_hint_max_workers_valid_boundary(self) -> None:
    """Test max_workers validation at boundaries."""
    hint1 = ParallelHint(max_workers=1)
    assert hint1.max_workers == 1

    hint32 = ParallelHint(max_workers=32)
    assert hint32.max_workers == 32


class TestTimeoutHint:
  """Tests for TimeoutHint."""

  def test_timeout_hint_creation(self) -> None:
    """Test creating timeout hint."""
    hint = TimeoutHint(seconds=30.0)
    assert hint.seconds == 30.0
    assert hint.to_surql() == '/* TIMEOUT 30.0s */'

  def test_timeout_hint_validation_zero(self) -> None:
    """Test timeout must be positive (zero)."""
    with pytest.raises(ValidationError):
      TimeoutHint(seconds=0)

  def test_timeout_hint_validation_negative(self) -> None:
    """Test timeout must be positive (negative)."""
    with pytest.raises(ValidationError):
      TimeoutHint(seconds=-10)


class TestFetchHint:
  """Tests for FetchHint."""

  def test_fetch_hint_batch_strategy(self) -> None:
    """Test batch fetch strategy."""
    hint = FetchHint(strategy='batch', batch_size=100)
    assert hint.strategy == 'batch'
    assert hint.batch_size == 100
    assert hint.to_surql() == '/* FETCH BATCH 100 */'

  def test_fetch_hint_eager_strategy(self) -> None:
    """Test eager fetch strategy."""
    hint = FetchHint(strategy='eager')
    assert hint.to_surql() == '/* FETCH EAGER */'

  def test_fetch_hint_lazy_strategy(self) -> None:
    """Test lazy fetch strategy."""
    hint = FetchHint(strategy='lazy')
    assert hint.to_surql() == '/* FETCH LAZY */'

  def test_fetch_hint_batch_requires_size(self) -> None:
    """Test batch strategy requires batch_size."""
    with pytest.raises(ValidationError):
      FetchHint(strategy='batch')

  def test_fetch_hint_batch_size_validation_low(self) -> None:
    """Test batch_size validation (too low)."""
    with pytest.raises(ValidationError):
      FetchHint(strategy='batch', batch_size=0)

  def test_fetch_hint_batch_size_validation_high(self) -> None:
    """Test batch_size validation (too high)."""
    with pytest.raises(ValidationError):
      FetchHint(strategy='batch', batch_size=10001)


class TestExplainHint:
  """Tests for ExplainHint."""

  def test_explain_hint_basic(self) -> None:
    """Test basic explain hint."""
    hint = ExplainHint()
    assert hint.full is False
    assert hint.to_surql() == '/* EXPLAIN */'

  def test_explain_hint_full(self) -> None:
    """Test full explain hint."""
    hint = ExplainHint(full=True)
    assert hint.full is True
    assert hint.to_surql() == '/* EXPLAIN FULL */'


class TestHintValidation:
  """Tests for validate_hint function."""

  def test_validate_index_hint_matching_table(self) -> None:
    """Test index hint validation with matching table."""
    hint = IndexHint(table='user', index='email_idx')
    errors = validate_hint(hint, table='user')
    assert len(errors) == 0

  def test_validate_index_hint_mismatched_table(self) -> None:
    """Test index hint validation with mismatched table."""
    hint = IndexHint(table='user', index='email_idx')
    errors = validate_hint(hint, table='post')
    assert len(errors) == 1
    assert 'does not match' in errors[0]

  def test_validate_index_hint_no_table_context(self) -> None:
    """Test index hint validation without table context."""
    hint = IndexHint(table='user', index='email_idx')
    errors = validate_hint(hint, table=None)
    assert len(errors) == 0

  def test_validate_other_hints(self) -> None:
    """Test validation of non-index hints."""
    timeout_hint = TimeoutHint(seconds=30)
    errors = validate_hint(timeout_hint, table='user')
    assert len(errors) == 0

    parallel_hint = ParallelHint(enabled=True)
    errors = validate_hint(parallel_hint, table='user')
    assert len(errors) == 0


class TestHintMerging:
  """Tests for merge_hints function."""

  def test_merge_duplicate_hint_types(self) -> None:
    """Test merging duplicate hint types."""
    hints = [
      TimeoutHint(seconds=10),
      TimeoutHint(seconds=20),
    ]
    merged = merge_hints(hints)
    assert len(merged) == 1
    assert isinstance(merged[0], TimeoutHint)
    assert merged[0].seconds == 20

  def test_merge_different_hint_types(self) -> None:
    """Test merging different hint types."""
    hints = [
      TimeoutHint(seconds=30),
      ParallelHint(enabled=True),
    ]
    merged = merge_hints(hints)
    assert len(merged) == 2

  def test_merge_multiple_duplicates(self) -> None:
    """Test merging with multiple duplicates."""
    hints = [
      TimeoutHint(seconds=10),
      ParallelHint(enabled=True, max_workers=4),
      TimeoutHint(seconds=20),
      ParallelHint(enabled=True, max_workers=8),
      TimeoutHint(seconds=30),
    ]
    merged = merge_hints(hints)
    assert len(merged) == 2

    timeout = next(h for h in merged if isinstance(h, TimeoutHint))
    assert timeout.seconds == 30

    parallel = next(h for h in merged if isinstance(h, ParallelHint))
    assert parallel.max_workers == 8

  def test_merge_empty_hints(self) -> None:
    """Test merging empty hint list."""
    merged = merge_hints([])
    assert len(merged) == 0


class TestHintRendering:
  """Tests for render_hints function."""

  def test_render_empty_hints(self) -> None:
    """Test rendering empty hint list."""
    assert render_hints([]) == ''

  def test_render_single_hint(self) -> None:
    """Test rendering single hint."""
    hints = [TimeoutHint(seconds=30)]
    rendered = render_hints(hints)
    assert rendered == '/* TIMEOUT 30.0s */'

  def test_render_multiple_hints(self) -> None:
    """Test rendering multiple hints."""
    hints = [
      TimeoutHint(seconds=30),
      ParallelHint(enabled=True, max_workers=4),
    ]
    rendered = render_hints(hints)
    assert '/* TIMEOUT 30.0s */' in rendered
    assert '/* PARALLEL 4 */' in rendered

  def test_render_merges_duplicates(self) -> None:
    """Test that rendering merges duplicate hint types."""
    hints = [
      TimeoutHint(seconds=10),
      TimeoutHint(seconds=20),
    ]
    rendered = render_hints(hints)
    assert rendered == '/* TIMEOUT 20.0s */'
    assert '10.0s' not in rendered


class TestHintRenderer:
  """Tests for HintRenderer class."""

  def test_renderer_render_hints(self) -> None:
    """Test HintRenderer.render_hints method."""
    renderer = HintRenderer()
    hints = [TimeoutHint(seconds=30), ParallelHint(max_workers=4)]
    sql = renderer.render_hints(hints)
    assert '/* TIMEOUT 30.0s */' in sql
    assert '/* PARALLEL 4 */' in sql


class TestQueryBuilderIntegration:
  """Tests for Query builder hint integration."""

  def test_add_hint_method(self) -> None:
    """Test adding hint to query."""
    query = Query().select().from_table('user').add_hint(TimeoutHint(seconds=30))
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], TimeoutHint)

  def test_with_hints_method(self) -> None:
    """Test adding multiple hints."""
    query = (
      Query()
      .select()
      .from_table('user')
      .with_hints(
        TimeoutHint(seconds=30),
        ParallelHint(enabled=True),
      )
    )
    assert len(query.hints) == 2

  def test_force_index_convenience(self) -> None:
    """Test force_index convenience method."""
    query = Query().select().from_table('user').force_index('email_idx')
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], IndexHint)
    assert query.hints[0].force is True
    assert query.hints[0].index == 'email_idx'

  def test_use_index_convenience(self) -> None:
    """Test use_index convenience method."""
    query = Query().select().from_table('user').use_index('email_idx')
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], IndexHint)
    assert query.hints[0].force is False

  def test_force_index_requires_table(self) -> None:
    """Test force_index requires table name."""
    with pytest.raises(ValueError, match='Table name required'):
      Query().select().force_index('email_idx')

  def test_with_timeout_convenience(self) -> None:
    """Test with_timeout convenience method."""
    query = Query().select().from_table('user').with_timeout(30.0)
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], TimeoutHint)
    assert query.hints[0].seconds == 30.0

  def test_parallel_convenience(self) -> None:
    """Test parallel convenience method."""
    query = Query().select().from_table('user').parallel(max_workers=4)
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], ParallelHint)
    assert query.hints[0].max_workers == 4

  def test_parallel_convenience_no_workers(self) -> None:
    """Test parallel convenience method without worker count."""
    query = Query().select().from_table('user').parallel()
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], ParallelHint)
    assert query.hints[0].max_workers is None

  def test_with_fetch_convenience(self) -> None:
    """Test with_fetch convenience method."""
    query = Query().select().from_table('user').with_fetch('batch', batch_size=100)
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], FetchHint)
    assert query.hints[0].strategy == 'batch'
    assert query.hints[0].batch_size == 100

  def test_explain_convenience(self) -> None:
    """Test explain convenience method."""
    query = Query().select().from_table('user').explain()
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], ExplainHint)
    assert query.hints[0].full is False

  def test_explain_full_convenience(self) -> None:
    """Test explain convenience method with full=True."""
    query = Query().select().from_table('user').explain(full=True)
    assert len(query.hints) == 1
    assert isinstance(query.hints[0], ExplainHint)
    assert query.hints[0].full is True

  def test_hints_in_surql_output(self) -> None:
    """Test hints are included in SurrealQL output."""
    query = Query().select().from_table('user').with_timeout(30.0)
    sql = query.to_surql()
    assert '/* TIMEOUT 30.0s */' in sql
    assert 'SELECT * FROM user' in sql

  def test_multiple_hints_in_surql_output(self) -> None:
    """Test multiple hints in SurrealQL output."""
    query = (
      Query()
      .select()
      .from_table('user')
      .with_hints(
        TimeoutHint(seconds=30),
        ParallelHint(max_workers=4),
      )
    )
    sql = query.to_surql()
    assert '/* TIMEOUT 30.0s */' in sql
    assert '/* PARALLEL 4 */' in sql
    assert 'SELECT * FROM user' in sql

  def test_query_immutability_with_hints(self) -> None:
    """Test that adding hints maintains query immutability."""
    query1 = Query().select().from_table('user')
    query2 = query1.with_timeout(30.0)

    assert len(query1.hints) == 0
    assert len(query2.hints) == 1
    assert query1 is not query2

  def test_hint_chaining(self) -> None:
    """Test chaining multiple hint methods."""
    query = (
      Query()
      .select()
      .from_table('user')
      .where('age > 18')
      .with_timeout(30.0)
      .parallel(max_workers=4)
      .use_index('age_idx')
      .explain()
    )
    assert len(query.hints) == 4

  def test_hints_with_complex_query(self) -> None:
    """Test hints with complex query."""
    query = (
      Query()
      .select(['name', 'email', 'age'])
      .from_table('user')
      .where('age > 18')
      .order_by('name')
      .limit(10)
      .use_index('age_idx')
      .with_timeout(30.0)
    )
    sql = query.to_surql()
    assert '/* USE INDEX user.age_idx */' in sql
    assert '/* TIMEOUT 30.0s */' in sql
    assert 'SELECT name, email, age FROM user' in sql
    assert 'WHERE (age > 18)' in sql
    assert 'ORDER BY name ASC' in sql
    assert 'LIMIT 10' in sql
