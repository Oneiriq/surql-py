"""Tests for DISKANN vector indexes, the F16 element type, and indexed KNN.

The rendered shapes here are the ones SurrealDB 3.2 echoes back from
``INFO FOR TABLE``. A definition that renders differently from its own echo
re-applies on every reconcile, so the assertions are exact strings rather than
substring checks wherever the echo shape is the point.
"""

import pytest
from pydantic import ValidationError

from surql.migration.diff import _diskann_index_to_sql, _generate_add_index_diff
from surql.query.builder import Query
from surql.schema.parser_index import _parse_index_definition
from surql.schema.sql import _generate_index_sql
from surql.schema.table import (
  DISKANN_DEFAULT_ALPHA,
  DISKANN_DEFAULT_DEGREE,
  DISKANN_DEFAULT_L_BUILD,
  DiskAnnDistanceType,
  HnswDistanceType,
  IndexDefinition,
  IndexType,
  MTreeDistanceType,
  MTreeVectorType,
  canonical_alpha,
  diskann_index,
  hnsw_index,
  mtree_index,
)


class TestDiskAnnRendering:
  """The DEFINE INDEX shape, which must match what the engine echoes."""

  def test_defaults_are_spelled_not_omitted(self) -> None:
    """An unstated DEGREE / L_BUILD / ALPHA still renders, because the engine
    fills them in on the way back and a silent omission would never compare
    equal to its own echo."""
    idx = diskann_index(
      'vec_idx', 'v', 3, distance=DiskAnnDistanceType.COSINE, vector_type=MTreeVectorType.F32
    )
    assert _generate_index_sql('doc', idx) == (
      'DEFINE INDEX vec_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 '
      'DIST COSINE TYPE F32 DEGREE 64 L_BUILD 100 ALPHA 1.2;'
    )

  def test_tuned_tail_and_hashed_vector(self) -> None:
    """HASHED_VECTOR echoes last, after the ALPHA tail."""
    idx = diskann_index(
      'vec_idx',
      'v',
      3,
      distance=DiskAnnDistanceType.COSINE,
      vector_type=MTreeVectorType.F16,
      degree=48,
      l_build=90,
      alpha=1.5,
      hashed_vector=True,
    )
    assert _generate_index_sql('doc', idx) == (
      'DEFINE INDEX vec_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 '
      'DIST COSINE TYPE F16 DEGREE 48 L_BUILD 90 ALPHA 1.5 HASHED_VECTOR;'
    )

  def test_if_not_exists_composes(self) -> None:
    idx = diskann_index('vec_idx', 'v', 3)
    sql = _generate_index_sql('doc', idx, if_not_exists=True)
    assert sql.startswith('DEFINE INDEX IF NOT EXISTS vec_idx')

  def test_every_metric_renders_its_keyword(self) -> None:
    for metric, keyword in [
      (DiskAnnDistanceType.COSINE, 'DIST COSINE'),
      (DiskAnnDistanceType.COSINE_NORMALIZED, 'DIST COSINE_NORMALIZED'),
      (DiskAnnDistanceType.EUCLIDEAN, 'DIST EUCLIDEAN'),
      (DiskAnnDistanceType.INNER_PRODUCT, 'DIST INNER_PRODUCT'),
    ]:
      idx = diskann_index('vec_idx', 'v', 3, distance=metric)
      assert keyword in _generate_index_sql('doc', idx)

  def test_migration_emitter_matches_the_schema_emitter(self) -> None:
    """The migration path and the schema path must agree, or a migrated index
    differs from a reconciled one."""
    idx = diskann_index(
      'emb_idx',
      'embedding',
      1536,
      distance=DiskAnnDistanceType.COSINE,
      vector_type=MTreeVectorType.F16,
    )
    assert _diskann_index_to_sql('documents', idx) == _generate_index_sql('documents', idx)

  def test_migration_emitter_refuses_a_missing_dimension(self) -> None:
    idx = IndexDefinition(name='bad', columns=['v'], type=IndexType.DISKANN)
    with pytest.raises(ValueError, match='must have dimension'):
      _diskann_index_to_sql('documents', idx)

  def test_add_index_diff_carries_the_diskann_form(self) -> None:
    idx = diskann_index('vec_idx', 'v', 3)
    diff = _generate_add_index_diff('doc', idx)
    assert 'DISKANN DIMENSION 3' in diff.forward_sql
    assert 'DEGREE 64 L_BUILD 100 ALPHA 1.2' in diff.forward_sql


class TestCanonicalAlpha:
  """ALPHA round-trips through the engine's own literal shape."""

  def test_fractional_alpha_keeps_its_decimal(self) -> None:
    assert canonical_alpha(1.2) == '1.2'

  def test_whole_alpha_drops_the_trailing_zero(self) -> None:
    """The engine echoes an integer ALPHA bare (``ALPHA 2``), so 2.0 must not
    render as ``2.0``."""
    assert canonical_alpha(2.0) == '2'

  def test_the_default_matches_the_engine_default(self) -> None:
    assert canonical_alpha(1.2) == DISKANN_DEFAULT_ALPHA


class TestDiskAnnParsing:
  """Reading a definition back off INFO FOR TABLE."""

  def test_round_trip_leaves_no_residual(self) -> None:
    """Rendering, then parsing the echo, returns the same definition. This is
    the property that keeps a reconciler from looping."""
    idx = diskann_index(
      'vec_idx',
      'v',
      3,
      distance=DiskAnnDistanceType.COSINE,
      vector_type=MTreeVectorType.F16,
      degree=48,
      l_build=90,
      alpha=1.5,
    )
    parsed = _parse_index_definition('vec_idx', _generate_index_sql('doc', idx))
    assert parsed == idx

  def test_the_engine_f_suffix_is_stripped(self) -> None:
    """A float ALPHA echoes as ``1.2f``; the parser must read ``1.2`` or every
    reconcile re-applies the index."""
    echo = (
      'DEFINE INDEX vec_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 '
      'DIST COSINE TYPE F16 DEGREE 64 L_BUILD 100 ALPHA 1.2f'
    )
    parsed = _parse_index_definition('vec_idx', echo)
    assert parsed is not None
    assert parsed.alpha == '1.2'

  def test_an_integer_alpha_echoes_bare(self) -> None:
    echo = (
      'DEFINE INDEX vec_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 '
      'DIST COSINE TYPE F32 DEGREE 64 L_BUILD 100 ALPHA 2'
    )
    parsed = _parse_index_definition('vec_idx', echo)
    assert parsed is not None
    assert parsed.alpha == '2'

  def test_hashed_vector_is_read_back(self) -> None:
    idx = diskann_index('vec_idx', 'v', 3, hashed_vector=True)
    parsed = _parse_index_definition('vec_idx', _generate_index_sql('doc', idx))
    assert parsed is not None
    assert parsed.hashed_vector is True

  def test_the_underscored_metrics_survive_the_capture(self) -> None:
    """COSINE_NORMALIZED and INNER_PRODUCT must not truncate at the underscore."""
    for metric in [DiskAnnDistanceType.COSINE_NORMALIZED, DiskAnnDistanceType.INNER_PRODUCT]:
      idx = diskann_index('vec_idx', 'v', 3, distance=metric)
      parsed = _parse_index_definition('vec_idx', _generate_index_sql('doc', idx))
      assert parsed is not None
      assert parsed.diskann_distance == metric

  def test_the_index_kind_is_recognised(self) -> None:
    parsed = _parse_index_definition(
      'vec_idx', 'DEFINE INDEX vec_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3'
    )
    assert parsed is not None
    assert parsed.type == IndexType.DISKANN
    assert parsed.columns == ['v']


class TestNarrowElementTypes:
  """F16 / I8 / U8, and the kinds that refuse them."""

  def test_hnsw_takes_the_narrow_types(self) -> None:
    for vector_type in [MTreeVectorType.F16, MTreeVectorType.I8, MTreeVectorType.U8]:
      idx = hnsw_index('feat_idx', 'features', 3, vector_type=vector_type)
      assert f'TYPE {vector_type.value}' in _generate_index_sql('doc', idx)

  def test_mtree_refuses_the_narrow_types(self) -> None:
    """MTREE still parses only its historical five; the engine answers a bare
    parse error, so the refusal has to carry the teaching here."""
    for vector_type in [MTreeVectorType.F16, MTreeVectorType.I8, MTreeVectorType.U8]:
      with pytest.raises(ValidationError, match='only accepts F64, F32, I64, I32, or I16'):
        mtree_index('bad_idx', 'v', 3, vector_type=vector_type)

  def test_mtree_keeps_its_historical_types(self) -> None:
    for vector_type in [
      MTreeVectorType.F64,
      MTreeVectorType.F32,
      MTreeVectorType.I64,
      MTreeVectorType.I32,
      MTreeVectorType.I16,
    ]:
      idx = mtree_index('ok_idx', 'v', 3, vector_type=vector_type)
      assert f'TYPE {vector_type.value}' in _generate_index_sql('doc', idx)

  def test_diskann_refuses_types_outside_its_set(self) -> None:
    for vector_type in [
      MTreeVectorType.F64,
      MTreeVectorType.I64,
      MTreeVectorType.I32,
      MTreeVectorType.I16,
    ]:
      with pytest.raises(ValidationError, match='only accepts F32, F16, I8, or U8'):
        diskann_index('bad_idx', 'v', 3, vector_type=vector_type)

  def test_diskann_takes_its_own_set(self) -> None:
    for vector_type in [
      MTreeVectorType.F32,
      MTreeVectorType.F16,
      MTreeVectorType.I8,
      MTreeVectorType.U8,
    ]:
      idx = diskann_index('ok_idx', 'v', 3, vector_type=vector_type)
      assert f'TYPE {vector_type.value}' in _generate_index_sql('doc', idx)

  def test_diskann_refuses_a_metric_from_another_kind(self) -> None:
    """An MTREE or HNSW metric aimed at DISKANN would be dropped by the
    emitter, so it is refused rather than silently ignored."""
    with pytest.raises(ValidationError, match='takes its metric through diskann_distance'):
      IndexDefinition(
        name='bad_idx',
        columns=['v'],
        type=IndexType.DISKANN,
        dimension=3,
        distance=MTreeDistanceType.COSINE,
      )
    with pytest.raises(ValidationError, match='takes its metric through diskann_distance'):
      IndexDefinition(
        name='bad_idx',
        columns=['v'],
        type=IndexType.DISKANN,
        dimension=3,
        hnsw_distance=HnswDistanceType.PEARSON,
      )


class TestDefaults:
  """The constants exist so a caller can assert against them by name."""

  def test_the_builder_fills_the_engine_defaults(self) -> None:
    idx = diskann_index('vec_idx', 'v', 3)
    assert idx.degree == DISKANN_DEFAULT_DEGREE
    assert idx.l_build == DISKANN_DEFAULT_L_BUILD
    assert idx.alpha == DISKANN_DEFAULT_ALPHA
    assert idx.diskann_distance == DiskAnnDistanceType.EUCLIDEAN
    assert idx.vector_type == MTreeVectorType.F32


class TestIndexedKnnOperator:
  """The second argument of the KNN operator decides the plan."""

  def test_an_integer_ef_renders_the_index_backed_form(self) -> None:
    sql = (
      Query()
      .select()
      .from_table('documents')
      .vector_search_indexed('embedding', [0.1, 0.2, 0.3], k=10, ef=40)
      .to_surql()
    )
    assert 'embedding <|10,40|> [0.1, 0.2, 0.3]' in sql

  def test_a_metric_still_renders_the_exhaustive_form(self) -> None:
    """vector_search keeps its meaning; it just is not the index-backed one."""
    sql = (
      Query()
      .select()
      .from_table('documents')
      .vector_search('embedding', [0.1, 0.2], k=5, distance='COSINE')
      .to_surql()
    )
    assert 'embedding <|5,COSINE|> [0.1, 0.2]' in sql

  def test_the_indexed_form_clears_a_previously_set_metric(self) -> None:
    """Chaining must not leave both forms armed, which would render the metric
    and quietly return to a table scan."""
    sql = (
      Query()
      .select()
      .from_table('documents')
      .vector_search('embedding', [0.1, 0.2], k=5, distance='COSINE', threshold=0.7)
      .vector_search_indexed('embedding', [0.1, 0.2], k=5, ef=64)
      .to_surql()
    )
    assert 'embedding <|5,64|> [0.1, 0.2]' in sql
    assert 'COSINE' not in sql

  def test_it_refuses_a_meaningless_k_or_ef(self) -> None:
    with pytest.raises(ValueError, match='k must be at least 1'):
      Query().vector_search_indexed('embedding', [0.1], k=0)
    with pytest.raises(ValueError, match='ef must be at least 1'):
      Query().vector_search_indexed('embedding', [0.1], ef=0)

  def test_it_refuses_an_empty_vector(self) -> None:
    with pytest.raises(ValueError, match='Vector cannot be empty'):
      Query().vector_search_indexed('embedding', [])
