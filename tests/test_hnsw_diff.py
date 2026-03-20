"""Tests for HNSW vector index diff generation."""

import pytest

from surql.migration.diff import (
  _generate_add_index_diff,
  _generate_drop_index_diff,
  _hnsw_index_to_sql,
  diff_indexes,
)
from surql.schema.fields import array_field, string_field
from surql.schema.table import (
  HnswDistanceType,
  IndexDefinition,
  IndexType,
  MTreeVectorType,
  hnsw_index,
  table_schema,
  with_fields,
  with_indexes,
)


class TestHnswIndexToSQL:
  """Test suite for _hnsw_index_to_sql function."""

  def test_hnsw_index_to_sql_basic(self) -> None:
    """Test basic HNSW index SQL generation."""
    idx = hnsw_index('vector_idx', 'vector', 128)
    sql = _hnsw_index_to_sql('documents', idx)

    assert (
      sql
      == 'DEFINE INDEX vector_idx ON TABLE documents COLUMNS vector HNSW DIMENSION 128 DIST EUCLIDEAN TYPE F64;'
    )

  def test_hnsw_index_to_sql_with_cosine_distance(self) -> None:
    """Test HNSW index SQL with COSINE distance."""
    idx = hnsw_index(
      'embedding_idx',
      'embedding',
      1536,
      distance=HnswDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
    )
    sql = _hnsw_index_to_sql('documents', idx)

    assert 'HNSW DIMENSION 1536' in sql
    assert 'DIST COSINE' in sql
    assert 'TYPE F32' in sql

  def test_hnsw_index_to_sql_with_euclidean_distance(self) -> None:
    """Test HNSW index SQL with EUCLIDEAN distance."""
    idx = hnsw_index(
      'features_idx',
      'features',
      256,
      distance=HnswDistanceType.EUCLIDEAN,
      vector_type=MTreeVectorType.F64,
    )
    sql = _hnsw_index_to_sql('images', idx)

    assert 'DIST EUCLIDEAN' in sql
    assert 'TYPE F64' in sql

  def test_hnsw_index_to_sql_with_manhattan_distance(self) -> None:
    """Test HNSW index SQL with MANHATTAN distance."""
    idx = hnsw_index(
      'desc_idx',
      'description_vector',
      512,
      distance=HnswDistanceType.MANHATTAN,
      vector_type=MTreeVectorType.I32,
    )
    sql = _hnsw_index_to_sql('products', idx)

    assert 'DIST MANHATTAN' in sql
    assert 'TYPE I32' in sql

  def test_hnsw_index_to_sql_with_hamming_distance(self) -> None:
    """Test HNSW index SQL with HAMMING distance."""
    idx = hnsw_index(
      'hash_idx',
      'hash_vector',
      64,
      distance=HnswDistanceType.HAMMING,
      vector_type=MTreeVectorType.I16,
    )
    sql = _hnsw_index_to_sql('hashes', idx)

    assert 'DIST HAMMING' in sql
    assert 'TYPE I16' in sql

  def test_hnsw_index_to_sql_with_efc_and_m(self) -> None:
    """Test HNSW index SQL with EFC and M tuning parameters."""
    idx = hnsw_index(
      'tuned_idx',
      'embedding',
      1536,
      distance=HnswDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
      efc=500,
      m=16,
    )
    sql = _hnsw_index_to_sql('documents', idx)

    assert 'HNSW DIMENSION 1536' in sql
    assert 'DIST COSINE' in sql
    assert 'TYPE F32' in sql
    assert 'EFC 500' in sql
    assert 'M 16' in sql

  def test_hnsw_index_to_sql_openai_embeddings(self) -> None:
    """Test HNSW index SQL for OpenAI embeddings configuration."""
    idx = hnsw_index(
      'openai_idx',
      'embedding',
      1536,
      distance=HnswDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
    )
    sql = _hnsw_index_to_sql('documents', idx)

    expected = 'DEFINE INDEX openai_idx ON TABLE documents COLUMNS embedding HNSW DIMENSION 1536 DIST COSINE TYPE F32;'
    assert sql == expected

  def test_hnsw_index_to_sql_3072_dimensions(self) -> None:
    """Test HNSW index SQL for 3072-dimensional embeddings with tuning."""
    idx = hnsw_index(
      'idx_chunk_embedding',
      'embedding',
      3072,
      distance=HnswDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
      efc=500,
      m=16,
    )
    sql = _hnsw_index_to_sql('chunk', idx)

    expected = 'DEFINE INDEX idx_chunk_embedding ON TABLE chunk COLUMNS embedding HNSW DIMENSION 3072 DIST COSINE TYPE F32 EFC 500 M 16;'
    assert sql == expected

  def test_hnsw_index_to_sql_no_dimension_raises_error(self) -> None:
    """Test that HNSW index without dimension raises error."""
    idx = IndexDefinition(
      name='bad_idx',
      columns=['vector'],
      type=IndexType.HNSW,
      dimension=None,
    )

    with pytest.raises(ValueError, match='must have dimension specified'):
      _hnsw_index_to_sql('table', idx)


class TestGenerateAddHnswIndexDiff:
  """Test suite for _generate_add_index_diff with HNSW indexes."""

  def test_generate_add_hnsw_index_diff(self) -> None:
    """Test generating diff for adding HNSW index."""
    idx = hnsw_index(
      'embedding_idx',
      'embedding',
      1536,
      distance=HnswDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
    )
    diff = _generate_add_index_diff('documents', idx)

    assert diff.table == 'documents'
    assert diff.index == 'embedding_idx'
    assert 'HNSW DIMENSION 1536' in diff.forward_sql
    assert 'DIST COSINE' in diff.forward_sql
    assert 'TYPE F32' in diff.forward_sql
    assert 'REMOVE INDEX embedding_idx' in diff.backward_sql

  def test_generate_add_hnsw_index_diff_defaults(self) -> None:
    """Test generating diff for HNSW index with defaults."""
    idx = hnsw_index('vector_idx', 'vector', 128)
    diff = _generate_add_index_diff('table', idx)

    assert 'DIMENSION 128' in diff.forward_sql
    assert 'DIST EUCLIDEAN' in diff.forward_sql
    assert 'TYPE F64' in diff.forward_sql

  def test_generate_add_hnsw_index_diff_with_efc_m(self) -> None:
    """Test generating diff for HNSW index with EFC and M."""
    idx = hnsw_index(
      'vector_idx',
      'vector',
      128,
      efc=500,
      m=16,
    )
    diff = _generate_add_index_diff('table', idx)

    assert 'EFC 500' in diff.forward_sql
    assert 'M 16' in diff.forward_sql


class TestGenerateDropHnswIndexDiff:
  """Test suite for _generate_drop_index_diff with HNSW indexes."""

  def test_generate_drop_hnsw_index_diff(self) -> None:
    """Test generating diff for dropping HNSW index."""
    idx = hnsw_index(
      'embedding_idx',
      'embedding',
      1536,
      distance=HnswDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
    )
    diff = _generate_drop_index_diff('documents', idx)

    assert diff.table == 'documents'
    assert diff.index == 'embedding_idx'
    assert 'REMOVE INDEX embedding_idx' in diff.forward_sql
    assert 'HNSW DIMENSION 1536' in diff.backward_sql
    assert 'DIST COSINE' in diff.backward_sql
    assert 'TYPE F32' in diff.backward_sql


class TestDiffIndexesWithHnsw:
  """Test suite for diff_indexes with HNSW indexes."""

  def test_diff_indexes_add_hnsw_index(self) -> None:
    """Test diffing when HNSW index is added."""
    old_table = table_schema('documents')
    old_table = with_fields(
      old_table,
      string_field('title'),
      array_field('embedding'),
    )

    new_table = table_schema('documents')
    new_table = with_fields(
      new_table,
      string_field('title'),
      array_field('embedding'),
    )
    new_table = with_indexes(
      new_table,
      hnsw_index(
        'embedding_idx',
        'embedding',
        1536,
        distance=HnswDistanceType.COSINE,
        vector_type=MTreeVectorType.F32,
      ),
    )

    diffs = diff_indexes(old_table, new_table)

    assert len(diffs) == 1
    assert diffs[0].index == 'embedding_idx'
    assert 'HNSW' in diffs[0].forward_sql

  def test_diff_indexes_remove_hnsw_index(self) -> None:
    """Test diffing when HNSW index is removed."""
    old_table = table_schema('documents')
    old_table = with_fields(
      old_table,
      array_field('embedding'),
    )
    old_table = with_indexes(
      old_table,
      hnsw_index('embedding_idx', 'embedding', 1536),
    )

    new_table = table_schema('documents')
    new_table = with_fields(
      new_table,
      array_field('embedding'),
    )

    diffs = diff_indexes(old_table, new_table)

    assert len(diffs) == 1
    assert diffs[0].index == 'embedding_idx'
    assert 'REMOVE INDEX' in diffs[0].forward_sql
    assert 'HNSW' in diffs[0].backward_sql

  def test_diff_indexes_mixed_index_types_with_hnsw(self) -> None:
    """Test diffing with multiple index types including HNSW."""
    from surql.schema.table import search_index, unique_index

    old_table = table_schema('products')
    old_table = with_fields(
      old_table,
      string_field('name'),
      array_field('image_embedding'),
    )
    old_table = with_indexes(
      old_table,
      unique_index('name_idx', ['name']),
    )

    new_table = table_schema('products')
    new_table = with_fields(
      new_table,
      string_field('name'),
      string_field('description'),
      array_field('image_embedding'),
    )
    new_table = with_indexes(
      new_table,
      unique_index('name_idx', ['name']),
      search_index('description_search', ['description']),
      hnsw_index(
        'image_similarity_idx',
        'image_embedding',
        512,
        distance=HnswDistanceType.EUCLIDEAN,
      ),
    )

    diffs = diff_indexes(old_table, new_table)

    assert len(diffs) == 2

    search_diffs = [d for d in diffs if 'SEARCH' in d.forward_sql]
    assert len(search_diffs) == 1

    hnsw_diffs = [d for d in diffs if 'HNSW' in d.forward_sql]
    assert len(hnsw_diffs) == 1
    assert 'DIMENSION 512' in hnsw_diffs[0].forward_sql


class TestHnswDistanceTypes:
  """Test all HNSW distance types generate correct SQL."""

  @pytest.mark.parametrize(
    'distance',
    [
      HnswDistanceType.CHEBYSHEV,
      HnswDistanceType.COSINE,
      HnswDistanceType.EUCLIDEAN,
      HnswDistanceType.HAMMING,
      HnswDistanceType.JACCARD,
      HnswDistanceType.MANHATTAN,
      HnswDistanceType.MINKOWSKI,
      HnswDistanceType.PEARSON,
    ],
  )
  def test_all_distance_types(self, distance: HnswDistanceType) -> None:
    """Test that all HNSW distance types produce valid SQL."""
    idx = hnsw_index(
      'test_idx',
      'vector',
      128,
      distance=distance,
    )
    sql = _hnsw_index_to_sql('table', idx)

    assert f'DIST {distance.value}' in sql
