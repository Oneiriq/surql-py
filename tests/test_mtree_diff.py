"""Tests for MTREE vector index diff generation."""

import pytest

from surql.migration.diff import (
  _generate_add_index_diff,
  _generate_drop_index_diff,
  _mtree_index_to_sql,
  diff_indexes,
)
from surql.schema.fields import array_field, string_field
from surql.schema.table import (
  IndexType,
  MTreeDistanceType,
  MTreeVectorType,
  mtree_index,
  table_schema,
  with_fields,
  with_indexes,
)


class TestMTreeIndexToSQL:
  """Test suite for _mtree_index_to_sql function."""

  def test_mtree_index_to_sql_basic(self) -> None:
    """Test basic MTREE index SQL generation."""
    idx = mtree_index('vector_idx', 'vector', 128)
    sql = _mtree_index_to_sql('documents', idx)

    assert (
      sql
      == 'DEFINE INDEX vector_idx ON TABLE documents COLUMNS vector MTREE DIMENSION 128 DIST EUCLIDEAN TYPE F64;'
    )

  def test_mtree_index_to_sql_with_cosine_distance(self) -> None:
    """Test MTREE index SQL with COSINE distance."""
    idx = mtree_index(
      'embedding_idx',
      'embedding',
      1536,
      distance=MTreeDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
    )
    sql = _mtree_index_to_sql('documents', idx)

    assert 'MTREE DIMENSION 1536' in sql
    assert 'DIST COSINE' in sql
    assert 'TYPE F32' in sql

  def test_mtree_index_to_sql_with_euclidean_distance(self) -> None:
    """Test MTREE index SQL with EUCLIDEAN distance."""
    idx = mtree_index(
      'features_idx',
      'features',
      256,
      distance=MTreeDistanceType.EUCLIDEAN,
      vector_type=MTreeVectorType.F64,
    )
    sql = _mtree_index_to_sql('images', idx)

    assert 'DIST EUCLIDEAN' in sql
    assert 'TYPE F64' in sql

  def test_mtree_index_to_sql_with_manhattan_distance(self) -> None:
    """Test MTREE index SQL with MANHATTAN distance."""
    idx = mtree_index(
      'desc_idx',
      'description_vector',
      512,
      distance=MTreeDistanceType.MANHATTAN,
      vector_type=MTreeVectorType.I32,
    )
    sql = _mtree_index_to_sql('products', idx)

    assert 'DIST MANHATTAN' in sql
    assert 'TYPE I32' in sql

  def test_mtree_index_to_sql_with_minkowski_distance(self) -> None:
    """Test MTREE index SQL with MINKOWSKI distance."""
    idx = mtree_index(
      'item_idx',
      'item_vector',
      64,
      distance=MTreeDistanceType.MINKOWSKI,
      vector_type=MTreeVectorType.I16,
    )
    sql = _mtree_index_to_sql('items', idx)

    assert 'DIST MINKOWSKI' in sql
    assert 'TYPE I16' in sql

  def test_mtree_index_to_sql_openai_embeddings(self) -> None:
    """Test MTREE index SQL for OpenAI embeddings configuration."""
    idx = mtree_index(
      'openai_idx',
      'embedding',
      1536,
      distance=MTreeDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
    )
    sql = _mtree_index_to_sql('documents', idx)

    expected = 'DEFINE INDEX openai_idx ON TABLE documents COLUMNS embedding MTREE DIMENSION 1536 DIST COSINE TYPE F32;'
    assert sql == expected

  def test_mtree_index_to_sql_driftnet_1024_dimensions(self) -> None:
    """Test MTREE index SQL for driftnet's 1024-dimensional embeddings with COSINE."""
    idx = mtree_index(
      'idx_chunk_embedding',
      'embedding',
      1024,  # Driftnet dimension
      distance=MTreeDistanceType.COSINE,
      vector_type=MTreeVectorType.F64,
    )
    sql = _mtree_index_to_sql('chunk', idx)

    expected = 'DEFINE INDEX idx_chunk_embedding ON TABLE chunk COLUMNS embedding MTREE DIMENSION 1024 DIST COSINE TYPE F64;'
    assert sql == expected
    # Verify all driftnet requirements are met
    assert 'DIMENSION 1024' in sql
    assert 'DIST COSINE' in sql
    assert 'COLUMNS embedding' in sql

  def test_mtree_index_to_sql_no_dimension_raises_error(self) -> None:
    """Test that MTREE index without dimension raises error."""
    from surql.schema.table import IndexDefinition

    idx = IndexDefinition(
      name='bad_idx',
      columns=['vector'],
      type=IndexType.MTREE,
      dimension=None,
    )

    with pytest.raises(ValueError, match='must have dimension specified'):
      _mtree_index_to_sql('table', idx)


class TestGenerateAddMTreeIndexDiff:
  """Test suite for _generate_add_index_diff with MTREE indexes."""

  def test_generate_add_mtree_index_diff(self) -> None:
    """Test generating diff for adding MTREE index."""
    idx = mtree_index(
      'embedding_idx',
      'embedding',
      1536,
      distance=MTreeDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
    )
    diff = _generate_add_index_diff('documents', idx)

    assert diff.table == 'documents'
    assert diff.index == 'embedding_idx'
    assert 'MTREE DIMENSION 1536' in diff.forward_sql
    assert 'DIST COSINE' in diff.forward_sql
    assert 'TYPE F32' in diff.forward_sql
    assert 'REMOVE INDEX embedding_idx' in diff.backward_sql

  def test_generate_add_mtree_index_diff_defaults(self) -> None:
    """Test generating diff for MTREE index with defaults."""
    idx = mtree_index('vector_idx', 'vector', 128)
    diff = _generate_add_index_diff('table', idx)

    assert 'DIMENSION 128' in diff.forward_sql
    assert 'DIST EUCLIDEAN' in diff.forward_sql
    assert 'TYPE F64' in diff.forward_sql


class TestGenerateDropMTreeIndexDiff:
  """Test suite for _generate_drop_index_diff with MTREE indexes."""

  def test_generate_drop_mtree_index_diff(self) -> None:
    """Test generating diff for dropping MTREE index."""
    idx = mtree_index(
      'embedding_idx',
      'embedding',
      1536,
      distance=MTreeDistanceType.COSINE,
      vector_type=MTreeVectorType.F32,
    )
    diff = _generate_drop_index_diff('documents', idx)

    assert diff.table == 'documents'
    assert diff.index == 'embedding_idx'
    assert 'REMOVE INDEX embedding_idx' in diff.forward_sql
    assert 'MTREE DIMENSION 1536' in diff.backward_sql
    assert 'DIST COSINE' in diff.backward_sql
    assert 'TYPE F32' in diff.backward_sql


class TestDiffIndexesWithMTree:
  """Test suite for diff_indexes with MTREE indexes."""

  def test_diff_indexes_add_mtree_index(self) -> None:
    """Test diffing when MTREE index is added."""
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
      mtree_index(
        'embedding_idx',
        'embedding',
        1536,
        distance=MTreeDistanceType.COSINE,
        vector_type=MTreeVectorType.F32,
      ),
    )

    diffs = diff_indexes(old_table, new_table)

    assert len(diffs) == 1
    assert diffs[0].index == 'embedding_idx'
    assert 'MTREE' in diffs[0].forward_sql

  def test_diff_indexes_remove_mtree_index(self) -> None:
    """Test diffing when MTREE index is removed."""
    old_table = table_schema('documents')
    old_table = with_fields(
      old_table,
      array_field('embedding'),
    )
    old_table = with_indexes(
      old_table,
      mtree_index('embedding_idx', 'embedding', 1536),
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
    assert 'MTREE' in diffs[0].backward_sql

  def test_diff_indexes_mixed_index_types(self) -> None:
    """Test diffing with multiple index types including MTREE."""
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
      mtree_index(
        'image_similarity_idx',
        'image_embedding',
        512,
        distance=MTreeDistanceType.EUCLIDEAN,
      ),
    )

    diffs = diff_indexes(old_table, new_table)

    assert len(diffs) == 2

    # Check for search index
    search_diffs = [d for d in diffs if 'SEARCH' in d.forward_sql]
    assert len(search_diffs) == 1

    # Check for MTREE index
    mtree_diffs = [d for d in diffs if 'MTREE' in d.forward_sql]
    assert len(mtree_diffs) == 1
    assert 'DIMENSION 512' in mtree_diffs[0].forward_sql
