"""HNSW Vector Index Example - Vector Search Pattern.

This example demonstrates HNSW vector index usage for
semantic search with various embedding dimensions and
distance metrics.

HNSW (Hierarchical Navigable Small World) indexes are the
recommended vector index type for SurrealDB 2.x+ and the
successor to MTREE indexes.

HNSW indexes support:
- 8 distance metrics (COSINE, EUCLIDEAN, MANHATTAN, etc.)
- 5 vector types (F64, F32, I64, I32, I16)
- EFC and M tuning parameters for performance optimization
"""

from surql.schema import (
  HnswDistanceType,
  MTreeVectorType,
  array_field,
  datetime_field,
  hnsw_index,
  int_field,
  string_field,
  table_schema,
  with_fields,
  with_indexes,
)

# Define chunk table with 1024-dimensional HNSW index (semantic search pattern)
chunk = table_schema('chunk')
chunk = with_fields(
  chunk,
  string_field('document_id'),
  string_field('text'),
  int_field('chunk_index'),
  int_field('char_start'),
  int_field('char_end'),
  array_field('embedding'),  # 1024-dimensional vector
  datetime_field('created_at', default='time::now()'),
)
chunk = with_indexes(
  chunk,
  hnsw_index(
    'idx_chunk_embedding',
    'embedding',
    1024,
    distance=HnswDistanceType.COSINE,
    vector_type=MTreeVectorType.F64,
  ),
)


# Define entity table with 1024-dimensional HNSW index
entity = table_schema('entity')
entity = with_fields(
  entity,
  string_field('name'),
  string_field('type'),
  array_field('embedding'),
  datetime_field('created_at', default='time::now()'),
)
entity = with_indexes(
  entity,
  hnsw_index(
    'idx_entity_embedding',
    'embedding',
    1024,
    distance=HnswDistanceType.COSINE,
    vector_type=MTreeVectorType.F64,
  ),
)


# OpenAI text-embedding-3-large (3072 dimensions) with EFC/M tuning
openai_docs = table_schema('openai_docs')
openai_docs = with_fields(
  openai_docs,
  string_field('text'),
  array_field('embedding'),
)
openai_docs = with_indexes(
  openai_docs,
  hnsw_index(
    'embedding_idx',
    'embedding',
    3072,
    distance=HnswDistanceType.COSINE,
    vector_type=MTreeVectorType.F32,
    efc=500,  # Higher EFC for better recall during construction
    m=16,     # More connections per node for better search quality
  ),
)


# OpenAI text-embedding-3-small (1536 dimensions)
openai_chunks = table_schema('openai_chunks')
openai_chunks = with_fields(
  openai_chunks,
  string_field('text'),
  array_field('embedding'),
)
openai_chunks = with_indexes(
  openai_chunks,
  hnsw_index(
    'embedding_idx',
    'embedding',
    1536,
    distance=HnswDistanceType.COSINE,
    vector_type=MTreeVectorType.F32,
  ),
)


# Image embeddings with Euclidean distance
image_features = table_schema('image_features')
image_features = with_fields(
  image_features,
  string_field('image_url'),
  array_field('features'),
)
image_features = with_indexes(
  image_features,
  hnsw_index(
    'features_idx',
    'features',
    512,
    distance=HnswDistanceType.EUCLIDEAN,
    vector_type=MTreeVectorType.F32,
  ),
)


# Binary hash vectors with Hamming distance
binary_hashes = table_schema('binary_hashes')
binary_hashes = with_fields(
  binary_hashes,
  string_field('source_id'),
  array_field('hash_vector'),
)
binary_hashes = with_indexes(
  binary_hashes,
  hnsw_index(
    'hash_idx',
    'hash_vector',
    256,
    distance=HnswDistanceType.HAMMING,
    vector_type=MTreeVectorType.I16,
  ),
)


# Custom integer vectors with Manhattan distance
user_preferences = table_schema('user_preferences')
user_preferences = with_fields(
  user_preferences,
  string_field('user_id'),
  array_field('preference_vector'),
)
user_preferences = with_indexes(
  user_preferences,
  hnsw_index(
    'preference_idx',
    'preference_vector',
    64,
    distance=HnswDistanceType.MANHATTAN,
    vector_type=MTreeVectorType.I32,
  ),
)


"""
Generated SurrealQL (for vector-indexed tables):

DEFINE TABLE chunk SCHEMAFULL;
DEFINE FIELD document_id ON TABLE chunk TYPE string;
DEFINE FIELD text ON TABLE chunk TYPE string;
DEFINE FIELD chunk_index ON TABLE chunk TYPE int;
DEFINE FIELD char_start ON TABLE chunk TYPE int;
DEFINE FIELD char_end ON TABLE chunk TYPE int;
DEFINE FIELD embedding ON TABLE chunk TYPE array;
DEFINE FIELD created_at ON TABLE chunk TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_chunk_embedding ON TABLE chunk COLUMNS embedding HNSW DIMENSION 1024 DIST COSINE TYPE F64;

DEFINE TABLE openai_docs SCHEMAFULL;
DEFINE FIELD text ON TABLE openai_docs TYPE string;
DEFINE FIELD embedding ON TABLE openai_docs TYPE array;
DEFINE INDEX embedding_idx ON TABLE openai_docs COLUMNS embedding HNSW DIMENSION 3072 DIST COSINE TYPE F32 EFC 500 M 16;

DEFINE TABLE binary_hashes SCHEMAFULL;
DEFINE FIELD source_id ON TABLE binary_hashes TYPE string;
DEFINE FIELD hash_vector ON TABLE binary_hashes TYPE array;
DEFINE INDEX hash_idx ON TABLE binary_hashes COLUMNS hash_vector HNSW DIMENSION 256 DIST HAMMING TYPE I16;


Vector Search Queries (used with the query builder or raw query execution):

-- Semantic search on chunks
SELECT
    id, document_id, text, chunk_index, char_start, char_end, created_at,
    vector::similarity::cosine(embedding, $embedding) AS similarity
FROM chunk
WHERE embedding <|10,COSINE|> $embedding
ORDER BY similarity DESC
LIMIT 10;

-- With the query builder:
-- Query().from_('chunk').vector_search('embedding', vector, k=10, distance=VectorDistanceType.COSINE)
"""


# Export schema definitions
__all__ = [
  'chunk',
  'entity',
  'openai_docs',
  'openai_chunks',
  'image_features',
  'binary_hashes',
  'user_preferences',
]
