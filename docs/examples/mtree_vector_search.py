"""MTREE Vector Index Example - Vector Search Pattern.

This example demonstrates MTREE vector index usage for
semantic search with 1024-dimensional embeddings and
COSINE similarity.

MTREE indexes are used for:
- chunk.embedding (1024 dimensions, COSINE)
- entity.embedding (1024 dimensions, COSINE)
- claim.embedding (1024 dimensions, COSINE)
"""

from reverie.schema import (
  MTreeDistanceType,
  MTreeVectorType,
  array_field,
  datetime_field,
  int_field,
  mtree_index,
  string_field,
  table_schema,
  with_fields,
  with_indexes,
)

# Define chunk table with 1024-dimensional MTREE index (semantic search pattern)
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
  mtree_index(
    'idx_chunk_embedding',
    'embedding',
    1024,  # Common embedding dimension
    distance=MTreeDistanceType.COSINE,  # Common distance metric
    vector_type=MTreeVectorType.F64,
  ),
)


# Define entity table with 1024-dimensional MTREE index
entity = table_schema('entity')
entity = with_fields(
  entity,
  string_field('name'),
  string_field('type'),
  array_field('embedding'),  # 1024-dimensional vector
  datetime_field('created_at', default='time::now()'),
)
entity = with_indexes(
  entity,
  mtree_index(
    'idx_entity_embedding',
    'embedding',
    1024,
    distance=MTreeDistanceType.COSINE,
    vector_type=MTreeVectorType.F64,
  ),
)


# Define claim table with 1024-dimensional MTREE index
claim = table_schema('claim')
claim = with_fields(
  claim,
  string_field('text'),
  string_field('source'),
  array_field('embedding'),  # 1024-dimensional vector
  datetime_field('created_at', default='time::now()'),
)
claim = with_indexes(
  claim,
  mtree_index(
    'idx_claim_embedding',
    'embedding',
    1024,
    distance=MTreeDistanceType.COSINE,
    vector_type=MTreeVectorType.F64,
  ),
)


# Example with different dimensions and distance metrics

# OpenAI text-embedding-3-small (1536 dimensions)
openai_chunks = table_schema('openai_chunks')
openai_chunks = with_fields(
  openai_chunks,
  string_field('text'),
  array_field('embedding'),
)
openai_chunks = with_indexes(
  openai_chunks,
  mtree_index(
    'embedding_idx',
    'embedding',
    1536,  # OpenAI text-embedding-3-small dimension
    distance=MTreeDistanceType.COSINE,
    vector_type=MTreeVectorType.F32,  # F32 is sufficient for embeddings
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
  mtree_index(
    'features_idx',
    'features',
    512,  # ResNet50 output dimension
    distance=MTreeDistanceType.EUCLIDEAN,
    vector_type=MTreeVectorType.F32,
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
  mtree_index(
    'preference_idx',
    'preference_vector',
    64,
    distance=MTreeDistanceType.MANHATTAN,
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
DEFINE INDEX idx_chunk_embedding ON TABLE chunk COLUMNS embedding MTREE DIMENSION 1024 DIST COSINE TYPE F64;

DEFINE TABLE entity SCHEMAFULL;
DEFINE FIELD name ON TABLE entity TYPE string;
DEFINE FIELD type ON TABLE entity TYPE string;
DEFINE FIELD embedding ON TABLE entity TYPE array;
DEFINE FIELD created_at ON TABLE entity TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_entity_embedding ON TABLE entity COLUMNS embedding MTREE DIMENSION 1024 DIST COSINE TYPE F64;

DEFINE TABLE claim SCHEMAFULL;
DEFINE FIELD text ON TABLE claim TYPE string;
DEFINE FIELD source ON TABLE claim TYPE string;
DEFINE FIELD embedding ON TABLE claim TYPE array;
DEFINE FIELD created_at ON TABLE claim TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_claim_embedding ON TABLE claim COLUMNS embedding MTREE DIMENSION 1024 DIST COSINE TYPE F64;


Vector Search Queries (to be used with raw query execution):

-- Semantic search on chunks
SELECT
    id, document_id, text, chunk_index, char_start, char_end, created_at,
    vector::similarity::cosine(embedding, $embedding) AS similarity
FROM chunk
WHERE embedding <|1024,COSINE,{similarity_threshold}|> $embedding
ORDER BY similarity DESC
LIMIT 10;

-- Entity similarity search
SELECT
    id, name, type,
    vector::similarity::cosine(embedding, $embedding) AS similarity
FROM entity
WHERE embedding <|1024,COSINE,{similarity_threshold}|> $embedding
ORDER BY similarity DESC
LIMIT 10;

-- Claim similarity search
SELECT
    id, text, source,
    vector::similarity::cosine(embedding, $embedding) AS similarity
FROM claim
WHERE embedding <|1024,COSINE,{similarity_threshold}|> $embedding
ORDER BY similarity DESC
LIMIT 10;

Note: Vector search query helpers are not yet implemented in Reverie.
Use the query() method directly for vector similarity searches.
"""


# Export schema definitions
__all__ = [
  'chunk',
  'entity',
  'claim',
  'openai_chunks',
  'image_features',
  'user_preferences',
]
