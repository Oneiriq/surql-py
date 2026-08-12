"""Index parsing for DEFINE INDEX statements.

Split out of :mod:`surql.schema.parser` so both modules stay under the
repository's 1000-line budget. Everything here is imported back into
:mod:`surql.schema.parser`, so existing import paths keep resolving.

Covers the ordinary, unique, and full-text forms plus the three vector kinds
(``MTREE``, ``HNSW``, ``DISKANN``).
"""

import re

import structlog

from surql.schema.table import (
  DiskAnnDistanceType,
  HnswDistanceType,
  IndexDefinition,
  IndexType,
  MTreeDistanceType,
  MTreeVectorType,
)

logger = structlog.get_logger(__name__)


def _parse_indexes(ix_dict: dict[str, str]) -> list[IndexDefinition]:
  """Parse index definitions from ix dictionary.

  Args:
    ix_dict: Dictionary of index name to DEFINE INDEX statement

  Returns:
    List of IndexDefinition objects
  """
  indexes = []

  for index_name, definition in ix_dict.items():
    try:
      index_def = _parse_index_definition(index_name, definition)
      if index_def:
        indexes.append(index_def)
    except Exception as e:
      logger.warning('index_parse_warning', index=index_name, error=str(e))

  return indexes


def _parse_index_definition(index_name: str, definition: str) -> IndexDefinition | None:
  """Parse a single index definition.

  Args:
    index_name: Index name
    definition: DEFINE INDEX statement

  Returns:
    IndexDefinition or None if parsing fails
  """
  if not definition:
    return None

  logger.debug('parsing_index', index=index_name, definition=definition)

  # Extract columns
  columns = _extract_index_columns(definition)
  if not columns:
    columns = _extract_index_fields(definition)

  # Determine index type
  index_type = _extract_index_type(definition)

  # For MTREE indexes, extract additional parameters
  dimension = None
  distance = None
  vector_type = None

  if index_type == IndexType.MTREE:
    dimension = _extract_mtree_dimension(definition)
    distance = _extract_mtree_distance(definition)
    vector_type = _extract_mtree_vector_type(definition)

  # For HNSW indexes, extract additional parameters
  hnsw_distance = None
  efc = None
  m = None

  if index_type == IndexType.HNSW:
    dimension = _extract_mtree_dimension(definition)
    vector_type = _extract_mtree_vector_type(definition)
    hnsw_distance = _extract_hnsw_distance(definition)
    efc = _extract_hnsw_efc(definition)
    m = _extract_hnsw_m(definition)

  # For DISKANN indexes, extract the graph parameters the engine echoes.
  diskann_distance = None
  degree = None
  l_build = None
  alpha = None
  hashed_vector = False

  if index_type == IndexType.DISKANN:
    dimension = _extract_mtree_dimension(definition)
    vector_type = _extract_mtree_vector_type(definition)
    diskann_distance = _extract_diskann_distance(definition)
    degree = _extract_diskann_degree(definition)
    l_build = _extract_diskann_l_build(definition)
    alpha = _extract_diskann_alpha(definition)
    hashed_vector = 'HASHED_VECTOR' in definition.upper()

  # For full-text (FULLTEXT / SEARCH) indexes, extract analyzer + flags.
  analyzer = None
  bm25 = False
  highlights = False

  if index_type == IndexType.SEARCH:
    analyzer = _extract_index_analyzer(definition)
    definition_upper = definition.upper()
    bm25 = 'BM25' in definition_upper
    highlights = 'HIGHLIGHTS' in definition_upper

  return IndexDefinition(
    name=index_name,
    columns=columns,
    type=index_type,
    dimension=dimension,
    distance=distance,
    vector_type=vector_type,
    hnsw_distance=hnsw_distance,
    efc=efc,
    m=m,
    diskann_distance=diskann_distance,
    degree=degree,
    l_build=l_build,
    alpha=alpha,
    hashed_vector=hashed_vector,
    analyzer=analyzer,
    bm25=bm25,
    highlights=highlights,
  )


def _extract_index_columns(definition: str) -> list[str]:
  """Extract COLUMNS from DEFINE INDEX statement.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    List of column names
  """
  # Match COLUMNS followed by comma-separated column names
  columns_pattern = r'COLUMNS\s+([^;]+?)(?:UNIQUE|FULLTEXT|SEARCH|HNSW|MTREE|DISKANN|\s*;|\s*$)'
  match = re.search(columns_pattern, definition, re.IGNORECASE)

  if match:
    columns_str = match.group(1).strip()
    columns = [c.strip() for c in columns_str.split(',')]
    return [c for c in columns if c]

  return []


def _extract_index_fields(definition: str) -> list[str]:
  """Extract FIELDS from DEFINE INDEX statement (alternative syntax).

  Args:
    definition: DEFINE INDEX statement

  Returns:
    List of field names
  """
  # Match FIELDS followed by comma-separated field names
  fields_pattern = r'FIELDS\s+([^;]+?)(?:UNIQUE|FULLTEXT|SEARCH|HNSW|MTREE|DISKANN|\s*;|\s*$)'
  match = re.search(fields_pattern, definition, re.IGNORECASE)

  if match:
    fields_str = match.group(1).strip()
    fields = [f.strip() for f in fields_str.split(',')]
    return [f for f in fields if f]

  return []


def _extract_index_type(definition: str) -> IndexType:
  """Extract index type from DEFINE INDEX statement.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    IndexType enum value
  """
  definition_upper = definition.upper()

  if 'UNIQUE' in definition_upper:
    return IndexType.UNIQUE
  # SurrealDB 3.x renamed `SEARCH` to `FULLTEXT`; accept both spellings.
  if 'FULLTEXT' in definition_upper or 'SEARCH' in definition_upper:
    return IndexType.SEARCH
  if 'HNSW' in definition_upper:
    return IndexType.HNSW
  if 'MTREE' in definition_upper:
    return IndexType.MTREE
  if 'DISKANN' in definition_upper:
    return IndexType.DISKANN

  return IndexType.STANDARD


def _extract_index_analyzer(definition: str) -> str | None:
  """Extract the ``ANALYZER <name>`` from a full-text index definition.

  The historical ``ascii`` default (what a plain :func:`search_index` renders)
  normalises back to ``None`` so a round-trip of the default form is an
  identity, leaving an explicit non-``ascii`` analyzer as a string.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    Analyzer name, or ``None`` when absent or equal to the ``ascii`` default
  """
  match = re.search(r'ANALYZER\s+(\w+)', definition, re.IGNORECASE)
  if not match:
    return None
  analyzer = match.group(1)
  if analyzer.lower() == 'ascii':
    return None
  return analyzer


def _extract_mtree_dimension(definition: str) -> int | None:
  """Extract DIMENSION from a vector index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    Dimension value or None
  """
  dim_pattern = r'DIMENSION\s+(\d+)'
  match = re.search(dim_pattern, definition, re.IGNORECASE)

  if match:
    return int(match.group(1))

  return None


def _extract_mtree_distance(definition: str) -> MTreeDistanceType | None:
  """Extract DIST/DISTANCE from MTREE index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    MTreeDistanceType or None
  """
  dist_pattern = r'(?:DIST|DISTANCE)\s+(\w+)'
  match = re.search(dist_pattern, definition, re.IGNORECASE)

  if not match:
    return None

  dist_str = match.group(1).upper()

  distance_mapping = {
    'COSINE': MTreeDistanceType.COSINE,
    'EUCLIDEAN': MTreeDistanceType.EUCLIDEAN,
    'MANHATTAN': MTreeDistanceType.MANHATTAN,
    'MINKOWSKI': MTreeDistanceType.MINKOWSKI,
  }

  return distance_mapping.get(dist_str)


def _extract_mtree_vector_type(definition: str) -> MTreeVectorType | None:
  """Extract TYPE from a vector index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    MTreeVectorType or None
  """
  # The vector component type, shared by MTREE, HNSW, and DISKANN.
  type_pattern = r'TYPE\s+(\w+)'
  match = re.search(type_pattern, definition, re.IGNORECASE)

  if not match:
    return None

  type_str = match.group(1).upper()

  type_mapping = {
    'F64': MTreeVectorType.F64,
    'F32': MTreeVectorType.F32,
    'F16': MTreeVectorType.F16,
    'I64': MTreeVectorType.I64,
    'I32': MTreeVectorType.I32,
    'I16': MTreeVectorType.I16,
    'I8': MTreeVectorType.I8,
    'U8': MTreeVectorType.U8,
  }

  return type_mapping.get(type_str)


def _extract_hnsw_distance(definition: str) -> HnswDistanceType | None:
  """Extract DIST/DISTANCE from HNSW index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    HnswDistanceType or None
  """
  dist_pattern = r'(?:DIST|DISTANCE)\s+(\w+)'
  match = re.search(dist_pattern, definition, re.IGNORECASE)

  if not match:
    return None

  dist_str = match.group(1).upper()

  distance_mapping = {
    'CHEBYSHEV': HnswDistanceType.CHEBYSHEV,
    'COSINE': HnswDistanceType.COSINE,
    'EUCLIDEAN': HnswDistanceType.EUCLIDEAN,
    'HAMMING': HnswDistanceType.HAMMING,
    'JACCARD': HnswDistanceType.JACCARD,
    'MANHATTAN': HnswDistanceType.MANHATTAN,
    'MINKOWSKI': HnswDistanceType.MINKOWSKI,
    'PEARSON': HnswDistanceType.PEARSON,
  }

  return distance_mapping.get(dist_str)


def _extract_hnsw_efc(definition: str) -> int | None:
  """Extract EFC from HNSW index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    EFC value or None
  """
  efc_pattern = r'EFC\s+(\d+)'
  match = re.search(efc_pattern, definition, re.IGNORECASE)

  if match:
    return int(match.group(1))

  return None


def _extract_hnsw_m(definition: str) -> int | None:
  """Extract M from HNSW index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    M value or None
  """
  m_pattern = r'\bM\s+(\d+)'
  match = re.search(m_pattern, definition, re.IGNORECASE)

  if match:
    return int(match.group(1))

  return None


def _extract_diskann_distance(definition: str) -> DiskAnnDistanceType | None:
  """Extract DIST/DISTANCE from DISKANN index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    DiskAnnDistanceType or None
  """
  # COSINE_NORMALIZED and INNER_PRODUCT carry an underscore, so the capture
  # takes word characters rather than stopping at the first letter run.
  dist_pattern = r'(?:DIST|DISTANCE)\s+(\w+)'
  match = re.search(dist_pattern, definition, re.IGNORECASE)

  if not match:
    return None

  dist_str = match.group(1).upper()

  distance_mapping = {
    'COSINE': DiskAnnDistanceType.COSINE,
    'COSINE_NORMALIZED': DiskAnnDistanceType.COSINE_NORMALIZED,
    'EUCLIDEAN': DiskAnnDistanceType.EUCLIDEAN,
    'INNER_PRODUCT': DiskAnnDistanceType.INNER_PRODUCT,
  }

  return distance_mapping.get(dist_str)


def _extract_diskann_degree(definition: str) -> int | None:
  """Extract DEGREE from DISKANN index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    DEGREE value or None
  """
  degree_pattern = r'\bDEGREE\s+(\d+)'
  match = re.search(degree_pattern, definition, re.IGNORECASE)

  if match:
    return int(match.group(1))

  return None


def _extract_diskann_l_build(definition: str) -> int | None:
  """Extract L_BUILD from DISKANN index definition.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    L_BUILD value or None
  """
  l_build_pattern = r'\bL_BUILD\s+(\d+)'
  match = re.search(l_build_pattern, definition, re.IGNORECASE)

  if match:
    return int(match.group(1))

  return None


def _extract_diskann_alpha(definition: str) -> str | None:
  """Extract ALPHA from DISKANN index definition.

  The engine echoes a float literal with a trailing ``f`` suffix
  (``ALPHA 1.2f``) and an integer literal bare (``ALPHA 2``). The capture
  excludes the suffix so the stored value matches what code declares.

  Args:
    definition: DEFINE INDEX statement

  Returns:
    ALPHA literal or None
  """
  alpha_pattern = r'\bALPHA\s+(\d+(?:\.\d+)?)'
  match = re.search(alpha_pattern, definition, re.IGNORECASE)

  if match:
    return match.group(1)

  return None
