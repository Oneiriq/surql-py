"""Table schema definition functions.

This module provides functions for defining table schemas with fields, indexes,
permissions, and events.
"""

from enum import Enum

from pydantic import BaseModel, ConfigDict, model_validator

from surql.schema.fields import FieldDefinition


class TableMode(Enum):
  """Table schema modes.

  Defines whether a table enforces strict schema validation.
  """

  SCHEMAFULL = 'SCHEMAFULL'
  SCHEMALESS = 'SCHEMALESS'
  DROP = 'DROP'


class IndexType(Enum):
  """Index types for table fields.

  Defines the type of index to create on table fields.
  """

  UNIQUE = 'UNIQUE'
  SEARCH = 'SEARCH'
  """Full-text index. Renders the SurrealDB 3.x ``FULLTEXT`` keyword (the v1/v2
  ``SEARCH`` spelling was renamed in 3.0); pair with an analyzer + BM25 via
  :func:`bm25_index` for scorable lexical recall."""
  STANDARD = 'INDEX'
  MTREE = 'MTREE'
  HNSW = 'HNSW'
  DISKANN = 'DISKANN'
  """On-disk approximate-nearest-neighbour graph (SurrealDB 3.2+). The graph
  lives on disk rather than in memory, so an index outgrows RAM without
  outgrowing the box; build it with :func:`diskann_index`."""


DISKANN_DEFAULT_DEGREE = 64
"""Graph out-degree the engine assumes (and echoes) for a DISKANN index that
never stated ``DEGREE``."""

DISKANN_DEFAULT_L_BUILD = 100
"""Build-time candidate list size the engine assumes (and echoes) for a DISKANN
index that never stated ``L_BUILD``."""

DISKANN_DEFAULT_ALPHA = '1.2'
"""Pruning slack the engine assumes (and echoes) for a DISKANN index that never
stated ``ALPHA``."""


class MTreeDistanceType(Enum):
  """Distance metric types for MTREE vector indexes.

  Defines the distance metric used for vector similarity search.
  """

  COSINE = 'COSINE'
  EUCLIDEAN = 'EUCLIDEAN'
  MANHATTAN = 'MANHATTAN'
  MINKOWSKI = 'MINKOWSKI'


class HnswDistanceType(Enum):
  """Distance metric types for HNSW vector indexes.

  Defines the distance metric used for HNSW vector similarity search.
  This is a superset of MTreeDistanceType with additional metrics.
  """

  CHEBYSHEV = 'CHEBYSHEV'
  COSINE = 'COSINE'
  EUCLIDEAN = 'EUCLIDEAN'
  HAMMING = 'HAMMING'
  JACCARD = 'JACCARD'
  MANHATTAN = 'MANHATTAN'
  MINKOWSKI = 'MINKOWSKI'
  PEARSON = 'PEARSON'


class DiskAnnDistanceType(Enum):
  """Distance metric types for DISKANN vector indexes.

  Its own enum rather than a reuse of :class:`HnswDistanceType`: the engine's
  DISKANN set both adds metrics HNSW lacks (``INNER_PRODUCT``,
  ``COSINE_NORMALIZED``) and refuses every HNSW metric outside it, so an
  out-of-set metric is unrepresentable here.
  """

  COSINE = 'COSINE'
  COSINE_NORMALIZED = 'COSINE_NORMALIZED'
  EUCLIDEAN = 'EUCLIDEAN'
  INNER_PRODUCT = 'INNER_PRODUCT'


class MTreeVectorType(Enum):
  """Numeric type for vector components in MTREE, HNSW, and DISKANN indexes.

  One shared vocabulary; each index kind accepts a subset. The engine takes
  every member for HNSW, refuses ``F16`` / ``I8`` / ``U8`` for MTREE, and
  refuses everything but ``F32`` / ``F16`` / ``I8`` / ``U8`` for DISKANN.
  :func:`surql.schema.validator.validate_index` teaches those limits before a
  statement is sent.
  """

  F64 = 'F64'
  F32 = 'F32'
  F16 = 'F16'
  I64 = 'I64'
  I32 = 'I32'
  I16 = 'I16'
  I8 = 'I8'
  U8 = 'U8'


_MTREE_REFUSED_TYPES = frozenset({MTreeVectorType.F16, MTreeVectorType.I8, MTreeVectorType.U8})
"""Element types MTREE refuses; the engine answers with a bare parse error."""

_DISKANN_ALLOWED_TYPES = frozenset(
  {
    MTreeVectorType.F32,
    MTreeVectorType.F16,
    MTreeVectorType.I8,
    MTreeVectorType.U8,
  }
)
"""The only element types DISKANN accepts."""


class IndexDefinition(BaseModel):
  """Immutable index definition.

  Represents an index on one or more fields in a table.

  Examples:
    Standard index:
    >>> idx = IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE)

    MTREE vector index:
    >>> idx = IndexDefinition(
    ...   name='embedding_idx',
    ...   columns=['embedding'],
    ...   type=IndexType.MTREE,
    ...   dimension=1536,
    ...   distance=MTreeDistanceType.COSINE,
    ...   vector_type=MTreeVectorType.F32
    ... )
  """

  name: str
  columns: list[str]
  type: IndexType = IndexType.STANDARD
  # MTREE-specific parameters
  dimension: int | None = None
  distance: MTreeDistanceType | None = None
  vector_type: MTreeVectorType | None = None
  # HNSW-specific parameters
  hnsw_distance: HnswDistanceType | None = None
  efc: int | None = None
  m: int | None = None
  # DISKANN-specific parameters
  diskann_distance: DiskAnnDistanceType | None = None
  degree: int | None = None
  """DISKANN graph out-degree (``DEGREE``, engine default 64)."""
  l_build: int | None = None
  """DISKANN build-time candidate list size (``L_BUILD``, engine default 100)."""
  alpha: str | None = None
  """DISKANN pruning slack (``ALPHA``, engine default 1.2), held as the decimal
  literal the statement carries. The engine echoes a float literal with a
  trailing ``f`` suffix (``ALPHA 1.2f``), which the parser strips so code and
  echo compare equal."""
  hashed_vector: bool = False
  """Whether a DISKANN index stores hashed vectors (``HASHED_VECTOR``)."""
  # Full-text (FULLTEXT) parameters
  analyzer: str | None = None
  """Full-text analyzer name. ``None`` renders the historical default (``ascii``)."""
  bm25: bool = False
  """Whether a full-text index emits the ``BM25`` relevance-scoring clause.

  Required for :meth:`~surql.query.builder.Query.search_score` to return a
  value. Uses the engine's default ``(k1, b)`` parameters."""
  highlights: bool = False
  """Whether a full-text index stores positional ``HIGHLIGHTS`` data (enables
  ``search::highlight`` / ``search::offsets``)."""

  model_config = ConfigDict(frozen=True)

  @model_validator(mode='after')
  def _validate_vector_members(self) -> 'IndexDefinition':
    """Refuse the vector member combinations the engine refuses.

    Probed against SurrealDB 3.2.4:

    - MTREE parses only ``F64`` / ``F32`` / ``I64`` / ``I32`` / ``I16``
      element types; ``F16`` / ``I8`` / ``U8`` are a parse error.
    - DISKANN accepts only ``F32`` / ``F16`` / ``I8`` / ``U8``.
    - DISKANN takes its metric through ``diskann_distance``; an MTREE or HNSW
      metric aimed at it would be dropped by the emitter, so that mistake is
      refused here instead.

    HNSW accepts every :class:`MTreeVectorType` member, so it needs no check.
    """
    if self.type == IndexType.MTREE and self.vector_type in _MTREE_REFUSED_TYPES:
      raise ValueError(
        f'MTREE index {self.name!r} cannot use TYPE {self.vector_type.value}: '
        'the engine only accepts F64, F32, I64, I32, or I16 for MTREE'
      )
    if self.type == IndexType.DISKANN:
      if self.vector_type is not None and self.vector_type not in _DISKANN_ALLOWED_TYPES:
        raise ValueError(
          f'DISKANN index {self.name!r} cannot use TYPE {self.vector_type.value}: '
          'the engine only accepts F32, F16, I8, or U8 for DISKANN'
        )
      if self.distance is not None or self.hnsw_distance is not None:
        raise ValueError(
          f'DISKANN index {self.name!r} takes its metric through diskann_distance '
          '(EUCLIDEAN, COSINE, INNER_PRODUCT, or COSINE_NORMALIZED); the engine '
          'refuses every other MTREE/HNSW metric for DISKANN'
        )
    return self


class EventDefinition(BaseModel):
  """Immutable event/trigger definition.

  Represents a database event that executes when a condition is met.

  Examples:
    >>> event = EventDefinition(
    ...   name='email_changed',
    ...   condition='$before.email != $after.email',
    ...   action='CREATE audit_log SET ...'
    ... )
  """

  name: str
  condition: str
  action: str

  model_config = ConfigDict(frozen=True)


class TableDefinition(BaseModel):
  """Immutable table schema definition.

  Represents a complete table schema with fields, indexes, permissions, and events.

  Examples:
    >>> table = TableDefinition(
    ...   name='user',
    ...   mode=TableMode.SCHEMAFULL,
    ...   fields=[
    ...     FieldDefinition(name='email', type=FieldType.STRING),
    ...   ],
    ...   indexes=[
    ...     IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE),
    ...   ]
    ... )
  """

  name: str
  mode: TableMode = TableMode.SCHEMAFULL
  fields: list[FieldDefinition] = []
  indexes: list[IndexDefinition] = []
  events: list[EventDefinition] = []
  permissions: dict[str, str] | None = None
  drop: bool = False

  model_config = ConfigDict(frozen=True)


# Table builder functions


def table_schema(
  name: str,
  *,
  mode: TableMode = TableMode.SCHEMAFULL,
  fields: list[FieldDefinition] | None = None,
  indexes: list[IndexDefinition] | None = None,
  events: list[EventDefinition] | None = None,
  permissions: dict[str, str] | None = None,
  drop: bool = False,
) -> TableDefinition:
  """Create a table schema definition.

  Pure function to create an immutable table definition.

  Args:
    name: Table name
    mode: Schema mode (SCHEMAFULL, SCHEMALESS, or DROP)
    fields: List of field definitions
    indexes: List of index definitions
    events: List of event definitions
    permissions: Dict of permission rules (select, create, update, delete)
    drop: If True, marks table for deletion

  Returns:
    Immutable TableDefinition instance

  Examples:
    Basic table:
    >>> table = table_schema('user')

    Table with fields and indexes:
    >>> table = table_schema(
    ...   'user',
    ...   mode=TableMode.SCHEMAFULL,
    ...   fields=[
    ...     string_field('email'),
    ...     int_field('age'),
    ...   ],
    ...   indexes=[
    ...     index('email_idx', ['email'], IndexType.UNIQUE),
    ...   ]
    ... )
  """
  return TableDefinition(
    name=name,
    mode=mode,
    fields=fields or [],
    indexes=indexes or [],
    events=events or [],
    permissions=permissions,
    drop=drop,
  )


def index(
  name: str,
  columns: list[str],
  index_type: IndexType = IndexType.STANDARD,
) -> IndexDefinition:
  """Create an index definition.

  Pure function to create an immutable index definition.

  Args:
    name: Index name
    columns: List of column names to index
    index_type: Type of index (UNIQUE, SEARCH, or STANDARD)

  Returns:
    Immutable IndexDefinition instance

  Examples:
    >>> index('email_idx', ['email'], IndexType.UNIQUE)
    IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE)

    >>> index('name_search', ['name.first', 'name.last'], IndexType.SEARCH)
    IndexDefinition(name='name_search', columns=['name.first', 'name.last'], type=IndexType.SEARCH)
  """
  return IndexDefinition(
    name=name,
    columns=columns,
    type=index_type,
  )


def unique_index(
  name: str,
  columns: list[str],
) -> IndexDefinition:
  """Create a unique index definition.

  Convenience function for creating unique indexes.

  Args:
    name: Index name
    columns: List of column names to index

  Returns:
    Immutable IndexDefinition with UNIQUE type

  Examples:
    >>> unique_index('email_idx', ['email'])
    IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE)
  """
  return index(name, columns, IndexType.UNIQUE)


def search_index(
  name: str,
  columns: list[str],
  *,
  analyzer: str | None = None,
  bm25: bool = False,
  highlights: bool = False,
) -> IndexDefinition:
  """Create a full-text (``FULLTEXT``) search index definition.

  With no analyzer set it renders the historical ``ascii`` default; set
  ``analyzer`` / ``bm25`` / ``highlights`` for a scorable index, or use
  :func:`bm25_index`.

  Args:
    name: Index name
    columns: List of column names to index
    analyzer: Full-text analyzer name (e.g. one defined via
      :func:`~surql.schema.analyzer.analyzer`). When ``None`` the index renders
      the historical ``ascii`` analyzer.
    bm25: Emit the ``BM25`` relevance-scoring clause (engine defaults). Required
      for :meth:`~surql.query.builder.Query.search_score`.
    highlights: Store positional ``HIGHLIGHTS`` data.

  Returns:
    Immutable IndexDefinition with SEARCH type

  Examples:
    >>> search_index('content_search', ['title', 'content'])
    IndexDefinition(name='content_search', columns=['title', 'content'], type=IndexType.SEARCH)
  """
  return IndexDefinition(
    name=name,
    columns=columns,
    type=IndexType.SEARCH,
    analyzer=analyzer,
    bm25=bm25,
    highlights=highlights,
  )


def bm25_index(
  name: str,
  columns: list[str],
  analyzer: str,
) -> IndexDefinition:
  """Create a BM25-scored full-text (``FULLTEXT``) index over ``columns``.

  Analyzed by ``analyzer``. This is the index to pair with
  :meth:`~surql.query.builder.Query.full_text_search` and
  :meth:`~surql.query.builder.Query.search_score` for lexical recall -- BM25 is
  what makes ``search::score`` return a relevance value.

  Args:
    name: Index name
    columns: List of column names to index
    analyzer: Full-text analyzer name (define it separately via
      :func:`~surql.schema.analyzer.analyzer` /
      :func:`~surql.schema.sql.generate_analyzer_sql`).

  Returns:
    Immutable IndexDefinition with SEARCH type, the analyzer set, and BM25 on.

  Examples:
    >>> idx = bm25_index('content_bm25', ['content'], 'text_en')
    >>> idx.type == IndexType.SEARCH and idx.bm25
    True
  """
  return IndexDefinition(
    name=name,
    columns=columns,
    type=IndexType.SEARCH,
    analyzer=analyzer,
    bm25=True,
  )


def mtree_index(
  name: str,
  column: str,
  dimension: int,
  *,
  distance: MTreeDistanceType = MTreeDistanceType.EUCLIDEAN,
  vector_type: MTreeVectorType = MTreeVectorType.F64,
) -> IndexDefinition:
  """Create an MTREE vector index definition.

  Convenience function for creating MTREE vector similarity search indexes.

  Args:
    name: Index name
    column: Column name containing the vector data
    dimension: Number of dimensions in the vector
    distance: Distance metric (COSINE, EUCLIDEAN, MANHATTAN, MINKOWSKI)
    vector_type: Vector component data type (F64, F32, I64, I32, I16)

  Returns:
    Immutable IndexDefinition with MTREE type

  Examples:
    OpenAI embeddings with cosine similarity:
    >>> mtree_index('embedding_idx', 'embedding', 1536, distance=MTreeDistanceType.COSINE, vector_type=MTreeVectorType.F32)

    Custom vector with Euclidean distance:
    >>> mtree_index('feature_idx', 'features', 128)
  """
  return IndexDefinition(
    name=name,
    columns=[column],
    type=IndexType.MTREE,
    dimension=dimension,
    distance=distance,
    vector_type=vector_type,
  )


def hnsw_index(
  name: str,
  column: str,
  dimension: int,
  *,
  distance: HnswDistanceType = HnswDistanceType.EUCLIDEAN,
  vector_type: MTreeVectorType = MTreeVectorType.F64,
  efc: int | None = None,
  m: int | None = None,
) -> IndexDefinition:
  """Create an HNSW vector index definition.

  Convenience function for creating HNSW vector similarity search indexes.

  Args:
    name: Index name
    column: Column name containing the vector data
    dimension: Number of dimensions in the vector
    distance: Distance metric (CHEBYSHEV, COSINE, EUCLIDEAN, HAMMING, JACCARD,
      MANHATTAN, MINKOWSKI, PEARSON)
    vector_type: Vector component data type (F64, F32, I64, I32, I16)
    efc: Exploration factor during construction (default: SurrealDB default 150)
    m: Max bidirectional links per node (default: SurrealDB default 12)

  Returns:
    Immutable IndexDefinition with HNSW type

  Examples:
    OpenAI embeddings with cosine similarity:
    >>> hnsw_index('embedding_idx', 'embedding', 1536, distance=HnswDistanceType.COSINE, vector_type=MTreeVectorType.F32)

    With tuning parameters:
    >>> hnsw_index('feature_idx', 'features', 128, efc=500, m=16)
  """
  return IndexDefinition(
    name=name,
    columns=[column],
    type=IndexType.HNSW,
    dimension=dimension,
    vector_type=vector_type,
    hnsw_distance=distance,
    efc=efc,
    m=m,
  )


def canonical_alpha(alpha: float) -> str:
  """Render a DISKANN ``ALPHA`` value the way the engine echoes it.

  A whole number echoes bare (``ALPHA 2``) and a fractional one echoes as a
  float literal with a trailing ``f`` the parser strips (``ALPHA 1.2f`` reads
  back as ``1.2``). Producing that same shape here is what lets a definition
  compare equal to its own echo instead of re-applying on every reconcile.

  Args:
    alpha: Pruning slack

  Returns:
    The canonical decimal literal

  Examples:
    >>> canonical_alpha(1.2)
    '1.2'
    >>> canonical_alpha(2.0)
    '2'
  """
  if alpha == int(alpha):
    return str(int(alpha))
  return str(alpha)


def diskann_index(
  name: str,
  column: str,
  dimension: int,
  *,
  distance: DiskAnnDistanceType = DiskAnnDistanceType.EUCLIDEAN,
  vector_type: MTreeVectorType = MTreeVectorType.F32,
  degree: int = DISKANN_DEFAULT_DEGREE,
  l_build: int = DISKANN_DEFAULT_L_BUILD,
  alpha: float = 1.2,
  hashed_vector: bool = False,
) -> IndexDefinition:
  """Create a DISKANN vector index definition.

  DISKANN keeps its graph on disk, which suits a corpus that outgrows the
  memory an HNSW graph would need. The engine echoes ``DEGREE`` / ``L_BUILD`` /
  ``ALPHA`` back with defaults filled in even when the definition never stated
  them, so this fills the same defaults up front.

  Args:
    name: Index name
    column: Column name holding the vector data
    dimension: Number of dimensions in the vector
    distance: Distance metric (COSINE, COSINE_NORMALIZED, EUCLIDEAN, INNER_PRODUCT)
    vector_type: Vector component type (F32, F16, I8, U8)
    degree: Graph out-degree
    l_build: Build-time candidate list size
    alpha: Pruning slack
    hashed_vector: Store hashed vectors

  Returns:
    Immutable IndexDefinition with DISKANN type

  Examples:
    Half-precision embeddings with cosine distance:
    >>> diskann_index('embedding_idx', 'embedding', 1024, distance=DiskAnnDistanceType.COSINE, vector_type=MTreeVectorType.F16)

    With a tuned graph:
    >>> diskann_index('feature_idx', 'features', 128, degree=48, l_build=90, alpha=1.5)
  """
  return IndexDefinition(
    name=name,
    columns=[column],
    type=IndexType.DISKANN,
    dimension=dimension,
    vector_type=vector_type,
    diskann_distance=distance,
    degree=degree,
    l_build=l_build,
    alpha=canonical_alpha(alpha),
    hashed_vector=hashed_vector,
  )


def event(
  name: str,
  condition: str,
  action: str,
) -> EventDefinition:
  """Create an event/trigger definition.

  Pure function to create an immutable event definition.

  Args:
    name: Event name
    condition: SurrealQL condition expression that triggers the event
    action: SurrealQL statements to execute when triggered

  Returns:
    Immutable EventDefinition instance

  Examples:
    >>> event(
    ...   'email_changed',
    ...   '$before.email != $after.email',
    ...   'CREATE audit_log SET user = $value.id, changed_at = time::now()'
    ... )
    EventDefinition(name='email_changed', condition='$before.email != $after.email', action='...')
  """
  return EventDefinition(
    name=name,
    condition=condition,
    action=action,
  )


# Functional composition helpers


def with_fields(
  table: TableDefinition,
  *fields: FieldDefinition,
) -> TableDefinition:
  """Add fields to a table definition.

  Pure function that returns a new table with additional fields.

  Args:
    table: Existing table definition
    fields: Field definitions to add

  Returns:
    New TableDefinition with added fields

  Examples:
    >>> table = table_schema('user')
    >>> table = with_fields(
    ...   table,
    ...   string_field('email'),
    ...   int_field('age'),
    ... )
  """
  return table.model_copy(update={'fields': [*table.fields, *fields]})


def with_indexes(
  table: TableDefinition,
  *indexes: IndexDefinition,
) -> TableDefinition:
  """Add indexes to a table definition.

  Pure function that returns a new table with additional indexes.

  Args:
    table: Existing table definition
    indexes: Index definitions to add

  Returns:
    New TableDefinition with added indexes

  Examples:
    >>> table = table_schema('user', fields=[string_field('email')])
    >>> table = with_indexes(
    ...   table,
    ...   unique_index('email_idx', ['email']),
    ... )
  """
  return table.model_copy(update={'indexes': [*table.indexes, *indexes]})


def with_events(
  table: TableDefinition,
  *events: EventDefinition,
) -> TableDefinition:
  """Add events to a table definition.

  Pure function that returns a new table with additional events.

  Args:
    table: Existing table definition
    events: Event definitions to add

  Returns:
    New TableDefinition with added events

  Examples:
    >>> table = table_schema('user')
    >>> table = with_events(
    ...   table,
    ...   event('email_changed', '$before.email != $after.email', '...'),
    ... )
  """
  return table.model_copy(update={'events': [*table.events, *events]})


def with_permissions(
  table: TableDefinition,
  permissions: dict[str, str],
) -> TableDefinition:
  """Add permissions to a table definition.

  Pure function that returns a new table with permissions.

  Args:
    table: Existing table definition
    permissions: Dict of permission rules (select, create, update, delete)

  Returns:
    New TableDefinition with permissions

  Examples:
    >>> table = table_schema('user')
    >>> table = with_permissions(
    ...   table,
    ...   {
    ...     'select': '$auth.id = id OR $auth.admin = true',
    ...     'update': '$auth.id = id',
    ...     'delete': '$auth.admin = true',
    ...   }
    ... )
  """
  return table.model_copy(update={'permissions': permissions})


def set_mode(
  table: TableDefinition,
  mode: TableMode,
) -> TableDefinition:
  """Set the schema mode for a table.

  Pure function that returns a new table with the specified mode.

  Args:
    table: Existing table definition
    mode: Schema mode to set

  Returns:
    New TableDefinition with updated mode

  Examples:
    >>> table = table_schema('user')
    >>> table = set_mode(table, TableMode.SCHEMALESS)
  """
  return table.model_copy(update={'mode': mode})
