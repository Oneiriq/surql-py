"""Full-text search analyzer definitions (``DEFINE ANALYZER``).

A SurrealDB full-text index references an *analyzer* that turns stored text and
query text into comparable tokens -- the lexical side of hybrid (sparse + dense)
retrieval. An analyzer is a tokenizer chain (how the text is split) followed by a
filter chain (how each token is normalised).

This module renders the ``DEFINE ANALYZER`` statement from a typed
:class:`AnalyzerDefinition`, so callers define the analyzer in code rather than
hand-authoring SurrealQL -- exactly as
:func:`~surql.schema.table.table_schema` does for tables. Pair it with a BM25
:func:`~surql.schema.table.bm25_index` and the
:meth:`~surql.query.builder.Query.full_text_search` query builder for end-to-end
lexical recall.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict


class Tokenizer(Enum):
  """Tokenizer that splits text into terms before the filter chain runs.

  Each value is the lowercase SurrealQL keyword used inside the
  ``TOKENIZERS ...`` clause.
  """

  BLANK = 'blank'
  """Split on whitespace (``blank``)."""
  CAMEL = 'camel'
  """Split on case transitions (``camelCase`` -> ``camel``, ``Case``)."""
  CLASS = 'class'
  """Split on Unicode character-class transitions -- letters, digits, and
  punctuation become separate tokens (``class``). The general-purpose default
  for prose and identifiers."""
  PUNCT = 'punct'
  """Split on punctuation (``punct``)."""


class TokenFilterType(Enum):
  """Discriminant for the kinds of token filter a :class:`TokenFilter` can be."""

  ASCII = 'ascii'
  """Fold accented / Unicode characters to their nearest ASCII equivalent."""
  LOWERCASE = 'lowercase'
  """Lowercase every token."""
  UPPERCASE = 'uppercase'
  """Uppercase every token."""
  EDGE_NGRAM = 'edgengram'
  """Emit edge n-grams (prefixes) for prefix / typeahead matching."""
  NGRAM = 'ngram'
  """Emit n-grams of a length range."""
  SNOWBALL = 'snowball'
  """Reduce each token to its Snowball stem for a given language."""


class TokenFilter(BaseModel):
  """A token filter that normalises or expands each token after tokenization.

  Filters run in declaration order; each renders as the SurrealQL keyword (or
  parameterised call) used inside the ``FILTERS ...`` clause. Construct one with
  the :func:`ascii_filter`, :func:`lowercase`, :func:`uppercase`,
  :func:`edge_ngram`, :func:`ngram`, or :func:`snowball` free functions rather
  than instantiating this model directly.

  Examples:
    >>> snowball('english').to_surql()
    'snowball(english)'
    >>> edge_ngram(2, 10).to_surql()
    'edgengram(2,10)'
  """

  kind: TokenFilterType
  language: str | None = None
  """Snowball stemmer language (only set for ``SNOWBALL``)."""
  min: int | None = None
  """Lower bound for ``EDGE_NGRAM`` / ``NGRAM`` token lengths."""
  max: int | None = None
  """Upper bound for ``EDGE_NGRAM`` / ``NGRAM`` token lengths."""

  model_config = ConfigDict(frozen=True)

  def to_surql(self) -> str:
    """Render as the SurrealQL filter keyword / call.

    Returns:
      The SurrealQL fragment for this filter (e.g. ``lowercase``,
      ``edgengram(2,10)``, ``snowball(english)``).
    """
    if self.kind == TokenFilterType.EDGE_NGRAM:
      return f'edgengram({self.min},{self.max})'
    if self.kind == TokenFilterType.NGRAM:
      return f'ngram({self.min},{self.max})'
    if self.kind == TokenFilterType.SNOWBALL:
      return f'snowball({self.language})'
    return str(self.kind.value)


def ascii_filter() -> TokenFilter:
  """Create an ``ascii`` token filter (fold Unicode to nearest ASCII).

  Returns:
    Immutable :class:`TokenFilter` for the ``ascii`` filter.
  """
  return TokenFilter(kind=TokenFilterType.ASCII)


def lowercase() -> TokenFilter:
  """Create a ``lowercase`` token filter.

  Returns:
    Immutable :class:`TokenFilter` for the ``lowercase`` filter.
  """
  return TokenFilter(kind=TokenFilterType.LOWERCASE)


def uppercase() -> TokenFilter:
  """Create an ``uppercase`` token filter.

  Returns:
    Immutable :class:`TokenFilter` for the ``uppercase`` filter.
  """
  return TokenFilter(kind=TokenFilterType.UPPERCASE)


def edge_ngram(min: int, max: int) -> TokenFilter:
  """Create an ``edgengram(min,max)`` token filter.

  Args:
    min: Smallest prefix length to emit.
    max: Largest prefix length to emit.

  Returns:
    Immutable :class:`TokenFilter` for the ``edgengram`` filter.
  """
  return TokenFilter(kind=TokenFilterType.EDGE_NGRAM, min=min, max=max)


def ngram(min: int, max: int) -> TokenFilter:
  """Create an ``ngram(min,max)`` token filter.

  Args:
    min: Smallest n-gram length to emit.
    max: Largest n-gram length to emit.

  Returns:
    Immutable :class:`TokenFilter` for the ``ngram`` filter.
  """
  return TokenFilter(kind=TokenFilterType.NGRAM, min=min, max=max)


def snowball(language: str) -> TokenFilter:
  """Create a ``snowball(language)`` stemming token filter.

  Args:
    language: Snowball stemmer language, e.g. ``'english'``.

  Returns:
    Immutable :class:`TokenFilter` for the ``snowball`` filter.
  """
  return TokenFilter(kind=TokenFilterType.SNOWBALL, language=language)


class AnalyzerDefinition(BaseModel):
  """Immutable ``DEFINE ANALYZER`` schema definition.

  A named tokenizer + filter chain referenced by a full-text
  :func:`~surql.schema.table.search_index`.

  Examples:
    >>> a = analyzer('text_en').with_tokenizer(Tokenizer.CLASS).with_filters(
    ...   [lowercase(), ascii_filter(), snowball('english')]
    ... )
    >>> a.to_surql()
    'DEFINE ANALYZER text_en TOKENIZERS class FILTERS lowercase,ascii,snowball(english);'
  """

  name: str
  tokenizers: list[Tokenizer] = []
  filters: list[TokenFilter] = []

  model_config = ConfigDict(frozen=True)

  def with_tokenizer(self, tokenizer: Tokenizer) -> AnalyzerDefinition:
    """Return a copy with one tokenizer appended.

    Args:
      tokenizer: Tokenizer to append.

    Returns:
      New :class:`AnalyzerDefinition` with the tokenizer added.
    """
    return self.model_copy(update={'tokenizers': [*self.tokenizers, tokenizer]})

  def with_tokenizers(self, tokenizers: list[Tokenizer]) -> AnalyzerDefinition:
    """Return a copy with several tokenizers appended.

    Args:
      tokenizers: Tokenizers to append, in order.

    Returns:
      New :class:`AnalyzerDefinition` with the tokenizers added.
    """
    return self.model_copy(update={'tokenizers': [*self.tokenizers, *tokenizers]})

  def with_filter(self, token_filter: TokenFilter) -> AnalyzerDefinition:
    """Return a copy with one filter appended.

    Args:
      token_filter: Filter to append.

    Returns:
      New :class:`AnalyzerDefinition` with the filter added.
    """
    return self.model_copy(update={'filters': [*self.filters, token_filter]})

  def with_filters(self, filters: list[TokenFilter]) -> AnalyzerDefinition:
    """Return a copy with several filters appended.

    Args:
      filters: Filters to append, in order.

    Returns:
      New :class:`AnalyzerDefinition` with the filters added.
    """
    return self.model_copy(update={'filters': [*self.filters, *filters]})

  def validate_definition(self) -> None:
    """Validate the analyzer definition.

    Raises:
      ValueError: When the analyzer name is empty.
    """
    if not self.name:
      raise ValueError('Analyzer name cannot be empty')

  def to_surql(self) -> str:
    """Render the ``DEFINE ANALYZER`` statement.

    Returns:
      The ``DEFINE ANALYZER ...`` SurrealQL statement.
    """
    return self.to_surql_with_options(if_not_exists=False)

  def to_surql_with_options(self, *, if_not_exists: bool = False) -> str:
    """Render the ``DEFINE ANALYZER`` statement, optionally with ``IF NOT EXISTS``.

    ``IF NOT EXISTS`` lets the statement be re-applied idempotently (e.g. a
    persistent store applying its schema on every connect). Empty tokenizer /
    filter chains omit their clause entirely.

    Args:
      if_not_exists: When True, inserts ``IF NOT EXISTS`` after ``DEFINE ANALYZER``.

    Returns:
      The rendered SurrealQL statement.
    """
    ine = 'IF NOT EXISTS ' if if_not_exists else ''
    sql = f'DEFINE ANALYZER {ine}{self.name}'
    if self.tokenizers:
      toks = ','.join(t.value for t in self.tokenizers)
      sql += f' TOKENIZERS {toks}'
    if self.filters:
      filters = ','.join(f.to_surql() for f in self.filters)
      sql += f' FILTERS {filters}'
    sql += ';'
    return sql


def analyzer(name: str) -> AnalyzerDefinition:
  """Create an empty :class:`AnalyzerDefinition` (no tokenizers or filters yet).

  Args:
    name: Analyzer name, referenced by a full-text index's ``ANALYZER <name>``.

  Returns:
    Immutable :class:`AnalyzerDefinition` ready for ``with_*`` chaining.
  """
  return AnalyzerDefinition(name=name)


def standard_analyzer(name: str) -> AnalyzerDefinition:
  """Create a sensible general-purpose analyzer for BM25 lexical recall.

  The ``class`` tokenizer with ``lowercase`` + ``ascii`` filters. Add
  :func:`snowball` for language-specific stemming.

  Args:
    name: Analyzer name.

  Returns:
    Immutable :class:`AnalyzerDefinition` configured as ``class`` +
    ``lowercase`` + ``ascii``.
  """
  return (
    AnalyzerDefinition(name=name)
    .with_tokenizer(Tokenizer.CLASS)
    .with_filters([lowercase(), ascii_filter()])
  )
