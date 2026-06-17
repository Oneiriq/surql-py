"""Tests for full-text analyzer definitions (DEFINE ANALYZER)."""

import pytest

from surql.schema.analyzer import (
  AnalyzerDefinition,
  Tokenizer,
  analyzer,
  ascii_filter,
  edge_ngram,
  lowercase,
  ngram,
  snowball,
  standard_analyzer,
  uppercase,
)
from surql.schema.sql import generate_analyzer_sql, generate_analyzer_sql_with_options


class TestTokenizer:
  """Tokenizer enum renders the lowercase SurrealQL keyword."""

  def test_tokenizer_values(self) -> None:
    """Each tokenizer maps to its SurrealQL keyword."""
    assert Tokenizer.BLANK.value == 'blank'
    assert Tokenizer.CAMEL.value == 'camel'
    assert Tokenizer.CLASS.value == 'class'
    assert Tokenizer.PUNCT.value == 'punct'


class TestTokenFilter:
  """Token filters render their SurrealQL keyword or parameterised call."""

  def test_simple_filters_render(self) -> None:
    """ascii / lowercase / uppercase render as bare keywords."""
    assert ascii_filter().to_surql() == 'ascii'
    assert lowercase().to_surql() == 'lowercase'
    assert uppercase().to_surql() == 'uppercase'

  def test_edge_ngram_renders_with_bounds(self) -> None:
    """edge_ngram renders edgengram(min,max)."""
    assert edge_ngram(2, 10).to_surql() == 'edgengram(2,10)'

  def test_ngram_renders_with_bounds(self) -> None:
    """ngram renders ngram(min,max)."""
    assert ngram(1, 3).to_surql() == 'ngram(1,3)'

  def test_snowball_renders_with_language(self) -> None:
    """snowball renders snowball(language)."""
    assert snowball('english').to_surql() == 'snowball(english)'
    assert snowball('german').to_surql() == 'snowball(german)'

  def test_token_filter_is_frozen(self) -> None:
    """TokenFilter is immutable."""
    f = lowercase()
    with pytest.raises(Exception):  # noqa: B017 - pydantic raises ValidationError on frozen
      f.kind = ascii_filter().kind  # type: ignore[misc]


class TestAnalyzerRendering:
  """AnalyzerDefinition.to_surql renders the DEFINE ANALYZER statement."""

  def test_minimal_renders_name_only(self) -> None:
    """An analyzer with no tokenizers/filters omits both clauses."""
    assert analyzer('plain').to_surql() == 'DEFINE ANALYZER plain;'

  def test_renders_tokenizers_and_filters(self) -> None:
    """Tokenizers and filters render in declaration order, comma-joined."""
    a = (
      analyzer('text_en')
      .with_tokenizers([Tokenizer.CLASS, Tokenizer.CAMEL])
      .with_filters([lowercase(), ascii_filter()])
    )
    assert a.to_surql() == 'DEFINE ANALYZER text_en TOKENIZERS class,camel FILTERS lowercase,ascii;'

  def test_with_snowball_filter(self) -> None:
    """Standard analyzer with a snowball stemmer appended."""
    a = standard_analyzer('text_en').with_filter(snowball('english'))
    assert (
      a.to_surql()
      == 'DEFINE ANALYZER text_en TOKENIZERS class FILTERS lowercase,ascii,snowball(english);'
    )

  def test_if_not_exists(self) -> None:
    """to_surql_with_options inserts IF NOT EXISTS for idempotent re-apply."""
    a = standard_analyzer('std')
    assert (
      a.to_surql_with_options(if_not_exists=True)
      == 'DEFINE ANALYZER IF NOT EXISTS std TOKENIZERS class FILTERS lowercase,ascii;'
    )

  def test_with_tokenizer_appends_single(self) -> None:
    """with_tokenizer appends one tokenizer."""
    a = analyzer('a').with_tokenizer(Tokenizer.BLANK).with_tokenizer(Tokenizer.PUNCT)
    assert a.tokenizers == [Tokenizer.BLANK, Tokenizer.PUNCT]


class TestStandardAnalyzer:
  """standard_analyzer is the class + lowercase + ascii recipe."""

  def test_standard_analyzer_is_class_lowercase_ascii(self) -> None:
    """standard_analyzer = class tokenizer, lowercase + ascii filters."""
    a = standard_analyzer('std')
    assert a.tokenizers == [Tokenizer.CLASS]
    assert a.filters == [lowercase(), ascii_filter()]

  def test_immutability_preserved_across_chain(self) -> None:
    """with_* returns a new instance, leaving the original untouched."""
    base = analyzer('a')
    extended = base.with_tokenizer(Tokenizer.CLASS)
    assert base.tokenizers == []
    assert extended.tokenizers == [Tokenizer.CLASS]


class TestValidate:
  """AnalyzerDefinition.validate_definition rejects an empty name."""

  def test_validate_rejects_empty_name(self) -> None:
    """An empty analyzer name fails validation."""
    a = AnalyzerDefinition(name='')
    with pytest.raises(ValueError, match='Analyzer name cannot be empty'):
      a.validate_definition()

  def test_validate_accepts_named(self) -> None:
    """A named analyzer passes validation."""
    standard_analyzer('text_en').validate_definition()


class TestGenerateAnalyzerSql:
  """generate_analyzer_sql wraps to_surql and validates first."""

  def test_standard(self) -> None:
    """Returns a single DEFINE ANALYZER statement."""
    stmts = generate_analyzer_sql(standard_analyzer('text_en'))
    assert stmts == ['DEFINE ANALYZER text_en TOKENIZERS class FILTERS lowercase,ascii;']

  def test_if_not_exists(self) -> None:
    """if_not_exists flag flows through to the rendered statement."""
    stmts = generate_analyzer_sql(analyzer('plain'), if_not_exists=True)
    assert stmts == ['DEFINE ANALYZER IF NOT EXISTS plain;']

  def test_with_options_alias(self) -> None:
    """generate_analyzer_sql_with_options matches the keyword form."""
    stmts = generate_analyzer_sql_with_options(analyzer('plain'), if_not_exists=True)
    assert stmts == ['DEFINE ANALYZER IF NOT EXISTS plain;']

  def test_validates(self) -> None:
    """An invalid analyzer raises before any SQL is produced."""
    with pytest.raises(ValueError, match='Analyzer name cannot be empty'):
      generate_analyzer_sql(AnalyzerDefinition(name=''))
