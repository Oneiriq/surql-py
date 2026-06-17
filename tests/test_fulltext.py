"""Tests for full-text (BM25) search: index rendering, query builder, parser.

Mirrors the sibling surql-rs full-text suite. The full-text index keyword is
SurrealDB 3.x ``FULLTEXT`` (the v1/v2 ``SEARCH`` spelling was renamed in 3.0);
see ``docs/v3-patterns.md``.
"""

import pytest
from pydantic import BaseModel

from surql.query.builder import Query
from surql.query.helpers import fulltext_search_query
from surql.schema.parser import _parse_index_definition
from surql.schema.sql import _generate_index_sql
from surql.schema.table import IndexType, bm25_index, search_index, table_schema


class _Doc(BaseModel):
  """Test model for the generic Query parameter."""

  content: str


class TestSearchIndexRendering:
  """search_index / bm25_index render the FULLTEXT keyword and clauses."""

  def test_search_index_default_analyzer_is_ascii(self) -> None:
    """A plain search_index renders FULLTEXT ANALYZER ascii (historical default)."""
    sql = _generate_index_sql('post', search_index('content_search', ['title', 'content']))
    assert (
      sql
      == 'DEFINE INDEX content_search ON TABLE post COLUMNS title, content FULLTEXT ANALYZER ascii;'
    )

  def test_bm25_index_renders_analyzer_and_bm25(self) -> None:
    """bm25_index renders the analyzer and the BM25 clause."""
    sql = _generate_index_sql('memory', bm25_index('content_bm25', ['content'], 'text_en'))
    assert (
      sql
      == 'DEFINE INDEX content_bm25 ON TABLE memory COLUMNS content FULLTEXT ANALYZER text_en BM25;'
    )

  def test_search_index_with_analyzer_bm25_highlights(self) -> None:
    """All three full-text clauses render in order."""
    idx = search_index('s', ['content'], analyzer='text_en', bm25=True, highlights=True)
    sql = _generate_index_sql('doc', idx)
    assert (
      sql
      == 'DEFINE INDEX s ON TABLE doc COLUMNS content FULLTEXT ANALYZER text_en BM25 HIGHLIGHTS;'
    )

  def test_bm25_index_if_not_exists(self) -> None:
    """if_not_exists flows through to the FULLTEXT index statement."""
    idx = bm25_index('content_bm25', ['content'], 'text_en')
    sql = _generate_index_sql('memory', idx, if_not_exists=True)
    assert sql == (
      'DEFINE INDEX IF NOT EXISTS content_bm25 ON TABLE memory '
      'COLUMNS content FULLTEXT ANALYZER text_en BM25;'
    )

  def test_search_index_type_is_search(self) -> None:
    """The builders set IndexType.SEARCH (rendered as FULLTEXT)."""
    assert search_index('s', ['c']).type == IndexType.SEARCH
    assert bm25_index('s', ['c'], 'text_en').type == IndexType.SEARCH

  def test_bm25_index_sets_analyzer_and_flag(self) -> None:
    """bm25_index pins analyzer and turns BM25 on, highlights off by default."""
    idx = bm25_index('s', ['c'], 'text_en')
    assert idx.analyzer == 'text_en'
    assert idx.bm25 is True
    assert idx.highlights is False


class TestFullTextSearchQuery:
  """Query.full_text_search renders the @ref@ match operator in WHERE."""

  def test_renders_match_operator(self) -> None:
    """A bare full-text predicate renders content @1@ 'query'."""
    q = Query().select().from_table('memory').full_text_search('content', 1, 'insider buying')
    assert q.to_surql() == "SELECT * FROM memory WHERE content @1@ 'insider buying'"

  def test_with_score_and_order(self) -> None:
    """search_score projects search::score(ref), orderable for ranking."""
    q = (
      Query()
      .select()
      .search_score(1, 'score')
      .from_table('memory')
      .full_text_search('content', 1, 'form 4')
      .order_by('score', 'DESC')
      .limit(5)
    )
    assert q.to_surql() == (
      'SELECT *, search::score(1) AS score FROM memory '
      "WHERE content @1@ 'form 4' ORDER BY score DESC LIMIT 5"
    )

  def test_escapes_single_quotes(self) -> None:
    """The query text is inlined as an escaped single-quoted literal."""
    q = Query().select().from_table('memory').full_text_search('content', 0, "o'brien")
    assert q.to_surql() == "SELECT * FROM memory WHERE content @0@ 'o\\'brien'"

  def test_rejects_empty_field(self) -> None:
    """An empty field is rejected."""
    with pytest.raises(ValueError, match='field cannot be empty'):
      Query().select().from_table('memory').full_text_search('', 1, 'x')

  def test_rejects_empty_query(self) -> None:
    """An empty query is rejected."""
    with pytest.raises(ValueError, match='query cannot be empty'):
      Query().select().from_table('memory').full_text_search('content', 1, '')

  def test_fulltext_and_vector_both_render_in_where(self) -> None:
    """Vector and full-text predicates both render, joined with AND."""
    q = (
      Query()
      .select()
      .from_table('memory')
      .vector_search('embedding', [0.1, 0.2], k=5, distance='COSINE')
      .full_text_search('content', 1, 'term')
    )
    sql = q.to_surql()
    assert 'embedding <|5,COSINE|> [0.1, 0.2]' in sql
    assert "content @1@ 'term'" in sql
    assert ' AND ' in sql

  def test_immutability_preserved(self) -> None:
    """full_text_search returns a new instance."""
    base = Query[_Doc]().select().from_table('memory')
    extended = base.full_text_search('content', 1, 'x')
    assert base.fulltext_field is None
    assert extended.fulltext_field == 'content'


class TestFullTextSearchQueryHelper:
  """fulltext_search_query wraps the builder methods."""

  def test_helper_renders_score_and_predicate(self) -> None:
    """The helper projects the score and renders the predicate."""
    q = fulltext_search_query('memory', 'content', 1, 'insider buying')
    assert q.to_surql() == (
      "SELECT *, search::score(1) AS score FROM memory WHERE content @1@ 'insider buying'"
    )

  def test_helper_custom_fields_and_alias(self) -> None:
    """Projection fields and the score alias are configurable."""
    q = fulltext_search_query('memory', 'content', 2, 'q', fields=['id'], score_alias='relevance')
    assert q.to_surql() == (
      "SELECT id, search::score(2) AS relevance FROM memory WHERE content @2@ 'q'"
    )


class TestFullTextParserRoundTrip:
  """The INFO FOR TABLE parser reads FULLTEXT/SEARCH indexes back."""

  def test_bm25_round_trip(self) -> None:
    """A rendered bm25_index parses back to the same fields."""
    idx = bm25_index('content_bm25', ['content'], 'text_en')
    sql = _generate_index_sql('memory', idx)
    parsed = _parse_index_definition('content_bm25', sql)
    assert parsed is not None
    assert parsed.type == IndexType.SEARCH
    assert parsed.columns == ['content']
    assert parsed.analyzer == 'text_en'
    assert parsed.bm25 is True
    assert parsed.highlights is False

  def test_default_analyzer_normalizes_to_none(self) -> None:
    """The historical ascii default round-trips to analyzer=None (identity)."""
    sql = _generate_index_sql('post', search_index('content_search', ['title', 'content']))
    parsed = _parse_index_definition('content_search', sql)
    assert parsed is not None
    assert parsed.type == IndexType.SEARCH
    assert parsed.columns == ['title', 'content']
    assert parsed.analyzer is None
    assert parsed.bm25 is False

  def test_highlights_round_trip(self) -> None:
    """BM25 + HIGHLIGHTS both round-trip."""
    idx = search_index('s', ['content'], analyzer='text_en', bm25=True, highlights=True)
    sql = _generate_index_sql('doc', idx)
    parsed = _parse_index_definition('s', sql)
    assert parsed is not None
    assert parsed.analyzer == 'text_en'
    assert parsed.bm25 is True
    assert parsed.highlights is True

  def test_legacy_search_keyword_still_parses(self) -> None:
    """Back-compat: the v1/v2 SEARCH spelling is still recognised."""
    legacy = 'DEFINE INDEX content_search ON TABLE post COLUMNS content SEARCH ANALYZER ascii;'
    parsed = _parse_index_definition('content_search', legacy)
    assert parsed is not None
    assert parsed.type == IndexType.SEARCH
    assert parsed.columns == ['content']
    assert parsed.analyzer is None


class TestFullTextSchemaSql:
  """generate_schema_sql emits analyzer DDL before the tables that use it."""

  def test_analyzer_emitted_before_table(self) -> None:
    """The DEFINE ANALYZER line precedes the DEFINE INDEX that references it."""
    from surql.schema.analyzer import standard_analyzer
    from surql.schema.sql import generate_schema_sql

    memory = table_schema('memory', indexes=[bm25_index('content_bm25', ['content'], 'text_en')])
    sql = generate_schema_sql(
      tables={'memory': memory}, analyzers={'text_en': standard_analyzer('text_en')}
    )
    analyzer_pos = sql.index('DEFINE ANALYZER text_en')
    index_pos = sql.index('FULLTEXT ANALYZER text_en')
    assert analyzer_pos < index_pos
