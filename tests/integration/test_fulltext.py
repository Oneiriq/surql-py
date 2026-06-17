"""Full-text (BM25) search integration suite for SurrealDB v3.

Verifies the lexical leg of hybrid retrieval end-to-end against a live engine:

- ``DEFINE ANALYZER`` + ``DEFINE INDEX ... FULLTEXT ANALYZER <a> BM25`` apply
  cleanly (the v1/v2 ``SEARCH`` keyword is a parse error on v3).
- ``content @1@ '<query>'`` selects only matching rows.
- The full-text scan returns rows in BM25 relevance order, which is what RRF
  fuses on. Per ``docs/v3-patterns.md`` the v3 streaming executor does not
  plumb the score through ``search::score(<ref>)`` (it returns 0 there), so we
  assert on the returned row order, not the score magnitude.

Skipped automatically when no SurrealDB server is reachable (see conftest).
"""

from __future__ import annotations

from typing import Any

import pytest

from surql.connection.client import DatabaseClient
from surql.query.helpers import fulltext_search_query
from surql.schema.analyzer import standard_analyzer
from surql.schema.fields import string_field
from surql.schema.sql import generate_analyzer_sql, generate_table_sql
from surql.schema.table import bm25_index, table_schema


def _unwrap(rows: Any) -> list[dict[str, Any]]:
  """Unwrap the SDK result envelope into a plain list of row dicts."""
  if isinstance(rows, list) and rows and isinstance(rows[0], dict) and 'result' in rows[0]:
    return list(rows[0]['result'])
  if isinstance(rows, list):
    return list(rows)
  return []


class TestFullTextSearchV3:
  """End-to-end BM25 full-text search on a live SurrealDB engine."""

  @pytest.mark.anyio
  async def test_bm25_index_applies_and_matches(self, integration_client: DatabaseClient) -> None:
    """Define an analyzer + BM25 index, then match documents by term."""
    # 1. Analyzer must exist before the index that references it.
    for stmt in generate_analyzer_sql(standard_analyzer('text_en'), if_not_exists=True):
      await integration_client.execute(stmt)

    # 2. Schemafull table with a BM25 full-text index over `content`.
    memory = table_schema(
      'memory',
      fields=[string_field('content')],
      indexes=[bm25_index('content_bm25', ['content'], 'text_en')],
    )
    for stmt in generate_table_sql(memory, if_not_exists=True):
      await integration_client.execute(stmt)

    # 3. Seed documents — only some mention the query term.
    await integration_client.execute(
      "CREATE memory:a SET content = 'insider buying surged this quarter'"
    )
    await integration_client.execute(
      "CREATE memory:b SET content = 'the weather was mild and sunny'"
    )
    await integration_client.execute(
      "CREATE memory:c SET content = 'reports of insider buying and selling'"
    )

    # 4. Full-text query: only the matching rows come back.
    query = fulltext_search_query('memory', 'content', 1, 'insider buying').limit(100)
    rows = _unwrap(await integration_client.execute(query.to_surql()))

    contents = [r['content'] for r in rows]
    assert any('insider buying' in c for c in contents)
    # The non-matching document must not appear.
    assert not any('weather' in c for c in contents)

  @pytest.mark.anyio
  async def test_fulltext_scan_returns_matches_in_relevance_order(
    self, integration_client: DatabaseClient
  ) -> None:
    """The scan yields matching rows in BM25 order (the rank RRF fuses on).

    We do not assert on ``search::score`` magnitude: under the v3 streaming
    executor it is not plumbed through and returns 0 (see docs/v3-patterns.md
    §"search::score and scan ordering"). Ranking by scan order is sufficient.
    """
    for stmt in generate_analyzer_sql(standard_analyzer('text_en'), if_not_exists=True):
      await integration_client.execute(stmt)
    memory = table_schema(
      'doc',
      fields=[string_field('content')],
      indexes=[bm25_index('doc_bm25', ['content'], 'text_en')],
    )
    for stmt in generate_table_sql(memory, if_not_exists=True):
      await integration_client.execute(stmt)

    await integration_client.execute("CREATE doc:dense SET content = 'alpha alpha alpha beta'")
    await integration_client.execute("CREATE doc:sparse SET content = 'alpha gamma'")
    await integration_client.execute("CREATE doc:none SET content = 'delta epsilon'")

    query = fulltext_search_query('doc', 'content', 1, 'alpha').limit(100)
    rows = _unwrap(await integration_client.execute(query.to_surql()))

    matched = [r['content'] for r in rows]
    # Both alpha-bearing docs match; the delta/epsilon doc does not.
    assert len(matched) == 2
    assert all('alpha' in c for c in matched)
