"""Sub-feature #4 (issue #47): result extraction helper aliases."""

from __future__ import annotations

from typing import Any

from surql.query.results import extract_many, extract_one, extract_scalar, has_result


class TestExtractMany:
  """Tests for ``extract_many`` (alias for ``extract_result``)."""

  def test_nested_format(self) -> None:
    result = [{'result': [{'id': 'user:1', 'name': 'Alice'}]}]
    out = extract_many(result)
    assert out == [{'id': 'user:1', 'name': 'Alice'}]

  def test_flat_format(self) -> None:
    result = [{'id': 'user:1'}, {'id': 'user:2'}]
    out = extract_many(result)
    assert len(out) == 2

  def test_scalar_aggregate(self) -> None:
    result = [{'count': 5}]
    out = extract_many(result)
    assert out == [{'count': 5}]

  def test_empty(self) -> None:
    assert extract_many([]) == []
    assert extract_many(None) == []

  def test_error_envelope(self) -> None:
    """Error envelopes without a 'result' key surface as flat data, not a crash."""
    result: list[dict[str, Any]] = [{'status': 'ERR', 'detail': 'boom'}]
    out = extract_many(result)
    assert isinstance(out, list)

  def test_multi_statement_nested(self) -> None:
    """Multi-statement query response (one envelope per statement) flattens."""
    result = [
      {'result': [{'id': 'a:1'}]},
      {'result': [{'id': 'b:2'}]},
    ]
    out = extract_many(result)
    assert len(out) == 2


class TestHasResult:
  """Tests for the singular ``has_result`` alias."""

  def test_truthy_nested(self) -> None:
    assert has_result([{'result': [{'id': 'x:1'}]}]) is True

  def test_truthy_flat(self) -> None:
    assert has_result([{'id': 'x:1'}]) is True

  def test_empty_list(self) -> None:
    assert has_result([]) is False

  def test_empty_nested(self) -> None:
    assert has_result([{'result': []}]) is False

  def test_none(self) -> None:
    assert has_result(None) is False


class TestExtractionPublicApi:
  """Ensure result extraction helpers are exported from ``surql``."""

  def test_top_level_exports(self) -> None:
    import surql

    assert hasattr(surql, 'extract_one')
    assert hasattr(surql, 'extract_scalar')
    assert hasattr(surql, 'extract_many')
    assert hasattr(surql, 'has_result')
    # Existing names stay exported.
    assert hasattr(surql, 'extract_result')
    assert hasattr(surql, 'has_results')


class TestExtractionExistingHelpers:
  """Smoke tests confirming existing helpers still behave after aliasing."""

  def test_extract_one_still_works(self) -> None:
    assert extract_one([{'id': 'x:1'}]) == {'id': 'x:1'}

  def test_extract_scalar_still_works(self) -> None:
    assert extract_scalar([{'result': [{'count': 9}]}], 'count') == 9
