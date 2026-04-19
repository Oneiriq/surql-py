"""Sub-feature #2 (issue #47): type_record / type_thing helpers."""

from __future__ import annotations

from typing import Any

from surql.query.builder import Query
from surql.types.record_id import RecordID
from surql.types.surreal_fn import SurrealFn, surql_fn, type_record, type_thing


class TestTypeRecord:
  """Tests for the ``type_record`` helper."""

  def test_type_record_with_string_id(self) -> None:
    fn = type_record('task', 'abc')
    assert isinstance(fn, SurrealFn)
    assert fn.to_surql() == "type::record('task', 'abc')"

  def test_type_record_with_int_id(self) -> None:
    fn = type_record('post', 42)
    assert fn.to_surql() == "type::record('post', 42)"

  def test_type_record_with_float_id(self) -> None:
    fn = type_record('metric', 3.14)
    assert fn.to_surql() == "type::record('metric', 3.14)"

  def test_type_record_with_bool_id(self) -> None:
    # Unusual but should not explode.
    fn = type_record('flag', True)
    assert fn.to_surql() == "type::record('flag', true)"

  def test_type_record_with_record_id(self) -> None:
    rid: RecordID[Any] = RecordID(table='user', id='alice')
    fn = type_record('target', rid)
    assert fn.to_surql() == "type::record('target', user:alice)"

  def test_type_record_with_nested_surreal_fn(self) -> None:
    inner = surql_fn('rand::uuid')
    fn = type_record('session', inner)
    assert fn.to_surql() == "type::record('session', rand::uuid())"

  def test_type_record_escapes_single_quotes(self) -> None:
    fn = type_record('user', "o'brien")
    assert "o\\'brien" in fn.to_surql()

  def test_type_record_escapes_backslashes(self) -> None:
    fn = type_record('file', 'a\\b')
    assert 'a\\\\b' in fn.to_surql()

  def test_type_record_renders_raw_in_insert(self) -> None:
    fn = type_record('user', 'alice')
    query = Query().insert('post', {'title': 'Hello', 'author': fn})
    sql = query.to_surql()
    assert "author: type::record('user', 'alice')" in sql

  def test_type_record_renders_raw_in_update(self) -> None:
    fn = type_record('user', 'alice')
    query = Query().update('post:123', {'author': fn})
    sql = query.to_surql()
    assert "author = type::record('user', 'alice')" in sql

  def test_type_record_top_level_export(self) -> None:
    # Replicates the regression call from the issue description.
    from surql import type_record as top_level_type_record

    rendered = top_level_type_record('user', 'alice').to_surql()
    assert rendered == "type::record('user', 'alice')"


class TestTypeThing:
  """Tests for the ``type_thing`` helper."""

  def test_type_thing_with_string_id(self) -> None:
    fn = type_thing('user', 'alice')
    assert isinstance(fn, SurrealFn)
    assert fn.to_surql() == "type::thing('user', 'alice')"

  def test_type_thing_with_int_id(self) -> None:
    fn = type_thing('order', 7)
    assert fn.to_surql() == "type::thing('order', 7)"

  def test_type_thing_escapes_single_quotes(self) -> None:
    fn = type_thing('user', "o'brien")
    assert "o\\'brien" in fn.to_surql()

  def test_type_thing_renders_raw_in_insert(self) -> None:
    fn = type_thing('user', 'alice')
    query = Query().insert('post', {'title': 'Hi', 'author': fn})
    sql = query.to_surql()
    assert "author: type::thing('user', 'alice')" in sql


class TestTypeRecordPublicApi:
  """Ensure new helpers are exported at every expected public surface."""

  def test_top_level_surql_exports(self) -> None:
    import surql

    assert hasattr(surql, 'type_record')
    assert hasattr(surql, 'type_thing')

  def test_types_submodule_exports(self) -> None:
    from surql.types import type_record as tr
    from surql.types import type_thing as tt

    assert callable(tr)
    assert callable(tt)
