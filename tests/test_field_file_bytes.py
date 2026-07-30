"""Tests for file_field / bytes_field builders and their DDL + parser round-trip."""

import pytest

from surql.migration.diff import _field_to_sql, diff_fields
from surql.schema.fields import FieldType, bytes_field, file_field
from surql.schema.parser import _parse_field_definition
from surql.schema.sql import _generate_field_sql
from surql.schema.table import TableMode, table_schema


class TestFileBytesFieldTypes:
  """FieldType gains FILE and BYTES variants."""

  def test_file_type_value(self) -> None:
    assert FieldType.FILE.value == 'file'

  def test_bytes_type_value(self) -> None:
    assert FieldType.BYTES.value == 'bytes'


class TestFileBytesBuilders:
  """Builders produce fields of the right type."""

  def test_file_field(self) -> None:
    f = file_field('avatar')
    assert f.name == 'avatar'
    assert f.type == FieldType.FILE

  def test_bytes_field(self) -> None:
    f = bytes_field('thumb')
    assert f.type == FieldType.BYTES

  def test_file_field_nullable(self) -> None:
    assert file_field('avatar', nullable=True).nullable is True

  def test_bytes_field_options(self) -> None:
    f = bytes_field('blob', readonly=True, default='<bytes>""')
    assert f.readonly is True
    assert f.default == '<bytes>""'


class TestFileBytesDdl:
  """schema/sql and migration/diff both emit TYPE file / TYPE bytes."""

  def test_emit_file(self) -> None:
    assert _generate_field_sql('t', file_field('avatar')) == (
      'DEFINE FIELD avatar ON TABLE t TYPE file;'
    )

  def test_emit_bytes(self) -> None:
    assert _generate_field_sql('t', bytes_field('thumb')) == (
      'DEFINE FIELD thumb ON TABLE t TYPE bytes;'
    )

  def test_emit_file_nullable(self) -> None:
    assert _generate_field_sql('t', file_field('avatar', nullable=True)) == (
      'DEFINE FIELD avatar ON TABLE t TYPE option<file>;'
    )

  def test_diff_field_to_sql_file(self) -> None:
    assert _field_to_sql('t', file_field('avatar')) == ('DEFINE FIELD avatar ON TABLE t TYPE file;')

  def test_diff_field_to_sql_bytes(self) -> None:
    assert _field_to_sql('t', bytes_field('thumb')) == ('DEFINE FIELD thumb ON TABLE t TYPE bytes;')


class TestFileBytesParserRoundTrip:
  """Parser maps `file` / `bytes` type words back to the right FieldType."""

  def test_parse_file(self) -> None:
    parsed = _parse_field_definition('avatar', 'DEFINE FIELD avatar ON TABLE t TYPE file')
    assert parsed is not None
    assert parsed.type == FieldType.FILE

  def test_parse_bytes(self) -> None:
    parsed = _parse_field_definition('thumb', 'DEFINE FIELD thumb ON TABLE t TYPE bytes')
    assert parsed is not None
    assert parsed.type == FieldType.BYTES

  def test_parse_option_file(self) -> None:
    parsed = _parse_field_definition('avatar', 'DEFINE FIELD avatar ON TABLE t TYPE option<file>')
    assert parsed is not None
    assert parsed.type == FieldType.FILE
    assert parsed.nullable is True

  def test_no_drift_after_round_trip(self) -> None:
    # A table with file/bytes fields should diff clean against its parsed form.
    code_table = table_schema(
      'media',
      mode=TableMode.SCHEMAFULL,
      fields=[file_field('source'), bytes_field('thumb')],
    )
    parsed_fields = []
    for f in code_table.fields:
      emitted = _generate_field_sql('media', f)
      parsed = _parse_field_definition(f.name, emitted.rstrip(';'))
      assert parsed is not None
      parsed_fields.append(parsed)
    parsed_table = table_schema('media', mode=TableMode.SCHEMAFULL, fields=parsed_fields)
    assert diff_fields(code_table, parsed_table) == []


if __name__ == '__main__':
  pytest.main([__file__, '-v'])
