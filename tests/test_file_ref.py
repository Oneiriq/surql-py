"""Tests for the FileRef value type and its normalize/denormalize integration."""

import pytest
from pydantic import ValidationError

from surql.connection.client import _denormalize_params, _normalize_sdk_value
from surql.types import FileRef


class TestFileRef:
  """FileRef construction, stringification, and round-tripping."""

  def test_str_is_pointer_body(self) -> None:
    assert str(FileRef(bucket='avatars', key='alice.png')) == 'avatars:/alice.png'

  def test_pointer_property_wraps_literal(self) -> None:
    assert FileRef(bucket='avatars', key='alice.png').pointer == 'f"avatars:/alice.png"'

  def test_to_sqon(self) -> None:
    assert FileRef(bucket='b', key='k').to_sqon() == {'bucket': 'b', 'key': 'k'}

  def test_from_sqon(self) -> None:
    assert FileRef.from_sqon({'bucket': 'b', 'key': 'k'}) == FileRef(bucket='b', key='k')

  def test_from_sqon_missing_key_raises(self) -> None:
    with pytest.raises(ValueError, match='bucket'):
      FileRef.from_sqon({'bucket': 'b'})

  def test_parse_bare(self) -> None:
    assert FileRef.parse('avatars:/alice.png') == FileRef(bucket='avatars', key='alice.png')

  def test_parse_literal_form(self) -> None:
    assert FileRef.parse('f"avatars:/alice.png"') == FileRef(bucket='avatars', key='alice.png')

  def test_parse_key_with_slashes(self) -> None:
    ref = FileRef.parse('docs:/a/b/c.txt')
    assert ref.bucket == 'docs'
    assert ref.key == 'a/b/c.txt'

  def test_parse_invalid_raises(self) -> None:
    with pytest.raises(ValueError, match='Invalid file pointer'):
      FileRef.parse('not-a-pointer')

  def test_sqon_round_trip(self) -> None:
    ref = FileRef(bucket='b', key='k')
    assert FileRef.from_sqon(ref.to_sqon()) == ref

  def test_is_file_object_true(self) -> None:
    assert FileRef.is_file_object({'bucket': 'b', 'key': 'k'}) is True

  def test_is_file_object_extra_key_false(self) -> None:
    assert FileRef.is_file_object({'bucket': 'b', 'key': 'k', 'size': 1}) is False

  def test_is_file_object_record_dict_false(self) -> None:
    assert FileRef.is_file_object({'id': 'user:1', 'name': 'x'}) is False

  def test_is_file_object_non_dict_false(self) -> None:
    assert FileRef.is_file_object('avatars:/x') is False

  def test_is_file_object_non_string_values_false(self) -> None:
    assert FileRef.is_file_object({'bucket': 1, 'key': 2}) is False

  def test_frozen(self) -> None:
    with pytest.raises(ValidationError):
      FileRef(bucket='b', key='k').key = 'other'  # type: ignore[misc]


class TestNormalizeFileRef:
  """_normalize_sdk_value recognises file objects; bytes pass through."""

  def test_file_object_becomes_fileref(self) -> None:
    out = _normalize_sdk_value({'bucket': 'b', 'key': 'k'})
    assert out == FileRef(bucket='b', key='k')

  def test_nested_file_object(self) -> None:
    out = _normalize_sdk_value({'rows': [{'bucket': 'b', 'key': 'k'}]})
    assert out == {'rows': [FileRef(bucket='b', key='k')]}

  def test_bytes_pass_through(self) -> None:
    assert _normalize_sdk_value(b'\x00\x01\x02') == b'\x00\x01\x02'

  def test_bytearray_passes_through(self) -> None:
    out = _normalize_sdk_value(bytearray(b'abc'))
    assert out == bytearray(b'abc')

  def test_ordinary_dict_untouched(self) -> None:
    out = _normalize_sdk_value({'name': 'x', 'count': 1})
    assert out == {'name': 'x', 'count': 1}


class TestDenormalizeFileRef:
  """_denormalize_params converts FileRef to SQON; bytes pass through."""

  def test_fileref_becomes_sqon(self) -> None:
    assert _denormalize_params(FileRef(bucket='b', key='k')) == {'bucket': 'b', 'key': 'k'}

  def test_fileref_nested_in_dict(self) -> None:
    out = _denormalize_params({'attachment': FileRef(bucket='b', key='k')})
    assert out == {'attachment': {'bucket': 'b', 'key': 'k'}}

  def test_fileref_in_list(self) -> None:
    out = _denormalize_params([FileRef(bucket='b', key='k')])
    assert out == [{'bucket': 'b', 'key': 'k'}]

  def test_bytes_pass_through(self) -> None:
    assert _denormalize_params(b'\x00\x01') == b'\x00\x01'

  def test_bytes_nested_pass_through(self) -> None:
    out = _denormalize_params({'blob': b'\xde\xad'})
    assert out == {'blob': b'\xde\xad'}


if __name__ == '__main__':
  pytest.main([__file__, '-v'])
