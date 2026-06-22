"""Tests for bucket schema definitions, DDL emission, diffing, and parsing."""

import pytest
from pydantic import ValidationError

from surql.migration.diff import diff_buckets
from surql.migration.models import DiffOperation, SchemaDiff
from surql.schema.bucket import (
  bucket_schema,
  file_bucket,
  memory_bucket,
)
from surql.schema.parser import parse_bucket_info, parse_db_buckets
from surql.schema.sql import (
  generate_alter_bucket_sql,
  generate_bucket_sql,
  generate_remove_bucket_sql,
  generate_schema_sql,
)
from surql.schema.table import TableMode, table_schema


class TestBucketDefinition:
  """Builders produce the expected immutable definitions."""

  def test_bucket_schema_defaults(self) -> None:
    b = bucket_schema('avatars', backend='memory')
    assert b.name == 'avatars'
    assert b.backend == 'memory'
    assert b.readonly is False
    assert b.permissions is None
    assert b.comment is None

  def test_memory_bucket(self) -> None:
    b = memory_bucket('avatars', comment='profile pics')
    assert b.backend == 'memory'
    assert b.comment == 'profile pics'

  def test_file_bucket_builds_backend(self) -> None:
    assert file_bucket('arc', '/srv/data').backend == 'file:/srv/data'

  def test_file_bucket_accepts_prefixed_path(self) -> None:
    # A path already carrying the file: scheme is not double-prefixed.
    assert file_bucket('arc', 'file:/srv/data').backend == 'file:/srv/data'

  def test_file_bucket_readonly(self) -> None:
    b = file_bucket('arc', '/srv/data', readonly=True)
    assert b.readonly is True

  def test_bucket_is_frozen(self) -> None:
    b = memory_bucket('avatars')
    with pytest.raises(ValidationError):
      b.name = 'other'  # type: ignore[misc]


class TestGenerateBucketSql:
  """DEFINE BUCKET emission."""

  def test_basic(self) -> None:
    assert generate_bucket_sql(memory_bucket('avatars')) == [
      'DEFINE BUCKET avatars BACKEND "memory";'
    ]

  def test_readonly_and_comment(self) -> None:
    sql = generate_bucket_sql(
      bucket_schema('arc', backend='s3://bucket', readonly=True, comment='cold storage')
    )[0]
    assert sql == 'DEFINE BUCKET arc BACKEND "s3://bucket" READONLY COMMENT "cold storage";'

  def test_if_not_exists(self) -> None:
    assert generate_bucket_sql(memory_bucket('b'), if_not_exists=True) == [
      'DEFINE BUCKET IF NOT EXISTS b BACKEND "memory";'
    ]

  def test_overwrite(self) -> None:
    assert generate_bucket_sql(memory_bucket('b'), overwrite=True) == [
      'DEFINE BUCKET OVERWRITE b BACKEND "memory";'
    ]

  def test_if_not_exists_wins_over_overwrite(self) -> None:
    sql = generate_bucket_sql(memory_bucket('b'), if_not_exists=True, overwrite=True)[0]
    assert 'IF NOT EXISTS' in sql
    assert 'OVERWRITE' not in sql

  def test_permissions(self) -> None:
    sql = generate_bucket_sql(
      bucket_schema('b', backend='memory', permissions={'get': 'true', 'put': '$auth'})
    )[0]
    assert 'PERMISSIONS FOR get WHERE true FOR put WHERE $auth' in sql

  def test_backend_quotes_are_escaped(self) -> None:
    # A double-quote in the backend string cannot break out of the literal.
    sql = generate_bucket_sql(bucket_schema('b', backend='file:/a"b'))[0]
    assert r'BACKEND "file:/a\"b"' in sql


class TestRemoveBucketSql:
  """REMOVE BUCKET emission."""

  def test_by_name(self) -> None:
    assert generate_remove_bucket_sql('avatars') == ['REMOVE BUCKET avatars;']

  def test_by_definition(self) -> None:
    assert generate_remove_bucket_sql(memory_bucket('avatars')) == ['REMOVE BUCKET avatars;']

  def test_if_exists(self) -> None:
    assert generate_remove_bucket_sql('avatars', if_exists=True) == [
      'REMOVE BUCKET IF EXISTS avatars;'
    ]


class TestAlterBucketSql:
  """ALTER BUCKET emission only includes changed clauses."""

  def test_readonly_added(self) -> None:
    sql = generate_alter_bucket_sql(memory_bucket('b'), memory_bucket('b', readonly=True))
    assert sql == ['ALTER BUCKET b READONLY;']

  def test_readonly_dropped(self) -> None:
    sql = generate_alter_bucket_sql(memory_bucket('b', readonly=True), memory_bucket('b'))
    assert sql == ['ALTER BUCKET b DROP READONLY;']

  def test_backend_changed(self) -> None:
    sql = generate_alter_bucket_sql(memory_bucket('b'), bucket_schema('b', backend='file:/x'))
    assert sql == ['ALTER BUCKET b BACKEND "file:/x";']

  def test_comment_added_then_dropped(self) -> None:
    added = generate_alter_bucket_sql(memory_bucket('b'), memory_bucket('b', comment='hi'))
    assert added == ['ALTER BUCKET b COMMENT "hi";']
    dropped = generate_alter_bucket_sql(memory_bucket('b', comment='hi'), memory_bucket('b'))
    assert dropped == ['ALTER BUCKET b DROP COMMENT;']

  def test_if_exists(self) -> None:
    sql = generate_alter_bucket_sql(
      memory_bucket('b'), memory_bucket('b', readonly=True), if_exists=True
    )
    assert sql == ['ALTER BUCKET IF EXISTS b READONLY;']

  def test_no_change_returns_empty(self) -> None:
    assert generate_alter_bucket_sql(memory_bucket('b'), memory_bucket('b')) == []

  def test_multiple_changes_combined(self) -> None:
    sql = generate_alter_bucket_sql(
      memory_bucket('b'),
      bucket_schema('b', backend='file:/x', readonly=True, comment='c'),
    )[0]
    assert sql.startswith('ALTER BUCKET b ')
    assert 'READONLY' in sql
    assert 'BACKEND "file:/x"' in sql
    assert 'COMMENT "c"' in sql


class TestDiffBuckets:
  """diff_buckets produces correct ADD/DROP/MODIFY operations."""

  def test_add(self) -> None:
    diffs = diff_buckets(None, memory_bucket('b'))
    assert len(diffs) == 1
    d = diffs[0]
    assert d.operation == DiffOperation.ADD_BUCKET
    assert d.bucket == 'b'
    assert d.forward_sql == 'DEFINE BUCKET b BACKEND "memory";'
    assert d.backward_sql == 'REMOVE BUCKET b;'

  def test_drop(self) -> None:
    diffs = diff_buckets(memory_bucket('b'), None)
    d = diffs[0]
    assert d.operation == DiffOperation.DROP_BUCKET
    assert d.forward_sql == 'REMOVE BUCKET b;'
    assert d.backward_sql == 'DEFINE BUCKET b BACKEND "memory";'

  def test_modify(self) -> None:
    diffs = diff_buckets(memory_bucket('b'), memory_bucket('b', readonly=True))
    d = diffs[0]
    assert d.operation == DiffOperation.MODIFY_BUCKET
    assert d.forward_sql == 'ALTER BUCKET b READONLY;'
    assert d.backward_sql == 'ALTER BUCKET b DROP READONLY;'

  def test_no_change(self) -> None:
    assert diff_buckets(memory_bucket('b'), memory_bucket('b')) == []

  def test_both_none(self) -> None:
    assert diff_buckets(None, None) == []

  def test_schema_diff_bucket_only_has_no_table(self) -> None:
    # A bucket diff sets `bucket` and leaves `table` at its empty default.
    d = SchemaDiff(
      operation=DiffOperation.ADD_BUCKET,
      bucket='b',
      description='x',
      forward_sql='',
      backward_sql='',
    )
    assert d.table == ''
    assert d.bucket == 'b'


class TestParseBucketInfo:
  """parse_bucket_info is the inverse of generate_bucket_sql."""

  def test_round_trip_basic(self) -> None:
    original = memory_bucket('avatars')
    sql = generate_bucket_sql(original)[0]
    parsed = parse_bucket_info('avatars', sql)
    assert parsed == original

  def test_round_trip_full(self) -> None:
    original = bucket_schema('arc', backend='file:/srv/data', readonly=True, comment='cold')
    sql = generate_bucket_sql(original)[0]
    parsed = parse_bucket_info('arc', sql)
    assert parsed == original

  def test_parse_readonly(self) -> None:
    parsed = parse_bucket_info('b', 'DEFINE BUCKET b BACKEND "memory" READONLY')
    assert parsed.readonly is True

  def test_parse_no_readonly(self) -> None:
    parsed = parse_bucket_info('b', 'DEFINE BUCKET b BACKEND "memory"')
    assert parsed.readonly is False

  def test_parse_escaped_backend(self) -> None:
    parsed = parse_bucket_info('b', r'DEFINE BUCKET b BACKEND "file:/a\"b"')
    assert parsed.backend == 'file:/a"b'

  def test_parse_db_buckets(self) -> None:
    info = {
      'buckets': {
        'a': 'DEFINE BUCKET a BACKEND "memory"',
        'b': 'DEFINE BUCKET b BACKEND "file:/x" READONLY',
      }
    }
    buckets = parse_db_buckets(info)
    assert set(buckets) == {'a', 'b'}
    assert buckets['b'].readonly is True

  def test_parse_db_buckets_legacy_key(self) -> None:
    info = {'bu': {'a': 'DEFINE BUCKET a BACKEND "memory"'}}
    assert 'a' in parse_db_buckets(info)

  def test_parse_db_buckets_empty(self) -> None:
    assert parse_db_buckets({}) == {}


class TestGenerateSchemaSqlWithBuckets:
  """Buckets render before tables in full schema generation."""

  def test_buckets_emitted_before_tables(self) -> None:
    sql = generate_schema_sql(
      tables={'doc': table_schema('doc', mode=TableMode.SCHEMALESS)},
      buckets={'files': memory_bucket('files')},
    )
    assert 'DEFINE BUCKET files' in sql
    assert sql.index('DEFINE BUCKET files') < sql.index('DEFINE TABLE doc')


if __name__ == '__main__':
  pytest.main([__file__, '-v'])
