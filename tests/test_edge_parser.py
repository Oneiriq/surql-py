"""Tests for `parse_edge_info` round-trip + diff symmetry.

`parse_edge_info` is the inverse of `generate_edge_sql` for the three
EdgeDefinition modes (RELATION, SCHEMAFULL, SCHEMALESS). Each test builds a
code-side EdgeDefinition, synthesizes the `INFO FOR TABLE` payload SurrealDB
would return after applying the DEFINE statement, parses the payload back,
and asserts the round-tripped value matches the code-side declaration (and
that `diff_edges` against the proxy table-wrapped form returns no diffs).
"""

from __future__ import annotations

from surql.migration.diff import diff_edges
from surql.schema.edge import EdgeMode, edge_schema
from surql.schema.fields import FieldType, datetime_field, int_field
from surql.schema.parser import parse_edge_info
from surql.schema.table import IndexDefinition, IndexType


def _index(name: str, columns: list[str]) -> IndexDefinition:
  return IndexDefinition(name=name, columns=columns, type=IndexType.STANDARD)


def test_parse_relation_edge_minimal() -> None:
  """`DEFINE TABLE likes TYPE RELATION FROM user TO post` round-trips."""
  define_table = 'DEFINE TABLE likes TYPE RELATION FROM user TO post'
  live = parse_edge_info(
    'likes', {'fields': {}, 'indexes': {}, 'events': {}}, define_table=define_table
  )
  assert live.name == 'likes'
  assert live.mode == EdgeMode.RELATION
  assert live.from_table == 'user'
  assert live.to_table == 'post'
  assert live.fields == []
  assert live.indexes == []
  assert live.permissions is None


def test_parse_relation_edge_strips_implicit_in_out_fields() -> None:
  """SurrealDB auto-emits `in`/`out` fields for TYPE RELATION; parser drops them."""
  define_table = 'DEFINE TABLE likes TYPE RELATION FROM user TO post'
  # SurrealDB v3 would return these in INFO FOR TABLE for a RELATION edge.
  fields_dict = {
    'in': 'DEFINE FIELD in ON likes TYPE record<user> PERMISSIONS FULL',
    'out': 'DEFINE FIELD out ON likes TYPE record<post> PERMISSIONS FULL',
    'created_at': 'DEFINE FIELD created_at ON likes TYPE datetime DEFAULT time::now() PERMISSIONS FULL',
  }
  live = parse_edge_info('likes', {'fields': fields_dict}, define_table=define_table)
  field_names = {f.name for f in live.fields}
  assert 'in' not in field_names
  assert 'out' not in field_names
  assert 'created_at' in field_names


def test_parse_schemafull_edge_keeps_in_out_fields() -> None:
  """SCHEMAFULL edges declare `in`/`out` explicitly; parser must NOT strip them."""
  define_table = 'DEFINE TABLE entity_relation SCHEMAFULL'
  fields_dict = {
    'in': 'DEFINE FIELD in ON entity_relation TYPE record<entity> PERMISSIONS FULL',
    'out': 'DEFINE FIELD out ON entity_relation TYPE record<entity> PERMISSIONS FULL',
  }
  live = parse_edge_info('entity_relation', {'fields': fields_dict}, define_table=define_table)
  assert live.mode == EdgeMode.SCHEMAFULL
  field_names = {f.name for f in live.fields}
  assert field_names == {'in', 'out'}


def test_parse_schemaless_edge_mode() -> None:
  define_table = 'DEFINE TABLE follows SCHEMALESS'
  live = parse_edge_info('follows', {}, define_table=define_table)
  assert live.mode == EdgeMode.SCHEMALESS
  assert live.from_table is None
  assert live.to_table is None


def test_parse_relation_edge_with_permissions_round_trip() -> None:
  """Per-action permissions on a RELATION edge parse back into the same dict."""
  code = edge_schema(
    'authored',
    from_table='user',
    to_table='post',
    permissions={'create': '$auth.id = in', 'delete': '$auth.id = in'},
  )
  define_table = (
    'DEFINE TABLE authored TYPE RELATION FROM user TO post '
    'PERMISSIONS FOR create WHERE $auth.id = in FOR delete WHERE $auth.id = in'
  )
  live = parse_edge_info('authored', {}, define_table=define_table)
  assert live.mode == EdgeMode.RELATION
  assert live.from_table == 'user'
  assert live.to_table == 'post'
  assert live.permissions == code.permissions
  assert diff_edges(live, code) == []


def test_parse_relation_edge_with_extra_field_round_trips_via_diff_edges() -> None:
  """An edge with a non-housekeeping field (weight) diffs clean against the code."""
  code = edge_schema(
    'likes',
    from_table='user',
    to_table='post',
    fields=[int_field('weight', default='1')],
  )
  define_table = 'DEFINE TABLE likes TYPE RELATION FROM user TO post'
  fields_dict = {
    'in': 'DEFINE FIELD in ON likes TYPE record<user> PERMISSIONS FULL',
    'out': 'DEFINE FIELD out ON likes TYPE record<post> PERMISSIONS FULL',
    'weight': 'DEFINE FIELD weight ON likes TYPE int DEFAULT 1 PERMISSIONS FULL',
  }
  live = parse_edge_info('likes', {'fields': fields_dict}, define_table=define_table)
  assert diff_edges(live, code) == []


def test_parse_relation_edge_with_index_round_trips() -> None:
  code = edge_schema(
    'likes',
    from_table='user',
    to_table='post',
    fields=[datetime_field('created_at', default='time::now()')],
    indexes=[_index('likes_created_idx', ['created_at'])],
  )
  define_table = 'DEFINE TABLE likes TYPE RELATION FROM user TO post'
  fields_dict = {
    'created_at': 'DEFINE FIELD created_at ON likes TYPE datetime DEFAULT time::now() PERMISSIONS FULL',
  }
  indexes_dict = {
    'likes_created_idx': 'DEFINE INDEX likes_created_idx ON likes FIELDS created_at',
  }
  live = parse_edge_info(
    'likes',
    {'fields': fields_dict, 'indexes': indexes_dict},
    define_table=define_table,
  )
  assert diff_edges(live, code) == []


def test_parse_edge_without_define_table_falls_back_to_legacy_tb_key() -> None:
  """SurrealDB v2 carried the DEFINE TABLE string inside INFO FOR TABLE as `tb`."""
  info = {
    'tb': 'DEFINE TABLE likes TYPE RELATION FROM user TO post',
    'fields': {},
    'indexes': {},
  }
  live = parse_edge_info('likes', info)
  assert live.mode == EdgeMode.RELATION
  assert live.from_table == 'user'
  assert live.to_table == 'post'


def test_parse_edge_returns_none_endpoints_when_clauses_missing() -> None:
  """Missing FROM/TO surfaces as None endpoints instead of a parse failure."""
  define_table = 'DEFINE TABLE likes TYPE RELATION'
  live = parse_edge_info('likes', {}, define_table=define_table)
  assert live.mode == EdgeMode.RELATION
  assert live.from_table is None
  assert live.to_table is None


def test_parse_edge_info_is_exported_from_schema_package() -> None:
  from surql.schema import parse_edge_info as exported

  assert exported is parse_edge_info


def test_parse_edge_info_default_mode_when_define_table_empty() -> None:
  """No DEFINE TABLE string defaults to SCHEMALESS — the safest fallback."""
  live = parse_edge_info('orphan', {'fields': {}}, define_table='')
  assert live.mode == EdgeMode.SCHEMALESS
  assert live.fields == []


def test_parse_edge_picks_up_field_with_nullable_record_type() -> None:
  """Common shape on SCHEMAFULL edges: nullable record fields for endpoints."""
  define_table = 'DEFINE TABLE rel SCHEMAFULL'
  fields_dict = {
    'in': 'DEFINE FIELD in ON rel TYPE none | record<entity> PERMISSIONS FULL',
    'out': 'DEFINE FIELD out ON rel TYPE none | record<entity> PERMISSIONS FULL',
  }
  live = parse_edge_info('rel', {'fields': fields_dict}, define_table=define_table)
  in_field = next(f for f in live.fields if f.name == 'in')
  assert in_field.type == FieldType.RECORD
  assert in_field.nullable is True
  assert in_field.target_table == 'entity'


def test_parse_edge_propagates_field_named_default_parsing_path() -> None:
  """Edge with a custom field named `default` should not crash the clause splitter.

  This mirrors the table-side regression test for the `_split_field_clauses`
  fix in 1.6.2; the parser path for edges goes through the same helpers.
  """
  define_table = 'DEFINE TABLE likes TYPE RELATION FROM user TO post'
  fields_dict = {
    'default': 'DEFINE FIELD default ON likes TYPE bool DEFAULT false PERMISSIONS FULL',
  }
  live = parse_edge_info('likes', {'fields': fields_dict}, define_table=define_table)
  default_field = next(f for f in live.fields if f.name == 'default')
  assert default_field.type == FieldType.BOOL
