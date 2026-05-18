"""Tests that `diff_tables(parsed_db_table, code_table)` returns no diffs
when the live DB is in-sync with the code declaration.

These are round-trip / symmetry tests for the emitter
(`surql.schema.sql._generate_field_sql` and
`surql.migration.diff._field_to_sql`) and the parser
(`surql.schema.parser.parse_table_info`). Each test:

  1. Builds a code-side `TableDefinition` exercising one of the 1.5.14+
     field shapes that consumers observed false-positive drift on
     (typed record link, FLEXIBLE object, sub-field, PERMISSIONS, etc.).
  2. Synthesizes the `INFO FOR TABLE` payload SurrealDB v3 returns when
     the same DEFINE statement has been applied (`option<X>` is folded
     to `none | X`, the table's `PERMISSIONS FULL` default is reported,
     array sub-fields like `<field>.*` appear as their own entries).
  3. Calls `parse_table_info` to round-trip the payload back to a
     `TableDefinition`.
  4. Asserts `diff_tables(live, code) == []`.

The bug being fixed (#1.6.2): each of these returned at least one
spurious `Modify field` / `Drop field` / `Modify permissions` entry
even when DB and code matched exactly.
"""

from __future__ import annotations

import pytest

from surql.migration.diff import diff_tables
from surql.schema.fields import (
  FieldType,
  field,
  object_field,
)
from surql.schema.parser import parse_table_info
from surql.schema.table import table_schema


def test_diff_typed_record_field_round_trip_is_empty() -> None:
  """Live `TYPE none | record<X>` should diff cleanly against code `record<X>` nullable.

  SurrealDB v3 unfolds `option<record<X>>` into `none | record<X>` and reports
  `PERMISSIONS FULL` for the implicit per-field default. Pre-1.6.2 the parser
  extracted `FieldType.ANY` from `TYPE none ...` because the regex only matched
  the first word after `TYPE`, producing a spurious `MODIFY_FIELD` per typed
  record link.
  """
  code_table = table_schema(
    'community',
    fields=[
      field('spec', FieldType.RECORD, target_table='data_capture_spec', nullable=True),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE community SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'spec': (
        'DEFINE FIELD spec ON community TYPE none | record<data_capture_spec> PERMISSIONS FULL'
      ),
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('community', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_flexible_object_field_round_trip_is_empty() -> None:
  """Live `TYPE none | object FLEXIBLE` should diff cleanly against code
  `object_field(nullable=True, flexible=True)`.

  Pre-1.6.2 the parser captured `none` as the type, losing both `FieldType.OBJECT`
  and the FLEXIBLE flag positioning, producing a spurious `MODIFY_FIELD`.
  """
  code_table = table_schema(
    'community',
    fields=[
      object_field('extras', flexible=True),  # object_field defaults flexible=True
    ],
  )
  # Force nullable to True to match the live `none | object` shape.
  code_table = code_table.model_copy(
    update={
      'fields': [
        field('extras', FieldType.OBJECT, flexible=True, nullable=True),
      ]
    }
  )
  live_info = {
    'tb': 'DEFINE TABLE community SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'extras': 'DEFINE FIELD extras ON community FLEXIBLE TYPE none | object PERMISSIONS FULL',
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('community', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_permissions_clause_round_trip_is_empty() -> None:
  """Live table with PERMISSIONS clause should NOT spuriously diff against
  the matching code-side PERMISSIONS dict.

  Pre-1.6.2 every PERMISSIONS-bearing table produced a `MODIFY_PERMISSIONS`
  entry per call because the parser left `permissions=None` regardless of
  what was on the table — the comparison was therefore always
  `None != {'select': '...'}` even when the live DB stored the exact
  same rule. After 1.6.2 the parser extracts the per-action PERMISSIONS
  clauses from the `tb` field and compares the resulting dicts.
  """
  code_table = table_schema(
    'community',
    fields=[
      field('name', FieldType.STRING),
    ],
    permissions={'select': 'true', 'create': '$auth.id != NONE'},
  )
  live_info = {
    'tb': (
      'DEFINE TABLE community SCHEMAFULL '
      'PERMISSIONS FOR select WHERE true FOR create WHERE $auth.id != NONE'
    ),
    'fields': {
      'name': 'DEFINE FIELD name ON community TYPE string PERMISSIONS FULL',
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('community', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_no_permissions_either_side_round_trip_is_empty() -> None:
  """When neither side has permissions, no spurious MODIFY_PERMISSIONS diff."""
  code_table = table_schema(
    'community',
    fields=[
      field('name', FieldType.STRING),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE community SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'name': 'DEFINE FIELD name ON community TYPE string PERMISSIONS FULL',
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('community', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_array_subfield_round_trip_is_empty() -> None:
  """Live DB exposes `<field>.*` array element type annotations as their own
  entries in `INFO FOR TABLE`'s fields dict. These are NOT orphan fields —
  they're the per-element type spec for the parent array field. Pre-1.6.2
  the parser surfaced them as full `FieldDefinition` objects with type
  `FieldType.ANY` and the diff emitted `DROP_FIELD` for each one.
  """
  code_table = table_schema(
    'lot',
    fields=[
      field('unresolved_refs', FieldType.ARRAY),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE lot SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'unresolved_refs': ('DEFINE FIELD unresolved_refs ON lot TYPE array PERMISSIONS FULL'),
      'unresolved_refs[*]': ('DEFINE FIELD unresolved_refs[*] ON lot TYPE any PERMISSIONS FULL'),
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('lot', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_array_subfield_dotstar_form_round_trip_is_empty() -> None:
  """Variant: SurrealDB also serializes the per-element entry as
  `<field>.*` (dot-star) in some response shapes. Both forms must be
  recognized as part of the parent array type spec.
  """
  code_table = table_schema(
    'lot',
    fields=[
      field('jurisdiction', FieldType.ARRAY),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE lot SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'jurisdiction': ('DEFINE FIELD jurisdiction ON lot TYPE array PERMISSIONS FULL'),
      'jurisdiction.*': ('DEFINE FIELD jurisdiction.* ON lot TYPE any PERMISSIONS FULL'),
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('lot', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_handles_non_function_default_without_raising() -> None:
  """A live field named `default` with a `TYPE none | string` clause used to
  trip `_extract_default` because the regex `DEFAULT\\s+(.+?)...` had no word
  boundary — it matched the field NAME `default`, then captured
  `ON data_capture_spec TYPE none | string` as the alleged default value,
  which then failed `_validate_default_value` in `_field_to_sql` with
  ``Unsafe default value expression: 'ON data_capture_spec TYPE none | string'``.

  The fix is to anchor DEFAULT (and the other clause keywords) at word
  boundaries so they only match the clause, not a substring of the field name.

  Acceptance: `diff_tables` does NOT raise. Whether it returns `[]` or
  some normalised diff is a separate concern — the contract here is that
  it never raises on a non-function-call default extraction.
  """
  code_table = table_schema(
    'data_capture_spec',
    fields=[
      field('default', FieldType.STRING, nullable=True),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE data_capture_spec SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'default': ('DEFINE FIELD default ON data_capture_spec TYPE none | string PERMISSIONS FULL'),
    },
    'indexes': {},
    'events': {},
  }
  # Must not raise — pre-fix this raised `ValueError: Unsafe default value expression: ...`.
  live_table = parse_table_info('data_capture_spec', live_info)
  diffs = diff_tables(live_table, code_table)
  # And the field round-trips cleanly — no spurious modify.
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_non_function_default_in_real_default_clause_round_trips() -> None:
  """A field that legitimately has a non-function-call `DEFAULT 'foo'`
  must round-trip through parser + emitter without raising the safety
  check at diff time. The DEFAULT validator must inspect ONLY the DEFAULT
  clause body, not the field-type slice that the broken regex used to
  hand it.
  """
  code_table = table_schema(
    'data_capture_spec',
    fields=[
      field('status', FieldType.STRING, default="'pending'"),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE data_capture_spec SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'status': (
        "DEFINE FIELD status ON data_capture_spec TYPE string DEFAULT 'pending' PERMISSIONS FULL"
      ),
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('data_capture_spec', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


# ---------- additional symmetry tests for completeness ----------


def test_diff_typed_record_field_non_nullable_round_trip_is_empty() -> None:
  """Non-nullable typed record fields should also round-trip."""
  code_table = table_schema(
    'community',
    fields=[
      field('owner', FieldType.RECORD, target_table='user'),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE community SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'owner': 'DEFINE FIELD owner ON community TYPE record<user> PERMISSIONS FULL',
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('community', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_nullable_string_field_round_trip_is_empty() -> None:
  """`TYPE option<string>` -> live `TYPE none | string` should round-trip."""
  code_table = table_schema(
    'community',
    fields=[
      field('description', FieldType.STRING, nullable=True),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE community SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'description': 'DEFINE FIELD description ON community TYPE none | string PERMISSIONS FULL',
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('community', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], f'expected zero drift; got: {[d.description for d in diffs]}'


def test_diff_real_consumer_shape_round_trip_is_empty() -> None:
  """End-to-end shape: a community-style table mixing typed records,
  FLEXIBLE objects, array sub-fields, and PERMISSIONS — the exact
  collection of false-positive patterns the consumer hit. All must
  round-trip with zero drift.
  """
  code_table = table_schema(
    'community',
    fields=[
      field('name', FieldType.STRING),
      field('spec', FieldType.RECORD, target_table='data_capture_spec', nullable=True),
      field('extras', FieldType.OBJECT, flexible=True, nullable=True),
      field('nodes', FieldType.ARRAY),
    ],
  )
  live_info = {
    'tb': 'DEFINE TABLE community SCHEMAFULL PERMISSIONS NONE',
    'fields': {
      'name': 'DEFINE FIELD name ON community TYPE string PERMISSIONS FULL',
      'spec': (
        'DEFINE FIELD spec ON community TYPE none | record<data_capture_spec> PERMISSIONS FULL'
      ),
      'extras': ('DEFINE FIELD extras ON community FLEXIBLE TYPE none | object PERMISSIONS FULL'),
      'nodes': 'DEFINE FIELD nodes ON community TYPE array PERMISSIONS FULL',
      'nodes[*]': 'DEFINE FIELD nodes[*] ON community TYPE any PERMISSIONS FULL',
    },
    'indexes': {},
    'events': {},
  }
  live_table = parse_table_info('community', live_info)
  diffs = diff_tables(live_table, code_table)
  assert diffs == [], (
    f'expected zero drift for end-to-end shape; got: {[d.description for d in diffs]}'
  )


# Avoid pyflakes "imported but unused" for pytest fixture conventions
_ = pytest
