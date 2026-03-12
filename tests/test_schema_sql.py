"""Tests for SQL generation from schema definitions."""

import pytest

from reverie.schema.edge import EdgeMode, edge_schema
from reverie.schema.fields import datetime_field, int_field, string_field
from reverie.schema.sql import generate_edge_sql, generate_schema_sql, generate_table_sql
from reverie.schema.table import (
  IndexType,
  TableMode,
  event,
  index,
  table_schema,
  unique_index,
)


class TestGenerateTableSql:
  """Tests for generate_table_sql function."""

  def test_schemafull_table_minimal(self) -> None:
    """Generates correct DEFINE TABLE statement for SCHEMAFULL table."""
    table = table_schema('user', mode=TableMode.SCHEMAFULL)

    stmts = generate_table_sql(table)

    assert stmts[0] == 'DEFINE TABLE user SCHEMAFULL;'

  def test_schemaless_table(self) -> None:
    """Generates correct DEFINE TABLE statement for SCHEMALESS table."""
    table = table_schema('log', mode=TableMode.SCHEMALESS)

    stmts = generate_table_sql(table)

    assert stmts[0] == 'DEFINE TABLE log SCHEMALESS;'

  def test_table_with_fields(self) -> None:
    """Generates DEFINE FIELD statements for each field."""
    table = table_schema(
      'user',
      mode=TableMode.SCHEMAFULL,
      fields=[string_field('name'), int_field('age')],
    )

    stmts = generate_table_sql(table)

    assert any('DEFINE FIELD name ON TABLE user TYPE string' in s for s in stmts)
    assert any('DEFINE FIELD age ON TABLE user TYPE int' in s for s in stmts)

  def test_table_with_field_assertion(self) -> None:
    """Generates ASSERT clause for fields with assertions."""
    table = table_schema(
      'user',
      fields=[string_field('email', assertion='string::is::email($value)')],
    )

    stmts = generate_table_sql(table)

    assert any('ASSERT string::is::email($value)' in s for s in stmts)

  def test_table_with_field_default(self) -> None:
    """Generates DEFAULT clause for fields with defaults."""
    table = table_schema(
      'event',
      fields=[datetime_field('created_at', default='time::now()')],
    )

    stmts = generate_table_sql(table)

    assert any('DEFAULT time::now()' in s for s in stmts)

  def test_table_with_readonly_field(self) -> None:
    """Generates READONLY clause for readonly fields."""
    table = table_schema(
      'event',
      fields=[datetime_field('created_at', readonly=True)],
    )

    stmts = generate_table_sql(table)

    assert any('READONLY' in s for s in stmts)

  def test_table_with_unique_index(self) -> None:
    """Generates DEFINE INDEX statement with UNIQUE constraint."""
    table = table_schema(
      'user',
      indexes=[unique_index('email_idx', ['email'])],
    )

    stmts = generate_table_sql(table)

    assert any('DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE' in s for s in stmts)

  def test_table_with_standard_index(self) -> None:
    """Generates DEFINE INDEX statement for standard index."""
    table = table_schema(
      'post',
      indexes=[index('title_idx', ['title'], IndexType.STANDARD)],
    )

    stmts = generate_table_sql(table)

    assert any('DEFINE INDEX title_idx ON TABLE post COLUMNS title' in s for s in stmts)

  def test_table_with_event(self) -> None:
    """Generates DEFINE EVENT statement."""
    table = table_schema(
      'user',
      events=[
        event(
          'email_changed',
          '$before.email != $after.email',
          'CREATE audit_log SET user = $value.id',
        )
      ],
    )

    stmts = generate_table_sql(table)

    assert any('DEFINE EVENT email_changed ON TABLE user' in s for s in stmts)
    assert any('WHEN $before.email != $after.email' in s for s in stmts)

  def test_table_with_permissions(self) -> None:
    """Generates DEFINE FIELD PERMISSIONS statements for permissions."""
    table = table_schema(
      'user',
      permissions={'select': '$auth.id = id'},
    )

    stmts = generate_table_sql(table)

    assert any('FOR SELECT' in s and '$auth.id = id' in s for s in stmts)

  def test_minimal_table_returns_single_statement(self) -> None:
    """Returns exactly one statement for a table with no components."""
    table = table_schema('empty')

    stmts = generate_table_sql(table)

    assert len(stmts) == 1

  def test_statement_order(self) -> None:
    """Table DEFINE TABLE statement is always first."""
    table = table_schema(
      'user',
      fields=[string_field('name')],
      indexes=[unique_index('name_idx', ['name'])],
    )

    stmts = generate_table_sql(table)

    assert stmts[0].startswith('DEFINE TABLE')


class TestGenerateEdgeSql:
  """Tests for generate_edge_sql function."""

  def test_relation_edge_with_from_to(self) -> None:
    """Generates TYPE RELATION statement with FROM and TO constraints."""
    edge = edge_schema('likes', from_table='user', to_table='post')

    stmts = generate_edge_sql(edge)

    assert stmts[0] == 'DEFINE TABLE likes TYPE RELATION FROM user TO post;'

  def test_schemafull_edge(self) -> None:
    """Generates SCHEMAFULL table for SCHEMAFULL edge mode."""
    edge = edge_schema('entity_relation', mode=EdgeMode.SCHEMAFULL)

    stmts = generate_edge_sql(edge)

    assert stmts[0] == 'DEFINE TABLE entity_relation SCHEMAFULL;'

  def test_schemaless_edge(self) -> None:
    """Generates SCHEMALESS table for SCHEMALESS edge mode."""
    edge = edge_schema('loose_rel', mode=EdgeMode.SCHEMALESS)

    stmts = generate_edge_sql(edge)

    assert stmts[0] == 'DEFINE TABLE loose_rel SCHEMALESS;'

  def test_edge_with_fields(self) -> None:
    """Generates DEFINE FIELD statements for edge fields."""
    edge = edge_schema(
      'likes',
      from_table='user',
      to_table='post',
      fields=[datetime_field('created_at', default='time::now()')],
    )

    stmts = generate_edge_sql(edge)

    assert any('DEFINE FIELD created_at ON TABLE likes TYPE datetime' in s for s in stmts)

  def test_relation_edge_missing_from_table_raises(self) -> None:
    """Raises ValueError for RELATION mode when from_table is missing."""
    edge = edge_schema('likes', to_table='post')

    with pytest.raises(ValueError, match='requires both from_table and to_table'):
      generate_edge_sql(edge)

  def test_relation_edge_missing_to_table_raises(self) -> None:
    """Raises ValueError for RELATION mode when to_table is missing."""
    edge = edge_schema('likes', from_table='user')

    with pytest.raises(ValueError, match='requires both from_table and to_table'):
      generate_edge_sql(edge)

  def test_relation_edge_missing_both_tables_raises(self) -> None:
    """Raises ValueError for RELATION mode when both tables are missing."""
    edge = edge_schema('likes')

    with pytest.raises(ValueError):
      generate_edge_sql(edge)

  def test_schemafull_edge_no_tables_required(self) -> None:
    """SCHEMAFULL edge does not require from_table or to_table."""
    edge = edge_schema('entity_rel', mode=EdgeMode.SCHEMAFULL)

    stmts = generate_edge_sql(edge)

    assert len(stmts) >= 1

  def test_edge_statement_starts_with_define_table(self) -> None:
    """First statement is always DEFINE TABLE."""
    edge = edge_schema('follows', from_table='user', to_table='user')

    stmts = generate_edge_sql(edge)

    assert stmts[0].startswith('DEFINE TABLE')


class TestGenerateSchemaSql:
  """Tests for generate_schema_sql function."""

  def test_combines_tables_and_edges(self) -> None:
    """Output includes SQL for both tables and edges."""
    user_table = table_schema('user', mode=TableMode.SCHEMAFULL)
    likes_edge = edge_schema('likes', from_table='user', to_table='post')

    sql = generate_schema_sql(
      tables={'user': user_table},
      edges={'likes': likes_edge},
    )

    assert 'DEFINE TABLE user SCHEMAFULL' in sql
    assert 'DEFINE TABLE likes TYPE RELATION FROM user TO post' in sql

  def test_tables_only(self) -> None:
    """Generates SQL for tables when no edges provided."""
    user_table = table_schema('user')

    sql = generate_schema_sql(tables={'user': user_table})

    assert 'DEFINE TABLE user' in sql

  def test_edges_only(self) -> None:
    """Generates SQL for edges when no tables provided."""
    edge = edge_schema('follows', from_table='user', to_table='user')

    sql = generate_schema_sql(edges={'follows': edge})

    assert 'DEFINE TABLE follows TYPE RELATION' in sql

  def test_empty_returns_empty_string(self) -> None:
    """Returns empty string when no tables or edges provided."""
    sql = generate_schema_sql()

    assert sql == ''

  def test_multiple_tables(self) -> None:
    """Generates SQL for multiple tables."""
    sql = generate_schema_sql(
      tables={
        'user': table_schema('user'),
        'post': table_schema('post'),
      }
    )

    assert 'DEFINE TABLE user' in sql
    assert 'DEFINE TABLE post' in sql

  def test_result_is_string(self) -> None:
    """Returns a string result."""
    user_table = table_schema('user')

    result = generate_schema_sql(tables={'user': user_table})

    assert isinstance(result, str)
