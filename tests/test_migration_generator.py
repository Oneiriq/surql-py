"""Tests for migration generator module."""

from pathlib import Path
from unittest.mock import patch

import pytest

from reverie.migration.diff import _generate_add_field_diff
from reverie.migration.generator import (
  MigrationGenerationError,
  _calculate_schema_diffs,
  _format_statements,
  _generate_filename,
  _generate_migration_content,
  _generate_version,
  create_blank_migration,
  generate_initial_migration,
  generate_migration,
  generate_migration_from_diffs,
)
from reverie.migration.models import DiffOperation, SchemaDiff
from reverie.schema.edge import EdgeDefinition, EdgeMode
from reverie.schema.fields import FieldDefinition, FieldType
from reverie.schema.table import IndexDefinition, IndexType, TableDefinition, TableMode


class TestGenerateVersion:
  """Test suite for _generate_version function."""

  def test_generate_version_format(self) -> None:
    """Test version timestamp generation format."""
    version = _generate_version()

    # Should be in format YYYYMMDD_HHMMSS
    assert len(version) == 15
    assert version[8] == '_'
    assert version[:8].isdigit()
    assert version[9:].isdigit()

  def test_generate_version_unique(self) -> None:
    """Test that consecutive versions are unique or equal."""
    version1 = _generate_version()
    version2 = _generate_version()

    # Versions should be valid and either equal (same second) or ordered
    assert len(version1) == 15
    assert len(version2) == 15
    assert version1 <= version2


class TestGenerateFilename:
  """Test suite for _generate_filename function."""

  def test_generate_filename_basic(self) -> None:
    """Test basic filename generation."""
    filename = _generate_filename('20260102_120000', 'Create user table')

    assert filename == '20260102_120000_create_user_table.py'
    assert filename.endswith('.py')
    assert filename.startswith('20260102_120000')

  def test_generate_filename_sanitization(self) -> None:
    """Test filename sanitization of special characters."""
    filename = _generate_filename('20260102_120000', 'Add @special #chars!')

    assert filename == '20260102_120000_add_special_chars.py'
    assert '@' not in filename
    assert '#' not in filename
    assert '!' not in filename

  def test_generate_filename_spaces_to_underscores(self) -> None:
    """Test that spaces are converted to underscores."""
    filename = _generate_filename('20260102_120000', 'Multiple Word Description')

    assert filename == '20260102_120000_multiple_word_description.py'
    assert ' ' not in filename

  def test_generate_filename_lowercase(self) -> None:
    """Test that description is lowercased."""
    filename = _generate_filename('20260102_120000', 'UPPERCASE')

    assert filename == '20260102_120000_uppercase.py'
    assert 'UPPERCASE' not in filename


class TestFormatStatements:
  """Test suite for _format_statements function."""

  def test_format_statements_empty(self) -> None:
    """Test formatting empty statement list."""
    result = _format_statements([])

    assert result == ''

  def test_format_statements_single(self) -> None:
    """Test formatting single statement."""
    statements = ['CREATE TABLE user;']
    result = _format_statements(statements)

    assert "'CREATE TABLE user;'," in result
    assert result.startswith('    ')

  def test_format_statements_multiple(self) -> None:
    """Test formatting multiple statements."""
    statements = [
      'CREATE TABLE user;',
      'CREATE TABLE post;',
      'CREATE INDEX idx ON TABLE user;',
    ]
    result = _format_statements(statements)

    lines = result.split('\n')
    assert len(lines) == 3
    assert all(line.startswith('    ') for line in lines)
    assert all(line.endswith(',') for line in lines)

  def test_format_statements_escapes_quotes(self) -> None:
    """Test that single quotes in statements are escaped."""
    statements = ["DEFINE FIELD name ON TABLE user ASSERT $value != '';"]
    result = _format_statements(statements)

    # Single quotes should be escaped
    assert "\\'" in result
    assert result.count("\\'") == 2


class TestGenerateMigrationContent:
  """Test suite for _generate_migration_content function."""

  def test_generate_migration_content_structure(self) -> None:
    """Test generated migration file structure."""
    diffs = [
      SchemaDiff(
        operation=DiffOperation.ADD_TABLE,
        table='user',
        description='Add user table',
        forward_sql='DEFINE TABLE user SCHEMAFULL;',
        backward_sql='REMOVE TABLE user;',
      )
    ]

    content = _generate_migration_content(
      version='20260102_120000',
      description='Create user table',
      diffs=diffs,
      author='test_author',
    )

    # Check required components
    assert 'def up() -> list[str]:' in content
    assert 'def down() -> list[str]:' in content
    assert 'metadata = {' in content
    assert "'version': '20260102_120000'" in content
    assert "'description': 'Create user table'" in content
    assert "'author': 'test_author'" in content
    assert 'DEFINE TABLE user SCHEMAFULL;' in content

  def test_generate_migration_content_reverse_order_down(self) -> None:
    """Test that down statements are in reverse order."""
    diffs = [
      SchemaDiff(
        operation=DiffOperation.ADD_TABLE,
        table='user',
        description='Add user table',
        forward_sql='DEFINE TABLE user SCHEMAFULL;',
        backward_sql='REMOVE TABLE user;',
      ),
      SchemaDiff(
        operation=DiffOperation.ADD_FIELD,
        table='user',
        field='name',
        description='Add name field',
        forward_sql='DEFINE FIELD name ON TABLE user TYPE string;',
        backward_sql='REMOVE FIELD name ON TABLE user;',
      ),
    ]

    content = _generate_migration_content(
      version='20260102_120000',
      description='Test',
      diffs=diffs,
      author='test',
    )

    # Find down function
    down_start = content.find('def down()')
    down_end = content.find('metadata = {')
    down_section = content[down_start:down_end]

    # Check that backward statements are in reverse order
    field_pos = down_section.find('REMOVE FIELD')
    table_pos = down_section.find('REMOVE TABLE')

    # Field removal should come before table removal in down()
    assert field_pos < table_pos

  def test_generate_migration_content_with_docstrings(self) -> None:
    """Test that generated content includes docstrings."""
    diffs = [
      SchemaDiff(
        operation=DiffOperation.ADD_TABLE,
        table='user',
        description='Add user table',
        forward_sql='DEFINE TABLE user SCHEMAFULL;',
        backward_sql='REMOVE TABLE user;',
      )
    ]

    content = _generate_migration_content('20260102_120000', 'Test', diffs, 'test')

    assert '"""Migration: Test' in content
    assert '"""Apply migration (forward)."""' in content
    assert '"""Rollback migration (backward)."""' in content


class TestCalculateSchemaDiffs:
  """Test suite for _calculate_schema_diffs function."""

  def test_calculate_schema_diffs_empty(self) -> None:
    """Test calculating diffs with no changes."""
    diffs = _calculate_schema_diffs({}, {}, {}, {})

    assert diffs == []

  def test_calculate_schema_diffs_add_table(self) -> None:
    """Test calculating diffs when adding a table."""
    new_table = TableDefinition(
      name='user',
      mode=TableMode.SCHEMAFULL,
      fields=[FieldDefinition(name='name', type=FieldType.STRING)],
    )

    diffs = _calculate_schema_diffs({}, {'user': new_table}, {}, {})

    assert len(diffs) > 0
    assert any(d.operation == DiffOperation.ADD_TABLE for d in diffs)
    assert any(d.table == 'user' for d in diffs)

  def test_calculate_schema_diffs_drop_table(self) -> None:
    """Test calculating diffs when dropping a table."""
    old_table = TableDefinition(
      name='user',
      mode=TableMode.SCHEMAFULL,
      fields=[FieldDefinition(name='name', type=FieldType.STRING)],
    )

    diffs = _calculate_schema_diffs({'user': old_table}, {}, {}, {})

    assert len(diffs) > 0
    assert any(d.operation == DiffOperation.DROP_TABLE for d in diffs)

  def test_calculate_schema_diffs_add_edge(self) -> None:
    """Test calculating diffs when adding an edge."""
    new_edge = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
    )

    diffs = _calculate_schema_diffs({}, {}, {}, {'likes': new_edge})

    assert len(diffs) > 0
    assert any(d.table == 'likes' for d in diffs)

  def test_calculate_schema_diffs_sorted_order(self) -> None:
    """Test that diffs are calculated in sorted order."""
    table_a = TableDefinition(name='a_table', mode=TableMode.SCHEMAFULL)
    table_z = TableDefinition(name='z_table', mode=TableMode.SCHEMAFULL)
    table_m = TableDefinition(name='m_table', mode=TableMode.SCHEMAFULL)

    new_tables = {'z_table': table_z, 'a_table': table_a, 'm_table': table_m}

    diffs = _calculate_schema_diffs({}, new_tables, {}, {})

    # Extract table names from diffs in order
    add_table_diffs = [d for d in diffs if d.operation == DiffOperation.ADD_TABLE]
    table_names = [d.table for d in add_table_diffs]

    # Should be sorted alphabetically
    assert table_names == ['a_table', 'm_table', 'z_table']


class TestCreateBlankMigration:
  """Test suite for create_blank_migration function."""

  def test_create_blank_migration_success(self, temp_migration_dir: Path) -> None:
    """Test creating a blank migration file."""
    description = 'add_new_feature'

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = create_blank_migration(temp_migration_dir, description)

    assert filepath.exists()
    assert filepath.name == '20260102_120000_add_new_feature.py'

    # Validate content structure
    content = filepath.read_text()
    assert 'def up() -> list[str]:' in content
    assert 'def down() -> list[str]:' in content
    assert description in content
    assert 'Add your forward migration SQL statements here.' in content
    assert 'Add your rollback SQL statements here' in content

  def test_create_blank_migration_creates_directory(self, tmp_path: Path) -> None:
    """Test that migration directory is created if it doesn't exist."""
    migrations_dir = tmp_path / 'new_migrations'

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = create_blank_migration(migrations_dir, 'test')

    assert migrations_dir.exists()
    assert filepath.exists()

  def test_create_blank_migration_custom_author(self, temp_migration_dir: Path) -> None:
    """Test creating blank migration with custom author."""
    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = create_blank_migration(temp_migration_dir, 'test', author='custom_author')

    content = filepath.read_text()
    assert "'author': 'custom_author'" in content

  def test_create_blank_migration_metadata(self, temp_migration_dir: Path) -> None:
    """Test that blank migration includes proper metadata."""
    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = create_blank_migration(temp_migration_dir, 'test_migration')

    content = filepath.read_text()
    assert 'metadata = {' in content
    assert "'version': '20260102_120000'" in content
    assert "'description': 'test_migration'" in content
    assert "'depends_on': []" in content


class TestGenerateInitialMigration:
  """Test suite for generate_initial_migration function."""

  def test_generate_initial_migration_success(self, temp_migration_dir: Path) -> None:
    """Test generating initial migration from tables."""
    tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[FieldDefinition(name='name', type=FieldType.STRING)],
      )
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_initial_migration(temp_migration_dir, tables)

    assert filepath.exists()
    assert 'initial_schema' in filepath.name.lower()

    content = filepath.read_text()
    assert 'DEFINE TABLE user SCHEMAFULL;' in content

  def test_generate_initial_migration_with_edges(self, temp_migration_dir: Path) -> None:
    """Test generating initial migration with edges."""
    tables = {
      'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
    }
    edges = {
      'likes': EdgeDefinition(
        name='likes',
        mode=EdgeMode.RELATION,
        from_table='user',
        to_table='post',
      )
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_initial_migration(temp_migration_dir, tables, edges=edges)

    content = filepath.read_text()
    assert filepath.exists()
    assert 'DEFINE TABLE likes' in content

  def test_generate_initial_migration_custom_description(self, temp_migration_dir: Path) -> None:
    """Test generating initial migration with custom description."""
    tables = {
      'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_initial_migration(
        temp_migration_dir, tables, description='Custom initial setup'
      )

    assert 'custom_initial_setup' in filepath.name.lower()

    content = filepath.read_text()
    assert 'Custom initial setup' in content


class TestGenerateMigration:
  """Test suite for generate_migration function."""

  def test_generate_migration_add_table(self, temp_migration_dir: Path) -> None:
    """Test generating migration for adding a table."""
    new_tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[FieldDefinition(name='name', type=FieldType.STRING)],
      )
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration(
        temp_migration_dir,
        'Create user table',
        new_tables=new_tables,
      )

    assert filepath.exists()
    assert filepath.name == '20260102_120000_create_user_table.py'

    content = filepath.read_text()
    assert 'DEFINE TABLE user SCHEMAFULL;' in content
    assert 'DEFINE FIELD name ON TABLE user TYPE string;' in content

  def test_generate_migration_no_changes_error(self, temp_migration_dir: Path) -> None:
    """Test that error is raised when no schema changes detected."""
    with pytest.raises(MigrationGenerationError) as exc_info:
      generate_migration(temp_migration_dir, 'No changes')

    assert 'No schema changes detected' in str(exc_info.value)

  def test_generate_migration_drop_table(self, temp_migration_dir: Path) -> None:
    """Test generating migration for dropping a table."""
    old_tables = {
      'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration(
        temp_migration_dir,
        'Drop user table',
        old_tables=old_tables,
      )

    content = filepath.read_text()
    assert 'REMOVE TABLE user;' in content

  def test_generate_migration_modify_table(self, temp_migration_dir: Path) -> None:
    """Test generating migration for modifying a table."""
    old_tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[FieldDefinition(name='name', type=FieldType.STRING)],
      )
    }
    new_tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[
          FieldDefinition(name='name', type=FieldType.STRING),
          FieldDefinition(name='email', type=FieldType.STRING),
        ],
      )
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration(
        temp_migration_dir,
        'Add email field',
        old_tables=old_tables,
        new_tables=new_tables,
      )

    content = filepath.read_text()
    assert 'DEFINE FIELD email ON TABLE user TYPE string;' in content

  def test_generate_migration_creates_directory(self, tmp_path: Path) -> None:
    """Test that migration directory is created if needed."""
    migrations_dir = tmp_path / 'new_migrations'
    new_tables = {
      'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration(
        migrations_dir,
        'Test',
        new_tables=new_tables,
      )

    assert migrations_dir.exists()
    assert filepath.exists()

  def test_generate_migration_custom_author(self, temp_migration_dir: Path) -> None:
    """Test generating migration with custom author."""
    new_tables = {
      'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration(
        temp_migration_dir,
        'Test',
        new_tables=new_tables,
        author='custom_author',
      )

    content = filepath.read_text()
    assert "'author': 'custom_author'" in content

  def test_generate_migration_with_index(self, temp_migration_dir: Path) -> None:
    """Test generating migration with index."""
    new_tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[FieldDefinition(name='email', type=FieldType.STRING)],
        indexes=[
          IndexDefinition(
            name='email_idx',
            columns=['email'],
            type=IndexType.UNIQUE,
          )
        ],
      )
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration(
        temp_migration_dir,
        'Add user with index',
        new_tables=new_tables,
      )

    content = filepath.read_text()
    assert 'DEFINE INDEX email_idx' in content
    assert 'UNIQUE' in content


class TestGenerateMigrationFromDiffs:
  """Test suite for generate_migration_from_diffs function."""

  def test_generate_migration_from_diffs_success(self, temp_migration_dir: Path) -> None:
    """Test generating migration from diff list."""
    diffs = [
      SchemaDiff(
        operation=DiffOperation.ADD_TABLE,
        table='user',
        description='Add user table',
        forward_sql='DEFINE TABLE user SCHEMAFULL;',
        backward_sql='REMOVE TABLE user;',
      )
    ]

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration_from_diffs(
        temp_migration_dir,
        'Update user table',
        diffs,
      )

    assert filepath.exists()
    content = filepath.read_text()
    assert 'DEFINE TABLE user SCHEMAFULL;' in content

  def test_generate_migration_from_diffs_empty_error(self, temp_migration_dir: Path) -> None:
    """Test that error is raised when no diffs provided."""
    with pytest.raises(MigrationGenerationError) as exc_info:
      generate_migration_from_diffs(temp_migration_dir, 'Test', [])

    assert 'No diffs provided' in str(exc_info.value)

  def test_generate_migration_from_diffs_multiple(self, temp_migration_dir: Path) -> None:
    """Test generating migration from multiple diffs."""
    diffs = [
      SchemaDiff(
        operation=DiffOperation.ADD_TABLE,
        table='user',
        description='Add user table',
        forward_sql='DEFINE TABLE user SCHEMAFULL;',
        backward_sql='REMOVE TABLE user;',
      ),
      SchemaDiff(
        operation=DiffOperation.ADD_FIELD,
        table='user',
        field='name',
        description='Add name field',
        forward_sql='DEFINE FIELD name ON TABLE user TYPE string;',
        backward_sql='REMOVE FIELD name ON TABLE user;',
      ),
    ]

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration_from_diffs(
        temp_migration_dir,
        'Complex migration',
        diffs,
      )

    content = filepath.read_text()
    assert 'DEFINE TABLE user SCHEMAFULL;' in content
    assert 'DEFINE FIELD name ON TABLE user TYPE string;' in content


class TestMigrationGenerationError:
  """Test suite for MigrationGenerationError exception."""

  def test_migration_generation_error_message(self) -> None:
    """Test MigrationGenerationError with custom message."""
    error = MigrationGenerationError('Custom error message')

    assert str(error) == 'Custom error message'
    assert isinstance(error, Exception)

  def test_migration_generation_error_inheritance(self) -> None:
    """Test that MigrationGenerationError inherits from Exception."""
    error = MigrationGenerationError('Test')

    assert isinstance(error, Exception)


class TestAddFieldBackfillSQL:
  """Test suite for backfill SQL generation when adding fields with defaults."""

  def test_field_with_default_includes_backfill_sql(self) -> None:
    """Test that adding a field with a default generates backfill UPDATE."""
    field = FieldDefinition(
      name='is_active',
      type=FieldType.BOOL,
      default='true',
    )

    diff = _generate_add_field_diff('user', field)

    # Forward SQL should contain both DEFINE FIELD and UPDATE
    assert 'DEFINE FIELD is_active ON TABLE user TYPE bool' in diff.forward_sql
    assert 'DEFAULT true' in diff.forward_sql
    assert 'UPDATE user SET is_active = true WHERE is_active IS NONE;' in diff.forward_sql

  def test_field_without_default_no_backfill_sql(self) -> None:
    """Test that adding a field without a default does NOT generate backfill."""
    field = FieldDefinition(
      name='name',
      type=FieldType.STRING,
    )

    diff = _generate_add_field_diff('user', field)

    # Forward SQL should only contain DEFINE FIELD, no UPDATE
    assert 'DEFINE FIELD name ON TABLE user TYPE string;' in diff.forward_sql
    assert 'UPDATE' not in diff.forward_sql
    assert 'WHERE' not in diff.forward_sql

  def test_field_with_string_default_includes_backfill(self) -> None:
    """Test backfill SQL with string default value."""
    field = FieldDefinition(
      name='status',
      type=FieldType.STRING,
      default="'pending'",
    )

    diff = _generate_add_field_diff('order', field)

    assert "DEFAULT 'pending'" in diff.forward_sql
    assert "UPDATE order SET status = 'pending' WHERE status IS NONE;" in diff.forward_sql

  def test_field_with_numeric_default_includes_backfill(self) -> None:
    """Test backfill SQL with numeric default value."""
    field = FieldDefinition(
      name='score',
      type=FieldType.INT,
      default='0',
    )

    diff = _generate_add_field_diff('player', field)

    assert 'DEFAULT 0' in diff.forward_sql
    assert 'UPDATE player SET score = 0 WHERE score IS NONE;' in diff.forward_sql

  def test_backfill_sql_format(self) -> None:
    """Test the exact format of backfill SQL (newline separated)."""
    field = FieldDefinition(
      name='enabled',
      type=FieldType.BOOL,
      default='false',
    )

    diff = _generate_add_field_diff('feature', field)

    # Should be two statements separated by newline
    lines = diff.forward_sql.strip().split('\n')
    assert len(lines) == 2
    assert lines[0].startswith('DEFINE FIELD')
    assert lines[1].startswith('UPDATE feature SET')

  def test_backward_sql_unchanged(self) -> None:
    """Test that backward SQL remains simple REMOVE FIELD."""
    field = FieldDefinition(
      name='count',
      type=FieldType.INT,
      default='100',
    )

    diff = _generate_add_field_diff('stats', field)

    # Backward SQL should just be REMOVE FIELD (no cleanup needed)
    assert diff.backward_sql == 'REMOVE FIELD count ON TABLE stats;'


class TestGenerateMigrationWithBackfill:
  """Test suite for end-to-end migration generation with backfill SQL."""

  def test_generate_migration_add_field_with_default(self, temp_migration_dir: Path) -> None:
    """Test generated migration includes backfill for field with default."""
    old_tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[FieldDefinition(name='name', type=FieldType.STRING)],
      )
    }
    new_tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[
          FieldDefinition(name='name', type=FieldType.STRING),
          FieldDefinition(name='is_active', type=FieldType.BOOL, default='true'),
        ],
      )
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration(
        temp_migration_dir,
        'Add is_active field',
        old_tables=old_tables,
        new_tables=new_tables,
      )

    content = filepath.read_text()
    assert 'DEFINE FIELD is_active ON TABLE user TYPE bool' in content
    assert 'DEFAULT true' in content
    assert 'UPDATE user SET is_active = true WHERE is_active IS NONE;' in content

  def test_generate_migration_add_field_without_default(self, temp_migration_dir: Path) -> None:
    """Test generated migration does NOT include backfill for field without default."""
    old_tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[FieldDefinition(name='name', type=FieldType.STRING)],
      )
    }
    new_tables = {
      'user': TableDefinition(
        name='user',
        mode=TableMode.SCHEMAFULL,
        fields=[
          FieldDefinition(name='name', type=FieldType.STRING),
          FieldDefinition(name='email', type=FieldType.STRING),
        ],
      )
    }

    with patch('reverie.migration.generator._generate_version', return_value='20260102_120000'):
      filepath = generate_migration(
        temp_migration_dir,
        'Add email field',
        old_tables=old_tables,
        new_tables=new_tables,
      )

    content = filepath.read_text()
    assert 'DEFINE FIELD email ON TABLE user TYPE string;' in content
    # Should NOT have any UPDATE backfill statement
    assert 'UPDATE user SET email' not in content
