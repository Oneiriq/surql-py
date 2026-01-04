"""Tests for the schema visualization module.

This module provides comprehensive tests for schema visualization including
output format enums, diagram generators (Mermaid, GraphViz, ASCII), and CLI commands.
"""

import re
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from reverie.cli.schema import app as schema_app
from reverie.schema.edge import EdgeDefinition, EdgeMode
from reverie.schema.fields import FieldDefinition, FieldType
from reverie.schema.table import (
  IndexDefinition,
  IndexType,
  TableDefinition,
  TableMode,
)
from reverie.schema.visualize import (
  ASCIIGenerator,
  GraphVizGenerator,
  MermaidGenerator,
  OutputFormat,
  visualize_from_registry,
  visualize_schema,
)


def strip_ansi(text: str) -> str:
  """Remove ANSI escape sequences from text."""
  ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
  return ansi_escape.sub('', text)


# Test Fixtures


@pytest.fixture
def simple_table() -> TableDefinition:
  """Provide a simple table with basic fields."""
  return TableDefinition(
    name='user',
    mode=TableMode.SCHEMAFULL,
    fields=[
      FieldDefinition(name='name', type=FieldType.STRING),
      FieldDefinition(name='email', type=FieldType.STRING),
      FieldDefinition(name='age', type=FieldType.INT),
    ],
  )


@pytest.fixture
def table_with_indexes() -> TableDefinition:
  """Provide a table with index definitions."""
  return TableDefinition(
    name='user',
    mode=TableMode.SCHEMAFULL,
    fields=[
      FieldDefinition(name='email', type=FieldType.STRING),
      FieldDefinition(name='username', type=FieldType.STRING),
    ],
    indexes=[
      IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE),
      IndexDefinition(name='username_idx', columns=['username'], type=IndexType.STANDARD),
    ],
  )


@pytest.fixture
def table_with_record_field() -> TableDefinition:
  """Provide a table with a record (foreign key) field."""
  return TableDefinition(
    name='post',
    mode=TableMode.SCHEMAFULL,
    fields=[
      FieldDefinition(name='title', type=FieldType.STRING),
      FieldDefinition(name='author', type=FieldType.RECORD),
      FieldDefinition(name='content', type=FieldType.STRING),
    ],
  )


@pytest.fixture
def empty_table() -> TableDefinition:
  """Provide a table with no fields."""
  return TableDefinition(
    name='empty',
    mode=TableMode.SCHEMALESS,
    fields=[],
  )


@pytest.fixture
def edge_definition() -> EdgeDefinition:
  """Provide a basic edge definition."""
  return EdgeDefinition(
    name='follows',
    mode=EdgeMode.RELATION,
    from_table='user',
    to_table='user',
    fields=[
      FieldDefinition(name='since', type=FieldType.DATETIME),
    ],
  )


@pytest.fixture
def edge_between_tables() -> EdgeDefinition:
  """Provide an edge between different tables."""
  return EdgeDefinition(
    name='wrote',
    mode=EdgeMode.RELATION,
    from_table='user',
    to_table='post',
    fields=[],
  )


@pytest.fixture
def multiple_tables() -> dict[str, TableDefinition]:
  """Provide multiple related tables."""
  return {
    'user': TableDefinition(
      name='user',
      mode=TableMode.SCHEMAFULL,
      fields=[
        FieldDefinition(name='name', type=FieldType.STRING),
        FieldDefinition(name='email', type=FieldType.STRING),
      ],
      indexes=[
        IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE),
      ],
    ),
    'post': TableDefinition(
      name='post',
      mode=TableMode.SCHEMAFULL,
      fields=[
        FieldDefinition(name='title', type=FieldType.STRING),
        FieldDefinition(name='content', type=FieldType.STRING),
      ],
    ),
    'comment': TableDefinition(
      name='comment',
      mode=TableMode.SCHEMAFULL,
      fields=[
        FieldDefinition(name='text', type=FieldType.STRING),
      ],
    ),
  }


@pytest.fixture
def multiple_edges() -> dict[str, EdgeDefinition]:
  """Provide multiple edge definitions."""
  return {
    'wrote': EdgeDefinition(
      name='wrote',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      fields=[FieldDefinition(name='published_at', type=FieldType.DATETIME)],
    ),
    'commented': EdgeDefinition(
      name='commented',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='comment',
      fields=[],
    ),
  }


# OutputFormat Enum Tests


class TestOutputFormatEnum:
  """Test suite for OutputFormat enum."""

  def test_mermaid_value(self) -> None:
    """Test MERMAID enum value."""
    assert OutputFormat.MERMAID.value == 'mermaid'

  def test_graphviz_value(self) -> None:
    """Test GRAPHVIZ enum value."""
    assert OutputFormat.GRAPHVIZ.value == 'graphviz'

  def test_ascii_value(self) -> None:
    """Test ASCII enum value."""
    assert OutputFormat.ASCII.value == 'ascii'

  def test_all_format_values(self) -> None:
    """Test all format values are present."""
    formats = [f.value for f in OutputFormat]
    assert 'mermaid' in formats
    assert 'graphviz' in formats
    assert 'ascii' in formats
    assert len(formats) == 3

  def test_format_string_conversion(self) -> None:
    """Test string conversion of enum values."""
    assert str(OutputFormat.MERMAID.value) == 'mermaid'
    assert str(OutputFormat.GRAPHVIZ.value) == 'graphviz'
    assert str(OutputFormat.ASCII.value) == 'ascii'


# MermaidGenerator Tests


class TestMermaidGenerator:
  """Test suite for MermaidGenerator class."""

  def test_generation_with_single_table(self, simple_table: TableDefinition) -> None:
    """Test Mermaid generation with a single table."""
    generator = MermaidGenerator()
    result = generator.generate({'user': simple_table}, {})

    assert 'erDiagram' in result
    assert 'user {' in result
    assert 'string id PK' in result
    assert 'string name' in result
    assert 'string email' in result
    assert 'int age' in result

  def test_generation_with_multiple_tables(
    self, multiple_tables: dict[str, TableDefinition]
  ) -> None:
    """Test Mermaid generation with multiple tables."""
    generator = MermaidGenerator()
    result = generator.generate(multiple_tables, {})

    assert 'erDiagram' in result
    assert 'user {' in result
    assert 'post {' in result
    assert 'comment {' in result

  def test_field_type_annotations_pk(self, simple_table: TableDefinition) -> None:
    """Test that id field gets PK annotation."""
    generator = MermaidGenerator()
    result = generator.generate({'user': simple_table}, {})

    # The implicit id field should have PK
    assert 'string id PK' in result

  def test_field_type_annotations_fk(self, table_with_record_field: TableDefinition) -> None:
    """Test that record fields get FK annotation."""
    generator = MermaidGenerator()
    result = generator.generate({'post': table_with_record_field}, {})

    # The author field is type RECORD, should have FK
    assert 'record author FK' in result

  def test_field_type_annotations_uk(self, table_with_indexes: TableDefinition) -> None:
    """Test that unique indexed fields get UK annotation."""
    generator = MermaidGenerator()
    result = generator.generate({'user': table_with_indexes}, {})

    # The email field has a unique index, should have UK
    assert 'string email UK' in result
    # The username field has a standard index, no UK
    lines = result.split('\n')
    username_lines = [ln for ln in lines if 'username' in ln]
    assert len(username_lines) == 1
    assert 'UK' not in username_lines[0]

  def test_relationship_cardinality_different_tables(
    self,
    multiple_tables: dict[str, TableDefinition],
    edge_between_tables: EdgeDefinition,
  ) -> None:
    """Test relationship cardinality for edges between different tables."""
    generator = MermaidGenerator()
    result = generator.generate(multiple_tables, {'wrote': edge_between_tables})

    # One-to-many cardinality for different tables
    assert '||--o{' in result
    assert 'user ||--o{ post : wrote' in result

  def test_relationship_cardinality_self_referential(
    self,
    simple_table: TableDefinition,
    edge_definition: EdgeDefinition,
  ) -> None:
    """Test relationship cardinality for self-referential edges."""
    generator = MermaidGenerator()
    result = generator.generate({'user': simple_table}, {'follows': edge_definition})

    # Many-to-many cardinality for self-referential
    assert '}o--o{' in result
    assert 'user }o--o{ user : follows' in result

  def test_generation_with_no_fields_include_false(self, simple_table: TableDefinition) -> None:
    """Test generation with include_fields=False."""
    generator = MermaidGenerator()
    result = generator.generate({'user': simple_table}, {}, include_fields=False)

    assert 'erDiagram' in result
    assert 'user {' in result
    # Fields should not be present
    assert 'string name' not in result
    assert 'string email' not in result

  def test_generation_with_no_edges_include_false(
    self,
    multiple_tables: dict[str, TableDefinition],
    multiple_edges: dict[str, EdgeDefinition],
  ) -> None:
    """Test generation with include_edges=False."""
    generator = MermaidGenerator()
    result = generator.generate(multiple_tables, multiple_edges, include_edges=False)

    assert 'erDiagram' in result
    # Relationships should not be present
    assert '||--o{' not in result
    assert ': wrote' not in result

  def test_generation_empty_table(self, empty_table: TableDefinition) -> None:
    """Test generation with a table that has no fields."""
    generator = MermaidGenerator()
    result = generator.generate({'empty': empty_table}, {})

    assert 'erDiagram' in result
    assert 'empty {' in result
    # Should still have implicit id field
    assert 'string id PK' in result

  def test_edge_with_fields_creates_entity(
    self,
    multiple_tables: dict[str, TableDefinition],
    multiple_edges: dict[str, EdgeDefinition],
  ) -> None:
    """Test that edges with fields create their own entity."""
    generator = MermaidGenerator()
    result = generator.generate(multiple_tables, multiple_edges)

    # The 'wrote' edge has fields, should create entity
    assert 'wrote {' in result
    assert 'datetime published_at' in result


# GraphVizGenerator Tests


class TestGraphVizGenerator:
  """Test suite for GraphVizGenerator class."""

  def test_dot_output_structure(self, simple_table: TableDefinition) -> None:
    """Test basic DOT output structure."""
    generator = GraphVizGenerator()
    result = generator.generate({'user': simple_table}, {})

    assert 'digraph schema {' in result
    assert 'rankdir=LR;' in result
    assert 'node [shape=record];' in result
    assert result.strip().endswith('}')

  def test_record_shaped_nodes(self, simple_table: TableDefinition) -> None:
    """Test that nodes have record shape with fields."""
    generator = GraphVizGenerator()
    result = generator.generate({'user': simple_table}, {})

    assert 'user [label="' in result
    # Check for record format with pipes
    assert '{user|' in result
    assert 'id : string (PK)' in result
    assert 'name : string' in result

  def test_edge_labels(
    self,
    multiple_tables: dict[str, TableDefinition],
    edge_between_tables: EdgeDefinition,
  ) -> None:
    """Test that edges have proper labels."""
    generator = GraphVizGenerator()
    result = generator.generate(multiple_tables, {'wrote': edge_between_tables})

    assert 'user -> post [label="wrote"' in result

  def test_self_referential_edge_style(
    self,
    simple_table: TableDefinition,
    edge_definition: EdgeDefinition,
  ) -> None:
    """Test that self-referential edges have dashed style."""
    generator = GraphVizGenerator()
    result = generator.generate({'user': simple_table}, {'follows': edge_definition})

    assert 'user -> user [label="follows", style=dashed];' in result

  def test_include_fields_false(self, simple_table: TableDefinition) -> None:
    """Test generation with include_fields=False."""
    generator = GraphVizGenerator()
    result = generator.generate({'user': simple_table}, {}, include_fields=False)

    # Should just have table name, no field details
    assert 'user [label="user"]' in result
    assert 'name : string' not in result

  def test_include_edges_false(
    self,
    multiple_tables: dict[str, TableDefinition],
    multiple_edges: dict[str, EdgeDefinition],
  ) -> None:
    """Test generation with include_edges=False."""
    generator = GraphVizGenerator()
    result = generator.generate(multiple_tables, multiple_edges, include_edges=False)

    # Edges should not be present
    assert '->' not in result

  def test_edge_table_with_fields(
    self,
    multiple_tables: dict[str, TableDefinition],
    multiple_edges: dict[str, EdgeDefinition],
  ) -> None:
    """Test that edge tables with fields create nodes."""
    generator = GraphVizGenerator()
    result = generator.generate(multiple_tables, multiple_edges)

    # The 'wrote' edge has fields, should create a node
    assert 'wrote [label="' in result
    assert 'published_at : datetime' in result

  def test_constraint_annotations(self, table_with_indexes: TableDefinition) -> None:
    """Test constraint annotations in labels."""
    generator = GraphVizGenerator()
    result = generator.generate({'user': table_with_indexes}, {})

    assert '(PK)' in result
    assert '(UK)' in result


# ASCIIGenerator Tests


class TestASCIIGenerator:
  """Test suite for ASCIIGenerator class."""

  def test_box_drawing_characters(self, simple_table: TableDefinition) -> None:
    """Test that ASCII boxes use proper characters."""
    generator = ASCIIGenerator()
    result = generator.generate({'user': simple_table}, {})

    # Check for box characters
    assert '+' in result
    assert '-' in result
    assert '|' in result

  def test_field_formatting(self, simple_table: TableDefinition) -> None:
    """Test proper field formatting in ASCII box."""
    generator = ASCIIGenerator()
    result = generator.generate({'user': simple_table}, {})

    assert 'id : string (PK)' in result
    assert 'name : string' in result
    assert 'email : string' in result
    assert 'age : int' in result

  def test_relationship_summary(
    self,
    multiple_tables: dict[str, TableDefinition],
    multiple_edges: dict[str, EdgeDefinition],
  ) -> None:
    """Test relationship summary section."""
    generator = ASCIIGenerator()
    result = generator.generate(multiple_tables, multiple_edges)

    assert 'Relationships:' in result
    assert '-' * 40 in result
    assert 'user --[wrote]--> post' in result
    assert 'user --[commented]--> comment' in result

  def test_table_name_centered(self, simple_table: TableDefinition) -> None:
    """Test that table name is in the box."""
    generator = ASCIIGenerator()
    result = generator.generate({'user': simple_table}, {})

    # Table name should be in a line between pipes
    lines = result.split('\n')
    name_lines = [ln for ln in lines if 'user' in ln and '|' in ln]
    assert len(name_lines) >= 1

  def test_include_fields_false(self, simple_table: TableDefinition) -> None:
    """Test generation with include_fields=False."""
    generator = ASCIIGenerator()
    result = generator.generate({'user': simple_table}, {}, include_fields=False)

    # Table name should be present but not fields
    assert 'user' in result
    assert 'name : string' not in result

  def test_include_edges_false(
    self,
    multiple_tables: dict[str, TableDefinition],
    multiple_edges: dict[str, EdgeDefinition],
  ) -> None:
    """Test generation with include_edges=False."""
    generator = ASCIIGenerator()
    result = generator.generate(multiple_tables, multiple_edges, include_edges=False)

    # Relationships section should not be present
    assert 'Relationships:' not in result

  def test_no_edges_no_relationship_section(self, simple_table: TableDefinition) -> None:
    """Test that no relationship section when no edges."""
    generator = ASCIIGenerator()
    result = generator.generate({'user': simple_table}, {})

    # No relationships section when no edges
    assert 'Relationships:' not in result

  def test_constraint_annotations(self, table_with_indexes: TableDefinition) -> None:
    """Test constraint annotations in fields."""
    generator = ASCIIGenerator()
    result = generator.generate({'user': table_with_indexes}, {})

    assert '(PK)' in result
    assert '(UK)' in result


# visualize_schema() Function Tests


class TestVisualizeSchemaFunction:
  """Test suite for visualize_schema() function."""

  def test_mermaid_format(self, simple_table: TableDefinition) -> None:
    """Test visualize_schema with MERMAID format."""
    result = visualize_schema({'user': simple_table}, output_format=OutputFormat.MERMAID)

    assert 'erDiagram' in result
    assert 'user {' in result

  def test_graphviz_format(self, simple_table: TableDefinition) -> None:
    """Test visualize_schema with GRAPHVIZ format."""
    result = visualize_schema({'user': simple_table}, output_format=OutputFormat.GRAPHVIZ)

    assert 'digraph schema {' in result
    assert 'user [label="' in result

  def test_ascii_format(self, simple_table: TableDefinition) -> None:
    """Test visualize_schema with ASCII format."""
    result = visualize_schema({'user': simple_table}, output_format=OutputFormat.ASCII)

    assert '+' in result
    assert '|' in result
    assert 'user' in result

  def test_include_fields_option(self, simple_table: TableDefinition) -> None:
    """Test include_fields option."""
    result = visualize_schema({'user': simple_table}, include_fields=False)

    # Mermaid format by default, should have table but no fields
    assert 'user {' in result
    assert 'string name' not in result

  def test_include_edges_option(
    self,
    multiple_tables: dict[str, TableDefinition],
    multiple_edges: dict[str, EdgeDefinition],
  ) -> None:
    """Test include_edges option."""
    result = visualize_schema(multiple_tables, multiple_edges, include_edges=False)

    # Should not have relationship lines
    assert '||--o{' not in result
    assert '}o--o{' not in result

  def test_empty_tables_dict(self) -> None:
    """Test with empty tables dictionary."""
    result = visualize_schema({}, output_format=OutputFormat.MERMAID)

    # Should still produce valid output structure
    assert 'erDiagram' in result

  def test_empty_edges_dict(self, simple_table: TableDefinition) -> None:
    """Test with empty edges dictionary."""
    result = visualize_schema({'user': simple_table}, edges={})

    assert 'erDiagram' in result
    assert 'user {' in result
    # No relationships
    assert '||--o{' not in result

  def test_none_edges(self, simple_table: TableDefinition) -> None:
    """Test with None edges parameter."""
    result = visualize_schema({'user': simple_table}, edges=None)

    assert 'erDiagram' in result
    assert 'user {' in result

  def test_default_format_is_mermaid(self, simple_table: TableDefinition) -> None:
    """Test that default format is MERMAID."""
    result = visualize_schema({'user': simple_table})

    assert 'erDiagram' in result


# visualize_from_registry() Function Tests


class TestVisualizeFromRegistryFunction:
  """Test suite for visualize_from_registry() function."""

  def test_with_mocked_registry(self, simple_table: TableDefinition) -> None:
    """Test visualize_from_registry with mocked registry."""
    from reverie.schema.registry import SchemaRegistry

    mock_registry = SchemaRegistry()
    mock_registry.register_table(simple_table)

    with patch('reverie.schema.visualize.get_registry', return_value=mock_registry):
      result = visualize_from_registry()

    assert 'erDiagram' in result
    assert 'user {' in result

  def test_format_option_mermaid(self, simple_table: TableDefinition) -> None:
    """Test with MERMAID format option."""
    from reverie.schema.registry import SchemaRegistry

    mock_registry = SchemaRegistry()
    mock_registry.register_table(simple_table)

    with patch('reverie.schema.visualize.get_registry', return_value=mock_registry):
      result = visualize_from_registry(output_format=OutputFormat.MERMAID)

    assert 'erDiagram' in result

  def test_format_option_graphviz(self, simple_table: TableDefinition) -> None:
    """Test with GRAPHVIZ format option."""
    from reverie.schema.registry import SchemaRegistry

    mock_registry = SchemaRegistry()
    mock_registry.register_table(simple_table)

    with patch('reverie.schema.visualize.get_registry', return_value=mock_registry):
      result = visualize_from_registry(output_format=OutputFormat.GRAPHVIZ)

    assert 'digraph schema {' in result

  def test_format_option_ascii(self, simple_table: TableDefinition) -> None:
    """Test with ASCII format option."""
    from reverie.schema.registry import SchemaRegistry

    mock_registry = SchemaRegistry()
    mock_registry.register_table(simple_table)

    with patch('reverie.schema.visualize.get_registry', return_value=mock_registry):
      result = visualize_from_registry(output_format=OutputFormat.ASCII)

    assert '+' in result
    assert 'user' in result

  def test_include_fields_option(self, simple_table: TableDefinition) -> None:
    """Test include_fields option from registry."""
    from reverie.schema.registry import SchemaRegistry

    mock_registry = SchemaRegistry()
    mock_registry.register_table(simple_table)

    with patch('reverie.schema.visualize.get_registry', return_value=mock_registry):
      result = visualize_from_registry(include_fields=False)

    assert 'user {' in result
    assert 'string name' not in result

  def test_include_edges_option(
    self,
    simple_table: TableDefinition,
    edge_definition: EdgeDefinition,
  ) -> None:
    """Test include_edges option from registry."""
    from reverie.schema.registry import SchemaRegistry

    mock_registry = SchemaRegistry()
    mock_registry.register_table(simple_table)
    mock_registry.register_edge(edge_definition)

    with patch('reverie.schema.visualize.get_registry', return_value=mock_registry):
      result = visualize_from_registry(include_edges=False)

    assert '}o--o{' not in result

  def test_empty_registry(self) -> None:
    """Test with empty registry."""
    from reverie.schema.registry import SchemaRegistry

    mock_registry = SchemaRegistry()

    with patch('reverie.schema.visualize.get_registry', return_value=mock_registry):
      result = visualize_from_registry()

    # Should produce valid output even with empty registry
    assert 'erDiagram' in result


# CLI Command Tests


class TestVisualizeCLICommand:
  """Test suite for 'reverie schema visualize' CLI command."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_visualize_help(self) -> None:
    """Test visualize command help."""
    result = self.runner.invoke(schema_app, ['visualize', '--help'])

    assert result.exit_code == 0
    assert 'visualize' in result.stdout.lower()
    assert '--schema' in result.stdout
    assert '--format' in result.stdout
    assert '--output' in result.stdout
    assert '--tables' in result.stdout
    assert '--no-fields' in result.stdout
    assert '--no-edges' in result.stdout

  def test_visualize_missing_schema_fails(self) -> None:
    """Test that missing --schema option fails."""
    result = self.runner.invoke(schema_app, ['visualize'])

    # Should fail - schema is required
    assert result.exit_code != 0

  def test_visualize_nonexistent_schema_file(self, tmp_path: Path) -> None:
    """Test with non-existent schema file."""
    result = self.runner.invoke(
      schema_app, ['visualize', '--schema', str(tmp_path / 'nonexistent.py')]
    )

    assert result.exit_code != 0

  def test_visualize_valid_schema_file(self, tmp_path: Path) -> None:
    """Test with valid schema file."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text("""
from reverie.schema.table import table_schema
from reverie.schema.fields import string_field
from reverie.schema.table import with_fields
from reverie.schema.registry import register_table

user_table = table_schema('user')
user_table = with_fields(user_table, string_field('name'))
register_table(user_table)
""")

    with patch('reverie.cli.schema._load_schemas_from_file') as mock_load:
      mock_load.return_value = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='name', type=FieldType.STRING)],
        )
      }

      result = self.runner.invoke(schema_app, ['visualize', '--schema', str(schema_file)])

      assert result.exit_code == 0

  def test_visualize_format_mermaid(self, tmp_path: Path) -> None:
    """Test --format mermaid option."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# schema')

    with (
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.registry.get_registered_edges', return_value={}),
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      result = self.runner.invoke(
        schema_app, ['visualize', '--schema', str(schema_file), '--format', 'mermaid']
      )

      assert result.exit_code == 0
      assert 'erDiagram' in result.stdout

  def test_visualize_format_graphviz(self, tmp_path: Path) -> None:
    """Test --format graphviz option."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# schema')

    with (
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.registry.get_registered_edges', return_value={}),
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      result = self.runner.invoke(
        schema_app, ['visualize', '--schema', str(schema_file), '--format', 'graphviz']
      )

      assert result.exit_code == 0
      assert 'digraph' in result.stdout

  def test_visualize_format_ascii(self, tmp_path: Path) -> None:
    """Test --format ascii option."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# schema')

    with (
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.registry.get_registered_edges', return_value={}),
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      result = self.runner.invoke(
        schema_app, ['visualize', '--schema', str(schema_file), '--format', 'ascii']
      )

      assert result.exit_code == 0
      # ASCII output has box characters
      assert '+' in result.stdout or 'user' in result.stdout

  def test_visualize_output_to_file(self, tmp_path: Path) -> None:
    """Test --output writes to file."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# schema')
    output_file = tmp_path / 'diagram.md'

    with (
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.registry.get_registered_edges', return_value={}),
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      result = self.runner.invoke(
        schema_app,
        ['visualize', '--schema', str(schema_file), '--output', str(output_file)],
      )

      assert result.exit_code == 0
      assert output_file.exists()
      content = output_file.read_text()
      assert 'erDiagram' in content

  def test_visualize_tables_filter(self, tmp_path: Path) -> None:
    """Test --tables filter option."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# schema')

    with (
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.registry.get_registered_edges', return_value={}),
    ):
      mock_load.return_value = {
        'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
        'post': TableDefinition(name='post', mode=TableMode.SCHEMAFULL),
        'comment': TableDefinition(name='comment', mode=TableMode.SCHEMAFULL),
      }

      result = self.runner.invoke(
        schema_app,
        ['visualize', '--schema', str(schema_file), '--tables', 'user,post'],
      )

      assert result.exit_code == 0
      # Should include user and post but not comment
      assert 'user' in result.stdout
      assert 'post' in result.stdout

  def test_visualize_no_fields_option(self, tmp_path: Path) -> None:
    """Test --no-fields option."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# schema')

    with (
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.registry.get_registered_edges', return_value={}),
    ):
      mock_load.return_value = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='email', type=FieldType.STRING)],
        )
      }

      result = self.runner.invoke(
        schema_app,
        ['visualize', '--schema', str(schema_file), '--no-fields'],
      )

      assert result.exit_code == 0
      # Fields should not be present in output
      stripped = strip_ansi(result.stdout)
      # In mermaid, no field details inside the entity
      assert 'string email' not in stripped

  def test_visualize_no_edges_option(self, tmp_path: Path) -> None:
    """Test --no-edges option."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# schema')

    with (
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.registry.get_registered_edges') as mock_edges,
    ):
      mock_load.return_value = {
        'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
        'post': TableDefinition(name='post', mode=TableMode.SCHEMAFULL),
      }
      mock_edges.return_value = {
        'wrote': EdgeDefinition(
          name='wrote',
          mode=EdgeMode.RELATION,
          from_table='user',
          to_table='post',
        )
      }

      result = self.runner.invoke(
        schema_app,
        ['visualize', '--schema', str(schema_file), '--no-edges'],
      )

      assert result.exit_code == 0
      # Edge relationships should not be present
      stripped = strip_ansi(result.stdout)
      assert '||--o{' not in stripped
      assert ': wrote' not in stripped

  def test_visualize_invalid_format(self, tmp_path: Path) -> None:
    """Test with invalid format option."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# schema')

    with patch('reverie.cli.schema._load_schemas_from_file') as mock_load:
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      result = self.runner.invoke(
        schema_app,
        ['visualize', '--schema', str(schema_file), '--format', 'invalid'],
      )

      assert result.exit_code != 0
      # Output contains "Invalid format" or "Valid formats"
      assert 'format' in result.stdout.lower()

  def test_visualize_no_schemas_in_file(self, tmp_path: Path) -> None:
    """Test with schema file that has no schema definitions."""
    schema_file = tmp_path / 'empty_schema.py'
    schema_file.write_text('# empty schema file')

    with patch('reverie.cli.schema._load_schemas_from_file') as mock_load:
      mock_load.return_value = {}

      result = self.runner.invoke(schema_app, ['visualize', '--schema', str(schema_file)])

      assert result.exit_code != 0
      assert 'no schemas found' in result.stdout.lower()


class TestVisualizeCLISchemaApp:
  """Test suite for schema app command structure."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_schema_app_has_visualize_command(self) -> None:
    """Test that schema app has visualize command."""
    result = self.runner.invoke(schema_app, ['--help'])

    assert result.exit_code == 0
    assert 'visualize' in result.stdout


# Edge case tests


class TestEdgeCases:
  """Test edge cases and boundary conditions."""

  def test_table_with_all_field_types(self) -> None:
    """Test visualization with various field types."""
    table = TableDefinition(
      name='mixed',
      mode=TableMode.SCHEMAFULL,
      fields=[
        FieldDefinition(name='text', type=FieldType.STRING),
        FieldDefinition(name='number', type=FieldType.INT),
        FieldDefinition(name='decimal', type=FieldType.FLOAT),
        FieldDefinition(name='flag', type=FieldType.BOOL),
        FieldDefinition(name='created', type=FieldType.DATETIME),
        FieldDefinition(name='data', type=FieldType.OBJECT),
        FieldDefinition(name='items', type=FieldType.ARRAY),
        FieldDefinition(name='ref', type=FieldType.RECORD),
      ],
    )

    result = visualize_schema({'mixed': table})

    assert 'string text' in result
    assert 'int number' in result
    assert 'float decimal' in result
    assert 'bool flag' in result
    assert 'datetime created' in result
    assert 'object data' in result
    assert 'array items' in result
    assert 'record ref FK' in result

  def test_edge_with_unknown_tables(self) -> None:
    """Test edge referencing tables not in tables dict."""
    tables = {
      'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
    }
    edges = {
      'likes': EdgeDefinition(
        name='likes',
        mode=EdgeMode.RELATION,
        from_table='user',
        to_table='unknown_table',
      )
    }

    # Should not crash, just skip the edge
    generator = MermaidGenerator()
    result = generator.generate(tables, edges)

    assert 'erDiagram' in result
    assert 'likes' not in result  # Edge should be skipped

  def test_very_long_table_name(self) -> None:
    """Test with very long table names."""
    long_name = 'a' * 100
    table = TableDefinition(name=long_name, mode=TableMode.SCHEMAFULL)

    result = visualize_schema({long_name: table})

    assert long_name in result

  def test_table_name_with_underscores(self) -> None:
    """Test with table name containing underscores."""
    table = TableDefinition(name='user_profile_settings', mode=TableMode.SCHEMAFULL)

    result = visualize_schema({'user_profile_settings': table})

    assert 'user_profile_settings' in result

  def test_multiple_unique_indexes(self) -> None:
    """Test table with multiple unique indexes."""
    table = TableDefinition(
      name='user',
      mode=TableMode.SCHEMAFULL,
      fields=[
        FieldDefinition(name='email', type=FieldType.STRING),
        FieldDefinition(name='username', type=FieldType.STRING),
        FieldDefinition(name='phone', type=FieldType.STRING),
      ],
      indexes=[
        IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE),
        IndexDefinition(name='username_idx', columns=['username'], type=IndexType.UNIQUE),
        IndexDefinition(name='phone_idx', columns=['phone'], type=IndexType.UNIQUE),
      ],
    )

    result = visualize_schema({'user': table})

    # All three should have UK annotation
    uk_count = result.count(' UK')
    assert uk_count >= 3
