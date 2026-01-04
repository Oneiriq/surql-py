"""Schema visualization module for generating diagrams.

This module provides functions to generate visual diagrams of database schemas
in multiple formats: Mermaid, GraphViz (DOT), and ASCII art.
"""

from enum import Enum
from typing import Protocol

from reverie.schema.edge import EdgeDefinition
from reverie.schema.fields import FieldType
from reverie.schema.registry import get_registry
from reverie.schema.table import IndexType, TableDefinition


class OutputFormat(Enum):
  """Output format for schema visualization diagrams."""

  MERMAID = 'mermaid'
  GRAPHVIZ = 'graphviz'
  ASCII = 'ascii'


class DiagramGenerator(Protocol):
  """Protocol for diagram generators."""

  def generate(
    self,
    tables: dict[str, TableDefinition],
    edges: dict[str, EdgeDefinition],
    include_fields: bool = True,
    include_edges: bool = True,
  ) -> str:
    """Generate diagram output.

    Args:
      tables: Dictionary of table name to TableDefinition
      edges: Dictionary of edge name to EdgeDefinition
      include_fields: Whether to include field definitions
      include_edges: Whether to include edge relationships

    Returns:
      String containing the diagram in the generator's format
    """
    ...


def _get_field_constraint(
  field_name: str,
  table: TableDefinition,
) -> str:
  """Get constraint suffix for a field (PK, FK, UK).

  Args:
    field_name: The field name to check
    table: The table definition containing indexes

  Returns:
    Constraint string (e.g., 'PK', 'FK', 'UK') or empty string
  """
  # Check if this is the id field (SurrealDB primary key)
  if field_name == 'id':
    return 'PK'

  # Check for unique indexes
  for idx in table.indexes:
    if idx.type == IndexType.UNIQUE and field_name in idx.columns:
      return 'UK'

  # Check for record fields (foreign keys)
  for f in table.fields:
    if f.name == field_name and f.type == FieldType.RECORD:
      return 'FK'

  return ''


def _get_field_type_str(field_type: FieldType) -> str:
  """Get a display string for a field type.

  Args:
    field_type: The FieldType enum value

  Returns:
    String representation of the type
  """
  return field_type.value


class MermaidGenerator:
  """Generator for Mermaid ER diagrams."""

  def generate(
    self,
    tables: dict[str, TableDefinition],
    edges: dict[str, EdgeDefinition],
    include_fields: bool = True,
    include_edges: bool = True,
  ) -> str:
    """Generate Mermaid ER diagram output.

    Args:
      tables: Dictionary of table name to TableDefinition
      edges: Dictionary of edge name to EdgeDefinition
      include_fields: Whether to include field definitions
      include_edges: Whether to include edge relationships

    Returns:
      String containing Mermaid ER diagram syntax
    """
    lines: list[str] = ['erDiagram']

    # Generate table entities
    for table_name, table in sorted(tables.items()):
      lines.append(f'    {table_name} {{')
      if include_fields:
        # Always add implicit id field
        lines.append('        string id PK')
        for field in table.fields:
          constraint = _get_field_constraint(field.name, table)
          constraint_str = f' {constraint}' if constraint else ''
          type_str = _get_field_type_str(field.type)
          lines.append(f'        {type_str} {field.name}{constraint_str}')
      lines.append('    }')

    # Generate edge entities (they are also tables in SurrealDB)
    for edge_name, edge in sorted(edges.items()):
      if include_fields and edge.fields:
        lines.append(f'    {edge_name} {{')
        for field in edge.fields:
          type_str = _get_field_type_str(field.type)
          lines.append(f'        {type_str} {field.name}')
        lines.append('    }')

    # Generate relationships from edges
    if include_edges:
      lines.append('')
      for edge_name, edge in sorted(edges.items()):
        from_table = edge.from_table or 'unknown'
        to_table = edge.to_table or 'unknown'

        # Skip if source or target not in tables
        if from_table not in tables and from_table != 'unknown':
          continue
        if to_table not in tables and to_table != 'unknown':
          continue

        # Determine cardinality based on edge semantics
        # Default to one-to-many for most edges
        cardinality = self._infer_cardinality(edge)
        lines.append(f'    {from_table} {cardinality} {to_table} : {edge_name}')

    return '\n'.join(lines)

  def _infer_cardinality(self, edge: EdgeDefinition) -> str:
    """Infer Mermaid cardinality notation from edge definition.

    Args:
      edge: The edge definition to analyze

    Returns:
      Mermaid cardinality string (e.g., '||--o{', '}o--o{')
    """
    # Self-referential edges (same from and to table) typically many-to-many
    if edge.from_table and edge.from_table == edge.to_table:
      return '}o--o{'

    # Default: one-to-many relationship
    return '||--o{'


class GraphVizGenerator:
  """Generator for GraphViz DOT format diagrams."""

  def generate(
    self,
    tables: dict[str, TableDefinition],
    edges: dict[str, EdgeDefinition],
    include_fields: bool = True,
    include_edges: bool = True,
  ) -> str:
    """Generate GraphViz DOT format output.

    Args:
      tables: Dictionary of table name to TableDefinition
      edges: Dictionary of edge name to EdgeDefinition
      include_fields: Whether to include field definitions
      include_edges: Whether to include edge relationships

    Returns:
      String containing GraphViz DOT format syntax
    """
    lines: list[str] = [
      'digraph schema {',
      '    rankdir=LR;',
      '    node [shape=record];',
      '',
    ]

    # Generate table nodes
    for table_name, table in sorted(tables.items()):
      label = self._build_table_label(table_name, table, include_fields)
      lines.append(f'    {table_name} [label="{label}"];')

    # Generate edge table nodes (if they have fields)
    for edge_name, edge in sorted(edges.items()):
      if include_fields and edge.fields:
        label = self._build_edge_label(edge_name, edge)
        lines.append(f'    {edge_name} [label="{label}"];')

    lines.append('')

    # Generate edges/relationships
    if include_edges:
      for edge_name, edge in sorted(edges.items()):
        from_table = edge.from_table or 'unknown'
        to_table = edge.to_table or 'unknown'

        # Skip if source or target not in tables
        if from_table not in tables:
          continue
        if to_table not in tables:
          continue

        # Determine edge style based on relationship type
        style = self._get_edge_style(edge)
        lines.append(f'    {from_table} -> {to_table} [label="{edge_name}"{style}];')

    lines.append('}')
    return '\n'.join(lines)

  def _build_table_label(
    self,
    table_name: str,
    table: TableDefinition,
    include_fields: bool,
  ) -> str:
    """Build GraphViz record label for a table.

    Args:
      table_name: The table name
      table: The table definition
      include_fields: Whether to include fields

    Returns:
      GraphViz record label string
    """
    if not include_fields:
      return table_name

    field_lines = [f'{table_name}']
    # Add implicit id field
    field_lines.append('id : string (PK)\\l')
    for field in table.fields:
      constraint = _get_field_constraint(field.name, table)
      constraint_str = f' ({constraint})' if constraint else ''
      type_str = _get_field_type_str(field.type)
      field_lines.append(f'{field.name} : {type_str}{constraint_str}\\l')

    return '{' + '|'.join(field_lines) + '}'

  def _build_edge_label(
    self,
    edge_name: str,
    edge: EdgeDefinition,
  ) -> str:
    """Build GraphViz record label for an edge table.

    Args:
      edge_name: The edge name
      edge: The edge definition

    Returns:
      GraphViz record label string
    """
    field_lines = [f'{edge_name}']
    for field in edge.fields:
      type_str = _get_field_type_str(field.type)
      field_lines.append(f'{field.name} : {type_str}\\l')

    return '{' + '|'.join(field_lines) + '}'

  def _get_edge_style(self, edge: EdgeDefinition) -> str:
    """Get GraphViz edge style based on edge type.

    Args:
      edge: The edge definition

    Returns:
      GraphViz edge style attributes string
    """
    # Self-referential edges get dashed style
    if edge.from_table and edge.from_table == edge.to_table:
      return ', style=dashed'
    return ''


class ASCIIGenerator:
  """Generator for ASCII art diagrams."""

  def generate(
    self,
    tables: dict[str, TableDefinition],
    edges: dict[str, EdgeDefinition],
    include_fields: bool = True,
    include_edges: bool = True,
  ) -> str:
    """Generate ASCII art diagram output.

    Args:
      tables: Dictionary of table name to TableDefinition
      edges: Dictionary of edge name to EdgeDefinition
      include_fields: Whether to include field definitions
      include_edges: Whether to include edge relationships

    Returns:
      String containing ASCII art diagram
    """
    lines: list[str] = []
    table_boxes: dict[str, list[str]] = {}

    # Generate table boxes
    for table_name, table in sorted(tables.items()):
      box = self._build_table_box(table_name, table, include_fields)
      table_boxes[table_name] = box
      lines.extend(box)
      lines.append('')

    # Add relationship section if edges exist
    if include_edges and edges:
      lines.append('Relationships:')
      lines.append('-' * 40)
      for edge_name, edge in sorted(edges.items()):
        from_table = edge.from_table or '?'
        to_table = edge.to_table or '?'
        lines.append(f'  {from_table} --[{edge_name}]--> {to_table}')

    return '\n'.join(lines)

  def _build_table_box(
    self,
    table_name: str,
    table: TableDefinition,
    include_fields: bool,
  ) -> list[str]:
    """Build ASCII box representation of a table.

    Args:
      table_name: The table name
      table: The table definition
      include_fields: Whether to include fields

    Returns:
      List of strings forming the ASCII box
    """
    # Calculate field lines
    field_lines: list[str] = []
    if include_fields:
      # Add implicit id field
      field_lines.append('id : string (PK)')
      for field in table.fields:
        constraint = _get_field_constraint(field.name, table)
        constraint_str = f' ({constraint})' if constraint else ''
        type_str = _get_field_type_str(field.type)
        field_lines.append(f'{field.name} : {type_str}{constraint_str}')

    # Calculate box width
    min_width = max(len(table_name) + 4, 20)
    content_width = max((len(line) for line in field_lines), default=0) if field_lines else 0
    width = max(min_width, content_width + 2)

    # Build box
    box_lines: list[str] = []
    top_bottom = '+' + '-' * width + '+'
    box_lines.append(top_bottom)

    # Table name (centered)
    name_padded = table_name.center(width)
    box_lines.append(f'|{name_padded}|')

    if include_fields:
      # Separator
      box_lines.append('+' + '-' * width + '+')
      # Fields
      for line in field_lines:
        padded = f' {line}'.ljust(width)
        box_lines.append(f'|{padded}|')

    box_lines.append(top_bottom)
    return box_lines


# Factory function to get appropriate generator
def _get_generator(output_format: OutputFormat) -> DiagramGenerator:
  """Get the appropriate diagram generator for the output format.

  Args:
    output_format: The desired output format

  Returns:
    DiagramGenerator instance for the format
  """
  generators: dict[OutputFormat, DiagramGenerator] = {
    OutputFormat.MERMAID: MermaidGenerator(),
    OutputFormat.GRAPHVIZ: GraphVizGenerator(),
    OutputFormat.ASCII: ASCIIGenerator(),
  }
  return generators[output_format]


def visualize_schema(
  tables: dict[str, TableDefinition],
  edges: dict[str, EdgeDefinition] | None = None,
  output_format: OutputFormat = OutputFormat.MERMAID,
  include_fields: bool = True,
  include_edges: bool = True,
) -> str:
  """Generate schema visualization in specified format.

  Args:
    tables: Dictionary of table name to TableDefinition
    edges: Dictionary of edge name to EdgeDefinition (optional)
    output_format: Output format (MERMAID, GRAPHVIZ, ASCII)
    include_fields: Whether to include field definitions
    include_edges: Whether to include edge relationships

  Returns:
    String containing the diagram in the specified format

  Examples:
    Generate Mermaid diagram:
    >>> from reverie.schema.table import table_schema
    >>> from reverie.schema.fields import string_field
    >>> user = table_schema('user', fields=[string_field('email')])
    >>> diagram = visualize_schema({'user': user})

    Generate GraphViz diagram:
    >>> diagram = visualize_schema(
    ...     {'user': user},
    ...     output_format=OutputFormat.GRAPHVIZ
    ... )

    Generate ASCII diagram:
    >>> diagram = visualize_schema(
    ...     {'user': user},
    ...     output_format=OutputFormat.ASCII
    ... )
  """
  generator = _get_generator(output_format)
  return generator.generate(
    tables,
    edges or {},
    include_fields=include_fields,
    include_edges=include_edges,
  )


def visualize_from_registry(
  output_format: OutputFormat = OutputFormat.MERMAID,
  include_fields: bool = True,
  include_edges: bool = True,
) -> str:
  """Generate schema visualization from the current registry.

  This function retrieves all registered tables and edges from the global
  schema registry and generates a visualization diagram.

  Args:
    output_format: Output format (MERMAID, GRAPHVIZ, ASCII)
    include_fields: Whether to include field definitions
    include_edges: Whether to include edge relationships

  Returns:
    String containing the diagram in the specified format

  Examples:
    Generate Mermaid diagram from registry:
    >>> from reverie.schema.registry import register_table
    >>> from reverie.schema.table import table_schema
    >>> register_table(table_schema('user'))
    >>> diagram = visualize_from_registry()

    Generate ASCII diagram from registry:
    >>> diagram = visualize_from_registry(output_format=OutputFormat.ASCII)
  """
  registry = get_registry()
  tables = registry.get_tables()
  edges = registry.get_edges()

  return visualize_schema(
    tables,
    edges,
    output_format=output_format,
    include_fields=include_fields,
    include_edges=include_edges,
  )
