"""Schema visualization module for generating diagrams.

This module provides functions to generate visual diagrams of database schemas
in multiple formats: Mermaid, GraphViz (DOT), and ASCII art.
"""

import re
import unicodedata
from enum import Enum
from typing import Protocol

from surql.schema.edge import EdgeDefinition
from surql.schema.fields import FieldType
from surql.schema.registry import get_registry
from surql.schema.table import IndexType, TableDefinition
from surql.schema.themes import (
  MODERN_THEME,
  ASCIITheme,
  GraphVizTheme,
  MermaidTheme,
  Theme,
  get_theme,
)

# ANSI escape code pattern for stripping color codes from strings
_ANSI_ESCAPE_PATTERN = re.compile(r'\x1b\[[0-9;]*m')


def _get_display_width(text: str) -> int:
  """Calculate terminal display width, stripping ANSI and counting wide chars.

  Python's len() counts emoji as 1 character, but terminals display them as
  2 columns wide. This function calculates the actual terminal display width
  by checking the Unicode East Asian Width property of each character.

  Args:
    text: Text possibly containing ANSI escape codes and/or emoji characters

  Returns:
    Actual display width in terminal columns
  """
  # Strip ANSI escape codes first
  stripped = _ANSI_ESCAPE_PATTERN.sub('', text)

  width = 0
  for char in stripped:
    # Check East Asian Width property
    ea_width = unicodedata.east_asian_width(char)
    if ea_width in ('W', 'F'):  # Wide or Fullwidth
      width += 2
    elif ea_width == 'A':  # Ambiguous - treat as narrow
      width += 1
    else:
      width += 1
  return width


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
  """Generator for Mermaid ER diagrams with theme support."""

  def __init__(self, theme: MermaidTheme | None = None) -> None:
    """Initialize Mermaid generator with optional theme.

    Args:
      theme: Mermaid theme configuration. Defaults to None for backward compatibility
    """
    self.theme = theme

  def generate(
    self,
    tables: dict[str, TableDefinition],
    edges: dict[str, EdgeDefinition],
    include_fields: bool = True,
    include_edges: bool = True,
  ) -> str:
    """Generate Mermaid ER diagram output with optional theming.

    Args:
      tables: Dictionary of table name to TableDefinition
      edges: Dictionary of edge name to EdgeDefinition
      include_fields: Whether to include field definitions
      include_edges: Whether to include edge relationships

    Returns:
      String containing Mermaid ER diagram syntax
    """
    lines: list[str] = []

    # Add theme initialization directive if theme is provided
    if self.theme:
      lines.append(f"%%{{init: {{'theme':'{self.theme.theme_name}'}}}}%%")

    lines.append('erDiagram')

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
  """Generator for GraphViz DOT format diagrams with theme support."""

  def __init__(self, theme: GraphVizTheme | None = None) -> None:
    """Initialize GraphViz generator with optional theme.

    Args:
      theme: GraphViz theme configuration. Defaults to a backward-compatible theme
            (MODERN_THEME without gradients for compatibility)
    """
    # For backward compatibility, default to MODERN theme without gradients
    # This preserves existing test behavior while allowing opt-in to modern features
    if theme is None:
      self.theme = GraphVizTheme(
        node_color=MODERN_THEME.graphviz.node_color,
        edge_color=MODERN_THEME.graphviz.edge_color,
        bg_color=MODERN_THEME.graphviz.bg_color,
        font_name=MODERN_THEME.graphviz.font_name,
        node_shape=MODERN_THEME.graphviz.node_shape,
        node_style=MODERN_THEME.graphviz.node_style,
        edge_style=MODERN_THEME.graphviz.edge_style,
        use_gradients=False,  # Disabled by default for backward compatibility
        use_clusters=False,
      )
    else:
      self.theme = theme

  def generate(
    self,
    tables: dict[str, TableDefinition],
    edges: dict[str, EdgeDefinition],
    include_fields: bool = True,
    include_edges: bool = True,
  ) -> str:
    """Generate GraphViz DOT format output with theme styling.

    Args:
      tables: Dictionary of table name to TableDefinition
      edges: Dictionary of edge name to EdgeDefinition
      include_fields: Whether to include field definitions
      include_edges: Whether to include edge relationships

    Returns:
      String containing GraphViz DOT format syntax
    """
    lines: list[str] = ['digraph schema {']

    # Apply theme to graph-level settings
    lines.append('    rankdir=LR;')

    # Only apply theme styling if gradients are enabled or node_style is set
    # This maintains backward compatibility with existing tests
    if self.theme.use_gradients or self.theme.node_style != 'filled,rounded':
      if self.theme.bg_color != 'transparent':
        lines.append(f'    bgcolor="{self.theme.bg_color}";')
      lines.append(f'    fontname="{self.theme.font_name}";')

      # Apply theme to default node settings
      node_attrs = [f'shape={self.theme.node_shape}']
      if self.theme.node_style:
        node_attrs.append(f'style="{self.theme.node_style}"')
      node_attrs.append(f'fontname="{self.theme.font_name}"')
      node_attrs.append('pad="0.5"')
      node_attrs.append('margin="0.2"')
      lines.append(f'    node [{", ".join(node_attrs)}];')

      # Apply theme to default edge settings
      edge_attrs = [f'color="{self.theme.edge_color}"']
      if self.theme.edge_style:
        edge_attrs.append(f'style={self.theme.edge_style}')
      edge_attrs.append(f'fontname="{self.theme.font_name}"')
      lines.append(f'    edge [{", ".join(edge_attrs)}];')
    else:
      # Minimal backward-compatible output
      lines.append('    node [shape=record];')

    lines.append('')

    # Generate table nodes
    for table_name, table in sorted(tables.items()):
      label = self._build_table_label(table_name, table, include_fields)
      lines.append(f'    {table_name} [label={label}];')

    # Generate edge table nodes (if they have fields)
    for edge_name, edge in sorted(edges.items()):
      if include_fields and edge.fields:
        label = self._build_edge_label(edge_name, edge)
        lines.append(f'    {edge_name} [label={label}];')

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

        # Determine edge style and color based on relationship type
        edge_style = self._get_edge_style(edge)
        lines.append(f'    {from_table} -> {to_table} [label="{edge_name}"{edge_style}];')

    lines.append('}')
    return '\n'.join(lines)

  def _build_table_label(
    self,
    table_name: str,
    table: TableDefinition,
    include_fields: bool,
  ) -> str:
    """Build GraphViz record label for a table with theme styling.

    Args:
      table_name: The table name
      table: The table definition
      include_fields: Whether to include fields

    Returns:
      GraphViz record label string (HTML-like or plain)
    """
    if not include_fields:
      return f'"{table_name}"'

    # Use HTML-like labels for gradients and semantic coloring
    if self.theme.use_gradients:
      return self._build_html_label(table_name, table)
    else:
      # Fallback to plain record labels
      field_lines = [f'{table_name}']
      # Add implicit id field
      field_lines.append('id : string (PK)\\l')
      for field in table.fields:
        constraint = _get_field_constraint(field.name, table)
        constraint_str = f' ({constraint})' if constraint else ''
        type_str = _get_field_type_str(field.type)
        field_lines.append(f'{field.name} : {type_str}{constraint_str}\\l')

      return '"' + '{' + '|'.join(field_lines) + '}"'

  def _build_html_label(
    self,
    table_name: str,
    table: TableDefinition,
  ) -> str:
    """Build HTML-like label with semantic coloring and gradients.

    Args:
      table_name: The table name
      table: The table definition

    Returns:
      HTML-like label string for GraphViz
    """
    # Start HTML table
    html = '<'
    html += '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">'

    # Header row with gradient or solid color
    header_bg = self.theme.node_color
    html += f'<TR><TD BGCOLOR="{header_bg}" COLSPAN="2">'
    html += f'<FONT COLOR="#FFFFFF"><B>{table_name}</B></FONT>'
    html += '</TD></TR>'

    # Add implicit id field (primary key)
    html += '<TR>'
    html += '<TD ALIGN="LEFT">id</TD>'
    html += '<TD ALIGN="LEFT">'
    html += f'<FONT COLOR="{MODERN_THEME.color_scheme.muted}">string</FONT>'
    html += f' <FONT COLOR="{MODERN_THEME.color_scheme.error}">PK</FONT>'
    html += '</TD>'
    html += '</TR>'

    # Add fields with semantic coloring
    for field in table.fields:
      constraint = _get_field_constraint(field.name, table)
      type_str = _get_field_type_str(field.type)

      # Determine field type color based on semantic meaning
      field_color = self._get_field_type_color(field.type)

      html += '<TR>'
      html += f'<TD ALIGN="LEFT">{field.name}</TD>'
      html += '<TD ALIGN="LEFT">'
      html += f'<FONT COLOR="{field_color}">{type_str}</FONT>'

      # Add constraint with semantic color
      if constraint:
        constraint_color = self._get_constraint_color(constraint)
        html += f' <FONT COLOR="{constraint_color}">{constraint}</FONT>'

      html += '</TD>'
      html += '</TR>'

    html += '</TABLE>>'
    return html

  def _get_field_type_color(self, field_type: FieldType) -> str:
    """Get semantic color for a field type.

    Args:
      field_type: The field type

    Returns:
      Hex color code for the field type
    """
    # Map field types to semantic colors from color scheme
    color_map = {
      FieldType.STRING: MODERN_THEME.color_scheme.success,  # Green for strings
      FieldType.INT: MODERN_THEME.color_scheme.warning,  # Amber for numbers
      FieldType.FLOAT: MODERN_THEME.color_scheme.warning,  # Amber for numbers
      FieldType.BOOL: MODERN_THEME.color_scheme.accent,  # Violet for booleans
      FieldType.DATETIME: MODERN_THEME.color_scheme.secondary,  # Pink for dates
      FieldType.RECORD: MODERN_THEME.color_scheme.primary,  # Indigo for records/FKs
      FieldType.OBJECT: MODERN_THEME.color_scheme.muted,  # Gray for objects
      FieldType.ARRAY: MODERN_THEME.color_scheme.muted,  # Gray for arrays
    }
    return color_map.get(field_type, MODERN_THEME.color_scheme.text)

  def _get_constraint_color(self, constraint: str) -> str:
    """Get semantic color for a constraint type.

    Args:
      constraint: The constraint type (PK, FK, UK)

    Returns:
      Hex color code for the constraint
    """
    # Map constraints to semantic colors
    constraint_map = {
      'PK': MODERN_THEME.color_scheme.error,  # Red for primary keys
      'FK': MODERN_THEME.color_scheme.primary,  # Indigo for foreign keys
      'UK': MODERN_THEME.color_scheme.accent,  # Violet for unique keys
    }
    return constraint_map.get(constraint, MODERN_THEME.color_scheme.text)

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
    if self.theme.use_gradients:
      # Use HTML-like label for edges too
      html = '<'
      html += '<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">'
      html += f'<TR><TD BGCOLOR="{self.theme.node_color}" COLSPAN="2">'
      html += f'<FONT COLOR="#FFFFFF"><B>{edge_name}</B></FONT>'
      html += '</TD></TR>'

      for field in edge.fields:
        type_str = _get_field_type_str(field.type)
        field_color = self._get_field_type_color(field.type)

        html += '<TR>'
        html += f'<TD ALIGN="LEFT">{field.name}</TD>'
        html += f'<TD ALIGN="LEFT"><FONT COLOR="{field_color}">{type_str}</FONT></TD>'
        html += '</TR>'

      html += '</TABLE>>'
      return html
    else:
      # Plain record label
      field_lines = [f'{edge_name}']
      for field in edge.fields:
        type_str = _get_field_type_str(field.type)
        field_lines.append(f'{field.name} : {type_str}\\l')

      return '"' + '{' + '|'.join(field_lines) + '}"'

  def _get_edge_style(self, edge: EdgeDefinition) -> str:
    """Get GraphViz edge style based on edge type with theme colors.

    Args:
      edge: The edge definition

    Returns:
      GraphViz edge style attributes string
    """
    # Self-referential edges get dashed style
    # Only add color if gradients are enabled (for backward compatibility)
    if edge.from_table and edge.from_table == edge.to_table:
      if self.theme.use_gradients:
        return f', style=dashed, color="{MODERN_THEME.color_scheme.secondary}"'
      else:
        return ', style=dashed'
    return ''


class ASCIIGenerator:
  """Generator for ASCII art diagrams with theme support."""

  def __init__(self, theme: ASCIITheme | None = None) -> None:
    """Initialize ASCII generator with optional theme.

    Args:
      theme: ASCII theme configuration. Defaults to None for backward compatibility
    """
    self.theme = theme

  def generate(
    self,
    tables: dict[str, TableDefinition],
    edges: dict[str, EdgeDefinition],
    include_fields: bool = True,
    include_edges: bool = True,
  ) -> str:
    """Generate ASCII art diagram output with optional theming.

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

  def _get_box_chars(self) -> dict[str, str]:
    """Get box drawing characters based on theme.

    Returns:
      Dictionary mapping box parts to characters
    """
    # Default to basic ASCII if no theme
    if not self.theme or not self.theme.use_unicode:
      return {
        'tl': '+',
        'tr': '+',
        'bl': '+',
        'br': '+',
        'h': '-',
        'v': '|',
        'ml': '+',
        'mr': '+',
      }

    # Unicode box drawing based on style
    box_styles = {
      'single': {
        'tl': '┌',
        'tr': '┐',
        'bl': '└',
        'br': '┘',
        'h': '─',
        'v': '│',
        'ml': '├',
        'mr': '┤',
      },
      'double': {
        'tl': '╔',
        'tr': '╗',
        'bl': '╚',
        'br': '╝',
        'h': '═',
        'v': '║',
        'ml': '╠',
        'mr': '╣',
      },
      'rounded': {
        'tl': '╭',
        'tr': '╮',
        'bl': '╰',
        'br': '╯',
        'h': '─',
        'v': '│',
        'ml': '├',
        'mr': '┤',
      },
      'heavy': {
        'tl': '┏',
        'tr': '┓',
        'bl': '┗',
        'br': '┛',
        'h': '━',
        'v': '┃',
        'ml': '┣',
        'mr': '┫',
      },
    }

    return box_styles.get(self.theme.box_style, box_styles['single'])

  def _colorize(self, text: str, color_type: str) -> str:
    """Apply ANSI color codes to text based on theme.

    Args:
      text: Text to colorize
      color_type: Type of color ('pk', 'fk', 'uk', 'header', or 'field')

    Returns:
      Text with ANSI color codes if theme supports colors
    """
    if not self.theme or not self.theme.use_colors:
      return text

    # ANSI color codes
    color_map = {
      'pk': '\033[91m',  # Bright red for primary keys
      'fk': '\033[94m',  # Bright blue for foreign keys
      'uk': '\033[95m',  # Bright magenta for unique keys
      'header': '\033[1m',  # Bold for headers
      'field': '\033[0m',  # Reset/default
    }

    reset = '\033[0m'
    color = color_map.get(color_type, '')

    if color:
      return f'{color}{text}{reset}'
    return text

  def _get_constraint_icon(self, constraint: str) -> str:
    """Get icon for a constraint type.

    Args:
      constraint: The constraint type (PK, FK, UK)

    Returns:
      Icon string if theme supports icons, otherwise empty
    """
    if not self.theme or not self.theme.use_icons:
      return ''

    icon_map = {
      'PK': '🔑 ',
      'FK': '🔗 ',
      'UK': '⭐ ',
    }

    return icon_map.get(constraint, '')

  def _build_table_box(
    self,
    table_name: str,
    table: TableDefinition,
    include_fields: bool,
  ) -> list[str]:
    """Build ASCII box representation of a table with theme styling.

    Args:
      table_name: The table name
      table: The table definition
      include_fields: Whether to include fields

    Returns:
      List of strings forming the ASCII box
    """
    chars = self._get_box_chars()

    # Calculate field lines with potential icons
    field_lines: list[str] = []
    if include_fields:
      # Add implicit id field
      pk_icon = self._get_constraint_icon('PK')
      pk_text = self._colorize(f'{pk_icon}(PK)', 'pk')
      field_lines.append(f'id : string {pk_text}')

      for field in table.fields:
        constraint = _get_field_constraint(field.name, table)
        constraint_str = ''
        if constraint:
          icon = self._get_constraint_icon(constraint)
          color_type = constraint.lower()
          constraint_str = f' {self._colorize(f"{icon}({constraint})", color_type)}'
        type_str = _get_field_type_str(field.type)
        field_lines.append(f'{field.name} : {type_str}{constraint_str}')

    # Calculate box width using display width to handle emoji properly
    min_width = max(len(table_name) + 4, 20)
    content_width = (
      max((_get_display_width(line) for line in field_lines), default=0) if field_lines else 0
    )
    width = max(min_width, content_width + 2)

    # Build box
    box_lines: list[str] = []
    top_line = chars['tl'] + chars['h'] * width + chars['tr']
    bottom_line = chars['bl'] + chars['h'] * width + chars['br']
    box_lines.append(top_line)

    # Table name (centered, with header styling)
    name_padded = table_name.center(width)
    styled_name = self._colorize(name_padded, 'header')
    # Need to account for color codes and wide chars in padding
    visible_len = _get_display_width(styled_name)
    padding_needed = width - visible_len
    left_pad = padding_needed // 2
    right_pad = padding_needed - left_pad
    box_lines.append(f'{chars["v"]}{" " * left_pad}{styled_name}{" " * right_pad}{chars["v"]}')

    if include_fields:
      # Separator
      sep_line = chars['ml'] + chars['h'] * width + chars['mr']
      box_lines.append(sep_line)

      # Fields
      for line in field_lines:
        visible_len = _get_display_width(line)
        padding = width - visible_len - 1  # -1 for the leading space
        padded = f' {line}{" " * padding}'
        box_lines.append(f'{chars["v"]}{padded}{chars["v"]}')

    box_lines.append(bottom_line)
    return box_lines


# Factory function to get appropriate generator
def _get_generator(
  output_format: OutputFormat,
  theme: GraphVizTheme | MermaidTheme | ASCIITheme | Theme | str | None = None,
) -> DiagramGenerator:
  """Get the appropriate diagram generator for the output format.

  Args:
    output_format: The desired output format
    theme: Optional theme. Can be:
           - Format-specific theme (GraphVizTheme, MermaidTheme, ASCIITheme)
           - Full Theme object (extracts appropriate sub-theme)
           - String theme name (e.g., "modern", "dark")
           - None for default/backward compatible output

  Returns:
    DiagramGenerator instance for the format
  """
  if output_format == OutputFormat.GRAPHVIZ:
    # Convert theme parameter to GraphVizTheme
    graphviz_theme: GraphVizTheme | None = None
    if isinstance(theme, str):
      graphviz_theme = get_theme(theme).graphviz
    elif isinstance(theme, Theme):
      graphviz_theme = theme.graphviz
    elif isinstance(theme, GraphVizTheme):
      graphviz_theme = theme
    return GraphVizGenerator(theme=graphviz_theme)

  if output_format == OutputFormat.MERMAID:
    # Convert theme parameter to MermaidTheme
    mermaid_theme: MermaidTheme | None = None
    if isinstance(theme, str):
      mermaid_theme = get_theme(theme).mermaid
    elif isinstance(theme, Theme):
      mermaid_theme = theme.mermaid
    elif isinstance(theme, MermaidTheme):
      mermaid_theme = theme
    return MermaidGenerator(theme=mermaid_theme)

  if output_format == OutputFormat.ASCII:
    # Convert theme parameter to ASCIITheme
    ascii_theme: ASCIITheme | None = None
    if isinstance(theme, str):
      ascii_theme = get_theme(theme).ascii
    elif isinstance(theme, Theme):
      ascii_theme = theme.ascii
    elif isinstance(theme, ASCIITheme):
      ascii_theme = theme
    return ASCIIGenerator(theme=ascii_theme)

  # Fallback for any other format (shouldn't happen)
  generators: dict[OutputFormat, DiagramGenerator] = {
    OutputFormat.MERMAID: MermaidGenerator(),
    OutputFormat.ASCII: ASCIIGenerator(),
  }
  return generators[output_format]


def visualize_schema(
  tables: dict[str, TableDefinition],
  edges: dict[str, EdgeDefinition] | None = None,
  output_format: OutputFormat = OutputFormat.MERMAID,
  include_fields: bool = True,
  include_edges: bool = True,
  theme: GraphVizTheme | MermaidTheme | ASCIITheme | Theme | str | None = None,
) -> str:
  """Generate schema visualization in specified format with theme support.

  Args:
    tables: Dictionary of table name to TableDefinition
    edges: Dictionary of edge name to EdgeDefinition (optional)
    output_format: Output format (MERMAID, GRAPHVIZ, ASCII)
    include_fields: Whether to include field definitions
    include_edges: Whether to include edge relationships
    theme: Optional theme for visualization. Can be:
           - Format-specific theme (GraphVizTheme, MermaidTheme, ASCIITheme)
           - Full Theme object (extracts appropriate sub-theme)
           - String theme name ("modern", "dark", "forest", "minimal")
           - None for default/backward compatible output

  Returns:
    String containing the diagram in the specified format

  Examples:
    Generate Mermaid diagram:
    >>> from surql.schema.table import table_schema
    >>> from surql.schema.fields import string_field
    >>> user = table_schema('user', fields=[string_field('email')])
    >>> diagram = visualize_schema({'user': user})

    Generate Mermaid diagram with forest theme:
    >>> diagram = visualize_schema(
    ...     {'user': user},
    ...     output_format=OutputFormat.MERMAID,
    ...     theme="forest"
    ... )

    Generate GraphViz diagram with dark theme:
    >>> diagram = visualize_schema(
    ...     {'user': user},
    ...     output_format=OutputFormat.GRAPHVIZ,
    ...     theme="dark"
    ... )

    Generate ASCII diagram with modern theme (unicode, colors, icons):
    >>> diagram = visualize_schema(
    ...     {'user': user},
    ...     output_format=OutputFormat.ASCII,
    ...     theme="modern"
    ... )
  """
  generator = _get_generator(output_format, theme=theme)
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
    >>> from surql.schema.registry import register_table
    >>> from surql.schema.table import table_schema
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
