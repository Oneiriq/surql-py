"""Schema visualization implementations.

Implementation for the visualize command including theme handling,
format mapping, and output generation.
"""

from pathlib import Path

import typer

from surql.cli.common import (
  display_code,
  display_error,
  display_info,
  display_panel,
  display_success,
  display_warning,
)
from surql.cli.schema_diff import _load_schemas_from_file

# Visualization format enum (separate from CLI OutputFormat)
VISUALIZE_FORMATS = ['mermaid', 'graphviz', 'ascii']


def _visualize_schema(
  schema_file: Path,
  format: str,
  output: Path | None,
  tables: str | None,
  no_fields: bool,
  no_edges: bool,
  theme: str,
  no_gradients: bool,
  ascii_style: str,
  no_unicode: bool,
  no_colors: bool,
  no_icons: bool,
  verbose: bool,
) -> None:
  """Implementation of schema visualization with theme support.

  Args:
    schema_file: Path to Python schema file
    format: Output format (mermaid, graphviz, ascii)
    output: Output file path (None for stdout)
    tables: Comma-separated list of tables to include
    no_fields: Exclude field definitions
    no_edges: Exclude edge relationships
    theme: Theme name or "none" for backward compatibility
    no_gradients: Disable gradients in GraphViz
    ascii_style: ASCII box drawing style
    no_unicode: Disable Unicode in ASCII
    no_colors: Disable colors in ASCII
    no_icons: Disable icons in ASCII
    verbose: Enable verbose output
  """
  from surql.schema.registry import clear_registry, get_registered_edges
  from surql.schema.themes import ASCIITheme, GraphVizTheme, MermaidTheme, Theme, get_theme
  from surql.schema.visualize import OutputFormat as VisualizeFormat
  from surql.schema.visualize import visualize_schema as generate_diagram

  # Validate format
  format_lower = format.lower()
  if format_lower not in VISUALIZE_FORMATS:
    display_error(f'Invalid format: {format}')
    display_info(f'Valid formats: {", ".join(VISUALIZE_FORMATS)}')
    raise typer.Exit(1)

  # Validate schema file exists
  if not schema_file.exists():
    display_error(f'Schema file not found: {schema_file}')
    raise typer.Exit(1)

  display_info(f'Loading schemas from: {schema_file}')

  # Clear registry and load schemas
  clear_registry()
  code_tables = _load_schemas_from_file(schema_file)
  code_edges = get_registered_edges()

  if not code_tables:
    display_warning('No schemas found in the specified file')
    display_info('Make sure your file registers schemas using register_table()')
    raise typer.Exit(1)

  display_success(f'Loaded {len(code_tables)} table schemas from file')
  if code_edges:
    display_info(f'Found {len(code_edges)} edge definitions')

  # Filter tables if specified
  if tables:
    table_list = [t.strip() for t in tables.split(',') if t.strip()]
    filtered_tables = {name: defn for name, defn in code_tables.items() if name in table_list}

    # Warn about missing tables
    missing = set(table_list) - set(filtered_tables.keys())
    if missing:
      display_warning(f'Tables not found: {", ".join(missing)}')

    if not filtered_tables:
      display_error('No matching tables found')
      raise typer.Exit(1)

    code_tables = filtered_tables
    display_info(f'Filtered to {len(code_tables)} tables')

    # Also filter edges to only include those between filtered tables
    if code_edges and not no_edges:
      table_names = set(code_tables.keys())
      filtered_edges = {
        name: edge
        for name, edge in code_edges.items()
        if edge.from_table in table_names and edge.to_table in table_names
      }
      code_edges = filtered_edges

  # Map format string to enum
  format_map = {
    'mermaid': VisualizeFormat.MERMAID,
    'graphviz': VisualizeFormat.GRAPHVIZ,
    'ascii': VisualizeFormat.ASCII,
  }
  output_format = format_map[format_lower]

  # Handle theme selection and customization
  final_theme: GraphVizTheme | MermaidTheme | ASCIITheme | Theme | None = (
    None  # None for backward compatibility
  )
  if theme.lower() != 'none':
    try:
      # Get the base theme
      base_theme = get_theme(theme.lower())

      # Apply format-specific overrides
      if output_format == VisualizeFormat.GRAPHVIZ and no_gradients:
        # Override GraphViz theme to disable gradients
        final_theme = GraphVizTheme(
          node_color=base_theme.graphviz.node_color,
          edge_color=base_theme.graphviz.edge_color,
          bg_color=base_theme.graphviz.bg_color,
          font_name=base_theme.graphviz.font_name,
          node_shape=base_theme.graphviz.node_shape,
          node_style=base_theme.graphviz.node_style,
          edge_style=base_theme.graphviz.edge_style,
          use_gradients=False,  # Override
          use_clusters=base_theme.graphviz.use_clusters,
        )
      elif output_format == VisualizeFormat.ASCII:
        # Apply ASCII-specific overrides
        final_theme = ASCIITheme(
          box_style=ascii_style,  # Use CLI-specified style
          use_unicode=not no_unicode,  # Invert the flag
          use_colors=not no_colors,  # Invert the flag
          use_icons=not no_icons,  # Invert the flag
          color_scheme=base_theme.ascii.color_scheme,
        )
      else:
        # Use theme as-is for Mermaid or unmodified GraphViz
        final_theme = base_theme

      if verbose:
        display_info(f'Using theme: {theme}')
    except ValueError as e:
      display_error(str(e))
      display_info('Available themes: modern, dark, forest, minimal, none')
      raise typer.Exit(1) from e

  # Generate diagram
  if verbose:
    display_info(f'Generating {format_lower} diagram...')

  diagram = generate_diagram(
    tables=code_tables,
    edges=code_edges if not no_edges else None,
    output_format=output_format,
    include_fields=not no_fields,
    include_edges=not no_edges,
    theme=final_theme,
  )

  # Output handling
  if output:
    # Write to file
    output.write_text(diagram, encoding='utf-8')
    display_success(f'Diagram written to: {output}')
  else:
    # Output to stdout
    if format_lower == 'ascii':
      # Use Rich panel for ASCII art
      display_panel(diagram, title='Schema Diagram', style='cyan')
    elif format_lower == 'mermaid':
      # Display Mermaid as code block
      display_code(diagram, language='mermaid', title='Mermaid ER Diagram')
    else:
      # GraphViz DOT
      display_code(diagram, language='dot', title='GraphViz DOT Diagram')
