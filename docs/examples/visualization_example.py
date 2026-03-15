"""Schema Visualization Examples.

This module demonstrates the powerful visualization capabilities of surql,
including multiple output formats, themes, and customization options.

All examples use a simple blog schema with user, post, and comment tables
to demonstrate relationships and styling.

IMPORTANT: VIEWING RENDERED OUTPUT
===================================

This script generates SOURCE CODE for visualizations (DOT, Mermaid, ASCII).
To see the beautiful styled output with colors, gradients, and themes, you need
to RENDER the output using the appropriate tools.

What You'll See When Running This Script:
------------------------------------------
- GraphViz: DOT language code with HTML tables and color attributes
- Mermaid: Markdown with theme directives and ER syntax
- ASCII: Text with ANSI color codes and Unicode box-drawing characters

What You SHOULD See (After Rendering):
---------------------------------------
- GraphViz: Professional diagrams with colored tables, gradients, and styled relationships
- Mermaid: Beautiful ER diagrams with themed colors and entity boxes
- ASCII: Colorful text diagrams with rounded boxes, bold text, and emoji icons

How to Render Each Format:
---------------------------

1. GRAPHVIZ (.dot files):
   # Save output to file
   $ python visualization_example.py > output.txt

   # Extract DOT code to schema.dot file, then render:
   $ dot -Tpng schema.dot -o schema.png
   $ dot -Tsvg schema.dot -o schema.svg

   # OR use online viewer:
   # Visit https://dreampuf.github.io/GraphvizOnline/ and paste DOT code

2. MERMAID (.md files):
   # Wrap in markdown code fence and view on GitHub:
   ```mermaid
   erDiagram
       ...
   ```

   # OR use Mermaid Live Editor:
   # Visit https://mermaid.live/ and paste code

3. ASCII (.txt files):
   # View in color-supporting terminal:
   $ python visualization_example.py

   # View saved file with colors:
   $ less -R output.txt
   $ bat output.txt  # requires 'bat' tool

For detailed rendering instructions, see: docs/VISUALIZATION_RENDERING_GUIDE.md

Quick Test Your Setup:
-----------------------
# Test GraphViz installation
$ dot -V

# Test terminal color support
$ python -c "print('\\033[91mRed\\033[0m \\033[94mBlue\\033[0m')"

# Test Unicode support
$ python -c "print('╭─╮')"
"""

import sys
from pathlib import Path

# Ensure UTF-8 encoding for console output (Windows compatibility)
if sys.platform == 'win32':
  try:
    sys.stdout.reconfigure(encoding='utf-8')
  except AttributeError:
    # Python < 3.7
    import codecs

    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

from surql.schema.edge import edge_schema
from surql.schema.fields import datetime_field, record_field, string_field
from surql.schema.registry import register_edge, register_table
from surql.schema.table import table_schema
from surql.schema.themes import (
  DARK_THEME,
  FOREST_THEME,
  MINIMAL_THEME,
  MODERN_THEME,
  ASCIITheme,
  ColorScheme,
  GraphVizTheme,
  MermaidTheme,
  Theme,
)
from surql.schema.visualize import OutputFormat, visualize_schema

# Define a simple blog schema
user_table = table_schema(
  'user',
  fields=[
    string_field('username'),
    string_field('email'),
    datetime_field('created_at'),
  ],
)

post_table = table_schema(
  'post',
  fields=[
    string_field('title'),
    string_field('content'),
    record_field('author', table='user'),  # Foreign key to user
    datetime_field('published_at'),
  ],
)

comment_table = table_schema(
  'comment',
  fields=[
    string_field('text'),
    record_field('author', table='user'),  # Foreign key to user
    record_field('post', table='post'),  # Foreign key to post
    datetime_field('created_at'),
  ],
)

# Edge definitions for relationships
wrote_edge = edge_schema(
  'wrote',
  from_table='user',
  to_table='post',
  fields=[datetime_field('at')],
)

commented_edge = edge_schema(
  'commented',
  from_table='user',
  to_table='comment',
)

has_comment_edge = edge_schema(
  'has_comment',
  from_table='post',
  to_table='comment',
)

# Register schemas (optional - for use with visualize_from_registry)
register_table(user_table)
register_table(post_table)
register_table(comment_table)
register_edge(wrote_edge)
register_edge(commented_edge)
register_edge(has_comment_edge)

# Collect schemas
tables = {
  'user': user_table,
  'post': post_table,
  'comment': comment_table,
}

edges = {
  'wrote': wrote_edge,
  'commented': commented_edge,
  'has_comment': has_comment_edge,
}


def example_1_basic_default():
  """Example 1: Basic visualization with default (modern) theme.

  Generates a Mermaid ER diagram with the modern theme, which is the default.
  This provides a clean, professional look suitable for documentation.
  """
  print('Example 1: Basic Mermaid Diagram with Default Theme')
  print('=' * 60)

  diagram = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.MERMAID,
  )

  print(diagram)
  print('\n\n')


def example_2_all_formats():
  """Example 2: Generate diagrams in all three output formats.

  Demonstrates Mermaid, GraphViz, and ASCII output formats.
  Each format has its own strengths:
  - Mermaid: Great for markdown/documentation, renders in GitHub
  - GraphViz: Professional diagrams, can be rendered to PNG/SVG
  - ASCII: Plain text, works in terminals and basic text files
  """
  print('Example 2: All Output Formats')
  print('=' * 60)

  # Mermaid format
  print('MERMAID FORMAT:')
  print('-' * 60)
  mermaid = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.MERMAID,
    theme='modern',
  )
  print(mermaid)
  print('\n')

  # GraphViz format
  print('GRAPHVIZ FORMAT:')
  print('-' * 60)
  graphviz = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.GRAPHVIZ,
    theme='modern',
  )
  print(graphviz)
  print('\n')

  # ASCII format
  print('ASCII FORMAT:')
  print('-' * 60)
  ascii_art = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.ASCII,
    theme='modern',
  )
  print(ascii_art)
  print('\n\n')


def example_3_preset_themes():
  """Example 3: Demonstrate all preset themes.

  Shows how different themes change the visual appearance:
  - modern: Clean, professional with gradients and semantic colors
  - dark: Dark mode optimized with muted colors
  - forest: Nature-inspired green theme
  - minimal: Simple, no-frills black and white
  """
  print('Example 3: Preset Themes')
  print('=' * 60)

  themes = ['modern', 'dark', 'forest', 'minimal']

  for theme_name in themes:
    print(f'\n{theme_name.upper()} THEME (Mermaid):')
    print('-' * 60)
    diagram = visualize_schema(
      tables=tables,
      edges=edges,
      output_format=OutputFormat.MERMAID,
      theme=theme_name,
    )
    print(diagram)

  print('\n\n')


def example_4_graphviz_customization():
  """Example 4: GraphViz-specific customization.

  Demonstrates GraphViz features like:
  - Gradients and semantic coloring (enabled by default in themes)
  - Disabling gradients for simpler output
  - Custom GraphViz themes
  """
  print('Example 4: GraphViz Customization')
  print('=' * 60)

  # With gradients (modern theme default)
  print('WITH GRADIENTS AND SEMANTIC COLORS:')
  print('-' * 60)
  with_gradients = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.GRAPHVIZ,
    theme='modern',  # Modern theme enables gradients
  )
  print(with_gradients)
  print('\n')

  # Without gradients (compatible mode)
  print('WITHOUT GRADIENTS (backward compatible):')
  print('-' * 60)
  no_gradients = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.GRAPHVIZ,
    theme=GraphVizTheme(
      node_color='#4F46E5',
      edge_color='#6366F1',
      bg_color='transparent',
      font_name='Arial',
      node_shape='record',
      node_style='filled,rounded',
      edge_style='solid',
      use_gradients=False,  # Disable gradients
      use_clusters=False,
    ),
  )
  print(no_gradients)
  print('\n\n')


def example_5_ascii_customization():
  """Example 5: ASCII-specific customization.

  Shows ASCII art features:
  - Different box drawing styles (single, double, rounded, heavy)
  - Unicode characters vs basic ASCII
  - ANSI color codes
  - Emoji/Unicode icons for constraints
  """
  print('Example 5: ASCII Customization')
  print('=' * 60)

  # Modern theme with all features
  print('MODERN THEME (Unicode, Colors, Icons):')
  print('-' * 60)
  modern_ascii = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.ASCII,
    theme='modern',
  )
  print(modern_ascii)
  print('\n')

  # Custom double-line boxes
  print('DOUBLE BOX STYLE:')
  print('-' * 60)
  double_box = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.ASCII,
    theme=ASCIITheme(
      box_style='double',
      use_unicode=True,
      use_colors=True,
      use_icons=False,  # Disable icons
    ),
  )
  print(double_box)
  print('\n')

  # Plain ASCII (no Unicode, colors, or icons)
  print('PLAIN ASCII (No Unicode/Colors/Icons):')
  print('-' * 60)
  plain_ascii = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.ASCII,
    theme=ASCIITheme(
      box_style='single',  # Ignored when unicode disabled
      use_unicode=False,
      use_colors=False,
      use_icons=False,
    ),
  )
  print(plain_ascii)
  print('\n\n')


def example_6_custom_theme():
  """Example 6: Create a completely custom theme.

  Shows how to build a custom Theme object with specific styling
  for all three output formats.
  """
  print('Example 6: Custom Theme')
  print('=' * 60)

  # Create a custom "ocean" theme
  ocean_theme = Theme(
    name='ocean',
    description='Ocean-inspired theme with cyan and blue tones for a fresh, aquatic look',
    color_scheme=ColorScheme(
      primary='#0891B2',  # Cyan-600
      secondary='#06B6D4',  # Cyan-500
      background='#F0F9FF',  # Sky-50
      text='#164E63',  # Cyan-900
      accent='#0EA5E9',  # Sky-500
      success='#10b981',  # Emerald-500
      warning='#f59e0b',  # Amber-500
      error='#ef4444',  # Red-500
      muted='#67E8F9',  # Cyan-300
    ),
    graphviz=GraphVizTheme(
      node_color='#0891B2',  # Cyan-600
      edge_color='#06B6D4',  # Cyan-500
      bg_color='#F0F9FF',  # Sky-50
      font_name='Helvetica',
      node_shape='record',
      node_style='filled,rounded',
      edge_style='solid',
      use_gradients=True,
      use_clusters=False,
    ),
    mermaid=MermaidTheme(
      theme_name='default',
      primary_color='#0891B2',
      secondary_color='#06B6D4',
      use_custom_css=True,
    ),
    ascii=ASCIITheme(
      box_style='rounded',
      use_unicode=True,
      use_colors=True,
      use_icons=True,
      color_scheme='default',
    ),
  )

  # Use custom theme
  print('CUSTOM OCEAN THEME (GraphViz):')
  print('-' * 60)
  diagram = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.GRAPHVIZ,
    theme=ocean_theme,
  )
  print(diagram)
  print('\n\n')


def example_7_filtering():
  """Example 7: Filter tables and control what to display.

  Demonstrates:
  - Filtering to specific tables
  - Excluding fields or edges
  - Generating focused diagrams
  """
  print('Example 7: Filtering and Control')
  print('=' * 60)

  # Show only user and post tables
  print('FILTERED TABLES (user and post only):')
  print('-' * 60)
  filtered_tables = {
    'user': user_table,
    'post': post_table,
  }
  filtered_edges = {
    'wrote': wrote_edge,
  }
  diagram = visualize_schema(
    tables=filtered_tables,
    edges=filtered_edges,
    output_format=OutputFormat.MERMAID,
    theme='modern',
  )
  print(diagram)
  print('\n')

  # Show tables without fields (just structure)
  print('WITHOUT FIELDS (structure only):')
  print('-' * 60)
  no_fields = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.MERMAID,
    include_fields=False,  # Hide field definitions
    theme='modern',
  )
  print(no_fields)
  print('\n')

  # Show tables without edges
  print('WITHOUT EDGES (tables only):')
  print('-' * 60)
  no_edges = visualize_schema(
    tables=tables,
    edges=None,  # No edges
    output_format=OutputFormat.ASCII,
    include_edges=False,
    theme='modern',
  )
  print(no_edges)
  print('\n\n')


def example_8_backward_compatibility():
  """Example 8: Backward compatibility mode.

  Shows how to get the original pre-theme output by using theme="none"
  or theme=None. Useful for maintaining existing workflows.
  """
  print('Example 8: Backward Compatibility')
  print('=' * 60)

  print('ORIGINAL OUTPUT (no theme):')
  print('-' * 60)
  original = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.GRAPHVIZ,
    theme=None,  # No theme applied
  )
  print(original)
  print('\n\n')


def example_9_save_to_files():
  """Example 9: Save visualizations to files.

  Demonstrates saving diagrams to files for use in:
  - Documentation (Mermaid in .md files)
  - Image generation (GraphViz to .dot then render to .png/.svg)
  - Plain text diagrams (ASCII to .txt)
  """
  print('Example 9: Save to Files')
  print('=' * 60)

  output_dir = Path('docs/examples/output')
  output_dir.mkdir(exist_ok=True, parents=True)

  # Save Mermaid
  mermaid_file = output_dir / 'schema_modern_mermaid.md'
  mermaid = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.MERMAID,
    theme='modern',
  )
  mermaid_file.write_text(f'# Blog Schema\n\n```mermaid\n{mermaid}\n```\n', encoding='utf-8')
  print(f'✓ Saved Mermaid diagram to: {mermaid_file}')

  # Save GraphViz
  graphviz_file = output_dir / 'schema_dark_graphviz.dot'
  graphviz = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.GRAPHVIZ,
    theme='dark',
  )
  graphviz_file.write_text(graphviz, encoding='utf-8')
  print(f'✓ Saved GraphViz diagram to: {graphviz_file}')
  print('  Render with: dot -Tpng schema_dark_graphviz.dot -o schema_dark.png')

  # Save ASCII
  ascii_file = output_dir / 'schema_forest_ascii.txt'
  ascii_art = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.ASCII,
    theme='forest',
  )
  ascii_file.write_text(ascii_art, encoding='utf-8')
  print(f'✓ Saved ASCII diagram to: {ascii_file}')

  print('\n\n')


def main():
  """Run all examples."""
  print('\n')
  print('=' * 60)
  print('SURQL SCHEMA VISUALIZATION EXAMPLES')
  print('=' * 60)
  print('\n')

  example_1_basic_default()
  example_2_all_formats()
  example_3_preset_themes()
  example_4_graphviz_customization()
  example_5_ascii_customization()
  example_6_custom_theme()
  example_7_filtering()
  example_8_backward_compatibility()
  example_9_save_to_files()

  print('=' * 60)
  print('All examples completed!')
  print('=' * 60)


if __name__ == '__main__':
  main()
