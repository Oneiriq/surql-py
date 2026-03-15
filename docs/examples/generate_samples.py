"""Generate sample visualization output files.

This script creates sample output files for the blog schema
in all three formats with different themes.
"""

from pathlib import Path

from surql.schema.edge import edge_schema
from surql.schema.fields import datetime_field, record_field, string_field
from surql.schema.table import table_schema
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
        record_field('author', table='user'),
        datetime_field('published_at'),
    ],
)

comment_table = table_schema(
    'comment',
    fields=[
        string_field('text'),
        record_field('author', table='user'),
        record_field('post', table='post'),
        datetime_field('created_at'),
    ],
)

# Edge definitions
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

# Create output directory
output_dir = Path('docs/examples')
output_dir.mkdir(exist_ok=True, parents=True)

# Generate sample files
print('Generating sample visualization files...')

# 1. Modern theme GraphViz
print('  Creating schema_modern_graphviz.dot...')
graphviz = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.GRAPHVIZ,
    theme='modern',
)
(output_dir / 'schema_modern_graphviz.dot').write_text(graphviz, encoding='utf-8')

# 2. Dark theme Mermaid
print('  Creating schema_dark_mermaid.md...')
mermaid = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.MERMAID,
    theme='dark',
)
mermaid_content = f'# Blog Schema (Dark Theme)\n\n```mermaid\n{mermaid}\n```\n'
(output_dir / 'schema_dark_mermaid.md').write_text(mermaid_content, encoding='utf-8')

# 3. Forest theme ASCII with Unicode
print('  Creating schema_forest_ascii.txt...')
ascii_art = visualize_schema(
    tables=tables,
    edges=edges,
    output_format=OutputFormat.ASCII,
    theme='forest',
)
(output_dir / 'schema_forest_ascii.txt').write_text(ascii_art, encoding='utf-8')

print('\nSample files generated successfully!')
print(f'Output directory: {output_dir.absolute()}')
print('\nGenerated files:')
print('  - schema_modern_graphviz.dot (GraphViz with modern theme)')
print('  - schema_dark_mermaid.md (Mermaid with dark theme)')
print('  - schema_forest_ascii.txt (ASCII with forest theme)')
