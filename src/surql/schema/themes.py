"""Theme system for schema visualization.

This module provides a comprehensive theming system for surql's schema visualization
outputs, supporting GraphViz, Mermaid, and ASCII formats with multiple preset themes
and customization options.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ColorScheme:
  """Base color scheme used across all visualization themes.

  Defines semantic colors for tables, fields, constraints, edges, and text.
  All colors are specified as hex color codes (e.g., "#3B82F6").

  Attributes:
      primary: Primary accent color
      secondary: Secondary accent color
      background: Background color
      text: Primary text color
      accent: Accent color for highlights
      success: Success/positive color
      warning: Warning/caution color
      error: Error/negative color
      muted: Muted/disabled color
  """

  primary: str = '#6366f1'  # Indigo-500
  secondary: str = '#ec4899'  # Pink-500
  background: str = '#f8fafc'  # Slate-50
  text: str = '#0f172a'  # Slate-900
  accent: str = '#8b5cf6'  # Violet-500
  success: str = '#10b981'  # Emerald-500
  warning: str = '#f59e0b'  # Amber-500
  error: str = '#ef4444'  # Red-500
  muted: str = '#94a3b8'  # Slate-400


@dataclass(frozen=True)
class GraphVizTheme:
  """Theme configuration for GraphViz DOT format output.

  Controls all visual aspects of GraphViz diagrams including node styling,
  edge styling, layout, and advanced features like gradients and clustering.

  Attributes:
      node_color: Node border color
      edge_color: Edge line color
      bg_color: Background color (use "transparent" for none)
      font_name: Font family for all text
      node_shape: GraphViz node shape (e.g., "record", "box")
      node_style: Node style attributes (e.g., "filled,rounded")
      edge_style: Edge style (e.g., "solid", "dashed")
      use_gradients: Enable gradient fills for nodes
      use_clusters: Enable table clustering/grouping
  """

  node_color: str = '#6366f1'
  edge_color: str = '#64748b'
  bg_color: str = 'transparent'
  font_name: str = 'Arial'
  node_shape: str = 'record'
  node_style: str = 'filled,rounded'
  edge_style: str = 'solid'
  use_gradients: bool = True
  use_clusters: bool = False


@dataclass(frozen=True)
class MermaidTheme:
  """Theme configuration for Mermaid diagram output.

  Controls Mermaid ER diagram appearance using Mermaid's theming system.
  Supports both built-in themes and custom CSS variables.

  Attributes:
      theme_name: Built-in Mermaid theme ("default", "dark", "forest", "neutral", "base")
      primary_color: Primary entity color
      secondary_color: Secondary UI color
      use_custom_css: Enable custom CSS variable injection
  """

  theme_name: str = 'default'
  primary_color: str = '#6366f1'
  secondary_color: str = '#ec4899'
  use_custom_css: bool = True


@dataclass(frozen=True)
class ASCIITheme:
  """Theme configuration for ASCII art diagram output.

  Controls ASCII diagram rendering including box drawing characters,
  ANSI colors, and Unicode icons for a modern terminal experience.

  Attributes:
      box_style: Box drawing style ("single", "double", "rounded", "heavy")
      use_unicode: Use Unicode box-drawing characters (vs basic ASCII)
      use_colors: Enable ANSI color codes
      use_icons: Show Unicode/emoji icons for constraints
      color_scheme: Color scheme name for ANSI colors
  """

  box_style: str = 'rounded'
  use_unicode: bool = True
  use_colors: bool = True
  use_icons: bool = True
  color_scheme: str = 'default'


@dataclass(frozen=True)
class Theme:
  """Complete theme configuration bundling all format-specific themes.

  A Theme combines a color scheme with format-specific configurations for
  GraphViz, Mermaid, and ASCII outputs into a coherent visual design.

  Attributes:
      name: Theme name (e.g., "modern", "dark")
      description: Human-readable theme description
      color_scheme: Base color palette
      graphviz: GraphViz-specific theme configuration
      mermaid: Mermaid-specific theme configuration
      ascii: ASCII-specific theme configuration
  """

  name: str
  description: str
  color_scheme: ColorScheme
  graphviz: GraphVizTheme
  mermaid: MermaidTheme
  ascii: ASCIITheme


# Preset Theme: Modern (Default)
# Clean, professional design with indigo and pink accents

MODERN_COLOR_SCHEME = ColorScheme(
  primary='#6366f1',  # Indigo-500
  secondary='#ec4899',  # Pink-500
  background='#f8fafc',  # Slate-50
  text='#0f172a',  # Slate-900
  accent='#8b5cf6',  # Violet-500
  success='#10b981',  # Emerald-500
  warning='#f59e0b',  # Amber-500
  error='#ef4444',  # Red-500
  muted='#94a3b8',  # Slate-400
)

MODERN_GRAPHVIZ = GraphVizTheme(
  node_color='#6366f1',
  edge_color='#64748b',
  bg_color='transparent',
  font_name='Arial',
  node_shape='record',
  node_style='filled,rounded',
  edge_style='solid',
  use_gradients=True,
  use_clusters=False,
)

MODERN_MERMAID = MermaidTheme(
  theme_name='default',
  primary_color='#6366f1',
  secondary_color='#ec4899',
  use_custom_css=True,
)

MODERN_ASCII = ASCIITheme(
  box_style='rounded',
  use_unicode=True,
  use_colors=True,
  use_icons=True,
  color_scheme='default',
)

MODERN_THEME = Theme(
  name='modern',
  description='Clean, professional design with indigo and pink accents',
  color_scheme=MODERN_COLOR_SCHEME,
  graphviz=MODERN_GRAPHVIZ,
  mermaid=MODERN_MERMAID,
  ascii=MODERN_ASCII,
)


# Preset Theme: Dark
# Dark background theme with violet and fuchsia for dark mode environments

DARK_COLOR_SCHEME = ColorScheme(
  primary='#8b5cf6',  # Violet-500
  secondary='#d946ef',  # Fuchsia-500
  background='#1e1b4b',  # Indigo-950
  text='#f1f5f9',  # Slate-100
  accent='#a78bfa',  # Violet-400
  success='#34d399',  # Emerald-400
  warning='#fbbf24',  # Amber-400
  error='#f87171',  # Red-400
  muted='#64748b',  # Slate-500
)

DARK_GRAPHVIZ = GraphVizTheme(
  node_color='#8b5cf6',
  edge_color='#64748b',
  bg_color='#1e1b4b',
  font_name='Arial',
  node_shape='record',
  node_style='filled,rounded',
  edge_style='solid',
  use_gradients=True,
  use_clusters=False,
)

DARK_MERMAID = MermaidTheme(
  theme_name='dark',
  primary_color='#8b5cf6',
  secondary_color='#d946ef',
  use_custom_css=True,
)

DARK_ASCII = ASCIITheme(
  box_style='rounded',
  use_unicode=True,
  use_colors=True,
  use_icons=True,
  color_scheme='dark',
)

DARK_THEME = Theme(
  name='dark',
  description='Dark background theme with violet and fuchsia for dark mode environments',
  color_scheme=DARK_COLOR_SCHEME,
  graphviz=DARK_GRAPHVIZ,
  mermaid=DARK_MERMAID,
  ascii=DARK_ASCII,
)


# Preset Theme: Forest
# Nature-inspired theme with emerald and teal on light green background

FOREST_COLOR_SCHEME = ColorScheme(
  primary='#10b981',  # Emerald-500
  secondary='#14b8a6',  # Teal-500
  background='#f0fdf4',  # Green-50
  text='#14532d',  # Green-900
  accent='#059669',  # Emerald-600
  success='#22c55e',  # Green-500
  warning='#f59e0b',  # Amber-500
  error='#ef4444',  # Red-500
  muted='#86efac',  # Green-300
)

FOREST_GRAPHVIZ = GraphVizTheme(
  node_color='#10b981',
  edge_color='#059669',
  bg_color='transparent',
  font_name='Arial',
  node_shape='record',
  node_style='filled,rounded',
  edge_style='solid',
  use_gradients=True,
  use_clusters=False,
)

FOREST_MERMAID = MermaidTheme(
  theme_name='forest',
  primary_color='#10b981',
  secondary_color='#14b8a6',
  use_custom_css=True,
)

FOREST_ASCII = ASCIITheme(
  box_style='rounded',
  use_unicode=True,
  use_colors=True,
  use_icons=True,
  color_scheme='forest',
)

FOREST_THEME = Theme(
  name='forest',
  description='Nature-inspired theme with emerald and teal on light green background',
  color_scheme=FOREST_COLOR_SCHEME,
  graphviz=FOREST_GRAPHVIZ,
  mermaid=FOREST_MERMAID,
  ascii=FOREST_ASCII,
)


# Preset Theme: Minimal
# Minimalist grayscale theme with subtle styling

MINIMAL_COLOR_SCHEME = ColorScheme(
  primary='#6b7280',  # Gray-500
  secondary='#64748b',  # Slate-500
  background='#ffffff',  # White
  text='#1f2937',  # Gray-800
  accent='#9ca3af',  # Gray-400
  success='#10b981',  # Emerald-500
  warning='#f59e0b',  # Amber-500
  error='#ef4444',  # Red-500
  muted='#d1d5db',  # Gray-300
)

MINIMAL_GRAPHVIZ = GraphVizTheme(
  node_color='#6b7280',
  edge_color='#9ca3af',
  bg_color='transparent',
  font_name='Arial',
  node_shape='record',
  node_style='filled',
  edge_style='solid',
  use_gradients=False,
  use_clusters=False,
)

MINIMAL_MERMAID = MermaidTheme(
  theme_name='neutral',
  primary_color='#6b7280',
  secondary_color='#64748b',
  use_custom_css=True,
)

MINIMAL_ASCII = ASCIITheme(
  box_style='single',
  use_unicode=True,
  use_colors=False,
  use_icons=False,
  color_scheme='minimal',
)

MINIMAL_THEME = Theme(
  name='minimal',
  description='Minimalist grayscale theme with subtle styling',
  color_scheme=MINIMAL_COLOR_SCHEME,
  graphviz=MINIMAL_GRAPHVIZ,
  mermaid=MINIMAL_MERMAID,
  ascii=MINIMAL_ASCII,
)


# Theme registry mapping theme names to Theme objects
_THEMES: dict[str, Theme] = {
  'modern': MODERN_THEME,
  'dark': DARK_THEME,
  'forest': FOREST_THEME,
  'minimal': MINIMAL_THEME,
}


def get_theme(name: str) -> Theme:
  """Get a preset theme by name.

  Retrieves one of the built-in themes from the theme registry.

  Args:
      name: Theme name ("modern", "dark", "forest", or "minimal")

  Returns:
      Theme object containing all format-specific configurations

  Raises:
      ValueError: If the theme name is not recognized

  Examples:
      >>> theme = get_theme("modern")
      >>> theme.name
      'modern'
      >>> theme = get_theme("dark")
      >>> theme.color_scheme.primary
      '#8b5cf6'
  """
  if name not in _THEMES:
    available = ', '.join(sorted(_THEMES.keys()))
    raise ValueError(f'Unknown theme: {name!r}. Available themes: {available}')

  return _THEMES[name]


def list_themes() -> list[str]:
  """List all available preset theme names.

  Returns a sorted list of all built-in theme names that can be used
  with get_theme().

  Returns:
      List of theme names as strings

  Examples:
      >>> themes = list_themes()
      >>> "modern" in themes
      True
      >>> "dark" in themes
      True
  """
  return sorted(_THEMES.keys())
