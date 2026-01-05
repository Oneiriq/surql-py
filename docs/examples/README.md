# Reverie Visualization Examples

This directory contains examples demonstrating Reverie's schema visualization capabilities.

## Important: Viewing Rendered Output

The visualization examples generate **source code** (DOT, Mermaid, ASCII) that needs to be **rendered** to see the beautiful styled diagrams with colors, gradients, and themes.

**What you see in the files**: Source code with styling directives
**What you should see**: Rendered diagrams with colors, gradients, and beautiful formatting

📖 **See the full guide**: [`docs/VISUALIZATION_RENDERING_GUIDE.md`](../VISUALIZATION_RENDERING_GUIDE.md)

---

## Quick Start

### Run the Examples

```bash
# Run all visualization examples
python docs/examples/visualization_example.py

# Run with color output (ASCII format)
python docs/examples/visualization_example.py | less -R
```

### Render Sample Output Files

This directory contains pre-generated sample outputs in the [`output/`](output/) subdirectory.

#### GraphViz (`.dot` files)

```bash
# Render to PNG
dot -Tpng docs/examples/output/schema_dark_graphviz.dot -o schema.png

# Render to SVG (recommended for documentation)
dot -Tsvg docs/examples/output/schema_dark_graphviz.dot -o schema.svg

# View online (no installation needed)
# Visit: https://dreampuf.github.io/GraphvizOnline/
# Copy/paste the contents of the .dot file
```

#### Mermaid (`.md` files)

```bash
# View on GitHub
# Push the file to GitHub and view it - Mermaid renders automatically!

# View in Mermaid Live Editor (no installation needed)
# Visit: https://mermaid.live/
# Copy/paste the Mermaid code (the content inside ```mermaid ... ```)

# View in VS Code
# Install the "Markdown Preview Mermaid Support" extension
# Open the .md file and press Ctrl+Shift+V (Cmd+Shift+V on Mac)
```

#### ASCII (`.txt` files)

```bash
# View with colors in terminal
cat docs/examples/output/schema_forest_ascii.txt

# View with less (preserves colors)
less -R docs/examples/output/schema_forest_ascii.txt

# View with bat (enhanced colors)
bat docs/examples/output/schema_forest_ascii.txt
```

---

## Example Files

### Main Example Script

- **[`visualization_example.py`](visualization_example.py)** - Comprehensive examples showing:
  - All output formats (GraphViz, Mermaid, ASCII)
  - All preset themes (modern, dark, forest, minimal)
  - Custom theme creation
  - Format-specific customization
  - Filtering and control options
  - Saving to files

### Sample Output Files (in `output/` directory)

- **[`schema_dark_graphviz.dot`](output/schema_dark_graphviz.dot)** - GraphViz DOT format with dark theme
- **[`schema_modern_mermaid.md`](output/schema_modern_mermaid.md)** - Mermaid ER diagram with modern theme
- **[`schema_forest_ascii.txt`](output/schema_forest_ascii.txt)** - ASCII art with forest theme

### Legacy Sample Files (Top-Level)

These are earlier examples showing different themes:
- [`schema_modern_graphviz.dot`](schema_modern_graphviz.dot) - Modern theme GraphViz
- [`schema_dark_mermaid.md`](schema_dark_mermaid.md) - Dark theme Mermaid
- [`schema_forest_ascii.txt`](schema_forest_ascii.txt) - Forest theme ASCII

---

## Rendering Tools

### Required for GraphViz

Install GraphViz to render `.dot` files:

```bash
# macOS
brew install graphviz

# Ubuntu/Debian
sudo apt-get install graphviz

# Windows (Chocolatey)
choco install graphviz

# Windows (Scoop)
scoop install graphviz
```

### Recommended for ASCII

Install `bat` for enhanced color display:

```bash
# macOS
brew install bat

# Ubuntu/Debian
sudo apt install bat

# Windows (Scoop)
scoop install bat
```

### Online Tools (No Installation)

- **GraphViz**: [dreampuf.github.io/GraphvizOnline](https://dreampuf.github.io/GraphvizOnline/)
- **Mermaid**: [mermaid.live](https://mermaid.live/)
- **Edotor** (GraphViz): [edotor.net](https://edotor.net/)

---

## Quick Reference: Rendering Commands

| Format | Source File | Render Command | Output |
|--------|-------------|----------------|--------|
| GraphViz | `schema.dot` | `dot -Tpng schema.dot -o out.png` | PNG image |
| GraphViz | `schema.dot` | `dot -Tsvg schema.dot -o out.svg` | SVG image |
| Mermaid | `schema.md` | Push to GitHub or paste to mermaid.live | Rendered diagram |
| ASCII | `schema.txt` | `cat schema.txt` or `less -R schema.txt` | Terminal output |
| ASCII | `schema.txt` | `bat schema.txt` | Enhanced terminal |

---

## Testing Your Setup

Verify your rendering tools are working:

```bash
# Test GraphViz installation
dot -V
# Should output: dot - graphviz version X.X.X

# Test terminal color support
python -c "print('\033[91mRed\033[0m \033[94mBlue\033[0m')"
# Should display "Red" in red and "Blue" in blue

# Test Unicode support  
python -c "print('╭─╮')"
# Should show box-drawing characters, not question marks
```

---

## Other Examples in This Directory

- [`basic_usage.py`](basic_usage.py) - Basic Reverie operations
- [`advanced_queries.py`](advanced_queries.py) - Advanced query examples
- [`graph_queries.py`](graph_queries.py) - Graph traversal examples
- [`migration_example.py`](migration_example.py) - Database migration workflow
- [`schema_definition.py`](schema_definition.py) - Schema definition examples
- [`orchestration_example.py`](orchestration_example.py) - Multi-server orchestration
- [`query_hints_example.py`](query_hints_example.py) - Query optimization hints
- [`versioning_rollback_example.py`](versioning_rollback_example.py) - Version control examples

---

## Need Help?

- 📖 **Full Rendering Guide**: [`docs/VISUALIZATION_RENDERING_GUIDE.md`](../VISUALIZATION_RENDERING_GUIDE.md)
- 📋 **Troubleshooting**: See the Troubleshooting section in the rendering guide
- 🎨 **Theme Documentation**: Check [`src/reverie/schema/themes.py`](../../src/reverie/schema/themes.py)
- 🔧 **Visualization API**: See [`src/reverie/schema/visualize.py`](../../src/reverie/schema/visualize.py)

---

## Summary

Remember these key points:

1. ✅ **The styling IS working** - colors, gradients, and themes are in the generated code
2. ✅ **You must RENDER the output** - use GraphViz, Mermaid.js, or a color terminal
3. ✅ **Each format needs different tools** - see the commands above
4. ✅ **Quick online options available** - no installation required to preview

Happy visualizing! 🎨✨
