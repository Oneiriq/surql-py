"""Common CLI utilities and helpers.

This module provides shared functionality for CLI commands including
output formatting, confirmations, and error handling.
"""

import json
import sys
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import structlog
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax

logger = structlog.get_logger(__name__)
console = Console()
err_console = Console(stderr=True, style='bold red')


class OutputFormat(Enum):
  """Output format options."""
  
  TABLE = 'table'
  JSON = 'json'
  TEXT = 'text'


def format_output(
  data: Any,
  format: OutputFormat = OutputFormat.TABLE,
  title: Optional[str] = None,
) -> None:
  """Format and display output to console.
  
  Args:
    data: Data to display
    format: Output format (table, json, or text)
    title: Optional title for table output
  """
  if format == OutputFormat.JSON:
    console.print_json(json.dumps(data, indent=2, default=str))
  elif format == OutputFormat.TABLE and isinstance(data, list):
    _display_table(data, title)
  else:
    console.print(data)


def _display_table(data: list[dict[str, Any]], title: Optional[str] = None) -> None:
  """Display data as a rich table.
  
  Args:
    data: List of dictionaries to display
    title: Optional table title
  """
  if not data:
    console.print('[yellow]No data to display[/yellow]')
    return
  
  # Create table
  table = Table(title=title, show_header=True, header_style='bold cyan')
  
  # Add columns from first row
  first_row = data[0]
  for key in first_row.keys():
    table.add_column(str(key).replace('_', ' ').title())
  
  # Add rows
  for row in data:
    table.add_row(*[str(v) for v in row.values()])
  
  console.print(table)


def display_success(message: str) -> None:
  """Display success message.
  
  Args:
    message: Success message to display
  """
  console.print(f'[green]✓[/green] {message}')


def display_info(message: str) -> None:
  """Display info message.
  
  Args:
    message: Info message to display
  """
  console.print(f'[blue]ℹ[/blue] {message}')


def display_warning(message: str) -> None:
  """Display warning message.
  
  Args:
    message: Warning message to display
  """
  console.print(f'[yellow]⚠[/yellow] {message}')


def display_error(message: str, exit_code: Optional[int] = None) -> None:
  """Display error message and optionally exit.
  
  Args:
    message: Error message to display
    exit_code: If provided, exit with this code
  """
  err_console.print(f'[bold red]✗[/bold red] {message}')
  if exit_code is not None:
    sys.exit(exit_code)


def display_panel(content: str, title: str, style: str = 'cyan') -> None:
  """Display content in a panel.
  
  Args:
    content: Content to display
    title: Panel title
    style: Panel style
  """
  console.print(Panel(content, title=title, border_style=style))


def display_code(code: str, language: str = 'sql', title: Optional[str] = None) -> None:
  """Display syntax-highlighted code.
  
  Args:
    code: Code to display
    language: Programming language for syntax highlighting
    title: Optional title
  """
  syntax = Syntax(code, language, theme='monokai', line_numbers=True)
  if title:
    console.print(Panel(syntax, title=title))
  else:
    console.print(syntax)


def confirm(message: str, default: bool = False) -> bool:
  """Prompt user for confirmation.
  
  Args:
    message: Confirmation message
    default: Default value if user just presses Enter
    
  Returns:
    True if confirmed, False otherwise
  """
  return typer.confirm(message, default=default)


def confirm_destructive(message: str) -> bool:
  """Prompt user for confirmation of destructive operation.
  
  Requires explicit 'yes' response.
  
  Args:
    message: Confirmation message
    
  Returns:
    True if confirmed, False otherwise
  """
  display_warning(f'{message}')
  display_warning('This action cannot be undone!')
  
  response = typer.prompt(
    'Type "yes" to confirm',
    default='',
  )
  
  return response.lower() == 'yes'


def get_migrations_directory(directory: Optional[Path] = None) -> Path:
  """Get migrations directory path, ensuring it exists.
  
  Args:
    directory: Optional custom directory path
    
  Returns:
    Path to migrations directory
  """
  if directory is None:
    directory = Path.cwd() / 'migrations'
  
  if not directory.exists():
    display_info(f'Creating migrations directory: {directory}')
    directory.mkdir(parents=True, exist_ok=True)
  
  return directory


def spinner(text: str = 'Working...'):
  """Create a progress spinner context manager.
  
  Args:
    text: Text to display with spinner
    
  Returns:
    Progress context manager
  """
  return Progress(
    SpinnerColumn(),
    TextColumn('[progress.description]{task.description}'),
    console=console,
    transient=True,
  )


def handle_error(error: Exception, verbose: bool = False) -> None:
  """Handle and display error appropriately.
  
  Args:
    error: Exception to handle
    verbose: Whether to show full traceback
  """
  error_type = type(error).__name__
  
  if verbose:
    console.print_exception()
  else:
    display_error(f'{error_type}: {str(error)}')
  
  logger.error('command_error', error_type=error_type, error=str(error))


def validate_file_exists(path: Path, file_type: str = 'File') -> None:
  """Validate that a file exists.
  
  Args:
    path: Path to validate
    file_type: Type of file (for error message)
    
  Raises:
    typer.BadParameter: If file doesn't exist
  """
  if not path.exists():
    raise typer.BadParameter(f'{file_type} not found: {path}')
  
  if not path.is_file():
    raise typer.BadParameter(f'Path is not a file: {path}')


def validate_directory_exists(path: Path, dir_type: str = 'Directory') -> None:
  """Validate that a directory exists.
  
  Args:
    path: Path to validate
    dir_type: Type of directory (for error message)
    
  Raises:
    typer.BadParameter: If directory doesn't exist
  """
  if not path.exists():
    raise typer.BadParameter(f'{dir_type} not found: {path}')
  
  if not path.is_dir():
    raise typer.BadParameter(f'Path is not a directory: {path}')


# Common Typer options

verbose_option = typer.Option(
  False,
  '--verbose',
  '-v',
  help='Enable verbose output with detailed logging',
)

dry_run_option = typer.Option(
  False,
  '--dry-run',
  help='Show what would be done without making changes',
)

directory_option = typer.Option(
  None,
  '--directory',
  '-d',
  help='Migrations directory path',
  exists=False,
)

format_option = typer.Option(
  OutputFormat.TABLE,
  '--format',
  '-f',
  help='Output format',
  case_sensitive=False,
)
