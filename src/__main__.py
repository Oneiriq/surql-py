"""Main CLI entry point for ethereal.

This module provides the main Typer application and command groups for
the ethereal database toolkit.
"""

import sys

import structlog
import typer
from rich.console import Console

from src.cli import db_app, migrate_app, schema_app
from src.settings import get_settings

# Initialize console
console = Console()

# Create main Typer app
app = typer.Typer(
  name='ethereal',
  help='Code-first database toolkit for SurrealDB',
  no_args_is_help=True,
  add_completion=True,
  rich_markup_mode='rich',
)

# Add command groups
app.add_typer(migrate_app, name='migrate')
app.add_typer(schema_app, name='schema')
app.add_typer(db_app, name='db')


@app.command()
def version() -> None:
  """Show ethereal version information."""
  settings = get_settings()
  console.print(f'[bold cyan]ethereal[/bold cyan] version {settings.version}')
  console.print(f'Environment: {settings.environment}')


@app.callback()
def main(
  ctx: typer.Context,
  verbose: bool = typer.Option(
    False,
    '--verbose',
    '-v',
    help='Enable verbose logging',
  ),
) -> None:
  """Ethereal - Code-first database toolkit for SurrealDB.

  Ethereal provides a modern, type-safe way to work with SurrealDB through:

  • Code-first schema definitions
  • Automatic migration generation
  • Type-safe query building
  • Async-first operations

  Use --help with any command to see detailed usage information.
  """
  # Configure logging based on verbose flag
  if verbose:
    settings = get_settings()
    settings.log_level = 'DEBUG'

    # Configure structlog for verbose output
    structlog.configure(
      processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt='iso'),
        structlog.dev.ConsoleRenderer(),
      ],
      wrapper_class=structlog.make_filtering_bound_logger(logging_level=10),
      context_class=dict,
      logger_factory=structlog.PrintLoggerFactory(),
      cache_logger_on_first_use=False,
    )
  else:
    # Configure for normal output (less verbose)
    structlog.configure(
      processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt='iso'),
        structlog.dev.ConsoleRenderer(),
      ],
      wrapper_class=structlog.make_filtering_bound_logger(logging_level=20),
      context_class=dict,
      logger_factory=structlog.PrintLoggerFactory(),
      cache_logger_on_first_use=False,
    )


def cli() -> None:
  """CLI entry point wrapper.

  This function is called when the package is run as a module or
  via the installed console script.
  """
  try:
    app()
  except KeyboardInterrupt:
    console.print('\n[yellow]Operation cancelled by user[/yellow]')
    sys.exit(130)
  except Exception as e:
    console.print(f'[bold red]Error:[/bold red] {e}')
    sys.exit(1)


if __name__ == '__main__':
  cli()
