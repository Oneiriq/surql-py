"""CLI command modules.

This module contains all CLI command groups for the surql application.
"""

from surql.cli.db import app as db_app
from surql.cli.migrate import app as migrate_app
from surql.cli.schema import app as schema_app

__all__ = [
  'db_app',
  'migrate_app',
  'schema_app',
]
