"""Migration data models and types.

This module defines the core data structures for the migration system,
including migration metadata, state tracking, and execution plans.
"""

from collections.abc import Callable
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class MigrationState(Enum):
  """State of a migration in the execution lifecycle."""

  PENDING = 'pending'
  APPLIED = 'applied'
  FAILED = 'failed'


class MigrationDirection(Enum):
  """Direction of migration execution."""

  UP = 'up'
  DOWN = 'down'


class Migration(BaseModel):
  """Immutable migration definition.

  Represents a single migration file with its metadata and functions.

  Examples:
    >>> migration = Migration(
    ...   version='20260102_120000',
    ...   description='Create user table',
    ...   path=Path('migrations/20260102_120000_create_user_table.py'),
    ...   up=up_function,
    ...   down=down_function,
    ... )
  """

  version: str = Field(..., description='Migration version (timestamp-based)')
  description: str = Field(..., description='Human-readable description')
  path: Path = Field(..., description='Path to migration file')
  up: Callable[[], list[str]] = Field(..., description='Forward migration function')
  down: Callable[[], list[str]] = Field(..., description='Rollback migration function')
  checksum: str | None = Field(None, description='Migration content checksum')
  depends_on: list[str] = Field(default_factory=list, description='Dependencies')

  class Config:
    """Pydantic configuration."""

    arbitrary_types_allowed = True
    frozen = True


class MigrationHistory(BaseModel):
  """Migration history record stored in database.

  Represents a migration that has been applied to the database.

  Examples:
    >>> history = MigrationHistory(
    ...   version='20260102_120000',
    ...   description='Create user table',
    ...   applied_at=datetime.now(),
    ...   checksum='abc123',
    ... )
  """

  version: str = Field(..., description='Migration version')
  description: str = Field(..., description='Migration description')
  applied_at: datetime = Field(..., description='When migration was applied')
  checksum: str = Field(..., description='Migration content checksum')
  execution_time_ms: int | None = Field(None, description='Execution time in milliseconds')

  class Config:
    """Pydantic configuration."""

    frozen = True


class MigrationPlan(BaseModel):
  """Execution plan for a set of migrations.

  Represents the ordered list of migrations to execute and their direction.

  Examples:
    >>> plan = MigrationPlan(
    ...   migrations=[migration1, migration2],
    ...   direction=MigrationDirection.UP,
    ... )
  """

  migrations: list[Migration] = Field(..., description='Ordered list of migrations')
  direction: MigrationDirection = Field(..., description='Execution direction')

  class Config:
    """Pydantic configuration."""

    frozen = True

  @property
  def count(self) -> int:
    """Get the number of migrations in the plan."""
    return len(self.migrations)

  def is_empty(self) -> bool:
    """Check if the plan is empty."""
    return len(self.migrations) == 0


class MigrationMetadata(BaseModel):
  """Metadata for a migration file.

  This is the data structure expected in migration files.

  Examples:
    >>> metadata = MigrationMetadata(
    ...   version='20260102_120000',
    ...   description='Create user table',
    ...   author='ethereal',
    ...   depends_on=[],
    ... )
  """

  version: str
  description: str
  author: str = 'ethereal'
  depends_on: list[str] = Field(default_factory=list)

  class Config:
    """Pydantic configuration."""

    frozen = True


class MigrationStatus(BaseModel):
  """Status information for a migration.

  Combines migration definition with its current state.

  Examples:
    >>> status = MigrationStatus(
    ...   migration=migration,
    ...   state=MigrationState.APPLIED,
    ...   applied_at=datetime.now(),
    ... )
  """

  migration: Migration
  state: MigrationState
  applied_at: datetime | None = None
  error: str | None = None

  class Config:
    """Pydantic configuration."""

    frozen = True


class DiffOperation(Enum):
  """Type of schema change operation."""

  ADD_TABLE = 'add_table'
  DROP_TABLE = 'drop_table'
  ADD_FIELD = 'add_field'
  DROP_FIELD = 'drop_field'
  MODIFY_FIELD = 'modify_field'
  ADD_INDEX = 'add_index'
  DROP_INDEX = 'drop_index'
  ADD_EVENT = 'add_event'
  DROP_EVENT = 'drop_event'
  MODIFY_PERMISSIONS = 'modify_permissions'


class SchemaDiff(BaseModel):
  """Represents a difference between two schema versions.

  Examples:
    >>> diff = SchemaDiff(
    ...   operation=DiffOperation.ADD_TABLE,
    ...   table='user',
    ...   description='Add user table',
    ...   forward_sql='DEFINE TABLE user SCHEMAFULL;',
    ...   backward_sql='REMOVE TABLE user;',
    ... )
  """

  operation: DiffOperation
  table: str
  field: str | None = None
  index: str | None = None
  event: str | None = None
  description: str
  forward_sql: str
  backward_sql: str
  details: dict[str, Any] = Field(default_factory=dict)

  class Config:
    """Pydantic configuration."""

    frozen = True
