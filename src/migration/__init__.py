"""Migration system for ethereal ORM.

This module provides code-first migration capabilities including:
- Auto-generation of migrations from schema changes
- Migration file discovery and loading
- Migration execution (up/down)
- Migration history tracking in database
- Schema diffing utilities
"""

# Models
from src.migration.models import (
  DiffOperation,
  Migration,
  MigrationDirection,
  MigrationHistory,
  MigrationMetadata,
  MigrationPlan,
  MigrationState,
  MigrationStatus,
  SchemaDiff,
)

# Discovery
from src.migration.discovery import (
  MigrationDiscoveryError,
  MigrationLoadError,
  discover_migrations,
  get_description_from_filename,
  get_version_from_filename,
  load_migration,
  validate_migration_name,
)

# History
from src.migration.history import (
  MigrationHistoryError,
  create_migration_table,
  ensure_migration_table,
  get_applied_migrations,
  get_applied_versions,
  get_migration_history,
  is_migration_applied,
  record_migration,
  remove_migration_record,
)

# Executor
from src.migration.executor import (
  MigrationExecutionError,
  create_migration_plan,
  execute_migration,
  execute_migration_plan,
  get_applied_migrations_ordered,
  get_migration_status,
  get_pending_migrations,
  migrate_down,
  migrate_up,
  validate_migrations,
)

# Generator
from src.migration.generator import (
  MigrationGenerationError,
  create_blank_migration,
  generate_initial_migration,
  generate_migration,
  generate_migration_from_diffs,
)

# Diff
from src.migration.diff import (
  diff_edges,
  diff_events,
  diff_fields,
  diff_indexes,
  diff_permissions,
  diff_tables,
)

__all__ = [
  # Models
  'Migration',
  'MigrationState',
  'MigrationDirection',
  'MigrationHistory',
  'MigrationMetadata',
  'MigrationPlan',
  'MigrationStatus',
  'DiffOperation',
  'SchemaDiff',
  # Discovery
  'discover_migrations',
  'load_migration',
  'validate_migration_name',
  'get_version_from_filename',
  'get_description_from_filename',
  'MigrationDiscoveryError',
  'MigrationLoadError',
  # History
  'create_migration_table',
  'ensure_migration_table',
  'record_migration',
  'remove_migration_record',
  'get_applied_migrations',
  'get_applied_versions',
  'is_migration_applied',
  'get_migration_history',
  'MigrationHistoryError',
  # Executor
  'execute_migration',
  'migrate_up',
  'migrate_down',
  'get_pending_migrations',
  'get_applied_migrations_ordered',
  'get_migration_status',
  'execute_migration_plan',
  'validate_migrations',
  'create_migration_plan',
  'MigrationExecutionError',
  # Generator
  'generate_migration',
  'generate_initial_migration',
  'create_blank_migration',
  'generate_migration_from_diffs',
  'MigrationGenerationError',
  # Diff
  'diff_tables',
  'diff_fields',
  'diff_indexes',
  'diff_events',
  'diff_permissions',
  'diff_edges',
]
