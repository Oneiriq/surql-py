"""Migration system for reverie ORM.

This module provides code-first migration capabilities including:
- Auto-generation of migrations from schema changes
- Migration file discovery and loading
- Migration execution (up/down)
- Migration history tracking in database
- Schema diffing utilities
"""

# Models
# Diff
from reverie.migration.diff import (
  diff_edges,
  diff_events,
  diff_fields,
  diff_indexes,
  diff_permissions,
  diff_tables,
)

# Discovery
from reverie.migration.discovery import (
  MigrationDiscoveryError,
  MigrationLoadError,
  discover_migrations,
  get_description_from_filename,
  get_version_from_filename,
  load_migration,
  validate_migration_name,
)

# Executor
from reverie.migration.executor import (
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
from reverie.migration.generator import (
  MigrationGenerationError,
  create_blank_migration,
  generate_initial_migration,
  generate_migration,
  generate_migration_from_diffs,
)

# History
from reverie.migration.history import (
  MigrationHistoryError,
  create_migration_table,
  create_snapshot_on_migration,
  disable_auto_snapshots,
  enable_auto_snapshots,
  ensure_migration_table,
  get_applied_migrations,
  get_applied_versions,
  get_migration_history,
  is_auto_snapshot_enabled,
  is_migration_applied,
  record_migration,
  remove_migration_record,
)
from reverie.migration.models import (
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

# Rollback
from reverie.migration.rollback import (
  RollbackIssue,
  RollbackPlan,
  RollbackResult,
  RollbackSafety,
  analyze_rollback_safety,
  create_rollback_plan,
  execute_rollback,
  plan_rollback_to_version,
)

# Versioning
from reverie.migration.versioning import (
  SchemaSnapshot,
  VersionGraph,
  VersionNode,
  compare_snapshots,
  create_snapshot,
  list_snapshots,
  load_snapshot,
  store_snapshot,
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
  'create_snapshot_on_migration',
  'enable_auto_snapshots',
  'disable_auto_snapshots',
  'is_auto_snapshot_enabled',
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
  # Versioning
  'SchemaSnapshot',
  'VersionNode',
  'VersionGraph',
  'create_snapshot',
  'store_snapshot',
  'load_snapshot',
  'list_snapshots',
  'compare_snapshots',
  # Rollback
  'RollbackSafety',
  'RollbackIssue',
  'RollbackPlan',
  'RollbackResult',
  'create_rollback_plan',
  'execute_rollback',
  'analyze_rollback_safety',
  'plan_rollback_to_version',
]
