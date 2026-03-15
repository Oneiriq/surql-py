# Schema Versioning and Rollback

Complete guide to schema versioning, snapshotting, and safe rollback strategies in surql.

## Table of Contents

- [Overview](#overview)
- [Core Concepts](#core-concepts)
- [Schema Snapshots](#schema-snapshots)
- [Version Graph](#version-graph)
- [Rollback Planning](#rollback-planning)
- [Safety Levels](#safety-levels)
- [Executing Rollbacks](#executing-rollbacks)
- [Auto-Snapshots](#auto-snapshots)
- [CLI Commands](#cli-commands)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)

## Overview

Schema versioning and rollback features enable you to:

- **Track schema evolution** - Capture schema state at each migration
- **Compare versions** - Identify differences between schema snapshots
- **Plan rollbacks safely** - Analyze data loss risks before rolling back
- **Execute rollbacks** - Safely revert to previous schema versions
- **Auto-snapshot** - Automatically create snapshots after migrations

### Why Version Tracking Matters

Version tracking provides:

- **Safety net** - Ability to revert problematic migrations
- **Audit trail** - Complete history of schema changes
- **Comparison tools** - Understand what changed between versions
- **Risk assessment** - Know data loss implications before rollback

## Core Concepts

### Version

A version identifies a specific point in migration history:

```python
version = '20260109_120000'  # Timestamp-based identifier
```

### Schema Snapshot

A snapshot captures complete database schema at a specific version:

```python
from surql.migration.versioning import SchemaSnapshot

snapshot = SchemaSnapshot(
  version='20260109_120000',
  created_at=datetime.now(UTC),
  tables={'user': {...}, 'post': {...}},
  edges={'likes': {...}},
  indexes={'user': [...]},
  checksum='sha256_hash',
  migration_count=5,
)
```

### Version Graph

A graph representing the migration history and relationships:

```python
from surql.migration.versioning import VersionGraph

graph = VersionGraph()
graph.add_version(migration1)
graph.add_version(migration2, parent='20260108_120000')
```

### Rollback Plan

A plan for safely rolling back to a previous version:

```python
from surql.migration.rollback import create_rollback_plan

plan = await create_rollback_plan(
  client,
  migrations,
  target_version='20260108_120000',
)
```

## Schema Snapshots

### Creating Snapshots

Capture current schema state:

```python
from surql.migration.versioning import create_snapshot
from surql.connection.client import get_client

async def capture_schema():
  async with get_client(config) as client:
    snapshot = await create_snapshot(
      client=client,
      version='20260109_120000',
      migration_count=5,
    )
    
    print(f'Snapshot created:')
    print(f'  Version: {snapshot.version}')
    print(f'  Tables: {len(snapshot.tables)}')
    print(f'  Edges: {len(snapshot.edges)}')
    print(f'  Checksum: {snapshot.checksum[:16]}...')
```

### Storing Snapshots

Save snapshots to the database:

```python
from surql.migration.versioning import store_snapshot

async def save_snapshot(snapshot):
  async with get_client(config) as client:
    await store_snapshot(client, snapshot)
    print('Snapshot stored successfully')
```

### Loading Snapshots

Retrieve previously saved snapshots:

```python
from surql.migration.versioning import load_snapshot

async def retrieve_snapshot(version):
  async with get_client(config) as client:
    snapshot = await load_snapshot(client, version)
    
    if snapshot:
      print(f'Loaded snapshot for version {version}')
      print(f'  Created at: {snapshot.created_at}')
      print(f'  Migration count: {snapshot.migration_count}')
    else:
      print(f'Snapshot not found for version {version}')
    
    return snapshot
```

### Listing Snapshots

Get all stored snapshots:

```python
from surql.migration.versioning import list_snapshots

async def show_all_snapshots():
  async with get_client(config) as client:
    snapshots = await list_snapshots(client)
    
    print(f'Found {len(snapshots)} snapshot(s):')
    for snapshot in snapshots:
      print(f'  {snapshot.version} - {snapshot.created_at}')
      print(f'    Tables: {len(snapshot.tables)}, Migrations: {snapshot.migration_count}')
```

### Comparing Snapshots

Identify differences between two schema versions:

```python
from surql.migration.versioning import compare_snapshots

async def compare_versions(version1, version2):
  async with get_client(config) as client:
    snapshot1 = await load_snapshot(client, version1)
    snapshot2 = await load_snapshot(client, version2)
    
    if not snapshot1 or not snapshot2:
      print('One or both snapshots not found')
      return
    
    diff = compare_snapshots(snapshot1, snapshot2)
    
    print(f'Comparing {version1} → {version2}')
    
    if diff['checksum_match']:
      print('Schemas are identical')
      return
    
    if diff['tables_added']:
      print(f"Tables added: {', '.join(diff['tables_added'])}")
    
    if diff['tables_removed']:
      print(f"Tables removed: {', '.join(diff['tables_removed'])}")
    
    if diff['tables_modified']:
      print(f"Tables modified: {', '.join(diff['tables_modified'])}")
    
    if diff['edges_added']:
      print(f"Edges added: {', '.join(diff['edges_added'])}")
    
    if diff['edges_removed']:
      print(f"Edges removed: {', '.join(diff['edges_removed'])}")
```

## Version Graph

### Building a Version Graph

Track migration history as a graph:

```python
from surql.migration.versioning import VersionGraph
from surql.migration.discovery import discover_migrations
from pathlib import Path

def build_version_graph():
  migrations = discover_migrations(Path('migrations'))
  
  graph = VersionGraph()
  
  for i, migration in enumerate(migrations):
    parent = migrations[i - 1].version if i > 0 else None
    graph.add_version(migration, parent=parent)
  
  print(f'Graph contains {len(graph.get_all_versions())} versions')
  return graph
```

### Querying the Graph

Find paths and relationships:

```python
def query_graph(graph):
  versions = graph.get_all_versions()
  
  if len(versions) >= 2:
    first = versions[0]
    last = versions[-1]
    
    # Get path between versions
    path = graph.get_path(first, last)
    if path:
      print(f'Path from {first} to {last}:')
      for v in path:
        print(f'  → {v}')
    
    # Get ancestors
    ancestors = graph.get_ancestors(last)
    print(f'\nAncestors of {last}:')
    for v in ancestors:
      print(f'  ← {v}')
    
    # Get descendants
    descendants = graph.get_descendants(first)
    print(f'\nDescendants of {first}:')
    for v in descendants:
      print(f'  → {v}')
```

### Using Graph for Rollback Planning

Determine migrations to rollback:

```python
def plan_using_graph(graph, current_version, target_version):
  # Get path from current to target
  path = graph.get_path(current_version, target_version)
  
  if not path:
    print(f'No path found from {current_version} to {target_version}')
    return
  
  # Migrations to rollback are those in the path (excluding target)
  to_rollback = [v for v in path if v != target_version]
  to_rollback.reverse()  # Newest first
  
  print(f'Will rollback {len(to_rollback)} migration(s):')
  for v in to_rollback:
    print(f'  - {v}')
```

## Rollback Planning

### Creating a Rollback Plan

Plan rollback with safety analysis:

```python
from surql.migration.rollback import create_rollback_plan
from surql.migration.discovery import discover_migrations

async def plan_rollback():
  migrations = discover_migrations(Path('migrations'))
  
  async with get_client(config) as client:
    plan = await create_rollback_plan(
      client=client,
      migrations=migrations,
      target_version='20260108_120000',
    )
    
    print('Rollback Plan:')
    print(f'  From: {plan.from_version}')
    print(f'  To: {plan.to_version}')
    print(f'  Migrations to rollback: {plan.migration_count}')
    print(f'  Overall safety: {plan.overall_safety.value.upper()}')
    
    if plan.issues:
      print(f'\nSafety Issues ({len(plan.issues)}):')
      for issue in plan.issues:
        print(f'  [{issue.safety.value.upper()}] {issue.migration}')
        print(f'    {issue.description}')
        if issue.recommendation:
          print(f'    → {issue.recommendation}')
    
    if plan.requires_approval:
      print('\n⚠️  This rollback requires manual approval')
    
    return plan
```

### Analyzing Rollback Safety

Get safety analysis without creating full plan:

```python
from surql.migration.rollback import analyze_rollback_safety

async def check_safety(target_version):
  migrations = discover_migrations(Path('migrations'))
  
  async with get_client(config) as client:
    issues = await analyze_rollback_safety(
      client,
      migrations,
      target_version,
    )
    
    if not issues:
      print('✓ Rollback is safe - no issues detected')
    else:
      print(f'Found {len(issues)} potential issue(s):')
      for issue in issues:
        print(f'  {issue.safety.value}: {issue.description}')
```

## Safety Levels

Rollback operations are classified by safety level:

### SAFE

No data loss expected.

**Characteristics:**
- Only adds/removes indexes
- Only adds fields with defaults
- Only adds tables

**Example:**

```python
# Migration added an index
def up():
  return ['DEFINE INDEX email_idx ON TABLE user COLUMNS email;']

def down():
  return ['REMOVE INDEX email_idx ON TABLE user;']

# Rollback: RollbackSafety.SAFE
```

### DATA_LOSS

Some data may be lost.

**Characteristics:**
- Removes fields (data in those fields lost)
- Changes field types (potential conversion issues)
- Modifies constraints (may reject existing data)

**Example:**

```python
# Migration added a field
def up():
  return ['DEFINE FIELD bio ON TABLE user TYPE string;']

def down():
  return ['REMOVE FIELD bio ON TABLE user;']

# Rollback: RollbackSafety.DATA_LOSS
# Reason: bio field data will be lost
```

### UNSAFE

Significant data loss likely.

**Characteristics:**
- Removes entire tables
- Drops multiple critical fields
- Irreversible data transformations

**Example:**

```python
# Migration created a table
def up():
  return ['DEFINE TABLE user SCHEMAFULL;', ...]

def down():
  return ['REMOVE TABLE user;']

# Rollback: RollbackSafety.UNSAFE
# Reason: All user table data will be lost
```

### Safety Level Matrix

| Operation | Forward (up) | Rollback (down) | Safety |
|-----------|--------------|-----------------|--------|
| Add table | Safe | UNSAFE | Table drop |
| Remove table | UNSAFE | Safe | N/A |
| Add field | Safe | DATA_LOSS | Field data lost |
| Remove field | DATA_LOSS | Safe | N/A |
| Add index | Safe | SAFE | Index drop |
| Remove index | SAFE | Safe | N/A |
| Modify field type | DATA_LOSS | DATA_LOSS | Conversion issues |

## Executing Rollbacks

### Safe Rollback Execution

Execute a safe rollback:

```python
from surql.migration.rollback import execute_rollback

async def rollback_safely():
  migrations = discover_migrations(Path('migrations'))
  
  async with get_client(config) as client:
    # Create plan
    plan = await create_rollback_plan(
      client,
      migrations,
      target_version='20260108_120000',
    )
    
    # Check safety
    if plan.overall_safety == RollbackSafety.SAFE:
      print('Executing safe rollback...')
      
      result = await execute_rollback(client, plan)
      
      if result.success:
        print(f'✓ Successfully rolled back {result.rolled_back_count} migration(s)')
        print(f'  Duration: {result.actual_duration_ms}ms')
      else:
        print(f'✗ Rollback failed after {result.rolled_back_count} migration(s)')
        for error in result.errors:
          print(f'  Error: {error}')
    else:
      print(f'Rollback is {plan.overall_safety.value} - review issues first')
```

### Force Rollback (Unsafe)

Execute rollback despite safety warnings:

```python
async def force_rollback():
  migrations = discover_migrations(Path('migrations'))
  
  async with get_client(config) as client:
    plan = await create_rollback_plan(
      client,
      migrations,
      target_version='20260107_120000',
    )
    
    # Display warnings
    print(f'Safety: {plan.overall_safety.value.upper()}')
    
    if plan.has_data_loss:
      print('\n⚠️  WARNING: This rollback may cause data loss!')
      print('Issues:')
      for issue in plan.issues:
        print(f'  - {issue.description}')
        if issue.affected_data:
          print(f'    Affected: {issue.affected_data}')
      
      # Simulate user confirmation
      confirm = input('\nProceed with rollback? [yes/NO]: ')
      
      if confirm.lower() == 'yes':
        print('\nProceeding with forced rollback...')
        result = await execute_rollback(client, plan, force=True)
        
        if result.success:
          print('✓ Rollback completed')
        else:
          print(f'✗ Rollback failed: {result.errors}')
      else:
        print('Rollback cancelled')
```

### Dry Run Rollback

Preview rollback without executing:

```python
async def preview_rollback():
  migrations = discover_migrations(Path('migrations'))
  
  async with get_client(config) as client:
    plan = await create_rollback_plan(
      client,
      migrations,
      target_version='20260108_120000',
    )
    
    print('=== Rollback Preview ===')
    print(f'From: {plan.from_version}')
    print(f'To: {plan.to_version}')
    print(f'Safety: {plan.overall_safety.value}')
    print()
    
    print(f'Migrations to rollback ({plan.migration_count}):')
    for migration in plan.migrations:
      print(f'  {migration.version}: {migration.description}')
      
      # Show down() SQL
      try:
        statements = migration.down()
        for stmt in statements:
          print(f'    {stmt[:60]}...' if len(stmt) > 60 else f'    {stmt}')
      except Exception as e:
        print(f'    Error getting down migration: {e}')
    
    print('\nDry run - no changes made')
```

## Auto-Snapshots

### Enabling Auto-Snapshots

Automatically create snapshots after migrations:

```python
from surql.migration.history import enable_auto_snapshots, disable_auto_snapshots

# Enable automatic snapshots
enable_auto_snapshots()
print('Automatic snapshots enabled')

# Future migrations will create snapshots automatically
# await apply_migrations(...)

# Disable when done
disable_auto_snapshots()
print('Automatic snapshots disabled')
```

### How Auto-Snapshots Work

When enabled:

1. Migration is applied successfully
2. Snapshot is created automatically
3. Snapshot is stored in `_schema_snapshot` table
4. Process continues with next migration

**Example Flow:**

```python
from surql.migration.history import enable_auto_snapshots
from surql.migration.executor import apply_migrations

async def migrate_with_snapshots():
  # Enable auto-snapshots
  enable_auto_snapshots()
  
  async with get_client(config) as client:
    # Apply migrations
    await apply_migrations(client, migrations)
    
    # Snapshots were created automatically for each migration
    snapshots = await list_snapshots(client)
    print(f'{len(snapshots)} snapshots created')
```

### Benefits of Auto-Snapshots

- **No manual steps** - Snapshots created automatically
- **Complete history** - Snapshot for every applied migration
- **Easy rollback** - Always have a snapshot to compare against
- **Audit trail** - Track schema evolution precisely

## CLI Commands

### migrate snapshot

Create a snapshot manually:

```shell
surql migrate snapshot [OPTIONS]
```

**Options:**

- `--version VERSION` - Version identifier (default: current)
- `--output FILE` - Save to file instead of database

**Examples:**

```shell
# Create snapshot of current state
surql migrate snapshot

# Create snapshot for specific version
surql migrate snapshot --version 20260109_120000

# Export snapshot to file
surql migrate snapshot --output snapshot.json
```

### migrate list-snapshots

List all stored snapshots:

```shell
surql migrate list-snapshots [OPTIONS]
```

**Options:**

- `--format FORMAT` - Output format: `table`, `json` (default: `table`)

**Example:**

```shell
surql migrate list-snapshots
```

**Output:**

```
Schema Snapshots
┌────────────────────┬─────────────────────┬────────┬────────────┐
│ Version            │ Created At          │ Tables │ Migrations │
├────────────────────┼─────────────────────┼────────┼────────────┤
│ 20260108_120000    │ 2026-01-08 12:00:00 │ 5      │ 3          │
│ 20260109_120000    │ 2026-01-09 12:00:00 │ 6      │ 4          │
└────────────────────┴─────────────────────┴────────┴────────────┘
```

### migrate plan-rollback

Plan rollback to a target version:

```shell
surql migrate plan-rollback VERSION [OPTIONS]
```

**Arguments:**

- `VERSION` - Target version to rollback to (required)

**Options:**

- `--show-sql` - Display SQL that will be executed
- `--format FORMAT` - Output format: `table`, `json` (default: `table`)

**Examples:**

```shell
# Plan rollback
surql migrate plan-rollback 20260108_120000

# Show SQL that will run
surql migrate plan-rollback 20260108_120000 --show-sql
```

**Output:**

```
Rollback Plan
From: 20260109_120000
To: 20260108_120000

Migrations to Rollback: 1
Overall Safety: DATA_LOSS

Safety Issues:
  [DATA_LOSS] 20260109_120000
    Dropping field: bio
    → Backup affected field data

⚠️  This rollback requires approval
```

### migrate rollback

Execute a rollback:

```shell
surql migrate rollback VERSION [OPTIONS]
```

**Arguments:**

- `VERSION` - Target version to rollback to (required)

**Options:**

- `--force` - Force rollback despite safety warnings
- `--dry-run` - Preview without executing
- `--yes` - Skip confirmation prompt

**Examples:**

```shell
# Rollback to version (with confirmation)
surql migrate rollback 20260108_120000

# Force unsafe rollback
surql migrate rollback 20260107_120000 --force

# Dry run to preview
surql migrate rollback 20260108_120000 --dry-run

# Skip confirmation
surql migrate rollback 20260108_120000 --yes
```

### migrate compare

Compare two snapshots:

```shell
surql migrate compare VERSION1 VERSION2
```

**Arguments:**

- `VERSION1` - First version (required)
- `VERSION2` - Second version (required)

**Example:**

```shell
surql migrate compare 20260108_120000 20260109_120000
```

**Output:**

```
Comparing 20260108_120000 → 20260109_120000

Tables Added:
  + profile

Fields Modified:
  ~ user.bio (added)

Indexes Added:
  + user.email_idx
```

## Best Practices

### 1. Create Snapshots at Key Points

Create snapshots before major changes:

```python
async def major_migration():
  async with get_client(config) as client:
    # Snapshot before
    snapshot_before = await create_snapshot(client, 'before_major_change', 10)
    await store_snapshot(client, snapshot_before)
    
    # Apply migrations
    await apply_migrations(client, migrations)
    
    # Snapshot after
    snapshot_after = await create_snapshot(client, 'after_major_change', 15)
    await store_snapshot(client, snapshot_after)
```

### 2. Always Review Rollback Plans

Never execute without reviewing:

```shell
# Step 1: Create plan
surql migrate plan-rollback 20260108_120000 --show-sql

# Step 2: Review output carefully

# Step 3: Execute if safe
surql migrate rollback 20260108_120000
```

### 3. Backup Data Before Unsafe Rollbacks

Export affected data:

```python
async def safe_unsafe_rollback():
  async with get_client(config) as client:
    # Create rollback plan
    plan = await create_rollback_plan(client, migrations, target_version)
    
    if plan.overall_safety == RollbackSafety.UNSAFE:
      # Identify affected tables
      affected_tables = set()
      for issue in plan.issues:
        if issue.affected_data:
          # Extract table name from affected_data
          if 'table' in issue.affected_data.lower():
            table_name = issue.affected_data.split()[0]
            affected_tables.add(table_name)
      
      # Backup affected tables
      for table in affected_tables:
        print(f'Backing up {table}...')
        records = await client.select(table)
        with open(f'backup_{table}.json', 'w') as f:
          json.dump(records, f)
      
      # Now safe to rollback
      result = await execute_rollback(client, plan, force=True)
```

### 4. Test Rollbacks in Development

Always test rollback process:

```shell
# In development
surql migrate up                    # Apply migration
surql migrate snapshot              # Create snapshot
surql migrate rollback <version>    # Test rollback
surql migrate up                    # Re-apply
```

### 5. Use Auto-Snapshots in Production

Enable for production deployments:

```python
async def production_migration():
  # Enable auto-snapshots for safety
  enable_auto_snapshots()
  
  try:
    async with get_client(config) as client:
      await apply_migrations(client, migrations)
  finally:
    disable_auto_snapshots()
```

### 6. Document Rollback Procedures

Add rollback notes to migrations:

```python
"""
Migration: Add user profile fields

ROLLBACK WARNING: Rolling back this migration will lose all user
profile data stored in the bio and avatar_url fields.

Before rollback:
1. Export user profiles: SELECT id, bio, avatar_url FROM user
2. Save to backup file
3. Verify backup integrity
4. Proceed with rollback
"""

def up():
  return [
    'DEFINE FIELD bio ON TABLE user TYPE string;',
    'DEFINE FIELD avatar_url ON TABLE user TYPE string;',
  ]

def down():
  return [
    'REMOVE FIELD avatar_url ON TABLE user;',
    'REMOVE FIELD bio ON TABLE user;',
  ]
```

### 7. Monitor Rollback Execution

Track rollback progress:

```python
import time

async def monitored_rollback(plan):
  async with get_client(config) as client:
    start = time.time()
    result = await execute_rollback(client, plan)
    duration = time.time() - start
    
    # Log results
    logger.info(
      'rollback_completed',
      success=result.success,
      rolled_back=result.rolled_back_count,
      planned=plan.migration_count,
      duration_seconds=duration,
      errors=result.errors,
    )
    
    return result
```

### 8. Keep Snapshot History Limited

Clean old snapshots periodically:

```python
async def cleanup_old_snapshots(keep_count=10):
  """Keep only the most recent snapshots."""
  async with get_client(config) as client:
    snapshots = await list_snapshots(client)
    
    if len(snapshots) > keep_count:
      to_delete = snapshots[:-keep_count]  # All except last N
      
      for snapshot in to_delete:
        await client.execute(
          'DELETE _schema_snapshot WHERE version = $version',
          {'version': snapshot.version},
        )
        print(f'Deleted snapshot: {snapshot.version}')
```

## Troubleshooting

### Snapshot Creation Fails

**Error**: `Failed to create snapshot: INFO FOR DB failed`

**Solutions**:

1. Check database connection:
   ```shell
   surql db ping
   ```

2. Verify permissions:
   ```python
   # Ensure user has INFO privileges
   async with get_client(config) as client:
     result = await client.execute('INFO FOR DB')
     print(result)
   ```

3. Check for schema corruption

### Rollback Plan Shows No Migrations

**Error**: Rollback plan has 0 migrations

**Causes**:

- Target version is current orнова
- Target version is newer than current
- Incorrect version identifier

**Solution**:

```python
async def debug_rollback_plan():
  from surql.migration.history import get_applied_migrations
  
  async with get_client(config) as client:
    applied = await get_applied_migrations(client)
    
    if applied:
      current = applied[-1].version
      print(f'Current version: {current}')
      print(f'Applied migrations: {[m.version for m in applied]}')
    else:
      print('No migrations applied')
```

### Rollback Fails Midway

**Error**: Rollback fails after rolling back some migrations

**Recovery**:

```python
async def recover_from_failed_rollback():
  async with get_client(config) as client:
    # Check current state
    history = await get_applied_migrations(client)
    
    if history:
      current = history[-1].version
      print(f'Rollback stopped at: {current}')
      
      # Option 1: Retry rollback from current position
      plan = await create_rollback_plan(client, migrations, target_version)
      result = await execute_rollback(client, plan)
      
      # Option 2: Roll forward to consistent state
      # await apply_migrations(client, pending_migrations)
```

### Checksum Mismatch

**Problem**: Snapshot checksum doesn't match expected value

**Causes**:

- Manual schema changes outside migrations
- Migration applied without recording
- Database corruption

**Solution**:

```python
async def verify_schema_integrity():
  async with get_client(config) as client:
    # Create new snapshot
    current_snapshot = await create_snapshot(client, 'current', 0)
    
    # Load expected snapshot
    expected_snapshot = await load_snapshot(client, expected_version)
    
    if current_snapshot.checksum == expected_snapshot.checksum:
      print('✓ Schema matches expected state')
    else:
      print('✗ Schema diverged from expected state')
      
      # Compare to find differences
      diff = compare_snapshots(expected_snapshot, current_snapshot)
      print('Differences:', diff)
```

## Examples

### Example 1: Complete Rollback Workflow

```python
from pathlib import Path
from surql.migration.discovery import discover_migrations
from surql.migration.rollback import create_rollback_plan, execute_rollback
from surql.migration.versioning import create_snapshot, store_snapshot

async def complete_rollback_workflow():
  """Complete workflow: snapshot, plan, review, execute."""
  
  async with get_client(config) as client:
    # Step 1: Create pre-rollback snapshot
    print('Step 1: Creating snapshot before rollback')
    snapshot = await create_snapshot(client, 'pre_rollback', 5)
    await store_snapshot(client, snapshot)
    print(f'  Snapshot created: {snapshot.checksum[:16]}...')
    
    # Step 2: Create rollback plan
    print('\nStep 2: Creating rollback plan')
    migrations = discover_migrations(Path('migrations'))
    plan = await create_rollback_plan(client, migrations, '20260108_120000')
    
    print(f'  From: {plan.from_version}')
    print(f'  To: {plan.to_version}')
    print(f'  Migrations: {plan.migration_count}')
    print(f'  Safety: {plan.overall_safety.value}')
    
    # Step 3: Review issues
    print('\nStep 3: Reviewing safety issues')
    if plan.issues:
      for issue in plan.issues:
        print(f'  [{issue.safety.value}] {issue.description}')
    else:
      print('  No issues found')
    
    # Step 4: Execute if safe
    print('\nStep 4: Executing rollback')
    if plan.is_safe:
      result = await execute_rollback(client, plan)
      
      if result.success:
        print(f'  ✓ Rolled back {result.rolled_back_count} migrations')
        print(f'  Duration: {result.actual_duration_ms}ms')
      else:
        print(f'  ✗ Rollback failed: {result.errors}')
    else:
      print(f'  ⚠️  Rollback is {plan.overall_safety.value} - requires review')
```

### Example 2: Automated Testing with Rollback

```python
async def test_migration_reversibility():
  """Test that migrations can be safely rolled back."""
  
  migrations = discover_migrations(Path('migrations'))
  
  async with get_client(config) as client:
    for migration in migrations:
      print(f'Testing {migration.version}...')
      
      # Apply migration
      await execute_migration(client, migration, MigrationDirection.UP)
      
      # Create snapshot
      snapshot_after = await create_snapshot(client, f'after_{migration.version}', 1)
      
      # Rollback
      await execute_migration(client, migration, MigrationDirection.DOWN)
      
      # Verify state
      snapshot_rolled_back = await create_snapshot(client, f'rolled_back_{migration.version}', 0)
      
      # Compare (should match initial state)
      # Note: This is simplified - real test would compare with before snapshot
      print(f'  ✓ {migration.version} is reversible')
```

### Example 3: Automated Snapshot Management

```python
from datetime import datetime, timedelta

async def manage_snapshots():
  """Manage snapshot lifecycle - create, clean old, export."""
  
  async with get_client(config) as client:
    # Create current snapshot
    current = await create_snapshot(client, datetime.now().strftime('%Y%m%d_%H%M%S'), 0)
    await store_snapshot(client, current)
    print(f'Created snapshot: {current.version}')
    
    # List all snapshots
    all_snapshots = await list_snapshots(client)
    print(f'Total snapshots: {len(all_snapshots)}')
    
    # Clean old snapshots (keep last 30 days)
    cutoff = datetime.now() - timedelta(days=30)
    old_snapshots = [s for s in all_snapshots if s.created_at < cutoff]
    
    for snapshot in old_snapshots:
      await client.execute(
        'DELETE _schema_snapshot WHERE version = $version',
        {'version': snapshot.version},
      )
      print(f'Deleted old snapshot: {snapshot.version}')
    
    print(f'Cleaned {len(old_snapshots)} old snapshot(s)')
```

## Additional Resources

- [Migration System Guide](migrations.md) - Migration creation and management
- [CLI Reference](cli.md) - Command-line interface documentation
- [Versioning Example Code](examples/versioning_rollback_example.py) - More code examples

## Summary

Schema versioning and rollback provide:

- **Schema snapshots** - Capture schema state at any point
- **Version graph** - Track migration relationships
- **Rollback planning** - Safety analysis before rollback
- **Safety levels** - SAFE, DATA_LOSS, UNSAFE classifications
- **Safe execution** - Controlled rollback with safety checks
- **Auto-snapshots** - Automatic snapshot creation

Use versioning and rollback to safely manage schema evolution and recover from problematic migrations.
