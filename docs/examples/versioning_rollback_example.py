"""Example: Schema Versioning and Rollback.

This example demonstrates the schema versioning and rollback features,
including creating snapshots, planning rollbacks, and executing safe rollbacks.
"""

import asyncio
from pathlib import Path

from surql.connection.client import get_client
from surql.connection.config import ConnectionConfig
from surql.migration.discovery import discover_migrations
from surql.migration.history import enable_auto_snapshots, get_applied_migrations
from surql.migration.rollback import (
  RollbackSafety,
  create_rollback_plan,
  execute_rollback,
)
from surql.migration.versioning import (
  VersionGraph,
  compare_snapshots,
  create_snapshot,
  list_snapshots,
  load_snapshot,
  store_snapshot,
)


async def example_create_snapshots() -> None:
  """Example: Create and store schema snapshots."""
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='test',
    db='example',
  )

  async with get_client(config) as client:
    # Get current migration state
    history = await get_applied_migrations(client)

    if not history:
      print('No migrations applied yet')
      return

    current_version = history[-1].version
    migration_count = len(history)

    # Create a snapshot at current version
    print(f'Creating snapshot for version {current_version}')
    snapshot = await create_snapshot(client, current_version, migration_count)

    print('Snapshot created:')
    print(f'  Version: {snapshot.version}')
    print(f'  Tables: {len(snapshot.tables)}')
    print(f'  Edges: {len(snapshot.edges)}')
    print(f'  Checksum: {snapshot.checksum[:16]}...')
    print(f'  Migration count: {snapshot.migration_count}')

    # Store the snapshot
    await store_snapshot(client, snapshot)
    print('Snapshot stored successfully')


async def example_list_snapshots() -> None:
  """Example: List all stored snapshots."""
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='test',
    db='example',
  )

  async with get_client(config) as client:
    snapshots = await list_snapshots(client)

    if not snapshots:
      print('No snapshots found')
      return

    print(f'Found {len(snapshots)} snapshot(s):')
    for snapshot in snapshots:
      print(f'\nVersion: {snapshot.version}')
      print(f'  Created: {snapshot.created_at}')
      print(f'  Migrations: {snapshot.migration_count}')
      print(f'  Checksum: {snapshot.checksum[:16]}...')


async def example_compare_snapshots() -> None:
  """Example: Compare two schema snapshots."""
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='test',
    db='example',
  )

  async with get_client(config) as client:
    # Load two snapshots
    snapshot1 = await load_snapshot(client, '20260108_120000')
    snapshot2 = await load_snapshot(client, '20260109_120000')

    if not snapshot1 or not snapshot2:
      print('One or both snapshots not found')
      return

    # Compare snapshots
    diff = compare_snapshots(snapshot1, snapshot2)

    print(f'Comparing {snapshot1.version} → {snapshot2.version}')
    print()

    if diff['checksum_match']:
      print('Schemas are identical')
      return

    # Display differences
    if diff['tables_added']:
      print('Tables added:')
      for table in diff['tables_added']:
        print(f'  + {table}')

    if diff['tables_removed']:
      print('Tables removed:')
      for table in diff['tables_removed']:
        print(f'  - {table}')

    if diff['tables_modified']:
      print('Tables modified:')
      for table in diff['tables_modified']:
        print(f'  ~ {table}')

    if diff['edges_added']:
      print('Edges added:')
      for edge in diff['edges_added']:
        print(f'  + {edge}')


async def example_version_graph() -> None:
  """Example: Build and query version graph."""
  migrations_dir = Path('migrations')
  migrations = discover_migrations(migrations_dir)

  # Build version graph
  graph = VersionGraph()

  for i, migration in enumerate(migrations):
    parent = migrations[i - 1].version if i > 0 else None
    graph.add_version(migration, parent=parent)

  print(f'Version graph contains {len(graph.get_all_versions())} versions')

  # Query graph
  if len(migrations) >= 2:
    first_version = migrations[0].version
    last_version = migrations[-1].version

    # Get path between versions
    path = graph.get_path(first_version, last_version)
    if path:
      print(f'\nPath from {first_version} to {last_version}:')
      for version in path:
        print(f'  → {version}')

    # Get ancestors
    ancestors = graph.get_ancestors(last_version)
    print(f'\nAncestors of {last_version}:')
    for ancestor in ancestors:
      print(f'  ← {ancestor}')

    # Get descendants
    descendants = graph.get_descendants(first_version)
    print(f'\nDescendants of {first_version}:')
    for descendant in descendants:
      print(f'  → {descendant}')


async def example_plan_rollback() -> None:
  """Example: Plan a rollback to a previous version."""
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='test',
    db='example',
  )

  migrations_dir = Path('migrations')
  migrations = discover_migrations(migrations_dir)

  async with get_client(config) as client:
    # Create rollback plan
    target_version = '20260108_120000'  # Version to rollback to

    print(f'Planning rollback to {target_version}')

    plan = await create_rollback_plan(client, migrations, target_version)

    print('\nRollback Plan:')
    print(f'  From: {plan.from_version}')
    print(f'  To: {plan.to_version}')
    print(f'  Migrations to rollback: {plan.migration_count}')
    print(f'  Overall safety: {plan.overall_safety.value.upper()}')

    # Display migrations
    print('\nMigrations to rollback:')
    for migration in plan.migrations:
      print(f'  - {migration.version}: {migration.description}')

    # Display safety issues
    if plan.issues:
      print(f'\nSafety Issues ({len(plan.issues)}):')
      for issue in plan.issues:
        print(f'  [{issue.safety.value.upper()}] {issue.migration}')
        print(f'    {issue.description}')
        if issue.recommendation:
          print(f'    Recommendation: {issue.recommendation}')

    # Check if approval required
    if plan.requires_approval:
      print('\n⚠️  This rollback requires approval due to safety concerns')


async def example_execute_safe_rollback() -> None:
  """Example: Execute a safe rollback."""
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='test',
    db='example',
  )

  migrations_dir = Path('migrations')
  migrations = discover_migrations(migrations_dir)

  async with get_client(config) as client:
    target_version = '20260108_120000'

    # Create plan
    plan = await create_rollback_plan(client, migrations, target_version)

    # Check safety
    if plan.overall_safety == RollbackSafety.SAFE:
      print('Executing safe rollback...')

      # Execute rollback
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
      print('Use force=True to execute anyway (not recommended)')


async def example_execute_unsafe_rollback_with_force() -> None:
  """Example: Execute an unsafe rollback with force flag."""
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='test',
    db='example',
  )

  migrations_dir = Path('migrations')
  migrations = discover_migrations(migrations_dir)

  async with get_client(config) as client:
    target_version = '20260107_120000'

    # Create plan
    plan = await create_rollback_plan(client, migrations, target_version)

    # Display warnings
    print(f'Safety: {plan.overall_safety.value.upper()}')

    if plan.has_data_loss:
      print('\n⚠️  WARNING: This rollback may cause data loss!')
      print('Issues:')
      for issue in plan.issues:
        print(f'  - {issue.description}')

      # In production, you would prompt for confirmation here
      confirm = True  # Simulated user confirmation

      if confirm:
        print('\nProceeding with forced rollback...')
        result = await execute_rollback(client, plan, force=True)

        if result.success:
          print('✓ Rollback completed')
        else:
          print(f'✗ Rollback failed: {result.errors}')
      else:
        print('Rollback cancelled')


async def example_auto_snapshots() -> None:
  """Example: Enable automatic snapshots after migrations."""
  # Enable automatic snapshots
  enable_auto_snapshots()
  print('Automatic snapshots enabled')

  # Future migrations will automatically create snapshots
  print('All future migrations will create snapshots automatically')

  # You can disable it later
  from surql.migration.history import disable_auto_snapshots

  disable_auto_snapshots()
  print('Automatic snapshots disabled')


async def example_complete_workflow() -> None:
  """Example: Complete versioning and rollback workflow."""
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='test',
    db='example',
  )

  migrations_dir = Path('migrations')
  migrations = discover_migrations(migrations_dir)

  async with get_client(config) as client:
    print('=== Complete Versioning & Rollback Workflow ===\n')

    # Step 1: Get current version
    history = await get_applied_migrations(client)
    if history:
      current = history[-1]
      print(f'Step 1: Current version is {current.version}')
    else:
      print('Step 1: No migrations applied yet')
      return

    # Step 2: Create snapshot
    print(f'\nStep 2: Creating snapshot for {current.version}')
    snapshot = await create_snapshot(client, current.version, len(history))
    await store_snapshot(client, snapshot)
    print(f'  Snapshot created with checksum {snapshot.checksum[:16]}...')

    # Step 3: Build version graph
    print('\nStep 3: Building version graph')
    graph = VersionGraph()
    for i, migration in enumerate(migrations):
      parent = migrations[i - 1].version if i > 0 else None
      graph.add_version(migration, parent=parent)
    print(f'  Graph contains {len(graph.get_all_versions())} versions')

    # Step 4: Plan rollback (if enough migrations)
    if len(history) >= 2:
      target = history[-2].version
      print(f'\nStep 4: Planning rollback to {target}')
      plan = await create_rollback_plan(client, migrations, target)
      print(f'  Will rollback {plan.migration_count} migration(s)')
      print(f'  Safety level: {plan.overall_safety.value}')

      # Step 5: Execute if safe
      if plan.overall_safety == RollbackSafety.SAFE:
        print('\nStep 5: Executing safe rollback')
        result = await execute_rollback(client, plan)
        if result.success:
          print(f'  ✓ Rolled back successfully in {result.actual_duration_ms}ms')
        else:
          print('  ✗ Rollback failed')
      else:
        print('\nStep 5: Skipping rollback - not safe')
        print('  Use force=True to execute anyway')
    else:
      print('\nStep 4: Not enough migrations to demonstrate rollback')


if __name__ == '__main__':
  # Run examples
  print('Example 1: Create Snapshots')
  asyncio.run(example_create_snapshots())

  print('\n' + '=' * 60 + '\n')
  print('Example 2: List Snapshots')
  asyncio.run(example_list_snapshots())

  print('\n' + '=' * 60 + '\n')
  print('Example 3: Compare Snapshots')
  asyncio.run(example_compare_snapshots())

  print('\n' + '=' * 60 + '\n')
  print('Example 4: Version Graph')
  asyncio.run(example_version_graph())

  print('\n' + '=' * 60 + '\n')
  print('Example 5: Plan Rollback')
  asyncio.run(example_plan_rollback())

  print('\n' + '=' * 60 + '\n')
  print('Example 6: Execute Safe Rollback')
  asyncio.run(example_execute_safe_rollback())

  print('\n' + '=' * 60 + '\n')
  print('Example 7: Complete Workflow')
  asyncio.run(example_complete_workflow())
