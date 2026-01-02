"""Migration workflow example for reverie.

This example demonstrates:
- Creating migration files programmatically
- Applying migrations
- Checking migration status
- Rolling back migrations
"""

import asyncio
from datetime import datetime
from pathlib import Path

from src.connection.client import get_client
from src.connection.config import ConnectionConfig
from src.migration.discovery import discover_migrations
from src.migration.executor import (
  create_migration_plan,
  execute_migration_plan,
  get_migration_status,
)
from src.migration.history import ensure_migration_table, get_applied_migrations
from src.migration.models import MigrationDirection

# Database configuration
config = ConnectionConfig(
  url='ws://localhost:8000/rpc',
  namespace='examples',
  database='migrations',
  username='root',
  password='root',
)


def create_user_migration(migrations_dir: Path) -> Path:
  """Create a migration for user table."""
  version = datetime.now().strftime('%Y%m%d_%H%M%S')
  description = 'create_user_table'

  file_path = migrations_dir / f'{version}_{description}.py'

  content = f'''"""Create user table."""

def up() -> list[str]:
  """Apply migration."""
  return [
    'DEFINE TABLE user SCHEMAFULL;',
    'DEFINE FIELD username ON TABLE user TYPE string ASSERT string::len($value) >= 3;',
    'DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);',
    'DEFINE FIELD age ON TABLE user TYPE int ASSERT $value >= 0 AND $value <= 150;',
    'DEFINE FIELD created_at ON TABLE user TYPE datetime DEFAULT time::now() READONLY;',
    'DEFINE INDEX username_idx ON TABLE user COLUMNS username UNIQUE;',
    'DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;',
  ]

def down() -> list[str]:
  """Rollback migration."""
  return [
    'REMOVE INDEX email_idx ON TABLE user;',
    'REMOVE INDEX username_idx ON TABLE user;',
    'REMOVE TABLE user;',
  ]

metadata = {{
  'version': '{version}',
  'description': 'Create user table',
  'author': 'reverie',
  'depends_on': [],
}}
'''

  file_path.write_text(content)
  return file_path


def create_post_migration(migrations_dir: Path) -> Path:
  """Create a migration for post table."""
  version = datetime.now().strftime('%Y%m%d_%H%M%S')
  description = 'create_post_table'

  file_path = migrations_dir / f'{version}_{description}.py'

  content = f'''"""Create post table."""

def up() -> list[str]:
  """Apply migration."""
  return [
    'DEFINE TABLE post SCHEMAFULL;',
    'DEFINE FIELD title ON TABLE post TYPE string ASSERT string::len($value) > 0;',
    'DEFINE FIELD content ON TABLE post TYPE string;',
    'DEFINE FIELD author ON TABLE post TYPE record<user>;',
    'DEFINE FIELD published ON TABLE post TYPE bool DEFAULT false;',
    'DEFINE FIELD created_at ON TABLE post TYPE datetime DEFAULT time::now() READONLY;',
  ]
def down() -> list[str]:
  """Rollback migration."""
  return [
    'REMOVE TABLE post;',
  ]

metadata = {{
  'version': '{version}',
  'description': 'Create post table',
  'author': 'reverie',
  'depends_on': [],
}}
'''

  file_path.write_text(content)
  return file_path


async def main():
  """Main example function."""

  # Setup migrations directory
  migrations_dir = Path('example_migrations')
  migrations_dir.mkdir(exist_ok=True)

  print('=== Creating Migration Files ===')
  user_migration = create_user_migration(migrations_dir)
  print(f'Created: {user_migration.name}')

  # Wait a moment to ensure different timestamps
  await asyncio.sleep(1)

  post_migration = create_post_migration(migrations_dir)
  print(f'Created: {post_migration.name}\n')

  async with get_client(config) as client:
    # Ensure migration history table exists
    print('=== Setting Up Migration Tracking ===')
    await ensure_migration_table(client)
    print('Migration history table ready\n')

    # Discover migrations
    print('=== Discovering Migrations ===')
    migrations = discover_migrations(migrations_dir)
    print(f'Found {len(migrations)} migration(s):')
    for mig in migrations:
      print(f'  - {mig.version}: {mig.description}')
    print()

    # Check status
    print('=== Migration Status ===')
    statuses = await get_migration_status(client, migrations)
    for status in statuses:
      print(f'  {status.migration.version}: {status.state.value}')
    print()

    # Create and execute migration plan
    print('=== Applying Migrations ===')
    plan = await create_migration_plan(
      client,
      migrations,
      MigrationDirection.UP,
      steps=None,  # Apply all
    )

    if not plan.is_empty():
      print(f'Applying {plan.count} migration(s):')
      for mig in plan.migrations:
        print(f'  • {mig.version}: {mig.description}')

      await execute_migration_plan(client, plan)
      print('✓ Migrations applied successfully\n')
    else:
      print('No pending migrations\n')

    # Check history
    print('=== Migration History ===')
    history = await get_applied_migrations(client)
    for record in history:
      print(f'  {record.version}: {record.description}')
      print(f'    Applied at: {record.applied_at}')
      if record.execution_time_ms:
        print(f'    Execution time: {record.execution_time_ms}ms')
    print()

    # Test rollback
    print('=== Rolling Back Last Migration ===')
    rollback_plan = await create_migration_plan(
      client,
      migrations,
      MigrationDirection.DOWN,
      steps=1,  # Rollback last one
    )

    if not rollback_plan.is_empty():
      print(f'Rolling back {rollback_plan.count} migration(s):')
      for mig in reversed(rollback_plan.migrations):
        print(f'  • {mig.version}: {mig.description}')

      await execute_migration_plan(client, rollback_plan)
      print('✓ Rollback successful\n')

    # Check final status
    print('=== Final Migration Status ===')
    final_statuses = await get_migration_status(client, migrations)
    for status in final_statuses:
      print(f'  {status.migration.version}: {status.state.value}')
    print()

    # Re-apply for complete state
    print('=== Re-applying All Migrations ===')
    final_plan = await create_migration_plan(
      client,
      migrations,
      MigrationDirection.UP,
      steps=None,
    )

    if not final_plan.is_empty():
      await execute_migration_plan(client, final_plan)
      print('✓ All migrations applied\n')
    else:
      print('All migrations already applied\n')

  # Cleanup
  print('=== Cleanup ===')
  for file in migrations_dir.glob('*.py'):
    file.unlink()
  migrations_dir.rmdir()
  print('Removed example migration files')


if __name__ == '__main__':
  print('reverie Migration Workflow Example')
  print('=' * 60)
  print()

  try:
    asyncio.run(main())
    print('\nExample completed successfully!')

  except Exception as e:
    print(f'\nError: {e}')
    import traceback

    traceback.print_exc()
