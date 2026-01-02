# Migration System Guide

This guide covers creating, managing, and executing database migrations with reverie's migration system.

## Table of Contents

- [Overview](#overview)
- [Migration Structure](#migration-structure)
- [Creating Migrations](#creating-migrations)
- [Migration Naming](#migration-naming)
- [Running Migrations](#running-migrations)
- [Migration History](#migration-history)
- [Rollback Strategies](#rollback-strategies)
- [Best Practices](#best-practices)
- [Advanced Topics](#advanced-topics)

## Overview

reverie's migration system provides:

- **Version control** - Track database schema changes over time
- **Reversibility** - Roll back changes using down migrations
- **Automation** - Generate migrations from schema definitions
- **Safety** - Validate migrations before execution
- **History tracking** - Store migration history in the database

### Migration Lifecycle

```shell
1. Create migration file → 2. Write SQL statements → 3. Validate migration
                          ↓
4. Apply to database ← 5. Track in history ← 6. Execute statements
```

## Migration Structure

### File Structure

Migration files are Python modules with a specific structure:

```python
# migrations/20260102_120000_create_user_table.py

def up() -> list[str]:
  """Apply forward migration."""
  return [
    'DEFINE TABLE user SCHEMAFULL;',
    'DEFINE FIELD name ON TABLE user TYPE string;',
    'DEFINE FIELD email ON TABLE user TYPE string;',
  ]

def down() -> list[str]:
  """Rollback migration."""
  return [
    'REMOVE TABLE user;',
  ]

metadata = {
  'version': '20260102_120000',
  'description': 'Create user table',
  'author': 'reverie',
  'depends_on': [],
}
```

### Required Components

1. **up() function** - Returns list of SQL statements to apply
2. **down() function** - Returns list of SQL statements to rollback
3. **metadata dict** - Contains migration information

### Metadata Fields

```python
metadata = {
  'version': '20260102_120000',        # Unique version (timestamp)
  'description': 'Create user table',   # Human-readable description
  'author': 'reverie',                 # Migration author
  'depends_on': [],                     # List of required migrations
}
```

## Creating Migrations

### Manual Creation

Create a blank migration file:

```shell
reverie migrate create "Add user table"
```

This generates:

```shell
migrations/20260102_120000_add_user_table.py
```

Edit the file to add your SQL statements.

### Common Migration Patterns

#### Create Table

```python
def up() -> list[str]:
  return [
    'DEFINE TABLE user SCHEMAFULL;',
    'DEFINE FIELD username ON TABLE user TYPE string;',
    'DEFINE FIELD email ON TABLE user TYPE string;',
    'DEFINE FIELD created_at ON TABLE user TYPE datetime DEFAULT time::now();',
  ]

def down() -> list[str]:
  return [
    'REMOVE TABLE user;',
  ]
```

#### Add Fields

```python
def up() -> list[str]:
  return [
    'DEFINE FIELD bio ON TABLE user TYPE string DEFAULT "";',
    'DEFINE FIELD avatar_url ON TABLE user TYPE string;',
  ]

def down() -> list[str]:
  return [
    'REMOVE FIELD avatar_url ON TABLE user;',
    'REMOVE FIELD bio ON TABLE user;',
  ]
```

#### Create Index

```python
def up() -> list[str]:
  return [
    'DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;',
    'DEFINE INDEX username_idx ON TABLE user COLUMNS username UNIQUE;',
  ]

def down() -> list[str]:
  return [
    'REMOVE INDEX username_idx ON TABLE user;',
    'REMOVE INDEX email_idx ON TABLE user;',
  ]
```

#### Create Edge Table

```python
def up() -> list[str]:
  return [
    'DEFINE TABLE follows SCHEMAFULL;',
    'DEFINE FIELD in ON TABLE follows TYPE record<user>;',
    'DEFINE FIELD out ON TABLE follows TYPE record<user>;',
    'DEFINE FIELD followed_at ON TABLE follows TYPE datetime DEFAULT time::now();',
  ]

def down() -> list[str]:
  return [
    'REMOVE TABLE follows;',
  ]
```

#### Modify Fields

```python
def up() -> list[str]:
  return [
    # Remove old field
    'REMOVE FIELD name ON TABLE user;',
    # Add new fields
    'DEFINE FIELD name.first ON TABLE user TYPE string;',
    'DEFINE FIELD name.last ON TABLE user TYPE string;',
  ]

def down() -> list[str]:
  return [
    'REMOVE FIELD name.last ON TABLE user;',
    'REMOVE FIELD name.first ON TABLE user;',
    'DEFINE FIELD name ON TABLE user TYPE string;',
  ]
```

#### Add Event/Trigger

```python
def up() -> list[str]:
  return [
    '''
    DEFINE EVENT email_changed ON TABLE user WHEN $before.email != $after.email THEN (
      CREATE audit_log SET
        table = 'user',
        record = $value.id,
        field = 'email',
        old_value = $before.email,
        new_value = $after.email,
        changed_at = time::now()
    )
    ''',
  ]

def down() -> list[str]:
  return [
    'REMOVE EVENT email_changed ON TABLE user;',
  ]
```

#### Add Permissions

```python
def up() -> list[str]:
  return [
    '''
    DEFINE FIELD email ON TABLE user
      PERMISSIONS
        FOR select WHERE $auth.id = $parent.id OR $auth.admin = true
        FOR update WHERE $auth.id = $parent.id
    ''',
  ]

def down() -> list[str]:
  return [
    'REMOVE FIELD email ON TABLE user;',
    'DEFINE FIELD email ON TABLE user TYPE string;',
  ]
```

## Migration Naming

### Naming Convention

Files must follow this format:

```shell
YYYYMMDD_HHMMSS_description.py
```

Examples:

- `20260102_120000_create_user_table.py`
- `20260102_120530_add_user_indexes.py`
- `20260103_093000_create_post_table.py`

### Best Practices for Names

```shell
# Good - Clear and descriptive
reverie migrate create "Create user and post tables"
reverie migrate create "Add email verification fields"
reverie migrate create "Create follows edge table"

# Avoid - Vague or generic
reverie migrate create "Update database"
reverie migrate create "Changes"
reverie migrate create "Fix"
```

## Running Migrations

### Apply All Pending Migrations

```shell
reverie migrate up
```

Output:

```shell
Discovering migrations in migrations
Found 2 pending migration(s):
  • 20260102_120000: Create user table
  • 20260102_130000: Create post table
Successfully applied 2 migration(s)
```

### Apply Specific Number

```shell
# Apply only the next migration
reverie migrate up --steps 1

# Apply next 3 migrations
reverie migrate up --steps 3
```

### Dry Run (Preview)

```shell
# See what will be executed without applying
reverie migrate up --dry-run
```

Output shows SQL that would be executed:

```shell
Found 1 pending migration(s):
  • 20260102_120000: Create user table

Dry run mode - no changes will be made

┌─────────────────────────────────────────┐
│ 20260102_120000: Create user table      │
├─────────────────────────────────────────┤
│ DEFINE TABLE user SCHEMAFULL;           │
│ DEFINE FIELD name ON TABLE user ...     │
└─────────────────────────────────────────┘
```

### Check Migration Status

```shell
reverie migrate status
```

Output:

```shell
Migration Status
┌───────────────────────────┬─────────┐
│ Version                   │ Status  │
├───────────────────────────┼─────────┤
│ 20260102_120000           │ APPLIED │
│ 20260102_130000           │ PENDING │
└───────────────────────────┴─────────┘
Total: 2 | Applied: 1 | Pending: 1
```

### View Migration History

```shell
reverie migrate history
```

Output:

```shell
Migration History
┌───────────────────────────┬─────────────┬────────────────────┐
│ Version                   │ Description │ Applied At         │
├───────────────────────────┼─────────────┼────────────────────┤
│ 20260102_120000           │ Create user │ 2026-01-02 12:00   │
└───────────────────────────┴─────────────┴────────────────────┘
```

## Migration History

### History Storage

reverie stores migration history in a special table:

```sql
CREATE TABLE _migration_history SCHEMAFULL;
DEFINE FIELD version ON TABLE _migration_history TYPE string;
DEFINE FIELD description ON TABLE _migration_history TYPE string;
DEFINE FIELD applied_at ON TABLE _migration_history TYPE datetime;
DEFINE FIELD execution_time_ms ON TABLE _migration_history TYPE int;
DEFINE FIELD checksum ON TABLE _migration_history TYPE string;
DEFINE INDEX version_idx ON TABLE _migration_history COLUMNS version UNIQUE;
```

### Querying History

```python
from src.migration.history import get_applied_migrations
from src.connection.client import get_client
from src.settings import get_db_config

async def view_history():
  config = get_db_config()
  async with get_client(config) as client:
    history = await get_applied_migrations(client)

    for migration in history:
      print(f"{migration.version}: {migration.description}")
      print(f"  Applied: {migration.applied_at}")
      print(f"  Execution time: {migration.execution_time_ms}ms")
```

## Rollback Strategies

### Rollback Last Migration

```shell
reverie migrate down
```

### Rollback Multiple Migrations

```shell
# Rollback last 3 migrations
reverie migrate down --steps 3
```

### Preview Rollback

```shell
reverie migrate down --dry-run
```

### Writing Reversible Migrations

Always ensure `down()` properly reverses `up()`:

```python
def up() -> list[str]:
  return [
    'DEFINE TABLE user SCHEMAFULL;',
    'DEFINE FIELD name ON TABLE user TYPE string;',
    'DEFINE INDEX name_idx ON TABLE user COLUMNS name UNIQUE;',
  ]

def down() -> list[str]:
  # Reverse in opposite order
  return [
    'REMOVE INDEX name_idx ON TABLE user;',  # Remove index first
    'REMOVE FIELD name ON TABLE user;',       # Then field
    'REMOVE TABLE user;',                     # Finally table
  ]
```

### Handling Data Migrations

For migrations that modify data:

```python
def up() -> list[str]:
  return [
    # Add new field with default
    'DEFINE FIELD status ON TABLE user TYPE string DEFAULT "active";',
    # Migrate existing data
    'UPDATE user SET status = "active" WHERE status = NONE;',
  ]

def down() -> list[str]:
  return [
    'REMOVE FIELD status ON TABLE user;',
  ]
```

### Non-Reversible Migrations

Some operations can't be reversed (e.g., dropping data):

```python
def up() -> list[str]:
  return [
    'DELETE user WHERE created_at < time::now() - 1y;',
  ]

def down() -> list[str]:
  # Cannot restore deleted data
  return [
    '-- WARNING: This migration is not reversible',
    '-- Deleted data cannot be restored',
  ]
```

## Best Practices

### 1. One Purpose Per Migration

```shell
# Good - Focused migrations
reverie migrate create "Create user table"
reverie migrate create "Create post table"
reverie migrate create "Add user indexes"

# Avoid - Multiple unrelated changes
reverie migrate create "Create all tables and indexes"
```

### 2. Test Migrations Locally

```shell
# Apply migration
reverie migrate up

# Verify it works
reverie schema show user

# Test rollback
reverie migrate down

# Verify rollback worked
reverie schema show user  # Should not exist
```

### 3. Use Transactions for Safety

Migrations are executed in transactions automatically. If any statement fails, all changes are rolled back.

### 4. Include Descriptive Comments

```python
def up() -> list[str]:
  return [
    # Create user table with authentication fields
    'DEFINE TABLE user SCHEMAFULL;',

    # Username: 3-20 characters, alphanumeric
    'DEFINE FIELD username ON TABLE user TYPE string ASSERT string::len($value) >= 3;',

    # Email: must be valid email format
    'DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);',
  ]
```

### 5. Version Schema Changes

```python
# Add schema version field
def up() -> list[str]:
  return [
    'DEFINE FIELD schema_version ON TABLE user TYPE int DEFAULT 2;',
    'UPDATE user SET schema_version = 2;',
  ]
```

### 6. Preserve Data During Schema Changes

```python
def up() -> list[str]:
  return [
    # Create temporary field
    'DEFINE FIELD email_temp ON TABLE user TYPE string;',

    # Copy data
    'UPDATE user SET email_temp = email;',

    # Remove old field
    'REMOVE FIELD email ON TABLE user;',

    # Create new field with validation
    'DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);',

    # Restore data
    'UPDATE user SET email = email_temp;',

    # Clean up
    'REMOVE FIELD email_temp ON TABLE user;',
  ]
```

### 7. Document Breaking Changes

```python
"""
Migration: Rename 'name' to 'full_name'

BREAKING CHANGE: Applications using field 'name' must be updated
to use 'full_name' before applying this migration.

Update code:
  OLD: user.name
  NEW: user.full_name
"""

def up() -> list[str]:
  return [
    'DEFINE FIELD full_name ON TABLE user TYPE string;',
    'UPDATE user SET full_name = name;',
    'REMOVE FIELD name ON TABLE user;',
  ]
```

### 8. Validate Migration Files

```shell
# Validate before committing
reverie migrate validate
```

### 9. Use Dependencies

```python
metadata = {
  'version': '20260102_130000',
  'description': 'Add user foreign key to posts',
  'depends_on': ['20260102_120000'],  # Requires user table
}
```

### 10. Keep Migration Files in Version Control

```shell
# Add migrations to git
git add migrations/
git commit -m "Add user table migration"
```

## Advanced Topics

### Conditional Migrations

```python
def up() -> list[str]:
  """Migrate only if table doesn't exist."""
  statements = []

  # Check if table exists
  statements.append('''
    IF (SELECT * FROM information_schema.tables WHERE table_name = 'user') = [] THEN
      DEFINE TABLE user SCHEMAFULL;
      DEFINE FIELD name ON TABLE user TYPE string;
    END;
  ''')

  return statements
```

### Data Backups

```python
def up() -> list[str]:
  return [
    # Create backup table
    'DEFINE TABLE user_backup SCHEMAFULL;',

    # Copy data
    'INSERT INTO user_backup (SELECT * FROM user);',

    # Perform migration
    'REMOVE FIELD old_field ON TABLE user;',
    'DEFINE FIELD new_field ON TABLE user TYPE string;',
  ]

def down() -> list[str]:
  return [
    # Restore from backup
    'DELETE user;',
    'INSERT INTO user (SELECT * FROM user_backup);',

    # Remove backup
    'REMOVE TABLE user_backup;',
  ]
```

### Large Data Migrations

For large datasets, use batch processing:

```python
def up() -> list[str]:
  """Migrate data in batches."""
  return [
    # Add new field
    'DEFINE FIELD computed_field ON TABLE user TYPE string;',

    # Process in batches (SurrealDB handles this efficiently)
    '''
    UPDATE user SET
      computed_field = string::concat(name.first, " ", name.last)
    WHERE computed_field IS NONE
    ''',
  ]
```

### Migration Dependencies

```python
# Migration 1: Create user table
metadata = {
  'version': '20260102_120000',
  'description': 'Create user table',
  'depends_on': [],
}

# Migration 2: Create post table (depends on user)
metadata = {
  'version': '20260102_130000',
  'description': 'Create post table',
  'depends_on': ['20260102_120000'],  # Requires user table
}
```

### Custom Migration Scripts

```python
# migrations/20260102_140000_custom_script.py

async def up_async(client):
  """Async migration with custom logic."""
  # Complex data transformation
  users = await client.execute('SELECT * FROM user')

  for user in users:
    # Custom processing
    processed = custom_transform(user)
    await client.update(user['id'], processed)

def up() -> list[str]:
  """Use this for async migrations."""
  return []  # Handled by up_async

def down() -> list[str]:
  return []
```

### Auto-Generation from Schema

```python
# Future feature - not yet implemented
from schemas.user import user_schema
from src.migration.generator import generate_migration

# Generate migration from schema
migration = generate_migration(
  user_schema,
  description='Update user schema',
)
```

## Troubleshooting

### Migration Fails Mid-Execution

Migrations run in transactions. If a statement fails, all changes are rolled back:

```shell
# Check what went wrong
reverie migrate status --verbose

# Fix the migration file
# Then try again
reverie migrate up
```

### Migration Already Applied

```shell
# If you need to re-run a migration:
# 1. Rollback first
reverie migrate down

# 2. Re-apply
reverie migrate up
```

### Duplicate Version Numbers

```shell
# Check for duplicates
reverie migrate validate

# Rename file with new timestamp
mv migrations/20260102_120000_old.py \
   migrations/20260102_120100_old.py
```

### Corrupted Migration History

```python
# Manually check history
from src.connection.client import get_client
from src.settings import get_db_config

async def check_history():
  async with get_client(get_db_config()) as client:
    result = await client.execute('SELECT * FROM _migration_history')
    print(result)
```

## Next Steps

- Learn about [Query Building](queries.md) to work with your migrated schema
- Explore [CLI Reference](cli.md) for all migration commands
- Check out [Examples](examples/migration_example.py) for migration patterns
