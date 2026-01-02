# CLI Reference

Complete reference for the reverie command-line interface.

## Table of Contents

- [Overview](#overview)
- [Global Options](#global-options)
- [Migration Commands](#migration-commands)
- [Schema Commands](#schema-commands)
- [Database Commands](#database-commands)
- [Common Workflows](#common-workflows)
- [Configuration](#configuration)
- [Environment Variables](#environment-variables)

## Overview

The reverie CLI provides commands for managing database schemas, migrations, and inspecting your database.

```shell
reverie [OPTIONS] COMMAND [ARGS]
```

### Getting Help

```shell
# Show all commands
reverie --help

# Show help for specific command
reverie migrate --help

# Show help for subcommand
reverie migrate up --help
```

### Version Information

```shell
reverie version
```

Output:

```shell
reverie version 0.1.0
Environment: development
```

## Global Options

These options work with any command:

### `--verbose, -v`

Enable verbose logging for detailed output.

```shell
reverie --verbose migrate up
reverie -v schema show
```

### `--help`

Show help message and exit.

```shell
reverie --help
reverie migrate --help
```

## Migration Commands

All migration commands are under the `migrate` subcommand:

```shell
reverie migrate [COMMAND] [OPTIONS]
```

### `migrate up`

Apply pending migrations to the database.

```shell
reverie migrate up [OPTIONS]
```

**Options:**

- `--directory PATH` - Migration directory (default: `migrations/`)
- `--steps N` - Number of migrations to apply (default: all)
- `--dry-run` - Preview changes without applying
- `--verbose, -v` - Enable verbose output

**Examples:**

```shell
# Apply all pending migrations
reverie migrate up

# Apply only the next migration
reverie migrate up --steps 1

# Apply next 3 migrations
reverie migrate up --steps 3

# Preview what will be applied
reverie migrate up --dry-run

# Use custom migration directory
reverie migrate up --directory ./db/migrations

# Verbose output
reverie migrate up --verbose
```

**Output:**

```shell
Discovering migrations in migrations
Found 2 pending migration(s):
  • 20260102_120000: Create user table
  • 20260102_130000: Create post table
Successfully applied 2 migration(s)
```

### `migrate down`

Rollback the last applied migration(s).

```shell
reverie migrate down [OPTIONS]
```

**Options:**

- `--directory PATH` - Migration directory (default: `migrations/`)
- `--steps N` - Number of migrations to rollback (default: 1)
- `--dry-run` - Preview changes without applying
- `--verbose, -v` - Enable verbose output

**Examples:**

```shell
# Rollback last migration
reverie migrate down

# Rollback last 3 migrations
reverie migrate down --steps 3

# Preview rollback
reverie migrate down --dry-run

# Verbose output
reverie migrate down --verbose
```

**Output:**

```shell
Will rollback 1 migration(s):
  • 20260102_130000: Create post table
Rollback migrations? [y/N]: y
Successfully rolled back 1 migration(s)
```

### `migrate status`

Show migration status (applied vs pending).

```shell
reverie migrate status [OPTIONS]
```

**Options:**

- `--directory PATH` - Migration directory (default: `migrations/`)
- `--format FORMAT` - Output format: `table`, `json`, `yaml` (default: `table`)
- `--verbose, -v` - Enable verbose output

**Examples:**

```shell
# Show status as table
reverie migrate status

# Show status as JSON
reverie migrate status --format json

# Use custom directory
reverie migrate status --directory ./db/migrations
```

**Output (table):**

```shell
Migration Status
┌────────────────────────────────────────┬─────────┐
│ Version                                │ Status  │
├────────────────────────────────────────┼─────────┤
│ 20260102_120000                        │ APPLIED │
│ 20260102_130000                        │ APPLIED │
│ 20260102_140000                        │ PENDING │
└────────────────────────────────────────┴─────────┘
Total: 3 | Applied: 2 | Pending: 1
```

**Output (json):**

```json
[
  {
    "version": "20260102_120000",
    "description": "Create user table",
    "status": "APPLIED",
    "path": "20260102_120000_create_user_table.py"
  },
  {
    "version": "20260102_130000",
    "description": "Create post table",
    "status": "APPLIED",
    "path": "20260102_130000_create_post_table.py"
  },
  {
    "version": "20260102_140000",
    "description": "Add indexes",
    "status": "PENDING",
    "path": "20260102_140000_add_indexes.py"
  }
]
```

### `migrate history`

Show applied migrations from database history.

```shell
reverie migrate history [OPTIONS]
```

**Options:**

- `--format FORMAT` - Output format: `table`, `json`, `yaml` (default: `table`)
- `--verbose, -v` - Enable verbose output

**Examples:**

```shell
# Show history as table
reverie migrate history

# Show history as JSON
reverie migrate history --format json
```

**Output:**

```shell
Migration History
┌─────────────────┬───────────────────┬─────────────────────┬──────────────────┐
│ Version         │ Description       │ Applied At          │ Execution Time   │
├─────────────────┼───────────────────┼─────────────────────┼──────────────────┤
│ 20260102_120000 │ Create user table │ 2026-01-02 12:00:00 │ 45ms             │
│ 20260102_130000 │ Create post table │ 2026-01-02 13:00:00 │ 32ms             │
└─────────────────┴───────────────────┴─────────────────────┴──────────────────┘
```

### `migrate create`

Create a new blank migration file.

```shell
reverie migrate create DESCRIPTION [OPTIONS]
```

**Arguments:**

- `DESCRIPTION` - Migration description (required)

**Options:**

- `--directory PATH` - Migration directory (default: `migrations/`)
- `--verbose, -v` - Enable verbose output

**Examples:**

```shell
# Create migration
reverie migrate create "Add user indexes"

# Create in custom directory
reverie migrate create "Add posts" --directory ./db/migrations
```

**Output:**

```shell
Created migration: 20260102_143000_add_user_indexes.py
Edit the file to add your migration SQL
Path: migrations/20260102_143000_add_user_indexes.py
```

### `migrate validate`

Validate migration files for errors.

```shell
reverie migrate validate [OPTIONS]
```

**Options:**

- `--directory PATH` - Migration directory (default: `migrations/`)
- `--verbose, -v` - Enable verbose output

**Examples:**

```shell
# Validate migrations
reverie migrate validate

# Validate with verbose output
reverie migrate validate --verbose
```

**Output:**

```shell
Validating migrations in migrations
All 3 migration(s) are valid
  ✓ 20260102_120000: Create user table
  ✓ 20260102_130000: Create post table
  ✓ 20260102_140000: Add indexes
```

### `migrate generate`

Generate migration from schema changes (auto-generation).

```shell
reverie migrate generate DESCRIPTION [OPTIONS]
```

**Note:** Full auto-generation is not yet implemented. Currently creates a blank migration.

**Arguments:**

- `DESCRIPTION` - Migration description (required)

**Options:**

- `--directory PATH` - Migration directory (default: `migrations/`)
- `--verbose, -v` - Enable verbose output

**Examples:**

```shell
reverie migrate generate "Update user schema"
```

## Schema Commands

Commands for inspecting database schema:

```shell
reverie schema [COMMAND] [OPTIONS]
```

### `schema show`

Display database or table schema.

```shell
reverie schema show [TABLE] [OPTIONS]
```

**Arguments:**

- `TABLE` - Optional table name to show specific table schema

**Examples:**

```shell
# Show entire database schema
reverie schema show

# Show specific table schema
reverie schema show user

# Show post table schema
reverie schema show post
```

**Output (database):**

```shell
Database: blog
Namespace: blog

Tables:
  - user
  - post
  - likes
  - follows
  - _migration_history
```

**Output (table):**

```shell
Table: user
Mode: SCHEMAFULL

Fields:
  - username: string
  - email: string
  - age: int
  - created_at: datetime
  - updated_at: datetime

Indexes:
  - username_idx: UNIQUE (username)
  - email_idx: UNIQUE (email)
```

## Database Commands

Commands for database management:

```shell
reverie db [COMMAND] [OPTIONS]
```

### `db ping`

Check database connection.

```shell
reverie db ping
```

**Examples:**

```shell
reverie db ping
```

**Output:**

```shell
✓ Database connection successful
Connected to: ws://localhost:8000/rpc
Namespace: blog
Database: blog
```

### `db info`

Show database information.

```shell
reverie db info
```

**Examples:**

```shell
reverie db info
```

**Output:**

```shell
Database Information

Connection:
  URL: ws://localhost:8000/rpc
  Namespace: blog
  Database: blog
  Status: Connected

Tables: 5
Migrations Applied: 3
```

## Common Workflows

### Initial Setup

```shell
# Create migrations directory
mkdir migrations

# Create first migration
reverie migrate create "Initial schema"

# Edit migration file
# ... add your SQL statements ...

# Apply migration
reverie migrate up
```

### Development Workflow

```shell
# 1. Make schema changes in code
# 2. Create migration
reverie migrate create "Add email verification"

# 3. Edit migration file with SQL
# 4. Preview changes
reverie migrate up --dry-run

# 5. Apply migration
reverie migrate up

# 6. Verify schema
reverie schema show
```

### Rollback Workflow

```shell
# Check current status
reverie migrate status

# Preview rollback
reverie migrate down --dry-run

# Rollback if needed
reverie migrate down

# Verify
reverie migrate status
```

### Debugging Migrations

```shell
# Validate migration files
reverie migrate validate

# Check what's pending
reverie migrate status

# Try dry run first
reverie migrate up --dry-run

# Apply with verbose logging
reverie migrate up --verbose
```

### Production Deployment

```shell
# 1. Check status on production
reverie migrate status --format json > status.json

# 2. Preview changes
reverie migrate up --dry-run

# 3. Apply migrations
reverie migrate up

# 4. Verify
reverie migrate history

# 5. Check database
reverie db info
```

## Configuration

### Configuration Files

reverie loads configuration from:

1. Environment variables
2. `.env` file in current directory
3. Default values

### `.env` File

Create a `.env` file in your project root:

```env
# Database Connection
SURREAL_URL=ws://localhost:8000/rpc
SURREAL_NAMESPACE=blog
SURREAL_DATABASE=blog
SURREAL_USERNAME=root
SURREAL_PASSWORD=root

# Connection Pool
SURREAL_MAX_CONNECTIONS=10

# Retry Configuration
SURREAL_RETRY_MAX_ATTEMPTS=3
SURREAL_RETRY_MIN_WAIT=1.0
SURREAL_RETRY_MAX_WAIT=10.0
SURREAL_RETRY_MULTIPLIER=2.0

# Logging
LOG_LEVEL=INFO
```

### Multiple Environments

Use different `.env` files:

```shell
# Development
cp .env .env.development

# Staging
cp .env .env.staging

# Production
cp .env .env.production
```

Load specific environment:

```shell
# Linux/macOS
export $(cat .env.production | xargs) && reverie migrate up

# Windows PowerShell
Get-Content .env.production | ForEach-Object {
  $name, $value = $_.split('=')
  Set-Content env:\$name $value
}
reverie migrate up
```

## Environment Variables

### Required Variables

- `SURREAL_URL` - Database URL (default: `ws://localhost:8000/rpc`)
- `SURREAL_NAMESPACE` - Namespace name (required)
- `SURREAL_DATABASE` - Database name (required)

### Authentication

- `SURREAL_USERNAME` - Database username (default: `root`)
- `SURREAL_PASSWORD` - Database password (default: `root`)

### Connection Pool

- `SURREAL_MAX_CONNECTIONS` - Max concurrent connections (default: `10`)

### Retry Configuration

- `SURREAL_RETRY_MAX_ATTEMPTS` - Max retry attempts (default: `3`)
- `SURREAL_RETRY_MIN_WAIT` - Min wait between retries in seconds (default: `1.0`)
- `SURREAL_RETRY_MAX_WAIT` - Max wait between retries in seconds (default: `10.0`)
- `SURREAL_RETRY_MULTIPLIER` - Exponential backoff multiplier (default: `2.0`)

### Logging

- `LOG_LEVEL` - Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`)

### Setting Environment Variables

**Linux/macOS:**

```shell
export SURREAL_NAMESPACE=blog
export SURREAL_DATABASE=blog
```

**Windows Command Prompt:**

```cmd
set SURREAL_NAMESPACE=blog
set SURREAL_DATABASE=blog
```

**Windows PowerShell:**

```powershell
$env:SURREAL_NAMESPACE="blog"
$env:SURREAL_DATABASE="blog"
```

## Exit Codes

The CLI uses standard exit codes:

- `0` - Success
- `1` - General error
- `130` - Interrupted (Ctrl+C)

Check exit code:

```shell
# Linux/macOS
reverie migrate up
echo $?

# Windows Command Prompt
reverie migrate up
echo %ERRORLEVEL%

# Windows PowerShell
reverie migrate up
echo $LASTEXITCODE
```

## Shell Completion

reverie supports shell completion for shell, zsh, and fish.

### shell

```shell
# Add to ~/.shellrc
eval "$(_reverie_COMPLETE=shell_source reverie)"
```

### Zsh

```shell
# Add to ~/.zshrc
eval "$(_reverie_COMPLETE=zsh_source reverie)"
```

### Fish

```shell
# Add to ~/.config/fish/config.fish
eval (env _reverie_COMPLETE=fish_source reverie)
```

## Troubleshooting

### Connection Errors

```shell
# Check database is running
reverie db ping

# Test connection with verbose output
reverie --verbose db info
```

### Migration Errors

```shell
# Validate migration files
reverie migrate validate

# Check migration status
reverie migrate status

# View migration history
reverie migrate history
```

### Permission Errors

```shell
# Check directory permissions
ls -la migrations/

# Create directory if missing
mkdir -p migrations
```

### Output Format Issues

```shell
# Use JSON for script parsing
reverie migrate status --format json | jq '.[] | select(.status == "PENDING")'

# Use table for human-readable output
reverie migrate status --format table
```

## Scripting Examples

### shell Script

```shell
#!/bin/shell
set -e

# Apply migrations in CI/CD
echo "Checking migration status..."
reverie migrate status

echo "Applying migrations..."
reverie migrate up

echo "Verifying migrations..."
reverie migrate history

echo "Done!"
```

### Python Script

```python
import subprocess
import sys

def run_migrations():
  """Run database migrations."""
  try:
    # Check status
    result = subprocess.run(
      ['reverie', 'migrate', 'status', '--format', 'json'],
      capture_output=True,
      text=True,
      check=True,
    )
    
    print("Migration status:", result.stdout)
    
    # Apply migrations
    subprocess.run(
      ['reverie', 'migrate', 'up'],
      check=True,
    )
    
    print("Migrations applied successfully")
    
  except subprocess.CalledProcessError as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)

if __name__ == '__main__':
  run_migrations()
```

## Best Practices

1. **Always validate** before applying migrations
2. **Use dry-run** to preview changes
3. **Check status** before and after migrations
4. **Keep migrations** in version control
5. **Test rollbacks** in development
6. **Use verbose mode** when debugging
7. **Set environment** variables properly
8. **Backup database** before major changes
9. **Review migration** history regularly
10. **Document breaking** changes in migrations

## Next Steps

- Learn about [Migrations](migrations.md) in detail
- Explore [Schema Definition](schema.md)
- See [Query Building](queries.md) for data operations
- Check [Examples](examples/) for common patterns
