# Multi-Database Migration Orchestration

Complete guide to deploying migrations across multiple database environments using surql's orchestration features.

## Table of Contents

- [Overview](#overview)
- [Core Concepts](#core-concepts)
- [Environment Configuration](#environment-configuration)
- [Deployment Strategies](#deployment-strategies)
- [Health Checking](#health-checking)
- [CLI Commands](#cli-commands)
- [Programmatic API](#programmatic-api)
- [Safety Features](#safety-features)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)

## Overview

Multi-database orchestration enables you to deploy migrations across multiple database instances in a controlled, safe, and automated way. This is essential for:

- **Multi-environment deployments** - Deploy to dev, staging, and production
- **Horizontal scaling** - Deploy to multiple database replicas
- **Multi-tenant architectures** - Deploy to tenant-specific databases
- **Blue-green deployments** - Deploy to standby clusters before switching
- **Canary deployments** - Test on a subset before full rollout

### Key Features

- **Multiple deployment strategies** - Sequential, parallel, rolling, canary
- **Health checking** - Verify environment health before deployment
- **Auto-rollback** - Automatically rollback on failures
- **Environment registry** - Centralized configuration management
- **Progress tracking** - Real-time deployment status
- **Safety features** - Approval requirements, destructive operation blocking

## Core Concepts

### Environment

An environment represents a single database instance or cluster:

```python
from surql.orchestration import EnvironmentConfig
from surql.connection.config import ConnectionConfig

env = EnvironmentConfig(
  name='production',
  connection=ConnectionConfig(
    db_url='ws://prod.example.com:8000/rpc',
    db_ns='production',
    db='main',
  ),
  priority=1,  # Lower = higher priority
  tags={'production', 'critical'},
  require_approval=True,
  allow_destructive=False,
)
```

### Environment Registry

The registry manages all configured environments:

```python
from surql.orchestration import EnvironmentRegistry

registry = EnvironmentRegistry()
registry.register_environment(
  name='staging',
  connection=staging_conn,
  priority=50,
  tags={'staging', 'test'},
)
```

### Migration Coordinator

The coordinator orchestrates deployments across environments:

```python
from surql.orchestration import MigrationCoordinator

coordinator = MigrationCoordinator(registry)
results = await coordinator.deploy_to_environments(
  environments=['staging', 'production'],
  migrations=migrations,
  strategy='sequential',
)
```

## Environment Configuration

### Configuration File Format

Create an `environments.json` file:

```json
{
  "environments": [
    {
      "name": "development",
      "connection": {
        "db_url": "ws://localhost:8000/rpc",
        "db_ns": "development",
        "db": "main",
        "username": "root",
        "password": "root"
      },
      "priority": 100,
      "tags": ["dev", "local"],
      "require_approval": false,
      "allow_destructive": true
    },
    {
      "name": "staging",
      "connection": {
        "db_url": "ws://staging.example.com:8000/rpc",
        "db_ns": "staging",
        "db": "main",
        "username": "admin",
        "password": "${STAGING_PASSWORD}"
      },
      "priority": 50,
      "tags": ["staging", "test"],
      "require_approval": false,
      "allow_destructive": true
    },
    {
      "name": "production",
      "connection": {
        "db_url": "ws://prod.example.com:8000/rpc",
        "db_ns": "production",
        "db": "main",
        "username": "admin",
        "password": "${PROD_PASSWORD}"
      },
      "priority": 1,
      "tags": ["production", "critical"],
      "require_approval": true,
      "allow_destructive": false
    }
  ]
}
```

### Environment Variables in Config

Use environment variables for sensitive data:

```json
{
  "connection": {
    "db_url": "${DATABASE_URL}",
    "username": "${DB_USERNAME}",
    "password": "${DB_PASSWORD}"
  }
}
```

Set variables before running:

```shell
export STAGING_PASSWORD="secret123"
export PROD_PASSWORD="prod_secret"
surql orchestrate deploy -e staging,production
```

### Loading Configuration

**From File:**

```python
from surql.orchestration import configure_environments
from pathlib import Path

configure_environments(Path('environments.json'))
```

**Programmatically:**

```python
from surql.orchestration import register_environment
from surql.connection.config import ConnectionConfig

register_environment(
  name='production',
  connection=ConnectionConfig(
    db_url='ws://prod.example.com:8000/rpc',
    db_ns='production',
    db='main',
  ),
  priority=1,
  tags={'production'},
)
```

### Environment Properties

| Property | Type | Description | Default |
|----------|------|-------------|---------|
| `name` | string | Unique environment identifier | Required |
| `connection` | ConnectionConfig | Database connection details | Required |
| `priority` | int | Deployment priority (lower = higher) | 100 |
| `tags` | set[str] | Environment tags for grouping | `set()` |
| `require_approval` | bool | Require manual approval | `false` |
| `allow_destructive` | bool | Allow destructive migrations | `true` |

## Deployment Strategies

surql supports four deployment strategies, each suited for different scenarios.

### Sequential Strategy

Deploy to environments one at a time, in priority order.

**Use Cases:**
- Multi-stage deployments (dev → staging → production)
- When environments depend on each other
- Maximum safety and control

**Behavior:**
- Deploys to environments in priority order (lowest first)
- Waits for each deployment to complete
- Stops on first failure

**Example:**

```shell
surql orchestrate deploy -e dev,staging,production --strategy sequential
```

```python
results = await coordinator.deploy_to_environments(
  environments=['dev', 'staging', 'production'],
  migrations=migrations,
  strategy='sequential',
  verify_health=True,
  auto_rollback=True,
)
```

**Timeline:**

```
Dev      [████████] ✓
Staging           [████████] ✓
Prod                       [████████] ✓
         ━━━━━━━━━━━━━━━━━━━━━━━━━━━→ Time
```

### Parallel Strategy

Deploy to all environments simultaneously.

**Use Cases:**
- Independent environments (different tenants, regions)
- Fast deployments when environments don't interact
- Development/testing environments

**Behavior:**
- Deploys to all environments at once
- Configurable concurrency limit
- Continues even if some fail (reports all results)

**Example:**

```shell
surql orchestrate deploy -e tenant1,tenant2,tenant3 --strategy parallel --max-concurrent 3
```

```python
results = await coordinator.deploy_to_environments(
  environments=['tenant1', 'tenant2', 'tenant3'],
  migrations=migrations,
  strategy='parallel',
  max_concurrent=3,
)
```

**Timeline:**

```
Tenant1  [████████] ✓
Tenant2  [████████] ✗
Tenant3  [████████] ✓
         ━━━━━━━━━━━→ Time
```

### Rolling Strategy

Deploy to environments in batches, waiting for each batch to complete.

**Use Cases:**
- Database replicas or clustered deployments
- Maintaining service availability
- Gradual rollouts with quick rollback capability

**Behavior:**
- Divides environments into batches
- Deploys one batch at a time
- Stops if batch fails
- Configurable batch size

**Example:**

```shell
surql orchestrate deploy -e db1,db2,db3,db4 --strategy rolling --batch-size 2
```

```python
results = await coordinator.deploy_to_environments(
  environments=['db1', 'db2', 'db3', 'db4'],
  migrations=migrations,
  strategy='rolling',
  batch_size=2,  # Deploy 2 at a time
  verify_health=True,
)
```

**Timeline:**

```
Batch 1:
  db1    [████████] ✓
  db2    [████████] ✓

Batch 2:
  db3             [████████] ✓
  db4             [████████] ✓

         ━━━━━━━━━━━━━━━━━━━━→ Time
```

### Canary Strategy

Deploy to a small percentage of environments first, then to the rest.

**Use Cases:**
- High-risk deployments
- Testing migrations in production with limited impact
- Gradual production rollouts

**Behavior:**
- Deploys to specified percentage first (canary)
- Waits for canary verification
- Deploys to remaining environments if successful
- Configurable canary percentage

**Example:**

```shell
surql orchestrate deploy -e prod1,prod2,prod3,prod4,prod5 --strategy canary --canary-percent 20
```

```python
results = await coordinator.deploy_to_environments(
  environments=['prod1', 'prod2', 'prod3', 'prod4', 'prod5'],
  migrations=migrations,
  strategy='canary',
  canary_percentage=20.0,  # Test on 20% first
  verify_health=True,
)
```

**Timeline:**

```
Canary (20%):
  prod1  [████████] ✓

Verification: ✓

Full Rollout:
  prod2           [████████] ✓
  prod3           [████████] ✓
  prod4           [████████] ✓
  prod5           [████████] ✓

         ━━━━━━━━━━━━━━━━━━━━━━━━━→ Time
```

### Strategy Comparison

| Strategy | Speed | Safety | Use Case |
|----------|-------|--------|----------|
| **Sequential** | Slowest | Highest | Multi-stage deployments |
| **Parallel** | Fastest | Lowest | Independent environments |
| **Rolling** | Medium | High | Replica sets, availability |
| **Canary** | Medium | Highest | High-risk production deploys |

## Health Checking

Health checking ensures environments are ready for deployment.

### What Gets Checked

1. **Connectivity** - Can connect to database
2. **Authentication** - Credentials are valid
3. **Migration table** - `_migration_history` exists
4. **Database state** - Schema is accessible

### Using Health Checks

**CLI:**

```shell
# Validate environments
surql orchestrate validate

# Deploy with health checks
surql orchestrate deploy -e production --verify-health
```

**Programmatically:**

```python
from surql.orchestration import HealthCheck

health = HealthCheck()
status = await health.check_environment(env_config)

print(f'Healthy: {status.is_healthy}')
print(f'Can connect: {status.can_connect}')
print(f'Migration table exists: {status.migration_table_exists}')

if status.error:
  print(f'Error: {status.error}')
```

### Health Check Results

```python
@dataclass
class HealthStatus:
  environment: str
  is_healthy: bool
  can_connect: bool
  migration_table_exists: bool
  error: str | None
  checked_at: datetime
```

### Batch Health Checks

```python
# Check multiple environments
statuses = await health.check_environments([env1, env2, env3])

for env_name, status in statuses.items():
  if status.is_healthy:
    print(f'✓ {env_name}')
  else:
    print(f'✗ {env_name}: {status.error}')
```

## CLI Commands

### deploy

Deploy migrations to multiple environments.

```shell
surql orchestrate deploy [OPTIONS]
```

**Options:**

- `--environments, -e` - Comma-separated environment names (required)
- `--strategy` - Deployment strategy (default: `sequential`)
- `--batch-size` - Batch size for rolling strategy (default: `1`)
- `--canary-percent` - Canary percentage (default: `10.0`)
- `--max-concurrent` - Max concurrent for parallel (default: `5`)
- `--dry-run` - Simulate without executing
- `--skip-health-check` - Skip health verification
- `--no-rollback` - Disable auto-rollback
- `--config` - Config file path (default: `environments.json`)
- `--migrations-dir, -m` - Migrations directory (default: `migrations`)

**Examples:**

```shell
# Basic deployment
surql orchestrate deploy -e staging,production

# Rolling deployment
surql orchestrate deploy -e db1,db2,db3,db4 --strategy rolling --batch-size 2

# Canary deployment
surql orchestrate deploy -e prod1,prod2,prod3 --strategy canary --canary-percent 33

# Dry run
surql orchestrate deploy -e production --dry-run

# Custom config
surql orchestrate deploy -e all --config ./config/prod-envs.json
```

### status

Check deployment status of environments.

```shell
surql orchestrate status -e ENVIRONMENTS [OPTIONS]
```

**Options:**

- `--environments, -e` - Comma-separated environment names (required)
- `--config` - Config file path (default: `environments.json`)

**Example:**

```shell
surql orchestrate status -e staging,production
```

**Output:**

```
Environment Deployment Status
┌─────────────┬─────────┐
│ Environment │ Status  │
├─────────────┼─────────┤
│ staging     │ Healthy │
│ production  │ Healthy │
└─────────────┴─────────┘
```

### validate

Validate environment configuration and connectivity.

```shell
surql orchestrate validate [OPTIONS]
```

**Options:**

- `--config` - Config file path (default: `environments.json`)

**Example:**

```shell
surql orchestrate validate
surql orchestrate validate --config prod-envs.json
```

**Output:**

```
Environment Validation
┌─────────────┬──────────────┬─────────────────┬─────────┐
│ Environment │ Connectivity │ Migration Table │ Status  │
├─────────────┼──────────────┼─────────────────┼─────────┤
│ development │ ✓            │ ✓               │ Healthy │
│ staging     │ ✓            │ ✓               │ Healthy │
│ production  │ ✓            │ ✓               │ Healthy │
└─────────────┴──────────────┴─────────────────┴─────────┘

✓ All environments validated successfully
```

## Programmatic API

### Basic Usage

```python
from pathlib import Path
from surql.orchestration import (
  configure_environments,
  get_registry,
  MigrationCoordinator,
)
from surql.migration.discovery import discover_migrations

# Load configuration
configure_environments(Path('environments.json'))

# Get registry
registry = get_registry()

# Discover migrations
migrations = discover_migrations(Path('migrations'))

# Create coordinator
coordinator = MigrationCoordinator(registry)

# Deploy
results = await coordinator.deploy_to_environments(
  environments=['staging', 'production'],
  migrations=migrations,
  strategy='sequential',
  verify_health=True,
  auto_rollback=True,
)

# Check results
for env_name, result in results.items():
  print(f'{env_name}: {result.status.value}')
  print(f'  Migrations applied: {result.migrations_applied}')
  print(f'  Duration: {result.duration_seconds:.2f}s')
```

### Deployment Results

```python
@dataclass
class DeploymentResult:
  environment: str
  status: DeploymentStatus  # SUCCESS, FAILED, ROLLED_BACK, SKIPPED
  migrations_applied: int
  duration_seconds: float | None
  error: str | None
  started_at: datetime
  completed_at: datetime | None
```

### Advanced Usage

```python
from surql.orchestration import (
  EnvironmentRegistry,
  EnvironmentConfig,
  MigrationCoordinator,
)
from surql.connection.config import ConnectionConfig

# Create custom registry
registry = EnvironmentRegistry()

# Register multiple environments
for i in range(4):
  conn = ConnectionConfig(
    db_url=f'ws://db{i}.example.com:8000/rpc',
    db_ns='production',
    db='main',
  )
  
  registry.register_environment(
    name=f'db{i}',
    connection=conn,
    priority=i,
    tags={'production', 'replica'},
  )

# Create coordinator
coordinator = MigrationCoordinator(registry)

# Deploy with custom settings
results = await coordinator.deploy_to_environments(
  environments=['db0', 'db1', 'db2', 'db3'],
  migrations=migrations,
  strategy='rolling',
  batch_size=2,
  verify_health=True,
  auto_rollback=True,
  dry_run=False,
)

# Analyze results
successful = [r for r in results.values() if r.status == DeploymentStatus.SUCCESS]
failed = [r for r in results.values() if r.status == DeploymentStatus.FAILED]

print(f'Successful: {len(successful)}/{len(results)}')
if failed:
  print('Failed deployments:')
  for result in failed:
    print(f'  {result.environment}: {result.error}')
```

### Convenience Functions

```python
from surql.orchestration import (
  register_environment,
  get_registry,
  deploy_to_environments,
)

# Quick registration
register_environment(
  name='staging',
  connection=staging_conn,
  priority=50,
)

# Quick deployment
results = await deploy_to_environments(
  registry=get_registry(),
  environments=['staging'],
  migrations=migrations,
  strategy='sequential',
)
```

## Safety Features

### Approval Requirements

Require manual approval for sensitive environments:

```json
{
  "name": "production",
  "require_approval": true
}
```

When deploying:

```shell
surql orchestrate deploy -e production

# Will prompt:
# Deploy to production? [y/N]: _
```

### Destructive Operation Blocking

Prevent destructive migrations in production:

```json
{
  "name": "production",
  "allow_destructive": false
}
```

Migrations that drop tables or remove fields will be blocked.

### Auto-Rollback on Failure

Automatically rollback failed deployments:

```python
results = await coordinator.deploy_to_environments(
  environments=['production'],
  migrations=migrations,
  auto_rollback=True,  # Rollback on failure
)
```

Rollback behavior:
- Executes `down()` for applied migrations
- Restores to pre-deployment state
- Marks deployment as `ROLLED_BACK`

### Dry Run Mode

Test deployments without making changes:

```shell
surql orchestrate deploy -e production --dry-run
```

```python
results = await coordinator.deploy_to_environments(
  environments=['production'],
  migrations=migrations,
  dry_run=True,  # Simulate only
)
```

### Health Verification

Verify environment health before deploying:

```python
results = await coordinator.deploy_to_environments(
  environments=['production'],
  migrations=migrations,
  verify_health=True,  # Check health first
)
```

Checks:
- Database connectivity
- Migration table exists
- Schema accessibility

## Best Practices

### 1. Use Configuration Files

Store environment configuration in version control:

```shell
# Separate configs for different contexts
environments.json           # Development defaults
environments.staging.json   # Staging config
environments.prod.json      # Production config
```

```shell
# Deploy with specific config
surql orchestrate deploy -e production --config environments.prod.json
```

### 2. Always Test with Dry Run

Preview deployments before executing:

```shell
# 1. Dry run to preview
surql orchestrate deploy -e production --dry-run

# 2. Review output

# 3. Execute if looks good
surql orchestrate deploy -e production
```

### 3. Use Environment Priorities

Set priorities to control deployment order:

```json
{
  "environments": [
    {"name": "dev", "priority": 100},
    {"name": "staging", "priority": 50},
    {"name": "production", "priority": 1}
  ]
}
```

Lower priority = deploys first in sequential mode.

### 4. Tag Environments for Grouping

Use tags to organize environments:

```json
{
  "tags": ["production", "us-west", "replica"]
}
```

Deploy to tagged groups:

```python
# Get all production environments
registry = get_registry()
prod_envs = [
  env.name for env in registry.list_environments()
  if 'production' in env.tags
]
```

### 5. Enable Health Checks in Production

Always verify health before production deployments:

```shell
# Validate first
surql orchestrate validate

# Deploy with health verification
surql orchestrate deploy -e production --verify-health
```

### 6. Use Rolling or Canary for Replicas

Maintain availability during replica deployments:

```shell
# Rolling deployment to maintain service
surql orchestrate deploy \
  -e db1,db2,db3,db4 \
  --strategy rolling \
  --batch-size 1 \
  --verify-health
```

### 7. Monitor Deployment Progress

Track progress for long deployments:

```python
import asyncio

async def deploy_with_progress():
  coordinator = MigrationCoordinator(registry)
  
  # Start deployment
  task = asyncio.create_task(
    coordinator.deploy_to_environments(
      environments=['prod1', 'prod2', 'prod3'],
      migrations=migrations,
      strategy='sequential',
    )
  )
  
  # Monitor progress
  while not task.done():
    statuses = await coordinator.get_deployment_status(
      ['prod1', 'prod2', 'prod3']
    )
    print(f'Status: {statuses}')
    await asyncio.sleep(5)
  
  results = await task
  return results
```

### 8. Secure Sensitive Credentials

Never commit passwords to version control:

```json
{
  "connection": {
    "password": "${PROD_PASSWORD}"  // ✓ Use env variable
  }
}
```

Not:

```json
{
  "connection": {
    "password": "hardcoded_secret"  // ✗ Never do this
  }
}
```

### 9. Keep Production Separate

Use separate configuration files for production:

```shell
# Development/staging together
environments.json

# Production isolated
environments.production.json
```

### 10. Document Environment Changes

Track environment configuration changes in git:

```shell
git add environments.json
git commit -m "Add production-west replica to orchestration"
```

## Troubleshooting

### Environment Not Found

**Error**: `Environment 'production' not found in registry`

**Solution**:

```shell
# Check if config file exists
ls -la environments.json

# Validate configuration
surql orchestrate validate

# Check environment names
cat environments.json | jq '.environments[].name'
```

### Connection Failures

**Error**: `Cannot connect to ws://prod.example.com:8000/rpc`

**Solutions**:

1. Verify connectivity:
   ```shell
   ping prod.example.com
   telnet prod.example.com 8000
   ```

2. Check credentials:
   ```shell
   echo $PROD_PASSWORD  # Should show password
   ```

3. Test connection manually:
   ```python
   from surql.connection.client import get_client
   from surql.connection.config import ConnectionConfig
   
   config = ConnectionConfig(
     db_url='ws://prod.example.com:8000/rpc',
     db_ns='production',
     db='main',
     username='admin',
     password=os.getenv('PROD_PASSWORD'),
   )
   
   async with get_client(config) as client:
     result = await client.execute('SELECT * FROM _migration_history LIMIT 1')
     print('Connection OK')
   ```

### Deployment Failures

**Error**: Deployment fails midway

**Solutions**:

1. Check auto-rollback results:
   ```python
   if result.status == DeploymentStatus.ROLLED_BACK:
     print(f'Deployment rolled back: {result.error}')
   ```

2. Review migration errors:
   ```shell
   # Check migration table
   surql migrate history --verbose
   ```

3. Test migrations locally:
   ```shell
   # Test on development first
   surql orchestrate deploy -e development --dry-run
   surql orchestrate deploy -e development
   ```

### Partial Deployment Success

**Problem**: Some environments succeed, others fail

**Solution**:

```python
results = await coordinator.deploy_to_environments(...)

successful = [env for env, r in results.items() if r.status == DeploymentStatus.SUCCESS]
failed = [env for env, r in results.items() if r.status == DeploymentStatus.FAILED]

print(f'Successful: {successful}')
print(f'Failed: {failed}')

# Retry failed environments
if failed:
  retry_results = await coordinator.deploy_to_environments(
    environments=failed,
    migrations=migrations,
    strategy='sequential',
  )
```

### Health Check Failures

**Error**: `Environment staging: Migration table does not exist`

**Solution**:

```shell
# Initialize migration table
surql migrate up --steps 0  # Creates table without applying migrations

# Or apply initial migration
surql migrate up --steps 1
```

## Examples

### Example 1: Multi-Stage Deployment

```python
from pathlib import Path
from surql.orchestration import configure_environments, get_registry, MigrationCoordinator
from surql.migration.discovery import discover_migrations

async def multi_stage_deployment():
  """Deploy to dev → staging → production sequentially."""
  
  # Load configuration
  configure_environments(Path('environments.json'))
  
  # Get migrations
  migrations = discover_migrations(Path('migrations'))
  
  # Deploy sequentially
  coordinator = MigrationCoordinator(get_registry())
  results = await coordinator.deploy_to_environments(
    environments=['development', 'staging', 'production'],
    migrations=migrations,
    strategy='sequential',  # One at a time
    verify_health=True,     # Check health first
    auto_rollback=True,     # Rollback on failure
  )
  
  # Report results
  for env_name, result in results.items():
    if result.status == DeploymentStatus.SUCCESS:
      print(f'✓ {env_name}: {result.migrations_applied} migrations in {result.duration_seconds:.2f}s')
    else:
      print(f'✗ {env_name}: {result.error}')
      break  # Stop reporting after first failure
```

### Example 2: Replica Set Deployment

```python
async def replica_deployment():
  """Deploy to database replicas with rolling strategy."""
  
  configure_environments(Path('prod-replicas.json'))
  migrations = discover_migrations(Path('migrations'))
  
  coordinator = MigrationCoordinator(get_registry())
  results = await coordinator.deploy_to_environments(
    environments=['prod-db1', 'prod-db2', 'prod-db3', 'prod-db4'],
    migrations=migrations,
    strategy='rolling',
    batch_size=2,  # Deploy 2 at a time
    verify_health=True,
    auto_rollback=True,
  )
  
  # Verify all succeeded
  all_successful = all(
    r.status == DeploymentStatus.SUCCESS
    for r in results.values()
  )
  
  if all_successful:
    print('✓ All replicas updated successfully')
  else:
    print('✗ Some replicas failed - may need manual intervention')
```

### Example 3: Canary Production Deployment

```python
async def canary_production_deployment():
  """Deploy to production with canary strategy."""
  
  configure_environments(Path('production.json'))
  migrations = discover_migrations(Path('migrations'))
  
  coordinator = MigrationCoordinator(get_registry())
  
  # Deploy to 20% first
  results = await coordinator.deploy_to_environments(
    environments=['prod1', 'prod2', 'prod3', 'prod4', 'prod5'],
    migrations=migrations,
    strategy='canary',
    canary_percentage=20.0,  # Test on 1 instance first
    verify_health=True,
    auto_rollback=True,
  )
  
  # Check canary results
  canary_env = list(results.keys())[0]
  canary_result = results[canary_env]
  
  if canary_result.status == DeploymentStatus.SUCCESS:
    print(f'✓ Canary deployment to {canary_env} successful')
    print('✓ Proceeding with full rollout')
  else:
    print(f'✗ Canary deployment to {canary_env} failed')
    print('✗ Full rollout cancelled')
```

### Example 4: Multi-Tenant Deployment

```python
async def multi_tenant_deployment():
  """Deploy to multiple tenant databases in parallel."""
  
  # Dynamically create environment configs
  registry = EnvironmentRegistry()
  
  tenants = ['tenant_a', 'tenant_b', 'tenant_c', 'tenant_d']
  for tenant in tenants:
    conn = ConnectionConfig(
      db_url='ws://db.example.com:8000/rpc',
      db_ns=tenant,
      db='main',
    )
    
    registry.register_environment(
      name=tenant,
      connection=conn,
      tags={'tenant', 'production'},
    )
  
  # Deploy to all tenants in parallel
  coordinator = MigrationCoordinator(registry)
  migrations = discover_migrations(Path('migrations'))
  
  results = await coordinator.deploy_to_environments(
    environments=tenants,
    migrations=migrations,
    strategy='parallel',
    max_concurrent=3,  # Limit concurrent deployments
    verify_health=True,
  )
  
  # Report per-tenant results
  for tenant, result in results.items():
    status_emoji = '✓' if result.status == DeploymentStatus.SUCCESS else '✗'
    print(f'{status_emoji} {tenant}: {result.migrations_applied} migrations')
```

## Additional Resources

- [Migration System Guide](migrations.md) - Migration creation and management
- [CLI Reference](cli.md) - Complete CLI documentation
- [Orchestration Example Code](examples/orchestration_example.py) - More code examples

## Summary

Multi-database orchestration provides:

- **Four deployment strategies** - Sequential, parallel, rolling, canary
- **Environment management** - Registry and configuration
- **Safety features** - Health checks, auto-rollback, dry run
- **Programmatic API** - Full Python API for custom workflows
- **CLI commands** - Easy command-line deployment

Use orchestration to deploy migrations safely and efficiently across multiple database environments.
