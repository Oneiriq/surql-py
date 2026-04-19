# API Reference

This document provides an overview of surql's API modules. For detailed implementation, refer to the source code.

## Table of Contents

- [Schema Module](#schema-module)
- [Migration Module](#migration-module)
- [Query Module](#query-module)
- [Connection Module](#connection-module)
- [Types Module](#types-module)
- [CLI Module](#cli-module)

## Schema Module

**Location:** `src/schema/`

Defines database schemas using functional composition.

### Key Files

#### `fields.py`

Field type definitions and builder functions.

**Key Classes:**

- `FieldType` - Enum of SurrealDB field types
- `FieldDefinition` - Immutable field definition model

**Key Functions:**

- `field()` - Create a field definition
- `string_field()` - Create a string field
- `int_field()` - Create an integer field
- `float_field()` - Create a float field
- `bool_field()` - Create a boolean field
- `datetime_field()` - Create a datetime field
- `record_field()` - Create a record (foreign key) field
- `array_field()` - Create an array field
- `object_field()` - Create an object field
- `computed_field()` - Create a computed field

**Example:**

```python
from surql.schema.fields import string_field, int_field

name_field = string_field('name', assertion='string::len($value) > 0')
age_field = int_field('age', assertion='$value >= 0')
```

#### `table.py`

Table schema composition and builders.

**Key Classes:**

- `TableMode` - Enum for table modes (SCHEMAFULL, SCHEMALESS, DROP)
- `IndexType` - Enum for index types (UNIQUE, SEARCH, STANDARD)
- `TableDefinition` - Immutable table schema definition
- `IndexDefinition` - Immutable index definition
- `EventDefinition` - Immutable event/trigger definition

**Key Functions:**

- `table_schema()` - Create a table schema
- `index()` - Create an index
- `unique_index()` - Create a unique index
- `search_index()` - Create a search index
- `event()` - Create an event/trigger
- `with_fields()` - Add fields to a table
- `with_indexes()` - Add indexes to a table
- `with_events()` - Add events to a table
- `with_permissions()` - Add permissions to a table

**Example:**

```python
from surql.schema.table import table_schema, unique_index, TableMode

schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[...],
  indexes=[unique_index('email_idx', ['email'])],
)
```

#### `edge.py`

Edge (relationship) schema definitions.

**Key Classes:**

- `EdgeDefinition` - Immutable edge schema definition

**Key Functions:**

- `edge_schema()` - Create an edge schema

**Example:**

```python
from surql.schema.edge import edge_schema

follows = edge_schema('follows', from_table='user', to_table='user')
```

## Migration Module

**Location:** `src/migration/`

Database migration generation, execution, and tracking.

### Key Migration Files

#### `generator.py`

Migration file generation.

**Key Functions:**

- `create_blank_migration()` - Create a blank migration file

#### `executor.py`

Migration execution engine.

**Key Functions:**

- `execute_migration_plan()` - Execute a migration plan
- `create_migration_plan()` - Create a migration execution plan
- `validate_migrations()` - Validate migration files

#### `discovery.py`

Migration file discovery and loading.

**Key Classes:**

- `MigrationFile` - Loaded migration file representation

**Key Functions:**

- `discover_migrations()` - Find and load migration files
- `validate_migration_name()` - Validate migration filename

#### `history.py`

Migration history tracking.

**Key Classes:**

- `MigrationHistory` - Migration history record

**Key Functions:**

- `ensure_migration_table()` - Create migration history table
- `get_applied_migrations()` - Get list of applied migrations
- `record_migration()` - Record a migration as applied

#### `models.py`

Migration data models.

**Key Classes:**

- `MigrationDirection` - Enum for UP/DOWN
- `MigrationState` - Enum for PENDING/APPLIED
- `MigrationPlan` - Migration execution plan

## Query Module

**Location:** `src/query/`

Query building and ORM operations.

### Key Query Files

#### `crud.py`

High-level CRUD operations.

**Key Functions:**

- `create_record()` - Create a single record
- `create_records()` - Create multiple records
- `get_record()` - Get a record by ID
- `update_record()` - Update entire record
- `merge_record()` - Partial update
- `delete_record()` - Delete a record
- `delete_records()` - Delete multiple records
- `query_records()` - Query with filters
- `count_records()` - Count records (emits `count() ... GROUP ALL` for v3)
- `aggregate_records()` - Typed `SELECT ... GROUP BY | GROUP ALL` returning list-of-dicts (added in 1.5.0)
- `exists()` - Check if record exists
- `first()` - Get first matching record
- `last()` - Get last matching record

**Example:**

```python
from surql.query.crud import create_record, query_records

user = await create_record('user', user_data, client=client)
users = await query_records('user', User, conditions=['age >= 18'], client=client)
```

#### `functions.py`

Pre-built SurrealQL function factories returning `SurrealFn` wrappers. Each factory composes with `Query.set(...)`, `Query.select([...])`, and `aggregate_records(select={...})`.

**Time:**

- `time_now_fn()` - `time::now()`

**Math:**

- `math_mean_fn(field)`, `math_sum_fn(field)`, `math_min_fn(field)`, `math_max_fn(field)`
- `math_ceil_fn(field)`, `math_floor_fn(field)`, `math_round_fn(field, precision=None)`, `math_abs_fn(field)`

**Strings:**

- `string_len(field)`, `string_concat(*parts)`, `string_lower(field)`, `string_upper(field)`

**Aggregation:**

- `count_if(predicate=None)` - renders `count()` or `count(<predicate>)` (v3 rejects `count(*)`)

See the [Query UX Helpers guide](../query-ux.md) for worked examples.

#### `results.py`

Result wrapper classes and extraction utilities for SurrealDB responses.

**Result Wrappers:**

- `QueryResult[T]` - Generic result container with metadata
- `RecordResult[T]` - Single record wrapper
- `ListResult[T]` - Multiple records wrapper
- `CountResult` - Aggregation result for count operations
- `AggregateResult` - Generic aggregation result
- `PaginatedResult[T]` - Paginated result with page metadata

**Result Extraction Utilities (SurrealDB Response Format Handling):**

- `extract_result(result)` - Extract data from nested/flat SurrealDB response formats
- `extract_many(result)` - Alias for `extract_result` (naming parity with `extract_one` / `extract_scalar`, added in 1.5.0)
- `extract_one(result)` - Extract first record or None
- `extract_scalar(result, key, default)` - Extract scalar value from aggregate queries
- `has_results(result)` - Check if result contains any records
- `has_result(result)` - Alias for `has_results` (added in 1.5.0)

**Example:**

```python
from surql.query.results import extract_result, extract_one, extract_scalar, has_results

# Handle nested format from db.query()
result = await client.execute('SELECT * FROM user WHERE age > 18')
records = extract_result(result)  # List of dicts

# Get single record
result = await client.execute('SELECT * FROM user:alice')
user = extract_one(result)  # Dict or None

# Extract aggregate values
result = await client.execute('SELECT count() AS total FROM user')
count = extract_scalar(result, 'total', 0)  # Scalar value

# Check if results exist
if has_results(result):
    records = extract_result(result)
```

**Why Use Extract Utilities:**

SurrealDB returns responses in different formats depending on the operation:

- Nested format: `[{"result": [{"id": "...", ...}]}]` (from `db.query()`)
- Flat format: `[{"id": "...", ...}]` (from `db.select()`)

The extraction utilities handle both formats seamlessly, eliminating the need for custom workarounds and making code robust across SurrealDB response formats.

#### `builder.py`

Composable query builder.

**Key Classes:**

- `Query` - Immutable query builder

**Key Methods:**

- `select()` - SELECT clause
- `from_table()` - FROM clause
- `where()` - WHERE condition
- `order_by()` - ORDER BY clause
- `limit()` - LIMIT clause
- `offset()` - OFFSET clause
- `group_by()` - GROUP BY clause
- `to_surql()` - Convert to SurrealQL

**Example:**

```python
from surql.query.builder import Query

query = (
  Query()
    .select(['name', 'email'])
    .from_table('user')
    .where('age >= 18')
    .limit(10)
)
```

#### `executor.py`

Query execution.

**Key Functions:**

- `fetch_all()` - Execute query and return all results
- `fetch_one()` - Execute query and return first result

#### `expressions.py`

Type-safe query expressions.

**Key Classes:**

- `Expression` - Query expression wrapper

## Connection Module

**Location:** `src/connection/`

Database connection management.

### Key Connection Files

#### `client.py`

Async database client.

**Key Classes:**

- `DatabaseClient` - Async SurrealDB client wrapper
- `DatabaseError` - Base database exception
- `ConnectionError` - Connection error exception
- `QueryError` - Query execution error exception

**Key Functions:**

- `get_client()` - Context manager for client lifecycle

**Key Methods:**

- `connect()` - Establish connection
- `disconnect()` - Close connection
- `execute()` - Execute raw SurrealQL
- `select()` - SELECT operation
- `create()` - CREATE operation
- `update()` - UPDATE operation
- `merge()` - MERGE operation
- `delete()` - DELETE operation
- `insert_relation()` - INSERT RELATION for edges

**Example:**

```python
from surql.connection.client import get_client

async with get_client(config) as client:
  result = await client.execute('SELECT * FROM user')
```

#### `config.py`

Connection configuration.

**Key Classes:**

- `ConnectionConfig` - Database connection configuration

#### `context.py`

Connection context management.

**Key Functions:**

- `get_db()` - Get current database client from context
- `set_db()` - Set database client in context
- `db_context()` - Context manager for scoped connections

#### `transaction.py`

Transaction support.

**Key Classes:**

- `Transaction` - Transaction wrapper

**Key Functions:**

- `transaction()` - Context manager for transactions

**Example:**

```python
from surql.connection.transaction import transaction

async with transaction(client):
  await client.execute('UPDATE user:alice SET credits -= 10')
  await client.execute('UPDATE user:bob SET credits += 10')
```

## Types Module

**Location:** `src/types/`

Type definitions and utilities.

### Key Types Files

#### `record_id.py`

RecordID type for SurrealDB record identifiers with angle bracket support.

**Key Classes:**

- `RecordID` - Type-safe record ID wrapper with support for complex IDs

**Key Methods:**

- `parse()` - Parse from string (supports both standard and angle bracket formats)
- `__str__()` - Convert to string

**Supported Formats:**

1. **Standard format**: `table:id` (alphanumeric + underscores)
2. **Angle bracket format**: `table:⟨complex-id⟩` (for complex record IDs)

**Example:**

```python
from surql.types.record_id import RecordID

# Standard format
user_rid = RecordID(table='user', id='alice')
print(user_rid)  # user:alice

# Angle bracket format
# Required for IDs with special characters (dots, hyphens, colons, etc.)
outlet_rid = RecordID.parse('outlet:⟨alaskabeacon.com⟩')
print(outlet_rid)  # outlet:⟨alaskabeacon.com⟩

# Complex compound IDs
doc_rid = RecordID.parse('document:⟨alaskabeacon.com:01HQXYZ...⟩')
print(doc_rid)  # document:⟨alaskabeacon.com:01HQXYZ...⟩

# Why use angle brackets:
# - Domains: outlet:⟨alaskabeacon.com⟩ (dots in domain)
# - URLs: page:⟨https://example.com/path⟩ (colons, slashes)
# - Compound keys: doc:⟨domain:ulid⟩ (multiple parts)
```

**Complex Record ID Support:**

The angle bracket format is valid SurrealDB syntax for escaping complex identifiers. This feature enables support for record IDs that contain special characters such as domain names, URLs, and compound keys.

#### `operators.py`

Query operators for type-safe conditions.

**Key Classes:**

- `Operator` - Base operator class

**Key Functions:**

- `eq()` - Equality operator
- `ne()` - Not equal operator
- `gt()` - Greater than operator
- `gte()` - Greater than or equal operator
- `lt()` - Less than operator
- `lte()` - Less than or equal operator
- `contains()` - String contains operator
- `in_list()` - IN operator

#### `surreal_fn.py`

Raw SurrealQL function-call wrapper that renders verbatim when used as a value.

**Key Classes:**

- `SurrealFn` - Immutable wrapper whose `.to_surql()` emits the bare function expression

**Key Functions:**

- `surql_fn(name, *args)` - Build a `SurrealFn` from a function name and arguments
- `type_record(table, id)` - Build `type::record('table', id)` (v3-preferred form)
- `type_thing(table, id)` - Build `type::thing('table', id)` (v2-compatible alias)

See [Query UX Helpers](../query-ux.md#type_record-type_thing) for composition examples.

## CLI Module

**Location:** `src/cli/`

Command-line interface implementation.

### Key CLI Files

#### `migrate.py`

Migration CLI commands.

**Commands:**

- `migrate up` - Apply migrations
- `migrate down` - Rollback migrations
- `migrate status` - Show migration status
- `migrate history` - Show migration history
- `migrate create` - Create new migration
- `migrate validate` - Validate migrations

#### `schema.py`

Schema inspection commands.

**Commands:**

- `schema show` - Show database schema

#### `db.py`

Database management commands.

**Commands:**

- `db init` - Initialize database and create migration tracking table
- `db ping` - Check database connection
- `db info` - Show database information
- `db reset` - Reset database by removing all tables
- `db query` - Execute a raw SurrealQL query
- `db version` - Show database version information

#### `orchestrate.py`

Multi-database orchestration commands.

**Commands:**

- `orchestrate deploy` - Deploy migrations across environments (sequential, parallel, rolling, canary)
- `orchestrate status` - Check deployment status of environments
- `orchestrate validate` - Validate environment configuration and connectivity

See the [CLI Reference](../cli.md#orchestrate-commands) for full options.

#### `common.py`

Common CLI utilities.

**Key Functions:**

- `display_info()` - Display info message
- `display_success()` - Display success message
- `display_error()` - Display error message
- `display_warning()` - Display warning message
- `spinner()` - Progress spinner context manager
- `format_output()` - Format output as table/json/yaml

## Settings

**Location:** `src/settings.py`

Application settings management.

**Key Functions:**

- `get_settings()` - Get application settings
- `get_db_config()` - Get database configuration from environment

**Example:**

```python
from surql.settings import get_db_config

config = get_db_config()  # Loads from environment variables
```

## Usage Patterns

### Basic CRUD

```python
from surql.connection.client import get_client
from surql.connection.config import ConnectionConfig
from surql.query.crud import create_record, query_records

config = ConnectionConfig(url='ws://localhost:8000/rpc', ...)

async with get_client(config) as client:
  # Create
  user = await create_record('user', user_data, client=client)

  # Read
  users = await query_records('user', User, client=client)

  # Update
  await merge_record('user', 'alice', {'age': 31}, client=client)

  # Delete
  await delete_record('user', 'alice', client=client)
```

### Schema Definition

```python
from surql.schema.fields import string_field, int_field
from surql.schema.table import table_schema, unique_index, TableMode

schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('username'),
    int_field('age'),
  ],
  indexes=[
    unique_index('username_idx', ['username']),
  ],
)
```

### Migrations

```python
from surql.migration.discovery import discover_migrations
from surql.migration.executor import create_migration_plan, execute_migration_plan

migrations = discover_migrations(Path('migrations'))
plan = await create_migration_plan(client, migrations, MigrationDirection.UP)
await execute_migration_plan(client, plan)
```

## Type Safety

All public APIs use strict type hints. Use mypy for static type checking:

```shell
mypy src/
```

## Further Reading

- [Schema Definition Guide](../schema.md)
- [Migration System Guide](../migrations.md)
- [Query Builder Guide](../queries.md)
- [Query UX Helpers](../query-ux.md) - typed wrappers for `time::now`, `math::*`, `string::*`, `count_if`, `type_record`, and `aggregate_records`
- [SurrealDB v3 Patterns](../v3-patterns.md) - v3-required SurrealQL forms
- [Upgrade Notes](../migration.md) - 1.3.1 -> 1.4.0 -> 1.5.0 -> 1.5.1 migration guide
- [CLI Reference](../cli.md) - `migrate`, `schema`, `db`, `orchestrate` subcommands
- [Examples](../examples/index.md)
