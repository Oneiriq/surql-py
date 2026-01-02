# API Reference

This document provides an overview of reverie's API modules. For detailed implementation, refer to the source code.

## Table of Contents

- [Schema Module](#schema-module)
- [Migration Module](#migration-module)
- [Query Module](#query-module)
- [Connection Module](#connection-module)
- [Types Module](#types-module)
- [CLI Module](#cli-module)

## Schema Module

**Location:** [`src/schema/`](../../src/schema/)

Defines database schemas using functional composition.

### Key Files

#### [`fields.py`](../../src/schema/fields.py)

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
from src.schema.fields import string_field, int_field

name_field = string_field('name', assertion='string::len($value) > 0')
age_field = int_field('age', assertion='$value >= 0')
```

#### [`table.py`](../../src/schema/table.py)

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
from src.schema.table import table_schema, unique_index, TableMode

schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[...],
  indexes=[unique_index('email_idx', ['email'])],
)
```

#### [`edge.py`](../../src/schema/edge.py)

Edge (relationship) schema definitions.

**Key Classes:**

- `EdgeDefinition` - Immutable edge schema definition

**Key Functions:**

- `edge_schema()` - Create an edge schema

**Example:**

```python
from src.schema.edge import edge_schema

follows = edge_schema('follows', from_table='user', to_table='user')
```

## Migration Module

**Location:** [`src/migration/`](../../src/migration/)

Database migration generation, execution, and tracking.

### Key Migration Files

#### [`generator.py`](../../src/migration/generator.py)

Migration file generation.

**Key Functions:**

- `create_blank_migration()` - Create a blank migration file

#### [`executor.py`](../../src/migration/executor.py)

Migration execution engine.

**Key Functions:**

- `execute_migration_plan()` - Execute a migration plan
- `create_migration_plan()` - Create a migration execution plan
- `validate_migrations()` - Validate migration files

#### [`discovery.py`](../../src/migration/discovery.py)

Migration file discovery and loading.

**Key Classes:**

- `MigrationFile` - Loaded migration file representation

**Key Functions:**

- `discover_migrations()` - Find and load migration files
- `validate_migration_name()` - Validate migration filename

#### [`history.py`](../../src/migration/history.py)

Migration history tracking.

**Key Classes:**

- `MigrationHistory` - Migration history record

**Key Functions:**

- `ensure_migration_table()` - Create migration history table
- `get_applied_migrations()` - Get list of applied migrations
- `record_migration()` - Record a migration as applied

#### [`models.py`](../../src/migration/models.py)

Migration data models.

**Key Classes:**

- `MigrationDirection` - Enum for UP/DOWN
- `MigrationState` - Enum for PENDING/APPLIED
- `MigrationPlan` - Migration execution plan

## Query Module

**Location:** [`src/query/`](../../src/query/)

Query building and ORM operations.

### Key Query Files

#### [`crud.py`](../../src/query/crud.py)

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
- `count_records()` - Count records
- `exists()` - Check if record exists
- `first()` - Get first matching record
- `last()` - Get last matching record

**Example:**

```python
from src.query.crud import create_record, query_records

user = await create_record('user', user_data, client=client)
users = await query_records('user', User, conditions=['age >= 18'], client=client)
```

#### [`builder.py`](../../src/query/builder.py)

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
from src.query.builder import Query

query = (
  Query()
    .select(['name', 'email'])
    .from_table('user')
    .where('age >= 18')
    .limit(10)
)
```

#### [`executor.py`](../../src/query/executor.py)

Query execution.

**Key Functions:**

- `fetch_all()` - Execute query and return all results
- `fetch_one()` - Execute query and return first result

#### [`expressions.py`](../../src/query/expressions.py)

Type-safe query expressions.

**Key Classes:**

- `Expression` - Query expression wrapper

## Connection Module

**Location:** [`src/connection/`](../../src/connection/)

Database connection management.

### Key Connection Files

#### [`client.py`](../../src/connection/client.py)

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
from src.connection.client import get_client

async with get_client(config) as client:
  result = await client.execute('SELECT * FROM user')
```

#### [`config.py`](../../src/connection/config.py)

Connection configuration.

**Key Classes:**

- `ConnectionConfig` - Database connection configuration

#### [`context.py`](../../src/connection/context.py)

Connection context management.

**Key Functions:**

- `get_db()` - Get current database client from context
- `set_db()` - Set database client in context
- `db_context()` - Context manager for scoped connections

#### [`transaction.py`](../../src/connection/transaction.py)

Transaction support.

**Key Classes:**

- `Transaction` - Transaction wrapper

**Key Functions:**

- `transaction()` - Context manager for transactions

**Example:**

```python
from src.connection.transaction import transaction

async with transaction(client):
  await client.execute('UPDATE user:alice SET credits -= 10')
  await client.execute('UPDATE user:bob SET credits += 10')
```

## Types Module

**Location:** [`src/types/`](../../src/types/)

Type definitions and utilities.

### Key Types Files

#### [`record_id.py`](../../src/types/record_id.py)

RecordID type for SurrealDB record identifiers.

**Key Classes:**

- `RecordID` - Type-safe record ID wrapper

**Key Methods:**

- `parse()` - Parse from string
- `__str__()` - Convert to string

**Example:**

```python
from src.types.record_id import RecordID

record_id = RecordID(table='user', id='alice')
print(record_id)  # user:alice
```

#### [`operators.py`](../../src/types/operators.py)

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

## CLI Module

**Location:** [`src/cli/`](../../src/cli/)

Command-line interface implementation.

### Key CLI Files

#### [`migrate.py`](../../src/cli/migrate.py)

Migration CLI commands.

**Commands:**

- `migrate up` - Apply migrations
- `migrate down` - Rollback migrations
- `migrate status` - Show migration status
- `migrate history` - Show migration history
- `migrate create` - Create new migration
- `migrate validate` - Validate migrations

#### [`schema.py`](../../src/cli/schema.py)

Schema inspection commands.

**Commands:**

- `schema show` - Show database schema

#### [`db.py`](../../src/cli/db.py)

Database management commands.

**Commands:**

- `db ping` - Check database connection
- `db info` - Show database information

#### [`common.py`](../../src/cli/common.py)

Common CLI utilities.

**Key Functions:**

- `display_info()` - Display info message
- `display_success()` - Display success message
- `display_error()` - Display error message
- `display_warning()` - Display warning message
- `spinner()` - Progress spinner context manager
- `format_output()` - Format output as table/json/yaml

## Settings

**Location:** [`src/settings.py`](../../src/settings.py)

Application settings management.

**Key Functions:**

- `get_settings()` - Get application settings
- `get_db_config()` - Get database configuration from environment

**Example:**

```python
from src.settings import get_db_config

config = get_db_config()  # Loads from environment variables
```

## Usage Patterns

### Basic CRUD

```python
from src.connection.client import get_client
from src.connection.config import ConnectionConfig
from src.query.crud import create_record, query_records

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
from src.schema.fields import string_field, int_field
from src.schema.table import table_schema, unique_index, TableMode

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
from src.migration.discovery import discover_migrations
from src.migration.executor import create_migration_plan, execute_migration_plan

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
- [CLI Reference](../cli.md)
- [Examples](../examples/)
