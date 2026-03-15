# surql

**Code-first database toolkit for SurrealDB.**

surql provides a seamless developer experience by integrating database operations directly into your codebase. Define schemas, manage migrations, build queries, and perform CRUD operations using familiar Python constructs.

## Key Features

- **Code-First Migrations** - Define and manage schema changes directly in code with automatic generation
- **Type Safety** - Pydantic integration for validation and reduced runtime errors
- **Vector Search** - MTREE index support with 9 distance metrics and similarity scoring
- **Query Builder** - Composable, type-safe query building with Pydantic model integration
- **Graph Traversal** - Native support for SurrealDB's graph features and edge relationships
- **Schema Visualization** - Generate Mermaid, GraphViz, and ASCII diagrams
- **Live Queries** - Real-time change notifications and streaming support
- **Multi-Database Orchestration** - Deploy migrations across multiple environments
- **Async-First** - Built with async/await for high-performance operations

## Quick Start

### Installation

```shell
pip install oneiriq-surql

# or with uv (recommended)
uv add oneiriq-surql
```

### Define a Schema

```python
from surql.schema.fields import string_field, int_field, datetime_field
from surql.schema.table import table_schema, unique_index, TableMode

user_schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('name', assertion='string::len($value) > 0'),
    string_field('email', assertion='string::is::email($value)'),
    int_field('age', assertion='$value >= 0 AND $value <= 150'),
    datetime_field('created_at', default='time::now()', readonly=True),
  ],
  indexes=[
    unique_index('email_idx', ['email']),
  ],
)
```

### Run Migrations

```shell
surql migrate create "Add user table"
surql migrate up
surql migrate status
```

### Query Data

```python
from surql.query.builder import Query

query = (
  Query()
    .select(['name', 'email'])
    .from_table('user')
    .where('age >= 18')
    .order_by('name')
    .limit(10)
)

sql = query.to_surql()
```

## Requirements

- Python 3.12+
- SurrealDB 1.0+

## License

Apache License 2.0
