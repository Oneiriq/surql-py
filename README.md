# surql

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/downloads/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-1.0%2B-ff00a0)](https://surrealdb.com/)

A code-first database toolkit for [SurrealDB](https://surrealdb.com/). Define schemas, generate migrations, build queries, and perform typed CRUD -- all from Python.

## Features

- **Code-First Migrations** - Schema changes defined in code with automatic migration generation
- **Type-Safe Query Builder** - Composable queries with Pydantic model integration
- **SurrealDB v3 Ready** - Emits v3-correct SurrealQL (datetime casts, `count() GROUP ALL`, `type::record`, buffered transactions, idempotent DDL)
- **Query UX Helpers** - First-class wrappers for `time::now`, `math::*`, `string::*`, `count_if`, `type_record`, and typed aggregations -- no raw SurrealQL required
- **Vector Search** - HNSW and MTREE index support with 8 distance metrics and EFC/M tuning
- **Graph Traversal** - Native SurrealDB graph features with edge relationships
- **Query Caching** - Memory and Redis-backed caching with `@cache_query` decorator
- **Live Queries** - Real-time change notifications and streaming
- **Schema Visualization** - Mermaid, GraphViz, and ASCII diagrams
- **CLI Tools** - Migrations, schema inspection, multi-environment orchestration, validation, and database management
- **Async-First** - Built with async/await, connection pooling, and retry logic

## Install

```shell
pip install oneiriq-surql

# or with uv
uv add oneiriq-surql
```

## Quick Start

### Define a schema

```python
from surql.schema.fields import string_field, int_field, datetime_field
from surql.schema.table import table_schema, unique_index, TableMode

user_schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('name'),
    string_field('email', assertion='string::is::email($value)'),
    int_field('age', assertion='$value >= 0 AND $value <= 150'),
    datetime_field('created_at', default='time::now()', readonly=True),
  ],
  indexes=[unique_index('email_idx', ['email'])],
)
```

### Run migrations

```shell
surql migrate create "Add user table"
surql migrate up
surql migrate status
```

### Build queries with first-class helpers

```python
from surql import (
  Query,
  aggregate_records,
  count_if,
  math_mean_fn,
  math_sum_fn,
  time_now_fn,
  type_record,
)

# Fluent UPDATE with server-side function values
sql = (
  Query()
    .update('user:alice')
    .set(status='active', last_seen=time_now_fn())
    .to_surql()
)

# Typed aggregate -- GROUP ALL + count() rendered correctly for v3
rows = await aggregate_records(
  table='order',
  select={
    'total': count_if(),
    'revenue': math_sum_fn('amount'),
    'avg_ticket': math_mean_fn('amount'),
  },
  where="status = 'paid'",
  group_all=True,
)

# Record-ID construction without string concatenation
ref = type_record('user', 'alice').to_surql()
# -> type::record('user', 'alice')
```

### Deploy across environments

```shell
# Sequential deploy to staging then production
surql orchestrate deploy -e staging,production

# Rolling deploy in batches of 2
surql orchestrate deploy -e prod1,prod2,prod3,prod4 \
  --strategy rolling --batch-size 2
```

## Documentation

Full documentation at **[oneiriq.github.io/surql-py](https://oneiriq.github.io/surql-py/)**:

- [SurrealDB v3 patterns](https://oneiriq.github.io/surql-py/v3-patterns/) -- the forms surql emits for v3 compatibility
- [Query UX helpers](https://oneiriq.github.io/surql-py/query-ux/) -- typed wrappers for common SurrealQL calls
- [Upgrade notes](https://oneiriq.github.io/surql-py/migration/) -- 1.3.1 -> 1.4.0 -> 1.5.0 -> 1.5.1

## Requirements

- Python 3.12+
- SurrealDB 1.0+ (integration CI runs against SurrealDB v3.0.5)

## License

Apache License 2.0 - see [LICENSE](LICENSE).

## TypeScript / Deno / Node.js

Looking for SurrealDB tooling in TypeScript? Check out **[surql](https://github.com/Oneiriq/surql)** -- a type-safe query builder and client for SurrealDB available on JSR and NPM.

## Support

- Documentation: [oneiriq.github.io/surql-py](https://oneiriq.github.io/surql-py/)
- Issues: [GitHub Issues](https://github.com/Oneiriq/surql-py/issues)
- Changelog: [CHANGES](CHANGES)
