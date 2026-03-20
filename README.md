# surql

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/downloads/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-1.0%2B-ff00a0)](https://surrealdb.com/)

A code-first database toolkit for [SurrealDB](https://surrealdb.com/). Define schemas, generate migrations, build queries, and perform typed CRUD -- all from Python.

## Features

- **Code-First Migrations** - Schema changes defined in code with automatic migration generation
- **Type-Safe Query Builder** - Composable queries with Pydantic model integration
- **Vector Search** - HNSW and MTREE index support with 8 distance metrics and EFC/M tuning
- **Graph Traversal** - Native SurrealDB graph features with edge relationships
- **Query Caching** - Memory and Redis-backed caching with `@cache_query` decorator
- **Live Queries** - Real-time change notifications and streaming
- **Schema Visualization** - Mermaid, GraphViz, and ASCII diagrams
- **CLI Tools** - Migrations, schema inspection, validation, and database management
- **Async-First** - Built with async/await, connection pooling, and retry logic

## Quick Start

```shell
pip install oneiriq-surql

# or with uv
uv add oneiriq-surql
```

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

```shell
surql migrate create "Add user table"
surql migrate up
surql migrate status
```

## Documentation

Full documentation at **[oneiriq.github.io/surql-py](https://oneiriq.github.io/surql-py/)**.

## Requirements

- Python 3.12+
- SurrealDB 1.0+

## License

Apache License 2.0 - see [LICENSE](LICENSE).

## TypeScript / Deno / Node.js

Looking for SurrealDB tooling in TypeScript? Check out **[surql](https://github.com/Oneiriq/surql)** -- a type-safe query builder and client for SurrealDB available on JSR and NPM.

## Support

- Documentation: [oneiriq.github.io/surql-py](https://oneiriq.github.io/surql-py/)
- Issues: [GitHub Issues](https://github.com/Oneiriq/surql-py/issues)
- Changelog: [CHANGES](CHANGES)
