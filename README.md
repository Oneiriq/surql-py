# surql

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/downloads/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-1.0%2B-ff00a0)](https://surrealdb.com/)

surql is a code-first database toolkit for building modern applications with SurrealDB. It provides a seamless developer experience by integrating database operations directly into the codebase, allowing developers to define, query, and manipulate data using familiar programming constructs and **code-first migrations**.

## Features

- **Code-First Migrations** - Define and manage database schema changes directly in code with automatic migration generation
- **Type Safety** - Leverage Python's type hints with Pydantic for validation and reduced runtime errors
- **Schema Compatibility** - Support for both TYPE RELATION and SCHEMAFULL edge table modes
- **Vector Search** - Complete MTREE index support with 9 distance metrics (COSINE, EUCLIDEAN, MANHATTAN, HAMMING, MINKOWSKI, CHEBYSHEV, PEARSON, JACCARD, DOT)
- **Query Caching** - Memory and Redis-backed caching with `@cache_query` decorator
- **Live Queries** - Real-time change notifications and streaming support
- **Authentication** - Multi-level auth (ROOT, NAMESPACE, DATABASE, SCOPE) with token management
- **Schema Validation** - Validate code schemas against database with CI/CD integration
- **Schema Visualization** - Generate Mermaid, GraphViz, and ASCII diagrams
- **Migration Squashing** - Consolidate multiple migrations into optimized versions
- **Query Optimization Hints** - Guide query execution with index hints, parallel processing, timeouts, and fetch strategies
- **Multi-Database Orchestration** - Deploy migrations across multiple environments with sequential, parallel, rolling, and canary strategies
- **Schema Versioning & Rollback** - Track schema evolution with snapshots and safely rollback to previous versions with safety analysis
- **Git Hooks** - Pre-commit schema drift detection
- **Batch Operations** - Efficient bulk inserts, upserts, and relationship creation
- **Advanced Graph Queries** - GraphQuery builder with shortest path and degree calculation
- **Result Utilities** - Built-in utilities for extracting data from SurrealDB responses ([`extract_result()`](src/query/results.py), [`extract_one()`](src/query/results.py), [`extract_scalar()`](src/query/results.py), [`has_results()`](src/query/results.py))
- **Functional Composition** - Pure functions and immutable data structures for predictable, testable code
- **Async-First** - Built with async/await for high-performance database operations with connection pooling and retry logic
- **Schema Definition** - Declarative schema definitions with fields, indexes, events, and permissions
- **Query Builder** - Composable, type-safe query building with Pydantic model integration
- **Graph Traversal** - Native support for SurrealDB's graph features and edge relationships with both TYPE RELATION and SCHEMAFULL modes
- **CLI Tools** - Comprehensive command-line interface for migrations and database management
- **Testing Utilities** - Tools to facilitate testing of database interactions

## Quick Start

### Installation

```shell
# Using pip
pip install oneiriq-surql

# Using uv (recommended)
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

### Create a Migration

```shell
# Create a new migration file
surql migrate create "Add user table"

# Apply migrations
surql migrate up

# Check migration status
surql migrate status
```

### Perform CRUD Operations

```python
from pydantic import BaseModel
from surql.connection.client import DatabaseClient, get_client
from surql.connection.config import ConnectionConfig
from surql.query.crud import create_record, query_records

class User(BaseModel):
  name: str
  email: str
  age: int

# Connect to database
config = ConnectionConfig(
  url='ws://localhost:8000/rpc',
  namespace='test',
  database='test',
  username='root',
  password='root',
)

async with get_client(config) as client:
  # Create a user
  user = await create_record('user', User(
    name='Alice',
    email='alice@example.com',
    age=30,
  ))

  # Query users
  users = await query_records(
    'user',
    User,
    conditions=['age > 18'],
    order_by=('created_at', 'DESC'),
    limit=10,
  )

  for user in users:
    print(f'{user.name} - {user.email}')
```

## Documentation

Full documentation is available at **[oneiriq.github.io/surql-py](https://oneiriq.github.io/surql-py/)**.

- [Installation Guide](https://oneiriq.github.io/surql-py/installation/) - Detailed installation and setup
- [Quick Start Tutorial](https://oneiriq.github.io/surql-py/quickstart/) - Step-by-step tutorial
- [Schema Definition Guide](https://oneiriq.github.io/surql-py/schema/) - Complete schema definition reference
- [Migration System](https://oneiriq.github.io/surql-py/migrations/) - Migration creation and management
- [Query Builder & ORM](https://oneiriq.github.io/surql-py/queries/) - Querying and CRUD operations
- [Query Caching](https://oneiriq.github.io/surql-py/caching/) - Memory and Redis-backed caching strategies
- [Live Queries & Streaming](https://oneiriq.github.io/surql-py/streaming/) - Real-time data notifications
- [CLI Reference](https://oneiriq.github.io/surql-py/cli/) - Command-line interface documentation
- [API Reference](https://oneiriq.github.io/surql-py/api/) - Module and function reference
- [Examples](https://oneiriq.github.io/surql-py/examples/) - Working code examples

## Architecture

surql is built on several core principles:

- **Functional Composition** - Favor pure functions and composition over inheritance
- **Type Safety** - Strict typing with mypy and runtime validation with Pydantic
- **Immutability** - Immutable data structures and pure transformations
- **Async-First** - All database operations are asynchronous
- **Modular Design** - Small, focused modules with single responsibilities

## Project Structure

```shell
surql/
├── src/
│   ├── schema/          # Schema definition layer
│   │   ├── fields.py    # Field type definitions
│   │   ├── table.py     # Table schema composition
│   │   ├── edge.py      # Edge/relationship schemas
│   │   ├── validator.py # Schema validation
│   │   └── visualize.py # Schema visualization
│   ├── migration/       # Migration system
│   │   ├── generator.py # Migration generation
│   │   ├── executor.py  # Migration execution
│   │   ├── discovery.py # Migration file discovery
│   │   ├── history.py   # Migration tracking
│   │   ├── hooks.py     # Git hooks integration
│   │   ├── squash.py    # Migration squashing
│   │   └── watcher.py   # Schema change watching
│   ├── query/           # Query builder and ORM
│   │   ├── builder.py   # Query builder
│   │   ├── crud.py      # CRUD operations
│   │   ├── executor.py  # Query execution
│   │   ├── graph.py     # Graph traversal
│   │   ├── batch.py     # Batch operations
│   │   ├── results.py   # Result extraction
│   │   └── expressions.py # Query expressions
│   ├── connection/      # Database connection
│   │   ├── client.py    # Async client wrapper
│   │   ├── config.py    # Connection configuration
│   │   ├── context.py   # Context management
│   │   ├── auth.py      # Authentication
│   │   ├── streaming.py # Live queries
│   │   ├── transaction.py # Transactions
│   │   └── registry.py  # Connection registry
│   ├── cache/           # Query caching
│   │   ├── backends.py  # Cache backends
│   │   ├── config.py    # Cache configuration
│   │   ├── decorator.py # Query decorators
│   │   └── manager.py   # Cache management
│   ├── types/           # Type definitions
│   │   ├── record_id.py # RecordID type
│   │   └── operators.py # Query operators
│   └── cli/             # CLI commands
│       ├── migrate.py   # Migration commands
│       ├── schema.py    # Schema commands
│       └── db.py        # Database commands
├── docs/                # Documentation
├── tests/               # Test suite
└── migrations/          # Migration files
```

## CLI Commands

```shell
# Migration commands
surql migrate up              # Apply pending migrations
surql migrate down            # Rollback last migration
surql migrate status          # Show migration status
surql migrate history         # Show applied migrations
surql migrate create <name>   # Create new migration
surql migrate squash          # Squash migrations

# Schema commands
surql schema show             # Show database schema
surql schema show <table>     # Show table schema
surql schema validate         # Validate against database
surql schema visualize        # Generate schema diagram
surql schema watch            # Watch for schema changes

# Database commands
surql db info                 # Show database information
surql db ping                 # Check database connection
surql db query <sql>          # Execute raw SurrealQL
```

## Requirements

- Python 3.12+
- SurrealDB 1.0+

## Development

```shell
# Clone the repository
git clone https://github.com/Oneiriq/surql-py.git
cd surql

# Install dependencies with uv
uv sync

# Run tests
pytest

# Run linting
ruff check src/

# Type checking
mypy src/
```

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes following the project's coding standards
4. Add tests for new functionality
5. Ensure all tests pass (`pytest`)
6. Run linting and type checking (`ruff check`, `mypy`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

### Coding Standards

- Follow PEP 8 style guidelines
- Use 2-space indentation
- Write docstrings for all public functions and classes
- Maintain type hints for all function signatures
- Prefer functional composition over inheritance
- Write tests for new features and bug fixes

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built on [SurrealDB](https://surrealdb.com/) - The ultimate multi-model database
- Uses [Pydantic](https://docs.pydantic.dev/) for data validation
- CLI powered by [Typer](https://typer.tiangolo.com/)

## Support

- Documentation: [oneiriq.github.io/surql-py](https://oneiriq.github.io/surql-py/)
- Issues: [GitHub Issues](https://github.com/Oneiriq/surql-py/issues)
- Discussions: [GitHub Discussions](https://github.com/Oneiriq/surql-py/discussions)

## Roadmap

See [CHANGES](CHANGES) for release history.
