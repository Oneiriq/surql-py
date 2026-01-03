# Reverie

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/downloads/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-1.0%2B-ff00a0)](https://surrealdb.com/)

Reverie is a code-first database toolkit for building modern applications with SurrealDB. It provides a seamless developer experience by integrating database operations directly into the codebase, allowing developers to define, query, and manipulate data using familiar programming constructs and **code-first migrations**.

## Features

- **Code-First Migrations** - Define and manage database schema changes directly in code with automatic migration generation
- **Type Safety** - Leverage Python's type hints with Pydantic for validation and reduced runtime errors
- **Driftnet Compatible** - Full compatibility with driftnet patterns: angle bracket RecordIDs, MTREE vector indexes, SCHEMAFULL edge tables, and result extraction utilities
- **Vector Search** - Complete MTREE index support with 1024 dimensions, COSINE similarity, and all distance metrics (EUCLIDEAN, MANHATTAN, MINKOWSKI, CHEBYSHEV, HAMMING)
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
pip install reverie

# Using uv (recommended)
uv add reverie
```

### Define a Schema

```python
from reverie.schema.fields import string_field, int_field, datetime_field
from reverie.schema.table import table_schema, unique_index, TableMode

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
reverie migrate create "Add user table"

# Apply migrations
reverie migrate up

# Check migration status
reverie migrate status
```

### Perform CRUD Operations

```python
from pydantic import BaseModel
from reverie.connection.client import DatabaseClient, get_client
from reverie.connection.config import ConnectionConfig
from reverie.query.crud import create_record, query_records

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

- [Installation Guide](docs/installation.md) - Detailed installation and setup
- [Quick Start Tutorial](docs/quickstart.md) - Step-by-step tutorial
- [Schema Definition Guide](docs/schema.md) - Complete schema definition reference
- [Migration System](docs/migrations.md) - Migration creation and management
- [Query Builder & ORM](docs/queries.md) - Querying and CRUD operations
- [CLI Reference](docs/cli.md) - Command-line interface documentation
- [API Reference](docs/api/README.md) - Module and function reference
- [Driftnet Migration Guide](docs/driftnet_migration.md) - Migrating from surrealdb-py to reverie
- [Compatibility Analysis](plans/compat.md) - Complete driftnet compatibility verification
- [Examples](docs/examples/) - Working code examples
  - [MTREE Vector Search](docs/examples/mtree_vector_search.py) - 1024-dim COSINE indexes (driftnet-compatible)
  - [Driftnet Edge Tables](docs/examples/driftnet_edge_example.py) - SCHEMAFULL edge patterns
  - [Advanced Queries](docs/examples/advanced_queries.py) - Complex query patterns

## Architecture

reverie is built on several core principles:

- **Functional Composition** - Favor pure functions and composition over inheritance
- **Type Safety** - Strict typing with mypy and runtime validation with Pydantic
- **Immutability** - Immutable data structures and pure transformations
- **Async-First** - All database operations are asynchronous
- **Modular Design** - Small, focused modules with single responsibilities

See the [Architecture Document](plans/architecture.md) for detailed design information.

## Project Structure

```shell
reverie/
├── src/
│   ├── schema/          # Schema definition layer
│   │   ├── fields.py    # Field type definitions
│   │   ├── table.py     # Table schema composition
│   │   └── edge.py      # Edge/relationship schemas
│   ├── migration/       # Migration system
│   │   ├── generator.py # Migration generation
│   │   ├── executor.py  # Migration execution
│   │   ├── discovery.py # Migration file discovery
│   │   └── history.py   # Migration tracking
│   ├── query/           # Query builder and ORM
│   │   ├── builder.py   # Query builder
│   │   ├── crud.py      # CRUD operations
│   │   ├── executor.py  # Query execution
│   │   └── graph.py     # Graph traversal
│   ├── connection/      # Database connection
│   │   ├── client.py    # Async client wrapper
│   │   ├── config.py    # Connection configuration
│   │   └── context.py   # Context management
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
reverie migrate up              # Apply pending migrations
reverie migrate down            # Rollback last migration
reverie migrate status          # Show migration status
reverie migrate history         # Show applied migrations
reverie migrate create <name>   # Create new migration

# Schema commands
reverie schema show             # Show database schema
reverie schema show <table>     # Show table schema

# Database commands
reverie db info                 # Show database information
reverie db ping                 # Check database connection
```

## Requirements

- Python 3.12+
- SurrealDB 1.0+

## Development

```shell
# Clone the repository
git clone https://github.com/yourusername/reverie.git
cd reverie

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

- Documentation: [docs/](docs/)
- Issues: [GitHub Issues](https://github.com/yourusername/reverie/issues)
- Discussions: [GitHub Discussions](https://github.com/yourusername/reverie/discussions)

## Roadmap

- [x] Core schema definition system
- [x] Migration generation and execution
- [x] CRUD operations and query builder
- [x] CLI interface
- [ ] Auto-migration generation from schema changes
- [ ] Schema validation against database
- [ ] Advanced graph query helpers
- [ ] Query result caching
- [ ] Migration squashing
- [ ] Schema visualization

---
