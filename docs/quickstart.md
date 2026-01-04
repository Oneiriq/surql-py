# Quick Start Tutorial

This tutorial will walk you through creating your first reverie project, defining schemas, creating migrations, and performing CRUD operations.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [Define Your First Schema](#define-your-first-schema)
- [Create Your First Migration](#create-your-first-migration)
- [Connect to the Database](#connect-to-the-database)
- [Run Migrations](#run-migrations)
- [Perform CRUD Operations](#perform-crud-operations)
- [Working with Relationships](#working-with-relationships)
- [Complete Example](#complete-example)
- [Multiple Connections](#multiple-connections)
- [Next Steps](#next-steps)

## Prerequisites

Before starting, ensure you have:

- Python 3.12+ installed
- SurrealDB 1.0+ installed and running
- reverie installed (`pip install reverie` or `uv add reverie`)

If you haven't completed these steps, see the [Installation Guide](installation.md).

## Project Setup

### 1. Create a New Project

```shell
mkdir my-blog
cd my-blog
```

### 2. Create Virtual Environment (Optional but Recommended)

```shell
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install reverie

```shell
pip install reverie
```

### 4. Create Project Structure

```shell
mkdir migrations schemas
touch .env main.py
```

Your project structure should look like:

```
my-blog/
├── .env
├── main.py
├── migrations/
└── schemas/
```

### 5. Configure Environment

Edit `.env`:

```env
SURREAL_URL=ws://localhost:8000/rpc
SURREAL_NAMESPACE=blog
SURREAL_DATABASE=blog
SURREAL_USERNAME=root
SURREAL_PASSWORD=root
```

### 6. Start SurrealDB

In a separate terminal:

```shell
surreal start --user root --pass root memory
```

## Define Your First Schema

Let's create a blog with users and posts.

### Create User Schema

Create `schemas/user.py`:

```python
from reverie.schema.fields import string_field, datetime_field
from reverie.schema.table import table_schema, unique_index, TableMode

user_schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('username', assertion='string::len($value) >= 3'),
    string_field('email', assertion='string::is::email($value)'),
    string_field('full_name'),
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
  ],
  indexes=[
    unique_index('username_idx', ['username']),
    unique_index('email_idx', ['email']),
  ],
)
```

### Create Post Schema

Create `schemas/post.py`:

```python
from reverie.schema.fields import string_field, record_field, datetime_field, bool_field
from reverie.schema.table import table_schema, search_index, TableMode

post_schema = table_schema(
  'post',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('title', assertion='string::len($value) > 0'),
    string_field('content'),
    string_field('slug', assertion='string::len($value) > 0'),
    record_field('author', table='user'),
    bool_field('published', default='false'),
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
  ],
  indexes=[
    unique_index('slug_idx', ['slug']),
    search_index('content_search', ['title', 'content']),
  ],
)
```

## Create Your First Migration

### 1. Create Migration File

```shell
reverie migrate create "Create user and post tables"
```

This creates a file like `migrations/20260102_120000_create_user_and_post_tables.py`.

### 2. Edit the Migration

Open the migration file and add your schema definitions:

```python
"""Create user and post tables."""

def up() -> list[str]:
  """Apply migration."""
  return [
    # Create user table
    'DEFINE TABLE user SCHEMAFULL;',
    'DEFINE FIELD username ON TABLE user TYPE string ASSERT string::len($value) >= 3;',
    'DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);',
    'DEFINE FIELD full_name ON TABLE user TYPE string;',
    'DEFINE FIELD created_at ON TABLE user TYPE datetime DEFAULT time::now() READONLY;',
    'DEFINE FIELD updated_at ON TABLE user TYPE datetime DEFAULT time::now();',
    'DEFINE INDEX username_idx ON TABLE user COLUMNS username UNIQUE;',
    'DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;',

    # Create post table
    'DEFINE TABLE post SCHEMAFULL;',
    'DEFINE FIELD title ON TABLE post TYPE string ASSERT string::len($value) > 0;',
    'DEFINE FIELD content ON TABLE post TYPE string;',
    'DEFINE FIELD slug ON TABLE post TYPE string ASSERT string::len($value) > 0;',
    'DEFINE FIELD author ON TABLE post TYPE record<user>;',
    'DEFINE FIELD published ON TABLE post TYPE bool DEFAULT false;',
    'DEFINE FIELD created_at ON TABLE post TYPE datetime DEFAULT time::now() READONLY;',
    'DEFINE FIELD updated_at ON TABLE post TYPE datetime DEFAULT time::now();',
    'DEFINE INDEX slug_idx ON TABLE post COLUMNS slug UNIQUE;',
    'DEFINE INDEX content_search ON TABLE post COLUMNS title, content SEARCH;',
  ]

def down() -> list[str]:
  """Rollback migration."""
  return [
    'REMOVE INDEX content_search ON TABLE post;',
    'REMOVE INDEX slug_idx ON TABLE post;',
    'REMOVE TABLE post;',
    'REMOVE INDEX email_idx ON TABLE user;',
    'REMOVE INDEX username_idx ON TABLE user;',
    'REMOVE TABLE user;',
  ]

metadata = {
  'version': '20260102_120000',
  'description': 'Create user and post tables',
  'author': 'reverie',
  'depends_on': [],
}
```

## Connect to the Database

### Create Connection Helper

Create `main.py`:

```python
import asyncio
from reverie.connection.client import get_client
from reverie.connection.config import ConnectionConfig
from reverie.settings import get_db_config

async def get_db_client():
  """Get database client from configuration."""
  config = get_db_config()  # Loads from .env
  return get_client(config)
```

## Run Migrations

### 1. Check Migration Status

```shell
reverie migrate status
```

Output:
```
Migration Status
┌─────────────────────────────────────────┬────────┐
│ Version                                 │ Status │
├─────────────────────────────────────────┼────────┤
│ 20260102_120000_create_user_and_post... │ PENDING│
└─────────────────────────────────────────┴────────┘
Total: 1 | Applied: 0 | Pending: 1
```

### 2. Apply Migrations

```shell
reverie migrate up
```

Output:
```
Discovering migrations in migrations
Found 1 pending migration(s):
  • 20260102_120000: Create user and post tables
Successfully applied 1 migration(s)
```

### 3. Verify

```shell
reverie schema show
```

## Perform CRUD Operations

### Define Pydantic Models

Add to `main.py`:

```python
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime

class User(BaseModel):
  username: str = Field(min_length=3)
  email: EmailStr
  full_name: str
  created_at: Optional[datetime] = None
  updated_at: Optional[datetime] = None

class Post(BaseModel):
  title: str
  content: str
  slug: str
  author: str  # Will be record ID like "user:alice"
  published: bool = False
  created_at: Optional[datetime] = None
  updated_at: Optional[datetime] = None
```

### Create Records

Add to `main.py`:

```python
from reverie.query.crud import create_record, query_records, get_record

async def create_user_example():
  """Create a new user."""
  async with await get_db_client() as client:
    user = await create_record(
      'user',
      User(
        username='alice',
        email='alice@example.com',
        full_name='Alice Johnson',
      ),
      client=client,
    )

    print(f"Created user: {user['id']}")
    return user

async def create_post_example(author_id: str):
  """Create a new post."""
  async with await get_db_client() as client:
    post = await create_record(
      'post',
      Post(
        title='My First Blog Post',
        content='This is my first post using reverie!',
        slug='my-first-post',
        author=author_id,
        published=True,
      ),
      client=client,
    )

    print(f"Created post: {post['id']}")
    return post
```

### Query Records

```python
async def query_users_example():
  """Query all users."""
  async with await get_db_client() as client:
    users = await query_records(
      'user',
      User,
      client=client,
    )

    for user in users:
      print(f"{user.username} - {user.email}")

async def query_published_posts():
  """Query published posts."""
  async with await get_db_client() as client:
    posts = await query_records(
      'post',
      Post,
      conditions=['published = true'],
      order_by=('created_at', 'DESC'),
      limit=10,
      client=client,
    )

    for post in posts:
      print(f"{post.title} by {post.author}")
```

### Update Records

```python
from reverie.query.crud import update_record, merge_record

async def update_user_example(user_id: str):
  """Update a user."""
  async with await get_db_client() as client:
    updated = await update_record(
      'user',
      user_id,
      User(
        username='alice',
        email='alice.new@example.com',
        full_name='Alice Smith',
      ),
      client=client,
    )

    print(f"Updated user: {updated}")

async def merge_post_example(post_id: str):
  """Merge data into a post."""
  async with await get_db_client() as client:
    updated = await merge_record(
      'post',
      post_id,
      {'published': True},
      client=client,
    )

    print(f"Published post: {updated}")
```

### Delete Records

```python
from reverie.query.crud import delete_record

async def delete_post_example(post_id: str):
  """Delete a post."""
  async with await get_db_client() as client:
    await delete_record('post', post_id, client=client)
    print("Post deleted")
```

## Working with Relationships

### Create Edge Schema

Create `schemas/likes.py`:

```python
from reverie.schema.edge import edge_schema
from reverie.schema.fields import datetime_field

likes_edge = edge_schema(
  'likes',
  from_table='user',
  to_table='post',
  fields=[
    datetime_field('liked_at', default='time::now()', readonly=True),
  ],
)
```

### Create Edge Migration

```shell
reverie migrate create "Create likes edge"
```

Edit the migration file:

```python
def up() -> list[str]:
  return [
    'DEFINE TABLE likes SCHEMAFULL;',
    'DEFINE FIELD in ON TABLE likes TYPE record<user>;',
    'DEFINE FIELD out ON TABLE likes TYPE record<post>;',
    'DEFINE FIELD liked_at ON TABLE likes TYPE datetime DEFAULT time::now() READONLY;',
  ]

def down() -> list[str]:
  return [
    'REMOVE TABLE likes;',
  ]
```

Apply the migration:

```shell
reverie migrate up
```

### Create Relationships

```python
async def like_post_example(user_id: str, post_id: str):
  """Create a like relationship."""
  async with await get_db_client() as client:
    result = await client.execute(
      f'RELATE {user_id}->likes->{post_id}'
    )
    print(f"User liked post: {result}")

async def query_user_likes(user_id: str):
  """Query posts liked by a user."""
  async with await get_db_client() as client:
    result = await client.execute(
      f'SELECT * FROM {user_id}->likes->post'
    )
    print(f"User's liked posts: {result}")
```

## Complete Example

Here's a complete working example in `main.py`:

```python
import asyncio
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime

from reverie.connection.client import get_client
from reverie.settings import get_db_config
from reverie.query.crud import (
  create_record,
  query_records,
  get_record,
  merge_record,
)

class User(BaseModel):
  username: str = Field(min_length=3)
  email: EmailStr
  full_name: str

class Post(BaseModel):
  title: str
  content: str
  slug: str
  author: str
  published: bool = False

async def main():
  """Main application entry point."""
  config = get_db_config()

  async with get_client(config) as client:
    # Create a user
    print("Creating user...")
    user = await create_record(
      'user',
      User(
        username='alice',
        email='alice@example.com',
        full_name='Alice Johnson',
      ),
      client=client,
    )
    print(f"Created user: {user['id']}")

    # Create a post
    print("\nCreating post...")
    post = await create_record(
      'post',
      Post(
        title='Getting Started with reverie',
        content='reverie makes working with SurrealDB easy!',
        slug='getting-started',
        author=user['id'],
      ),
      client=client,
    )
    print(f"Created post: {post['id']}")

    # Publish the post
    print("\nPublishing post...")
    await merge_record(
      'post',
      post['id'].split(':')[1],
      {'published': True},
      client=client,
    )

    # Query published posts
    print("\nQuerying published posts...")
    posts = await query_records(
      'post',
      Post,
      conditions=['published = true'],
      client=client,
    )

    for p in posts:
      print(f"  - {p.title} (by {p.author})")

    # Create a like relationship
    print("\nCreating like relationship...")
    await client.execute(
      f"RELATE {user['id']}->likes->{post['id']}"
    )

    # Query liked posts
    print("\nQuerying user's liked posts...")
    result = await client.execute(
      f"SELECT * FROM {user['id']}->likes->post"
    )
    print(f"Liked posts: {result}")

if __name__ == '__main__':
  asyncio.run(main())
```

### Run the Example

```shell
python main.py
```

Expected output:

```
Creating user...
Created user: user:alice

Creating post...
Created post: post:abc123

Publishing post...

Querying published posts...
  - Getting Started with reverie (by user:alice)

Creating like relationship...

Querying user's liked posts...
Liked posts: [...]
```

## Next Steps

Now that you've completed the quick start, explore these topics:

1. **[Schema Definition Guide](schema.md)** - Learn about all schema features:
   - Field types and assertions
   - Events and triggers
   - Permissions
   - Computed fields

2. **[Migration System](migrations.md)** - Advanced migration topics:
   - Auto-generation from schema changes
   - Migration dependencies
   - Rollback strategies

3. **[Query Builder & ORM](queries.md)** - Advanced querying:
   - Complex filters and operators
   - Graph traversal
   - Transactions
   - Aggregations

4. **[CLI Reference](cli.md)** - Complete CLI documentation

5. **[Examples](examples/)** - More working examples:
   - Advanced schema patterns
   - Graph queries
   - Complex relationships

## Common Patterns

### Multiple Environments

Use different `.env` files for each environment:

```shell
# Development
cp .env .env.development

# Production
cp .env .env.production
```

Load the appropriate config:

```python
import os
from dotenv import load_dotenv

# Load environment-specific config
env = os.getenv('ENV', 'development')
load_dotenv(f'.env.{env}')
```

### Testing

Create a test configuration:

```python
# conftest.py
import pytest
from reverie.connection.config import ConnectionConfig

@pytest.fixture
async def test_db():
  config = ConnectionConfig(
    url='ws://localhost:8000/rpc',
    namespace='test',
    database='test_db',
    username='root',
    password='root',
  )

  async with get_client(config) as client:
    yield client
    # Cleanup after tests
    await client.execute('REMOVE DATABASE test_db')
```

### Context Manager Pattern

Use context managers for automatic cleanup:

```python
from reverie.connection.context import db_context

async def with_context_example():
  async with db_context(get_db_config()) as client:
    # Client is automatically connected
    user = await create_record('user', user_data, client=client)
    # Client is automatically disconnected
```

## Multiple Connections

For applications requiring connections to multiple databases (read replicas, analytics,
different environments), reverie provides a connection registry.

### When to Use Multiple Connections

- **Read replicas** - Route read queries to replicas for performance
- **Analytics databases** - Separate OLAP workloads from OLTP
- **Multi-tenant** - Connect to different tenant databases
- **Blue-green deployments** - Maintain connections to multiple environments

### Register Named Connections

```python
import asyncio
from reverie.connection.registry import ConnectionRegistry, get_registry
from reverie.connection.config import ConnectionConfig

async def setup_connections():
  """Register multiple database connections."""
  registry = get_registry()

  # Primary database (writes)
  await registry.register(
    'primary',
    ConnectionConfig(
      url='ws://localhost:8000/rpc',
      namespace='app',
      database='production',
      username='root',
      password='root',
    ),
    set_default=True,  # Use as default connection
  )

  # Read replica (reads)
  await registry.register(
    'replica',
    ConnectionConfig(
      url='ws://replica.internal:8000/rpc',
      namespace='app',
      database='production',
      username='reader',
      password='reader_pass',
    ),
  )

  # Analytics database
  await registry.register(
    'analytics',
    ConnectionConfig(
      url='ws://analytics.internal:8000/rpc',
      namespace='app',
      database='analytics',
      username='analyst',
      password='analyst_pass',
    ),
  )

  print(f"Registered connections: {registry.list_connections()}")
  # ['primary', 'replica', 'analytics']
```

### Use Named Connections

```python
from reverie.connection.registry import get_registry
from reverie.query.crud import create_record, query_records

async def use_connections():
  """Use different connections for different operations."""
  registry = get_registry()

  # Get default (primary) connection for writes
  primary = registry.get()  # or registry.get('primary')

  # Create record on primary
  user = await create_record(
    'user',
    User(username='alice', email='alice@example.com', full_name='Alice'),
    client=primary,
  )

  # Query from read replica
  replica = registry.get('replica')
  users = await query_records('user', User, client=replica)

  # Run analytics queries on analytics db
  analytics = registry.get('analytics')
  result = await analytics.execute('''
    SELECT category, count() as total
    FROM pageviews
    GROUP BY category
  ''')

async def cleanup():
  """Disconnect all connections on shutdown."""
  registry = get_registry()
  await registry.disconnect_all()
```

### Environment-Based Configuration

Configure named connections via environment variables:

```env
# Primary database
REVERIE_PRIMARY_DB_URL=ws://localhost:8000/rpc
REVERIE_PRIMARY_DB_NS=app
REVERIE_PRIMARY_DB=production
REVERIE_PRIMARY_DB_USER=root
REVERIE_PRIMARY_DB_PASS=root

# Read replica
REVERIE_REPLICA_DB_URL=ws://replica:8000/rpc
REVERIE_REPLICA_DB_NS=app
REVERIE_REPLICA_DB=production
REVERIE_REPLICA_DB_USER=reader
REVERIE_REPLICA_DB_PASS=reader_pass
```

Load from environment:

```python
from reverie.connection.config import NamedConnectionConfig
from reverie.connection.registry import get_registry

async def setup_from_env():
  """Load named connections from environment variables."""
  registry = get_registry()

  # Load configurations using REVERIE_{NAME}_ prefix
  primary_config = NamedConnectionConfig.from_env('PRIMARY')
  replica_config = NamedConnectionConfig.from_env('REPLICA')

  await registry.register(primary_config.name, primary_config.config, set_default=True)
  await registry.register(replica_config.name, replica_config.config)
```

## Troubleshooting

### Migration Not Found

If migrations aren't found:

```shell
# Verify migrations directory
ls migrations/

# Check migration file naming
# Should be: YYYYMMDD_HHMMSS_description.py
```

### Connection Issues

If connection fails:

```shell
# Test database connectivity
reverie db ping

# Check SurrealDB is running
surreal version
```

### Schema Validation Errors

If you get validation errors:

```python
# Enable verbose logging
import structlog
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(10))

# Run again to see detailed errors
```

## Additional Resources

- [SurrealDB Query Language](https://surrealdb.com/docs/surrealql)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [Python AsyncIO](https://docs.python.org/3/library/asyncio.html)
