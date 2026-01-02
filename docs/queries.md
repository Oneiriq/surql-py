# Query Builder & ORM Guide

This guide covers querying and manipulating data using reverie's type-safe query builder and ORM functions.

## Table of Contents

- [Overview](#overview)
- [CRUD Operations](#crud-operations)
- [Query Builder](#query-builder)
- [Filtering and Conditions](#filtering-and-conditions)
- [Graph Traversal](#graph-traversal)
- [Transactions](#transactions)
- [Type Safety](#type-safety)
- [Advanced Queries](#advanced-queries)
- [Best Practices](#best-practices)

## Overview

reverie provides two ways to interact with your database:

1. **High-level CRUD functions** - Simple async functions for common operations
2. **Query Builder** - Composable query construction with type safety

Both approaches integrate with Pydantic for data validation.

### Key Features

- **Type-safe queries** - Use Pydantic models for validation
- **Async-first** - All operations are asynchronous
- **Composable** - Build complex queries through composition
- **Graph-aware** - Native support for SurrealDB's graph features

## CRUD Operations

### Setup

```python
from pydantic import BaseModel, EmailStr
from src.connection.client import get_client
from src.settings import get_db_config

class User(BaseModel):
  username: str
  email: EmailStr
  age: int

# Get database client
config = get_db_config()
```

### Create Records

#### Create Single Record

```python
from src.query.crud import create_record

async def create_user():
  async with get_client(config) as client:
    user = await create_record(
      'user',
      User(
        username='alice',
        email='alice@example.com',
        age=30,
      ),
      client=client,
    )
    
    print(f"Created user: {user['id']}")
    return user
```

#### Create Multiple Records

```python
from src.query.crud import create_records

async def create_multiple_users():
  async with get_client(config) as client:
    users_data = [
      User(username='alice', email='alice@example.com', age=30),
      User(username='bob', email='bob@example.com', age=25),
      User(username='charlie', email='charlie@example.com', age=35),
    ]
    
    users = await create_records('user', users_data, client=client)
    print(f"Created {len(users)} users")
    return users
```

### Read Records

#### Get Single Record by ID

```python
from src.query.crud import get_record

async def get_user(user_id: str):
  async with get_client(config) as client:
    user = await get_record('user', user_id, User, client=client)
    
    if user:
      print(f"Found user: {user.username}")
    else:
      print("User not found")
    
    return user
```

#### Query Multiple Records

```python
from src.query.crud import query_records

async def get_adult_users():
  async with get_client(config) as client:
    users = await query_records(
      'user',
      User,
      conditions=['age >= 18'],
      order_by=('age', 'DESC'),
      limit=10,
      client=client,
    )
    
    for user in users:
      print(f"{user.username}: {user.age} years old")
    
    return users
```

#### Get First/Last Record

```python
from src.query.crud import first, last

async def get_newest_user():
  async with get_client(config) as client:
    user = await first(
      'user',
      User,
      order_by=('created_at', 'DESC'),
      client=client,
    )
    return user

async def get_oldest_user():
  async with get_client(config) as client:
    user = await first(
      'user',
      User,
      order_by=('created_at', 'ASC'),
      client=client,
    )
    return user
```

#### Count Records

```python
from src.query.crud import count_records

async def count_active_users():
  async with get_client(config) as client:
    total = await count_records('user', client=client)
    active = await count_records('user', 'is_active = true', client=client)
    
    print(f"Total users: {total}")
    print(f"Active users: {active}")
    
    return active
```

#### Check if Record Exists

```python
from src.query.crud import exists

async def user_exists(user_id: str):
  async with get_client(config) as client:
    if await exists('user', user_id, client=client):
      print("User exists")
    else:
      print("User not found")
```

### Update Records

#### Update Entire Record

```python
from src.query.crud import update_record

async def update_user(user_id: str):
  async with get_client(config) as client:
    updated = await update_record(
      'user',
      user_id,
      User(
        username='alice',
        email='alice.new@example.com',
        age=31,
      ),
      client=client,
    )
    
    print(f"Updated user: {updated}")
    return updated
```

#### Merge Partial Data

```python
from src.query.crud import merge_record

async def update_user_email(user_id: str, new_email: str):
  async with get_client(config) as client:
    updated = await merge_record(
      'user',
      user_id,
      {'email': new_email},
      client=client,
    )
    
    print(f"Updated email: {updated}")
    return updated
```

### Delete Records

#### Delete Single Record

```python
from src.query.crud import delete_record

async def delete_user(user_id: str):
  async with get_client(config) as client:
    await delete_record('user', user_id, client=client)
    print("User deleted")
```

#### Delete Multiple Records

```python
from src.query.crud import delete_records

async def delete_inactive_users():
  async with get_client(config) as client:
    await delete_records(
      'user',
      'is_active = false AND last_login < time::now() - 1y',
      client=client,
    )
    print("Inactive users deleted")
```

## Query Builder

The query builder provides a functional approach to building complex queries.

### Basic Query Construction

```python
from src.query.builder import Query

# Build a query
query = (
  Query()
    .select(['username', 'email', 'age'])
    .from_table('user')
    .where('age >= 18')
    .order_by('age', 'DESC')
    .limit(10)
)

# Convert to SurrealQL
sql = query.to_surql()
print(sql)
# SELECT username, email, age FROM user WHERE age >= 18 ORDER BY age DESC LIMIT 10
```

### Query Execution

```python
from src.query.executor import fetch_all, fetch_one

async def execute_query():
  async with get_client(config) as client:
    query = Query().select().from_table('user').where('age >= 18')
    
    # Fetch all results
    users = await fetch_all(query, User, client)
    
    for user in users:
      print(user.username)
```

### Select Queries

```python
# Select all fields
Query().select().from_table('user')

# Select specific fields
Query().select(['username', 'email']).from_table('user')

# Select with alias
Query().select(['username AS name', 'email']).from_table('user')

# Select with function
Query().select(['count()']).from_table('user')

# Select with aggregation
Query().select([
  'category',
  'count() AS total',
  'avg(price) AS avg_price'
]).from_table('product').group_by(['category'])
```

### Insert Queries

```python
# Insert single record
Query().insert('user', {
  'username': 'alice',
  'email': 'alice@example.com',
  'age': 30,
})

# Insert multiple records
Query().insert('user', [
  {'username': 'alice', 'email': 'alice@example.com'},
  {'username': 'bob', 'email': 'bob@example.com'},
])
```

### Update Queries

```python
# Update all records
Query().update('user').set({'is_active': True})

# Update with condition
(
  Query()
    .update('user')
    .set({'is_active': False})
    .where('last_login < time::now() - 30d')
)

# Update specific record
Query().update('user:alice').set({'email': 'new@example.com'})
```

### Delete Queries

```python
# Delete all records
Query().delete('user')

# Delete with condition
Query().delete('user').where('is_active = false')

# Delete specific record
Query().delete('user:alice')
```

## Filtering and Conditions

### Simple Conditions

```python
# String comparison
Query().select().from_table('user').where('status = "active"')

# Numeric comparison
Query().select().from_table('user').where('age >= 18')

# Boolean
Query().select().from_table('user').where('is_verified = true')

# NULL check
Query().select().from_table('user').where('deleted_at IS NULL')
```

### Multiple Conditions

```python
# AND conditions
query = (
  Query()
    .select()
    .from_table('user')
    .where('age >= 18')
    .where('is_active = true')
    .where('is_verified = true')
)

# OR conditions (use raw SQL)
query = Query().select().from_table('user').where(
  'age < 18 OR (age >= 18 AND is_verified = true)'
)
```

### Using Operators

```python
from src.types.operators import eq, gt, lt, gte, lte, contains, in_list

# Equality
Query().select().from_table('user').where(eq('status', 'active'))

# Comparison
Query().select().from_table('user').where(gte('age', 18))

# String contains
Query().select().from_table('user').where(contains('email', '@example.com'))

# IN list
Query().select().from_table('user').where(
  in_list('status', ['active', 'pending'])
)
```

### Range Queries

```python
# Between dates
Query().select().from_table('post').where(
  'created_at >= "2024-01-01" AND created_at <= "2024-12-31"'
)

# Price range
Query().select().from_table('product').where(
  'price >= 10 AND price <= 100'
)
```

### Pattern Matching

```python
# Contains
Query().select().from_table('user').where('email CONTAINS "@example.com"')

# Starts with
Query().select().from_table('user').where('username ~ "^admin"')

# Case-insensitive
Query().select().from_table('user').where('LOWERCASE(email) CONTAINS "gmail"')
```

## Graph Traversal

SurrealDB's graph features allow traversing relationships.

### Basic Graph Queries

```python
# Get posts liked by a user
async def get_user_likes(user_id: str):
  async with get_client(config) as client:
    query = f"SELECT * FROM {user_id}->likes->post"
    result = await client.execute(query)
    return result

# Get users who liked a post
async def get_post_likes(post_id: str):
  async with get_client(config) as client:
    query = f"SELECT * FROM {post_id}<-likes<-user"
    result = await client.execute(query)
    return result
```

### Multi-Hop Traversal

```python
# Get followers of followers
async def get_followers_of_followers(user_id: str):
  async with get_client(config) as client:
    query = f"SELECT * FROM {user_id}<-follows<-user<-follows<-user"
    result = await client.execute(query)
    return result

# Get friend suggestions (friends of friends)
async def get_friend_suggestions(user_id: str):
  async with get_client(config) as client:
    query = f"""
      SELECT * FROM {user_id}->follows->user->follows->user
      WHERE id != {user_id}
    """
    result = await client.execute(query)
    return result
```

### Graph with Filters

```python
# Get active followers
async def get_active_followers(user_id: str):
  async with get_client(config) as client:
    query = f"""
      SELECT * FROM {user_id}<-follows<-user
      WHERE is_active = true
      ORDER BY followed_at DESC
    """
    result = await client.execute(query)
    return result

# Get recent likes
async def get_recent_likes(user_id: str):
  async with get_client(config) as client:
    query = f"""
      SELECT * FROM {user_id}->likes->post
      WHERE liked_at > time::now() - 7d
      ORDER BY liked_at DESC
    """
    result = await client.execute(query)
    return result
```

### Creating Relationships

```python
async def create_follow(follower_id: str, followed_id: str):
  async with get_client(config) as client:
    result = await client.execute(
      f"RELATE {follower_id}->follows->{followed_id}"
    )
    return result

async def create_like(user_id: str, post_id: str, reaction: str = 'like'):
  async with get_client(config) as client:
    result = await client.execute(
      f"RELATE {user_id}->likes->{post_id} SET reaction = '{reaction}'"
    )
    return result
```

### Deleting Relationships

```python
async def delete_follow(follower_id: str, followed_id: str):
  async with get_client(config) as client:
    result = await client.execute(
      f"DELETE {follower_id}->follows WHERE out = {followed_id}"
    )
    return result
```

## Transactions

Execute multiple operations atomically.

### Using Context Manager

```python
from src.connection.transaction import transaction

async def transfer_credits(from_user: str, to_user: str, amount: int):
  async with get_client(config) as client:
    async with transaction(client):
      # Deduct from sender
      await client.execute(
        f"UPDATE {from_user} SET credits -= {amount}"
      )
      
      # Add to receiver
      await client.execute(
        f"UPDATE {to_user} SET credits += {amount}"
      )
      
      # Log transaction
      await client.execute(
        f"""
        CREATE transaction SET
          from = {from_user},
          to = {to_user},
          amount = {amount},
          timestamp = time::now()
        """
      )
```

### Manual Transaction Control

```python
async def manual_transaction():
  async with get_client(config) as client:
    try:
      # Begin transaction
      await client.execute('BEGIN TRANSACTION')
      
      # Perform operations
      await client.execute('CREATE user SET name = "Alice"')
      await client.execute('CREATE post SET title = "Hello"')
      
      # Commit
      await client.execute('COMMIT TRANSACTION')
      
    except Exception as e:
      # Rollback on error
      await client.execute('CANCEL TRANSACTION')
      raise e
```

## Type Safety

### Using Pydantic Models

```python
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime

class User(BaseModel):
  username: str = Field(min_length=3, max_length=20)
  email: str
  age: int = Field(ge=0, le=150)
  is_active: bool = True
  created_at: Optional[datetime] = None
  
  @field_validator('email')
  @classmethod
  def validate_email(cls, v: str) -> str:
    if '@' not in v:
      raise ValueError('Invalid email')
    return v.lower()

# Type-safe CRUD
async def type_safe_create():
  async with get_client(config) as client:
    # Pydantic validates on creation
    user = User(
      username='alice',
      email='alice@example.com',
      age=30,
    )
    
    # Type-safe create
    result = await create_record('user', user, client=client)
    
    # Type-safe read
    fetched = await get_record('user', 'alice', User, client=client)
    
    # TypedDict hints work in IDE
    if fetched:
      print(fetched.username)  # IDE autocomplete works
```

### Generic Queries

```python
from typing import TypeVar, Generic
from pydantic import BaseModel

T = TypeVar('T', bound=BaseModel)

async def get_all_records(table: str, model: type[T]) -> list[T]:
  """Generic function to fetch all records of any type."""
  async with get_client(config) as client:
    return await query_records(table, model, client=client)

# Use with different models
users = await get_all_records('user', User)
posts = await get_all_records('post', Post)
```

## Advanced Queries

### Aggregation

```python
# Count by category
async def count_by_category():
  async with get_client(config) as client:
    query = """
      SELECT
        category,
        count() AS total,
        avg(price) AS avg_price,
        min(price) AS min_price,
        max(price) AS max_price
      FROM product
      GROUP BY category
      ORDER BY total DESC
    """
    result = await client.execute(query)
    return result
```

### Subqueries

```python
# Get users with above-average age
async def users_above_avg_age():
  async with get_client(config) as client:
    query = """
      SELECT * FROM user
      WHERE age > (SELECT math::mean(age) FROM user)
    """
    result = await client.execute(query)
    return result
```

### Joins (Graph Relations)

```python
# Get posts with author information
async def posts_with_authors():
  async with get_client(config) as client:
    query = """
      SELECT
        *,
        author.* AS author_info
      FROM post
      FETCH author
    """
    result = await client.execute(query)
    return result
```

### Full-Text Search

```python
# Search across indexed fields
async def search_posts(search_term: str):
  async with get_client(config) as client:
    query = f"""
      SELECT * FROM post
      WHERE title @@ '{search_term}' OR content @@ '{search_term}'
      ORDER BY created_at DESC
    """
    result = await client.execute(query)
    return result
```

### Pagination

```python
async def paginate_users(page: int = 1, page_size: int = 10):
  async with get_client(config) as client:
    offset = (page - 1) * page_size
    
    users = await query_records(
      'user',
      User,
      order_by=('created_at', 'DESC'),
      limit=page_size,
      offset=offset,
      client=client,
    )
    
    total = await count_records('user', client=client)
    
    return {
      'users': users,
      'page': page,
      'page_size': page_size,
      'total': total,
      'total_pages': (total + page_size - 1) // page_size,
    }
```

### Batch Operations

```python
async def batch_update_status(user_ids: list[str], status: str):
  async with get_client(config) as client:
    for user_id in user_ids:
      await merge_record('user', user_id, {'status': status}, client=client)

# Or use SurrealQL for efficiency
async def batch_update_efficient(status: str):
  async with get_client(config) as client:
    query = f"""
      UPDATE user SET status = '{status}'
      WHERE id IN {user_ids}
    """
    await client.execute(query)
```

## Best Practices

### 1. Use Type-Safe Models

```python
# Good - Type-safe with Pydantic
class User(BaseModel):
  username: str
  email: str

user = await get_record('user', 'alice', User, client)

# Avoid - Untyped dictionaries
user = await client.select('user:alice')
```

### 2. Reuse Database Connections

```python
# Good - Reuse connection
async with get_client(config) as client:
  user1 = await create_record('user', user_data1, client=client)
  user2 = await create_record('user', user_data2, client=client)

# Avoid - Multiple connections
user1 = await create_record('user', user_data1)
user2 = await create_record('user', user_data2)
```

### 3. Handle Errors Gracefully

```python
from src.connection.client import QueryError

async def safe_query():
  try:
    async with get_client(config) as client:
      user = await get_record('user', 'alice', User, client=client)
  except QueryError as e:
    print(f"Query failed: {e}")
    return None
```

### 4. Use Indexes for Performance

```python
# Good - Uses index on email
users = await query_records(
  'user',
  User,
  conditions=['email = "alice@example.com"'],
  client=client,
)

# Avoid - Full table scan
users = await query_records(
  'user',
  User,
  conditions=['LOWERCASE(email) = "alice@example.com"'],
  client=client,
)
```

### 5. Limit Result Sets

```python
# Good - Limited results
users = await query_records('user', User, limit=100, client=client)

# Avoid - Unbounded queries
users = await query_records('user', User, client=client)  # Could return millions
```

### 6. Use Transactions for Related Operations

```python
# Good - Atomic operation
async with transaction(client):
  await create_record('user', user, client=client)
  await create_record('profile', profile, client=client)

# Avoid - Separate operations (can fail partially)
await create_record('user', user, client=client)
await create_record('profile', profile, client=client)
```

### 7. Validate Input Data

```python
# Pydantic validates automatically
try:
  user = User(username='ab', email='invalid')  # Raises ValidationError
except ValidationError as e:
  print(f"Invalid data: {e}")
```

### 8. Use Context Managers

```python
# Good - Automatic cleanup
async with get_client(config) as client:
  result = await client.execute(query)

# Avoid - Manual cleanup
client = DatabaseClient(config)
await client.connect()
result = await client.execute(query)
await client.disconnect()
```

## Next Steps

- Explore [Schema Definition](schema.md) for defining data structures
- Learn about [Migrations](migrations.md) for schema management
- Check out [CLI Reference](cli.md) for command-line tools
- See [Examples](examples/advanced_queries.py) for complex query patterns
