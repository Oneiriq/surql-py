# Query Result Caching Guide

This guide covers reverie's query result caching system for improving application performance.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Backends](#backends)
- [The @cache_query Decorator](#the-cache_query-decorator)
- [Manual Cache Management](#manual-cache-management)
- [Invalidation Strategies](#invalidation-strategies)
- [Performance Considerations](#performance-considerations)
- [API Reference](#api-reference)

## Overview

reverie provides an optional caching layer for query results to reduce database load and improve response times. The caching system supports:

- **Multiple backends** - In-memory LRU cache or Redis for distributed caching
- **Configurable TTL** - Per-query and global time-to-live settings
- **Automatic invalidation** - Table-based and pattern-based cache invalidation
- **Decorator API** - Simple `@cache_query` decorator for automatic caching
- **Manager API** - Low-level control for advanced use cases

### Key Features

- **Async-first** - All cache operations are asynchronous
- **Type-safe** - Full type hints for better IDE support
- **Zero-config defaults** - Works out of the box with sensible defaults
- **Pluggable backends** - Easy to extend with custom backends

## Quick Start

### Basic Setup

```python
from reverie.cache import configure_cache, cache_query, CacheConfig

# Configure global cache (do this once at startup)
config = CacheConfig(
  backend='memory',
  default_ttl=300,  # 5 minutes
  max_size=1000,
)
configure_cache(config)

# Use the decorator for automatic caching
@cache_query(ttl=60)
async def get_active_users() -> list[dict]:
  async with get_client(config) as client:
    return await query_records('user', User, conditions=['is_active = true'], client=client)

# First call hits the database
users = await get_active_users()

# Subsequent calls within 60 seconds return cached results
users = await get_active_users()  # From cache - no database hit
```

### Invalidating Cache

```python
from reverie.cache import invalidate, clear_cache

# Invalidate specific key
await invalidate(key='user:123')

# Invalidate all queries for a table
await invalidate(table='user')

# Invalidate by pattern
await invalidate(pattern='user:*')

# Clear all cache
await clear_cache()
```

## Configuration

### CacheConfig Options

The [`CacheConfig`](src/reverie/cache/config.py:15) dataclass controls global cache behavior:

```python
from reverie.cache import CacheConfig

config = CacheConfig(
  enabled=True,           # Enable/disable caching globally
  backend='memory',       # 'memory' or 'redis'
  default_ttl=300,        # Default TTL in seconds (5 minutes)
  max_size=1000,          # Max entries for memory backend
  redis_url='redis://localhost:6379',  # Redis connection URL
  key_prefix='reverie:',  # Prefix for all cache keys
)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | `bool` | `True` | Enable/disable caching globally |
| `backend` | `str` | `'memory'` | Backend type: `'memory'` or `'redis'` |
| `default_ttl` | `int` | `300` | Default time-to-live in seconds |
| `max_size` | `int` | `1000` | Maximum entries for memory backend |
| `redis_url` | `str` | `'redis://localhost:6379'` | Redis connection URL |
| `key_prefix` | `str` | `'reverie:'` | Prefix applied to all cache keys |

### Per-Query Options

The [`CacheOptions`](src/reverie/cache/config.py:47) dataclass allows per-query customization:

```python
from reverie.cache import CacheOptions

options = CacheOptions(
  ttl=60,                    # Override default TTL
  key='my_custom_key',       # Use a static cache key
  invalidate_on=['user', 'role'],  # Tables that trigger invalidation
)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ttl` | `int \| None` | `None` | TTL override (uses global default if None) |
| `key` | `str \| None` | `None` | Static cache key (auto-generated if None) |
| `invalidate_on` | `list[str] \| None` | `None` | Tables that should invalidate this cache |

### Environment-Based Configuration

```python
import os
from reverie.cache import configure_cache, CacheConfig

config = CacheConfig(
  enabled=os.getenv('CACHE_ENABLED', 'true').lower() == 'true',
  backend=os.getenv('CACHE_BACKEND', 'memory'),
  default_ttl=int(os.getenv('CACHE_TTL', '300')),
  max_size=int(os.getenv('CACHE_MAX_SIZE', '1000')),
  redis_url=os.getenv('REDIS_URL', 'redis://localhost:6379'),
)

configure_cache(config)
```

## Backends

### Memory Backend (LRU)

The [`MemoryCache`](src/reverie/cache/backends.py:87) backend uses an in-memory LRU (Least Recently Used) cache. It's ideal for single-instance applications.

```python
from reverie.cache import configure_cache, CacheConfig

# Memory backend (default)
config = CacheConfig(
  backend='memory',
  max_size=2000,     # Maximum 2000 entries
  default_ttl=600,   # 10 minute TTL
)
configure_cache(config)
```

**Features:**

- Fast access (no network latency)
- Automatic LRU eviction when max_size is reached
- Per-entry TTL with automatic expiration
- Thread-safe with async locks

**Limitations:**

- Not shared across processes or instances
- Lost on application restart
- Memory usage grows with cache size

**When to use:**

- Single-instance applications
- Development and testing
- Low-latency requirements
- Small to medium cache sizes

### Redis Backend

The [`RedisCache`](src/reverie/cache/backends.py:219) backend uses Redis for distributed caching. It's ideal for multi-instance deployments.

```python
from reverie.cache import configure_cache, CacheConfig

# Redis backend
config = CacheConfig(
  backend='redis',
  redis_url='redis://localhost:6379',
  key_prefix='myapp:cache:',
  default_ttl=300,
)
configure_cache(config)
```

**Installation:**

The Redis backend requires the `redis` package:

```bash
# Install with cache extras
pip install reverie[cache]

# Or install redis directly
pip install redis
```

**Features:**

- Distributed caching across instances
- Persistent storage (survives restarts)
- Pattern-based key scanning
- Native TTL support

**Configuration options:**

```python
# Redis with authentication
config = CacheConfig(
  backend='redis',
  redis_url='redis://:password@localhost:6379/0',
)

# Redis with SSL
config = CacheConfig(
  backend='redis',
  redis_url='rediss://localhost:6379',  # Note: rediss:// for SSL
)

# Redis Sentinel
config = CacheConfig(
  backend='redis',
  redis_url='redis://sentinel1:26379,sentinel2:26379/mymaster',
)
```

**When to use:**

- Multi-instance deployments
- Kubernetes/container environments
- Large cache sizes
- Cache persistence requirements

## The @cache_query Decorator

The [`@cache_query`](src/reverie/cache/decorator.py:89) decorator provides automatic caching for async functions.

### Basic Usage

```python
from reverie.cache import cache_query

# Cache with default settings
@cache_query
async def get_all_users() -> list[User]:
  async with get_client(config) as client:
    return await query_records('user', User, client=client)
```

### With Custom TTL

```python
@cache_query(ttl=60)  # Cache for 60 seconds
async def get_recent_posts() -> list[Post]:
  async with get_client(config) as client:
    return await query_records(
      'post',
      Post,
      conditions=['created_at > time::now() - 1d'],
      client=client,
    )
```

### With Static Key

```python
@cache_query(ttl=300, key='featured_products')
async def get_featured_products() -> list[Product]:
  async with get_client(config) as client:
    return await query_records(
      'product',
      Product,
      conditions=['is_featured = true'],
      client=client,
    )
```

### With Custom Key Builder

```python
@cache_query(key_builder=lambda user_id: f'user:{user_id}:profile')
async def get_user_profile(user_id: str) -> User | None:
  async with get_client(config) as client:
    return await get_record('user', user_id, User, client=client)

# Cache key will be 'user:123:profile' for user_id='123'
```

### Combining Options

```python
@cache_query(
  ttl=120,
  key_builder=lambda category, page: f'products:{category}:page:{page}',
)
async def get_products_by_category(category: str, page: int = 1) -> list[Product]:
  async with get_client(config) as client:
    return await query_records(
      'product',
      Product,
      conditions=[f'category = "{category}"'],
      limit=20,
      offset=(page - 1) * 20,
      client=client,
    )
```

### Graceful Degradation

If the cache is not configured, decorated functions execute normally without caching:

```python
# Without configure_cache() called, this still works
@cache_query(ttl=60)
async def get_users():
  return await fetch_users()  # Always hits database

await get_users()  # Works fine, just no caching
```

## Manual Cache Management

For advanced use cases, use the [`CacheManager`](src/reverie/cache/manager.py:30) directly.

### Getting the Manager

```python
from reverie.cache import get_cache_manager, configure_cache, CacheConfig

# Configure first
configure_cache(CacheConfig(backend='memory'))

# Get the manager
manager = get_cache_manager()
```

### Get or Set Pattern

The most common pattern using [`get_or_set()`](src/reverie/cache/manager.py:112):

```python
async def get_user_stats(user_id: str) -> dict:
  manager = get_cache_manager()

  async def fetch_stats():
    # Expensive computation
    async with get_client(config) as client:
      posts = await count_records('post', f'author = "{user_id}"', client)
      followers = await client.execute(
        f'SELECT count() FROM user:*<-follows WHERE out = user:{user_id}'
      )
      return {'posts': posts, 'followers': followers}

  return await manager.get_or_set(
    f'user:{user_id}:stats',
    fetch_stats,
    ttl=300,
    tables=['post', 'follows'],  # Track for invalidation
  )
```

### Direct Get/Set

```python
manager = get_cache_manager()

# Set a value
await manager.set('my_key', {'data': 'value'}, ttl=60)

# Get a value
value = await manager.get('my_key')
if value is not None:
  print(f'Cache hit: {value}')
```

### Check Existence

```python
if await manager.exists('my_key'):
  print('Key exists in cache')
```

### Delete Specific Key

```python
await manager.delete('my_key')
```

### Track Table Associations

```python
# Associate cache entries with tables for invalidation
manager.track_table('user', 'active_users')
manager.track_table('user', 'user_count')

# Later, invalidate all user-related cache
await manager.invalidate(table='user')
```

### Cache Statistics

```python
stats = manager.stats
print(f'Hits: {stats.hits}')
print(f'Misses: {stats.misses}')
print(f'Hit ratio: {stats.hit_ratio:.2%}')
print(f'Size: {stats.size}')
```

## Invalidation Strategies

### By Specific Key

```python
from reverie.cache import invalidate, cache_key_for

# Invalidate known key
await invalidate(key='user:123')

# Get the key that would be used for a cached function
@cache_query(key_builder=lambda user_id: f'user:{user_id}')
async def get_user(user_id: str) -> User:
  ...

# Invalidate based on the function's key builder
key = cache_key_for(get_user, user_id='123')
await invalidate(key=key)
```

### By Table

Table-based invalidation removes all cached queries associated with a table:

```python
from reverie.cache import invalidate

# After updating user data
async def update_user(user_id: str, data: dict):
  async with get_client(config) as client:
    await merge_record('user', user_id, data, client=client)

  # Invalidate all user-related cache
  await invalidate(table='user')
```

### By Pattern

Pattern matching using glob syntax:

```python
# Invalidate all keys starting with 'user:'
await invalidate(pattern='user:*')

# Invalidate all profile keys
await invalidate(pattern='*:profile')

# Invalidate specific user's data
await invalidate(pattern='user:123:*')
```

### Clear All

```python
from reverie.cache import clear_cache

# Clear entire cache (use sparingly)
count = await clear_cache()
print(f'Cleared {count} entries')
```

### Automatic Invalidation on Write

A common pattern is invalidating related cache after write operations:

```python
async def create_post(author_id: str, content: str):
  async with get_client(config) as client:
    post = await create_record('post', {'author': author_id, 'content': content}, client=client)

  # Invalidate author's post cache
  await invalidate(pattern=f'user:{author_id}:posts:*')
  # Invalidate general post feeds
  await invalidate(table='post')

  return post
```

## Performance Considerations

### When to Cache

**Cache when:**

- Query results are read frequently
- Data changes infrequently
- Query execution is expensive (complex joins, aggregations)
- Response latency is critical

**Avoid caching when:**

- Data changes frequently (high write ratio)
- Every request needs fresh data
- Cache size would be very large
- Query results are user-specific and numerous

### TTL Recommendations

| Data Type | Recommended TTL | Rationale |
|-----------|-----------------|-----------|
| Static content | 1 hour+ | Rarely changes |
| Product listings | 5-15 minutes | Balance freshness and performance |
| User profile | 2-5 minutes | Changes occasionally |
| Real-time data | 30-60 seconds | Must be relatively fresh |
| Dashboard stats | 1-5 minutes | Can be slightly stale |

### Memory Management

```python
# Monitor cache size for memory backend
manager = get_cache_manager()
if hasattr(manager._backend, 'size'):
  current_size = manager._backend.size
  print(f'Cache entries: {current_size}')

# Configure appropriate max_size based on average entry size
# Example: 1000 entries * ~1KB average = ~1MB memory
config = CacheConfig(
  backend='memory',
  max_size=1000,  # Adjust based on available memory
)
```

### Cache Key Design

Good cache keys are:

- **Deterministic** - Same inputs produce same key
- **Unique** - Different queries produce different keys
- **Readable** - Easy to understand and debug

```python
# Good - Clear and unique
@cache_query(key_builder=lambda user_id, page: f'user:{user_id}:posts:page:{page}')

# Avoid - Too generic
@cache_query(key='posts')  # Will conflict if called with different params
```

### Measuring Cache Effectiveness

```python
import structlog

logger = structlog.get_logger()

async def log_cache_stats():
  manager = get_cache_manager()
  if manager:
    stats = manager.stats
    logger.info(
      'cache_stats',
      hits=stats.hits,
      misses=stats.misses,
      hit_ratio=f'{stats.hit_ratio:.2%}',
      size=stats.size,
    )

# Log periodically or on request
```

## API Reference

### Global Functions

| Function | Description |
|----------|-------------|
| [`configure_cache(config)`](src/reverie/cache/__init__.py:80) | Initialize global cache manager |
| [`get_cache_manager()`](src/reverie/cache/__init__.py:110) | Get the global cache manager |
| [`invalidate(key, table, pattern)`](src/reverie/cache/__init__.py:124) | Invalidate cache entries |
| [`clear_cache()`](src/reverie/cache/__init__.py:160) | Clear all cache entries |

### CacheConfig

```python
@dataclass(frozen=True)
class CacheConfig:
  enabled: bool = True
  backend: Literal['memory', 'redis'] = 'memory'
  default_ttl: int = 300
  max_size: int = 1000
  redis_url: str = 'redis://localhost:6379'
  key_prefix: str = 'reverie:'
```

### CacheOptions

```python
@dataclass(frozen=True)
class CacheOptions:
  ttl: int | None = None
  key: str | None = None
  invalidate_on: list[str] | None = None
```

### CacheStats

```python
@dataclass(frozen=True)
class CacheStats:
  hits: int = 0
  misses: int = 0
  size: int = 0
  evictions: int = 0

  @property
  def hit_ratio(self) -> float: ...
```

### CacheManager Methods

| Method | Description |
|--------|-------------|
| [`get_or_set(key, factory, ttl, tables)`](src/reverie/cache/manager.py:112) | Get from cache or execute factory |
| [`get(key)`](src/reverie/cache/manager.py:177) | Get value from cache |
| [`set(key, value, ttl, tables)`](src/reverie/cache/manager.py:203) | Set value in cache |
| [`delete(key)`](src/reverie/cache/manager.py:236) | Delete key from cache |
| [`invalidate(key, table, pattern)`](src/reverie/cache/manager.py:256) | Invalidate cache entries |
| [`clear()`](src/reverie/cache/manager.py:316) | Clear all cache entries |
| [`exists(key)`](src/reverie/cache/manager.py:333) | Check if key exists |
| [`build_key(*parts)`](src/reverie/cache/manager.py:349) | Build a prefixed cache key |
| [`track_table(table, key)`](src/reverie/cache/manager.py:370) | Associate key with table |
| [`close()`](src/reverie/cache/manager.py:395) | Close manager and release resources |

### cache_query Decorator

```python
@cache_query
async def func(): ...

@cache_query(ttl=60)
async def func(): ...

@cache_query(ttl=60, key='static_key')
async def func(): ...

@cache_query(key_builder=lambda x: f'key:{x}')
async def func(x): ...
```

### Helper Functions

| Function | Description |
|----------|-------------|
| [`cache_key_for(func, *args, **kwargs)`](src/reverie/cache/decorator.py:186) | Get cache key for a function call |
| [`is_cached(func)`](src/reverie/cache/decorator.py:226) | Check if function is decorated |

## Complete Example

```python
from pydantic import BaseModel
from reverie.cache import (
  configure_cache,
  cache_query,
  invalidate,
  get_cache_manager,
  CacheConfig,
)
from reverie.connection.client import get_client
from reverie.settings import get_db_config
from reverie.query.crud import query_records, create_record, get_record

# Models
class User(BaseModel):
  username: str
  email: str
  is_active: bool = True

class Post(BaseModel):
  title: str
  content: str
  author_id: str

# Configure cache at startup
def init_cache():
  config = CacheConfig(
    backend='memory',
    default_ttl=300,
    max_size=2000,
  )
  configure_cache(config)

# Cached queries
@cache_query(ttl=120)
async def get_active_users() -> list[User]:
  db_config = get_db_config()
  async with get_client(db_config) as client:
    return await query_records('user', User, conditions=['is_active = true'], client=client)

@cache_query(key_builder=lambda user_id: f'user:{user_id}')
async def get_user_by_id(user_id: str) -> User | None:
  db_config = get_db_config()
  async with get_client(db_config) as client:
    return await get_record('user', user_id, User, client=client)

@cache_query(key_builder=lambda user_id, page: f'user:{user_id}:posts:page:{page}')
async def get_user_posts(user_id: str, page: int = 1) -> list[Post]:
  db_config = get_db_config()
  async with get_client(db_config) as client:
    return await query_records(
      'post',
      Post,
      conditions=[f'author_id = "{user_id}"'],
      limit=10,
      offset=(page - 1) * 10,
      client=client,
    )

# Write operations with cache invalidation
async def create_user(username: str, email: str) -> User:
  db_config = get_db_config()
  async with get_client(db_config) as client:
    user = await create_record('user', User(username=username, email=email), client=client)

  # Invalidate user list caches
  await invalidate(table='user')

  return user

async def create_post(user_id: str, title: str, content: str) -> Post:
  db_config = get_db_config()
  async with get_client(db_config) as client:
    post = await create_record(
      'post',
      Post(title=title, content=content, author_id=user_id),
      client=client,
    )

  # Invalidate user's post cache
  await invalidate(pattern=f'user:{user_id}:posts:*')

  return post

# Application lifecycle
async def main():
  # Initialize cache
  init_cache()

  # Use cached queries
  users = await get_active_users()  # Cache miss
  users = await get_active_users()  # Cache hit

  # Get specific user
  user = await get_user_by_id('user:123')

  # Get paginated posts
  posts_page1 = await get_user_posts('user:123', page=1)
  posts_page2 = await get_user_posts('user:123', page=2)

  # Create new data (triggers invalidation)
  new_user = await create_user('alice', 'alice@example.com')
  new_post = await create_post('user:123', 'Hello', 'World')

  # Log cache stats
  manager = get_cache_manager()
  if manager:
    stats = manager.stats
    print(f'Cache hit ratio: {stats.hit_ratio:.2%}')

  # Cleanup
  if manager:
    await manager.close()

if __name__ == '__main__':
  import asyncio
  asyncio.run(main())
```

## Next Steps

- Explore [Query Building](queries.md) for database operations
- Learn about [Schema Definition](schema.md) for data structures
- Check out [Migrations](migrations.md) for schema management
- See [CLI Reference](cli.md) for command-line tools
