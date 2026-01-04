"""Cache manager for query integration.

This module provides the CacheManager class which orchestrates
cache operations and integrates with the query system.
"""

from __future__ import annotations

import asyncio
import builtins
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast

import structlog

from reverie.cache.backends import CacheBackend, MemoryCache, RedisCache
from reverie.cache.config import CacheConfig, CacheStats

if TYPE_CHECKING:
  pass

logger = structlog.get_logger(__name__)

T = TypeVar('T')

# Type alias to avoid conflict with CacheManager.set method
StringSet = builtins.set[str]


class CacheManager:
  """Manages cache operations and invalidation.

  The CacheManager provides a high-level interface for cache operations,
  including automatic key generation, table-based invalidation, and
  statistics tracking.

  Attributes:
      config: The cache configuration.
      backend: The active cache backend.
      stats: Current cache statistics.

  Example:
      >>> config = CacheConfig(backend="memory", default_ttl=300)
      >>> manager = CacheManager(config)
      >>> await manager.get_or_set("my_key", fetch_data, ttl=60)
  """

  def __init__(self, config: CacheConfig) -> None:
    """Initialize the cache manager.

    Args:
        config: Cache configuration settings.
    """
    self._config = config
    self._backend: CacheBackend | None = None
    self._stats = CacheStats()
    self._table_keys: dict[str, set[str]] = {}  # table -> set of cache keys
    self._lock = asyncio.Lock()
    self._initialized = False

  async def _ensure_backend(self) -> CacheBackend:
    """Ensure the cache backend is initialized.

    Returns:
        The initialized cache backend.

    Raises:
        RuntimeError: If cache is disabled.
    """
    if not self._config.enabled:
      raise RuntimeError('Cache is disabled')

    if self._backend is None:
      async with self._lock:
        if self._backend is None:
          if self._config.backend == 'redis':
            self._backend = RedisCache(
              url=self._config.redis_url,
              prefix=self._config.key_prefix,
              default_ttl=self._config.default_ttl,
            )
          else:
            self._backend = MemoryCache(
              max_size=self._config.max_size,
              default_ttl=self._config.default_ttl,
            )
          self._initialized = True
          logger.debug('cache_backend_initialized', backend=self._config.backend)

    return self._backend

  @property
  def config(self) -> CacheConfig:
    """Get the cache configuration."""
    return self._config

  @property
  def stats(self) -> CacheStats:
    """Get current cache statistics."""
    return self._stats

  @property
  def is_enabled(self) -> bool:
    """Check if cache is enabled."""
    return self._config.enabled

  @property
  def is_initialized(self) -> bool:
    """Check if the backend is initialized."""
    return self._initialized

  async def get_or_set(
    self,
    key: str,
    factory: Callable[[], Awaitable[T]],
    ttl: int | None = None,
    tables: list[str] | None = None,
  ) -> T:
    """Get from cache or execute factory and cache result.

    This is the primary method for caching query results. It attempts
    to retrieve a cached value, and if not found, executes the factory
    function and caches the result.

    Args:
        key: The cache key.
        factory: Async function to generate the value if not cached.
        ttl: Time-to-live in seconds. Uses config default if None.
        tables: List of table names for invalidation tracking.

    Returns:
        The cached or freshly generated value.

    Example:
        >>> async def fetch_users():
        ...     return await db.query("SELECT * FROM user")
        >>> users = await manager.get_or_set(
        ...     "all_users",
        ...     fetch_users,
        ...     ttl=60,
        ...     tables=["user"]
        ... )
    """
    if not self._config.enabled:
      return await factory()

    backend = await self._ensure_backend()
    prefixed_key = self.build_key(key)

    # Try to get from cache
    cached_value = await backend.get(prefixed_key)

    if cached_value is not None:
      self._stats = self._stats.with_hit()
      logger.debug('cache_hit', key=key)
      return cast(T, cached_value)

    # Cache miss - execute factory
    self._stats = self._stats.with_miss()
    logger.debug('cache_miss', key=key)

    value = await factory()

    # Cache the result
    effective_ttl = ttl if ttl is not None else self._config.default_ttl
    await backend.set(prefixed_key, value, effective_ttl)

    # Track table associations for invalidation
    if tables:
      for table in tables:
        if table not in self._table_keys:
          self._table_keys[table] = set()
        self._table_keys[table].add(prefixed_key)

    return value

  async def get(self, key: str) -> Any | None:
    """Get a value from cache.

    Args:
        key: The cache key.

    Returns:
        The cached value, or None if not found.
    """
    if not self._config.enabled:
      return None

    backend = await self._ensure_backend()
    prefixed_key = self.build_key(key)

    value = await backend.get(prefixed_key)

    if value is not None:
      self._stats = self._stats.with_hit()
      logger.debug('cache_hit', key=key)
    else:
      self._stats = self._stats.with_miss()
      logger.debug('cache_miss', key=key)

    return value

  async def set(
    self,
    key: str,
    value: T,
    ttl: int | None = None,
    tables: list[str] | None = None,
  ) -> None:
    """Set a value in cache.

    Args:
        key: The cache key.
        value: The value to cache.
        ttl: Time-to-live in seconds. Uses config default if None.
        tables: List of table names for invalidation tracking.
    """
    if not self._config.enabled:
      return

    backend = await self._ensure_backend()
    prefixed_key = self.build_key(key)
    effective_ttl = ttl if ttl is not None else self._config.default_ttl

    await backend.set(prefixed_key, value, effective_ttl)

    # Track table associations
    if tables:
      for table in tables:
        if table not in self._table_keys:
          self._table_keys[table] = set()
        self._table_keys[table].add(prefixed_key)

    logger.debug('cache_set', key=key, ttl=effective_ttl)

  async def delete(self, key: str) -> None:
    """Delete a key from cache.

    Args:
        key: The cache key to delete.
    """
    if not self._config.enabled:
      return

    backend = await self._ensure_backend()
    prefixed_key = self.build_key(key)

    await backend.delete(prefixed_key)

    # Remove from table tracking
    for table_keys in self._table_keys.values():
      table_keys.discard(prefixed_key)

    logger.debug('cache_delete', key=key)

  async def invalidate(
    self,
    key: str | None = None,
    table: str | None = None,
    pattern: str | None = None,
  ) -> int:
    """Invalidate cache entries.

    Provides flexible cache invalidation based on key, table name,
    or pattern matching.

    Args:
        key: Specific key to invalidate.
        table: Invalidate all queries associated with this table.
        pattern: Glob pattern to match keys (e.g., "user:*").

    Returns:
        Number of invalidated entries.

    Example:
        Invalidate specific key::

            await manager.invalidate(key="user:123")

        Invalidate all user-related cache::

            await manager.invalidate(table="user")

        Invalidate by pattern::

            await manager.invalidate(pattern="user:*")
    """
    if not self._config.enabled:
      return 0

    backend = await self._ensure_backend()
    count = 0

    if key is not None:
      prefixed_key = self.build_key(key)
      await backend.delete(prefixed_key)
      count = 1
      logger.debug('cache_invalidate_key', key=key)

    if table is not None:
      table_keys = self._table_keys.get(table, set())
      for cached_key in list(table_keys):
        await backend.delete(cached_key)
        count += 1
      self._table_keys[table] = set()
      logger.debug('cache_invalidate_table', table=table, count=count)

    if pattern is not None:
      prefixed_pattern = self.build_key(pattern)
      cleared = await backend.clear(prefixed_pattern)
      count += cleared
      logger.debug('cache_invalidate_pattern', pattern=pattern, count=cleared)

    return count

  async def clear(self) -> int:
    """Clear all cache entries.

    Returns:
        Number of cleared entries.
    """
    if not self._config.enabled:
      return 0

    backend = await self._ensure_backend()
    count = await backend.clear()
    self._table_keys.clear()
    self._stats = CacheStats()

    logger.info('cache_cleared', count=count)
    return count

  async def exists(self, key: str) -> bool:
    """Check if a key exists in cache.

    Args:
        key: The cache key to check.

    Returns:
        True if the key exists, False otherwise.
    """
    if not self._config.enabled:
      return False

    backend = await self._ensure_backend()
    prefixed_key = self.build_key(key)
    return await backend.exists(prefixed_key)

  def build_key(self, *parts: str) -> str:
    """Build a cache key from parts.

    Combines parts with colons and applies the configured prefix.

    Args:
        *parts: Key parts to combine.

    Returns:
        The complete cache key.

    Example:
        >>> manager.build_key("user", "123", "profile")
        'reverie:user:123:profile'
    """
    key = ':'.join(parts)
    # Don't double-prefix if already prefixed
    if key.startswith(self._config.key_prefix):
      return key
    return f'{self._config.key_prefix}{key}'

  def track_table(self, table: str, key: str) -> None:
    """Track a cache key's association with a table.

    This allows for table-based invalidation when data changes.

    Args:
        table: The table name.
        key: The cache key to associate.
    """
    prefixed_key = self.build_key(key)
    if table not in self._table_keys:
      self._table_keys[table] = set()
    self._table_keys[table].add(prefixed_key)

  def get_table_keys(self, table: str) -> StringSet:
    """Get all cache keys associated with a table.

    Args:
        table: The table name.

    Returns:
        Set of cache keys associated with the table.
    """
    return self._table_keys.get(table, set()).copy()

  async def close(self) -> None:
    """Close the cache manager and release resources."""
    if self._backend is not None:
      if hasattr(self._backend, 'close'):
        await self._backend.close()
      self._backend = None
      self._initialized = False
      logger.debug('cache_manager_closed')
