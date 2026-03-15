"""Query result caching module for surql.

This module provides an optional caching layer for query results,
supporting configurable TTL and automatic cache invalidation.

Components:
    - CacheBackend: Abstract interface for cache backends
    - MemoryCache: In-memory LRU cache using cachetools
    - RedisCache: Redis-based distributed cache (optional)
    - CacheConfig: Global cache configuration
    - CacheOptions: Per-query cache options
    - CacheManager: High-level cache operations manager
    - cache_query: Decorator for caching async functions

Example:
    Configure and use the cache::

        from surql.cache import configure_cache, cache_query, CacheConfig

        # Configure global cache
        config = CacheConfig(
            backend="memory",
            default_ttl=300,
            max_size=1000,
        )
        configure_cache(config)

        # Use decorator for automatic caching
        @cache_query(ttl=60)
        async def get_users() -> list[User]:
            return await crud.find_all(User)

        # Or use the manager directly
        from surql.cache import get_cache_manager

        manager = get_cache_manager()
        if manager:
            value = await manager.get_or_set("my_key", fetch_data)
            await manager.invalidate(table="user")
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from surql.cache.backends import CacheBackend, MemoryCache, RedisCache
from surql.cache.config import CacheConfig, CacheOptions, CacheStats
from surql.cache.decorator import cache_key_for, cache_query, is_cached
from surql.cache.manager import CacheManager

if TYPE_CHECKING:
  pass

__all__ = [
  # Backends
  'CacheBackend',
  'MemoryCache',
  'RedisCache',
  # Config
  'CacheConfig',
  'CacheOptions',
  'CacheStats',
  # Decorator
  'cache_query',
  'cache_key_for',
  'is_cached',
  # Manager
  'CacheManager',
  # Global functions
  'configure_cache',
  'get_cache_manager',
  'invalidate',
  'clear_cache',
]

# Global cache manager instance
_cache_manager: CacheManager | None = None


def configure_cache(config: CacheConfig) -> CacheManager:
  """Configure the global cache manager.

  This function initializes or reconfigures the global cache manager
  with the provided configuration. It should be called once during
  application startup.

  Args:
      config: Cache configuration settings.

  Returns:
      The configured cache manager instance.

  Example:
      >>> config = CacheConfig(
      ...     enabled=True,
      ...     backend="memory",
      ...     default_ttl=300,
      ... )
      >>> manager = configure_cache(config)

  Note:
      Calling this function multiple times will replace the existing
      cache manager. Ensure to close the previous manager if needed.
  """
  global _cache_manager
  _cache_manager = CacheManager(config)
  return _cache_manager


def get_cache_manager() -> CacheManager | None:
  """Get the global cache manager.

  Returns:
      The configured cache manager, or None if not configured.

  Example:
      >>> manager = get_cache_manager()
      >>> if manager:
      ...     await manager.set("key", "value")
  """
  return _cache_manager


async def invalidate(
  key: str | None = None,
  table: str | None = None,
  pattern: str | None = None,
) -> int:
  """Invalidate cache entries using the global manager.

  This is a convenience function that calls invalidate on the
  global cache manager.

  Args:
      key: Specific key to invalidate.
      table: Invalidate all queries for this table.
      pattern: Glob pattern to match keys.

  Returns:
      Number of invalidated entries, or 0 if cache not configured.

  Example:
      Invalidate by key::

          await invalidate(key="user:123")

      Invalidate by table::

          await invalidate(table="user")

      Invalidate by pattern::

          await invalidate(pattern="user:*")
  """
  if _cache_manager is None:
    return 0
  return await _cache_manager.invalidate(key=key, table=table, pattern=pattern)


async def clear_cache() -> int:
  """Clear all cache entries using the global manager.

  Returns:
      Number of cleared entries, or 0 if cache not configured.

  Example:
      >>> count = await clear_cache()
      >>> print(f"Cleared {count} cache entries")
  """
  if _cache_manager is None:
    return 0
  return await _cache_manager.clear()


async def close_cache() -> None:
  """Close the global cache manager and release resources.

  This should be called during application shutdown to properly
  clean up cache resources (especially for Redis connections).

  Example:
      >>> await close_cache()
  """
  global _cache_manager
  if _cache_manager is not None:
    await _cache_manager.close()
    _cache_manager = None
