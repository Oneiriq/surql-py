"""Cache backend implementations for reverie.

This module provides different cache backend implementations:
- MemoryCache: In-memory LRU cache using cachetools
- RedisCache: Redis-based distributed cache (optional dependency)
"""

from __future__ import annotations

import asyncio
import fnmatch
import re
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from cachetools import TTLCache

if TYPE_CHECKING:
  pass


class CacheBackend(ABC):
  """Abstract cache backend interface.

  All cache backends must implement this interface to provide
  consistent cache operations across different storage implementations.
  """

  @abstractmethod
  async def get(self, key: str) -> Any | None:
    """Get value from cache.

    Args:
        key: The cache key to retrieve.

    Returns:
        The cached value, or None if not found or expired.
    """
    ...

  @abstractmethod
  async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
    """Set value in cache with optional TTL in seconds.

    Args:
        key: The cache key.
        value: The value to cache.
        ttl: Time-to-live in seconds. Uses backend default if None.
    """
    ...

  @abstractmethod
  async def delete(self, key: str) -> None:
    """Delete key from cache.

    Args:
        key: The cache key to delete.
    """
    ...

  @abstractmethod
  async def clear(self, pattern: str | None = None) -> int:
    """Clear cache entries matching pattern.

    Args:
        pattern: Glob pattern to match keys. Clears all if None.

    Returns:
        Count of deleted keys.
    """
    ...

  @abstractmethod
  async def exists(self, key: str) -> bool:
    """Check if key exists in cache.

    Args:
        key: The cache key to check.

    Returns:
        True if key exists and is not expired, False otherwise.
    """
    ...


class MemoryCache(CacheBackend):
  """In-memory LRU cache implementation using cachetools.

  This backend stores cache entries in memory with automatic
  expiration based on TTL. Suitable for single-instance applications.

  Attributes:
      max_size: Maximum number of entries in the cache.
      default_ttl: Default TTL in seconds for entries without explicit TTL.
  """

  def __init__(self, max_size: int = 1000, default_ttl: int = 300) -> None:
    """Initialize the memory cache.

    Args:
        max_size: Maximum number of cache entries.
        default_ttl: Default TTL in seconds (default: 5 minutes).
    """
    self._max_size = max_size
    self._default_ttl = default_ttl
    self._cache: TTLCache[str, Any] = TTLCache(maxsize=max_size, ttl=default_ttl)
    self._custom_ttls: dict[str, tuple[float, int]] = {}  # key -> (set_time, ttl)
    self._lock = asyncio.Lock()

  async def get(self, key: str) -> Any | None:
    """Get value from cache.

    Args:
        key: The cache key to retrieve.

    Returns:
        The cached value, or None if not found or expired.
    """
    async with self._lock:
      # Check custom TTL expiration
      if key in self._custom_ttls:
        set_time, ttl = self._custom_ttls[key]
        if time.monotonic() - set_time > ttl:
          # Expired, clean up
          self._cache.pop(key, None)
          del self._custom_ttls[key]
          return None

      return self._cache.get(key)

  async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
    """Set value in cache with optional TTL.

    Args:
        key: The cache key.
        value: The value to cache.
        ttl: Time-to-live in seconds. Uses default_ttl if None.
    """
    async with self._lock:
      # Store with custom TTL tracking if different from default
      if ttl is not None and ttl != self._default_ttl:
        self._custom_ttls[key] = (time.monotonic(), ttl)
      elif key in self._custom_ttls:
        del self._custom_ttls[key]

      # Store in cache (TTLCache handles its own expiration for default TTL)
      self._cache[key] = value

  async def delete(self, key: str) -> None:
    """Delete key from cache.

    Args:
        key: The cache key to delete.
    """
    async with self._lock:
      self._cache.pop(key, None)
      self._custom_ttls.pop(key, None)

  async def clear(self, pattern: str | None = None) -> int:
    """Clear cache entries matching pattern.

    Args:
        pattern: Glob pattern to match keys (e.g., "user:*").
                 Clears all entries if None.

    Returns:
        Count of deleted keys.
    """
    async with self._lock:
      if pattern is None:
        count = len(self._cache)
        self._cache.clear()
        self._custom_ttls.clear()
        return count

      # Find matching keys
      keys_to_delete = [k for k in self._cache if fnmatch.fnmatch(k, pattern)]

      for key in keys_to_delete:
        self._cache.pop(key, None)
        self._custom_ttls.pop(key, None)

      return len(keys_to_delete)

  async def exists(self, key: str) -> bool:
    """Check if key exists in cache.

    Args:
        key: The cache key to check.

    Returns:
        True if key exists and is not expired, False otherwise.
    """
    async with self._lock:
      # Check custom TTL expiration
      if key in self._custom_ttls:
        set_time, ttl = self._custom_ttls[key]
        if time.monotonic() - set_time > ttl:
          # Expired, clean up
          self._cache.pop(key, None)
          del self._custom_ttls[key]
          return False

      return key in self._cache

  @property
  def size(self) -> int:
    """Get current number of entries in cache."""
    return len(self._cache)


class RedisCache(CacheBackend):
  """Redis-based cache implementation.

  This backend uses Redis for distributed caching, suitable for
  multi-instance applications. Requires the 'redis' package.

  Note:
      Redis client is optional - the package checks if redis is installed.
      Install with: pip install reverie[cache]

  Attributes:
      url: Redis connection URL.
      prefix: Key prefix for all cache entries.
      default_ttl: Default TTL in seconds.
  """

  def __init__(
    self,
    url: str = 'redis://localhost:6379',
    prefix: str = 'reverie:',
    default_ttl: int = 300,
  ) -> None:
    """Initialize the Redis cache.

    Args:
        url: Redis connection URL.
        prefix: Prefix for all cache keys (default: "reverie:").
        default_ttl: Default TTL in seconds (default: 5 minutes).

    Raises:
        ImportError: If redis package is not installed.
    """
    self._url = url
    self._prefix = prefix
    self._default_ttl = default_ttl
    self._client: Any | None = None
    self._lock = asyncio.Lock()

    # Check if redis is available
    try:
      import redis.asyncio  # noqa: F401
    except ImportError as e:
      raise ImportError(
        "Redis cache backend requires the 'redis' package. "
        'Install it with: pip install reverie[cache]'
      ) from e

  async def _get_client(self) -> Any:
    """Get or create the Redis client.

    Returns:
        The Redis async client instance.
    """
    if self._client is None:
      import redis.asyncio as redis

      self._client = redis.from_url(self._url, decode_responses=True)
    return self._client

  def _make_key(self, key: str) -> str:
    """Create a prefixed cache key.

    Args:
        key: The base key.

    Returns:
        The prefixed key.
    """
    return f'{self._prefix}{key}'

  async def get(self, key: str) -> Any | None:
    """Get value from cache.

    Args:
        key: The cache key to retrieve.

    Returns:
        The cached value, or None if not found or expired.
    """
    import json

    client = await self._get_client()
    prefixed_key = self._make_key(key)

    value = await client.get(prefixed_key)
    if value is None:
      return None

    try:
      return json.loads(value)
    except (json.JSONDecodeError, TypeError):
      return value

  async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
    """Set value in cache with optional TTL.

    Args:
        key: The cache key.
        value: The value to cache (will be JSON serialized).
        ttl: Time-to-live in seconds. Uses default_ttl if None.
    """
    import json

    client = await self._get_client()
    prefixed_key = self._make_key(key)
    effective_ttl = ttl if ttl is not None else self._default_ttl

    # Serialize value to JSON
    serialized = json.dumps(value)

    await client.set(prefixed_key, serialized, ex=effective_ttl)

  async def delete(self, key: str) -> None:
    """Delete key from cache.

    Args:
        key: The cache key to delete.
    """
    client = await self._get_client()
    prefixed_key = self._make_key(key)
    await client.delete(prefixed_key)

  async def clear(self, pattern: str | None = None) -> int:
    """Clear cache entries matching pattern.

    Args:
        pattern: Glob pattern to match keys (e.g., "user:*").
                 Clears all entries with prefix if None.

    Returns:
        Count of deleted keys.
    """
    client = await self._get_client()

    # Determine pattern - clear all keys with our prefix if None
    redis_pattern = f'{self._prefix}*' if pattern is None else self._make_key(pattern)

    # Use SCAN to find matching keys (safer than KEYS for large datasets)
    count = 0
    cursor = 0
    while True:
      cursor, keys = await client.scan(cursor, match=redis_pattern, count=100)
      if keys:
        await client.delete(*keys)
        count += len(keys)
      if cursor == 0:
        break

    return count

  async def exists(self, key: str) -> bool:
    """Check if key exists in cache.

    Args:
        key: The cache key to check.

    Returns:
        True if key exists and is not expired, False otherwise.
    """
    client = await self._get_client()
    prefixed_key = self._make_key(key)
    return bool(await client.exists(prefixed_key))

  async def close(self) -> None:
    """Close the Redis connection."""
    if self._client is not None:
      await self._client.close()
      self._client = None


def _glob_to_regex(pattern: str) -> re.Pattern[str]:
  """Convert a glob pattern to a regex pattern.

  Args:
      pattern: Glob pattern with * and ? wildcards.

  Returns:
      Compiled regex pattern.
  """
  regex = re.escape(pattern)
  regex = regex.replace(r'\*', '.*')
  regex = regex.replace(r'\?', '.')
  return re.compile(f'^{regex}$')
