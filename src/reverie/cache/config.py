"""Cache configuration for reverie.

This module provides configuration classes for the caching system:
- CacheConfig: Global cache configuration
- CacheOptions: Per-query cache options
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class CacheConfig:
  """Global cache configuration.

  This configuration controls the behavior of the cache system,
  including which backend to use and default settings.

  Attributes:
      enabled: Whether caching is enabled globally.
      backend: Cache backend type ("memory" or "redis").
      default_ttl: Default time-to-live in seconds (default: 5 minutes).
      max_size: Maximum entries for memory backend (default: 1000).
      redis_url: Redis connection URL for redis backend.
      key_prefix: Prefix for all cache keys.

  Example:
      >>> config = CacheConfig(
      ...     enabled=True,
      ...     backend="memory",
      ...     default_ttl=600,
      ...     max_size=2000,
      ... )
  """

  enabled: bool = True
  backend: Literal['memory', 'redis'] = 'memory'
  default_ttl: int = 300  # 5 minutes
  max_size: int = 1000  # For memory backend
  redis_url: str = 'redis://localhost:6379'
  key_prefix: str = 'reverie:'


@dataclass(frozen=True)
class CacheOptions:
  """Per-query cache options.

  These options can be applied to individual queries to customize
  caching behavior on a per-operation basis.

  Attributes:
      ttl: Time-to-live in seconds. Uses global default if None.
      key: Custom cache key. Auto-generated if None.
      invalidate_on: List of table names that should trigger
                     cache invalidation when modified.

  Example:
      >>> options = CacheOptions(
      ...     ttl=60,
      ...     key="active_users",
      ...     invalidate_on=["user", "role"],
      ... )
  """

  ttl: int | None = None
  key: str | None = None
  invalidate_on: list[str] | None = field(default=None)

  def __post_init__(self) -> None:
    """Validate options after initialization."""
    if self.ttl is not None and self.ttl <= 0:
      raise ValueError('TTL must be a positive integer')


@dataclass(frozen=True)
class CacheStats:
  """Cache statistics for monitoring.

  Attributes:
      hits: Number of cache hits.
      misses: Number of cache misses.
      size: Current number of entries.
      evictions: Number of entries evicted due to size limits.
  """

  hits: int = 0
  misses: int = 0
  size: int = 0
  evictions: int = 0

  @property
  def hit_ratio(self) -> float:
    """Calculate the cache hit ratio.

    Returns:
        Hit ratio as a float between 0 and 1, or 0 if no requests.
    """
    total = self.hits + self.misses
    return self.hits / total if total > 0 else 0.0

  def with_hit(self) -> CacheStats:
    """Return new stats with incremented hit count."""
    return CacheStats(
      hits=self.hits + 1,
      misses=self.misses,
      size=self.size,
      evictions=self.evictions,
    )

  def with_miss(self) -> CacheStats:
    """Return new stats with incremented miss count."""
    return CacheStats(
      hits=self.hits,
      misses=self.misses + 1,
      size=self.size,
      evictions=self.evictions,
    )
