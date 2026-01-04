"""Tests for the query result caching module.

This module provides comprehensive tests for the caching system including
configuration, backends, manager, decorator, and global functions.
"""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from reverie.cache import (
  CacheBackend,
  CacheConfig,
  CacheManager,
  CacheOptions,
  CacheStats,
  MemoryCache,
  RedisCache,
  cache_key_for,
  cache_query,
  clear_cache,
  close_cache,
  configure_cache,
  get_cache_manager,
  invalidate,
  is_cached,
)

# CacheConfig Tests


class TestCacheConfig:
  """Test suite for CacheConfig dataclass."""

  def test_cache_config_defaults(self) -> None:
    """Test CacheConfig default values."""
    config = CacheConfig()

    assert config.enabled is True
    assert config.backend == 'memory'
    assert config.default_ttl == 300
    assert config.max_size == 1000
    assert config.redis_url == 'redis://localhost:6379'
    assert config.key_prefix == 'reverie:'

  def test_cache_config_custom_values(self) -> None:
    """Test CacheConfig with custom values."""
    config = CacheConfig(
      enabled=False,
      backend='redis',
      default_ttl=600,
      max_size=5000,
      redis_url='redis://custom:6380',
      key_prefix='myapp:',
    )

    assert config.enabled is False
    assert config.backend == 'redis'
    assert config.default_ttl == 600
    assert config.max_size == 5000
    assert config.redis_url == 'redis://custom:6380'
    assert config.key_prefix == 'myapp:'

  def test_cache_config_immutability(self) -> None:
    """Test that CacheConfig is frozen (immutable)."""
    config = CacheConfig()

    with pytest.raises(AttributeError):
      config.enabled = False  # type: ignore[misc]

  def test_cache_config_memory_backend(self) -> None:
    """Test CacheConfig with memory backend."""
    config = CacheConfig(backend='memory', max_size=2000)

    assert config.backend == 'memory'
    assert config.max_size == 2000

  def test_cache_config_redis_backend(self) -> None:
    """Test CacheConfig with redis backend."""
    config = CacheConfig(backend='redis', redis_url='redis://localhost:6379/0')

    assert config.backend == 'redis'
    assert config.redis_url == 'redis://localhost:6379/0'


# CacheOptions Tests


class TestCacheOptions:
  """Test suite for CacheOptions dataclass."""

  def test_cache_options_defaults(self) -> None:
    """Test CacheOptions default values."""
    options = CacheOptions()

    assert options.ttl is None
    assert options.key is None
    assert options.invalidate_on is None

  def test_cache_options_custom_values(self) -> None:
    """Test CacheOptions with custom values."""
    options = CacheOptions(
      ttl=60,
      key='my_custom_key',
      invalidate_on=['user', 'profile'],
    )

    assert options.ttl == 60
    assert options.key == 'my_custom_key'
    assert options.invalidate_on == ['user', 'profile']

  def test_cache_options_invalid_ttl_negative(self) -> None:
    """Test CacheOptions rejects negative TTL."""
    with pytest.raises(ValueError, match='TTL must be a positive integer'):
      CacheOptions(ttl=-1)

  def test_cache_options_invalid_ttl_zero(self) -> None:
    """Test CacheOptions rejects zero TTL."""
    with pytest.raises(ValueError, match='TTL must be a positive integer'):
      CacheOptions(ttl=0)

  def test_cache_options_valid_ttl(self) -> None:
    """Test CacheOptions accepts positive TTL."""
    options = CacheOptions(ttl=1)
    assert options.ttl == 1

  def test_cache_options_immutability(self) -> None:
    """Test that CacheOptions is frozen (immutable)."""
    options = CacheOptions(ttl=60)

    with pytest.raises(AttributeError):
      options.ttl = 120  # type: ignore[misc]


# CacheStats Tests


class TestCacheStats:
  """Test suite for CacheStats dataclass."""

  def test_cache_stats_defaults(self) -> None:
    """Test CacheStats default values."""
    stats = CacheStats()

    assert stats.hits == 0
    assert stats.misses == 0
    assert stats.size == 0
    assert stats.evictions == 0

  def test_cache_stats_custom_values(self) -> None:
    """Test CacheStats with custom values."""
    stats = CacheStats(hits=100, misses=20, size=50, evictions=5)

    assert stats.hits == 100
    assert stats.misses == 20
    assert stats.size == 50
    assert stats.evictions == 5

  def test_hit_ratio_calculation(self) -> None:
    """Test hit_ratio property calculates correctly."""
    stats = CacheStats(hits=80, misses=20)

    assert stats.hit_ratio == 0.8

  def test_hit_ratio_zero_requests(self) -> None:
    """Test hit_ratio returns 0 when no requests."""
    stats = CacheStats()

    assert stats.hit_ratio == 0.0

  def test_hit_ratio_all_hits(self) -> None:
    """Test hit_ratio returns 1.0 for all hits."""
    stats = CacheStats(hits=100, misses=0)

    assert stats.hit_ratio == 1.0

  def test_hit_ratio_all_misses(self) -> None:
    """Test hit_ratio returns 0.0 for all misses."""
    stats = CacheStats(hits=0, misses=100)

    assert stats.hit_ratio == 0.0

  def test_with_hit(self) -> None:
    """Test with_hit returns new stats with incremented hit count."""
    stats = CacheStats(hits=5, misses=2, size=10, evictions=1)
    new_stats = stats.with_hit()

    # Original unchanged
    assert stats.hits == 5
    # New has incremented hits
    assert new_stats.hits == 6
    assert new_stats.misses == 2
    assert new_stats.size == 10
    assert new_stats.evictions == 1

  def test_with_miss(self) -> None:
    """Test with_miss returns new stats with incremented miss count."""
    stats = CacheStats(hits=5, misses=2, size=10, evictions=1)
    new_stats = stats.with_miss()

    # Original unchanged
    assert stats.misses == 2
    # New has incremented misses
    assert new_stats.misses == 3
    assert new_stats.hits == 5
    assert new_stats.size == 10
    assert new_stats.evictions == 1

  def test_cache_stats_immutability(self) -> None:
    """Test that CacheStats is frozen (immutable)."""
    stats = CacheStats()

    with pytest.raises(AttributeError):
      stats.hits = 100  # type: ignore[misc]


# MemoryCache Backend Tests


class TestMemoryCacheGet:
  """Test suite for MemoryCache.get() method."""

  def test_get_existing_key(self) -> None:
    """Test get returns value for existing key."""

    async def run_test() -> str | None:
      cache = MemoryCache(max_size=100, default_ttl=300)
      await cache.set('test_key', 'test_value')
      return await cache.get('test_key')

    result = asyncio.run(run_test())
    assert result == 'test_value'

  def test_get_non_existing_key(self) -> None:
    """Test get returns None for non-existing key."""

    async def run_test() -> str | None:
      cache = MemoryCache()
      return await cache.get('nonexistent')

    result = asyncio.run(run_test())
    assert result is None

  def test_get_complex_value(self) -> None:
    """Test get returns complex values correctly."""

    async def run_test() -> dict | None:
      cache = MemoryCache()
      data = {'name': 'Alice', 'age': 30, 'items': [1, 2, 3]}
      await cache.set('user', data)
      return await cache.get('user')

    result = asyncio.run(run_test())
    assert result == {'name': 'Alice', 'age': 30, 'items': [1, 2, 3]}


class TestMemoryCacheSet:
  """Test suite for MemoryCache.set() method."""

  def test_set_without_ttl(self) -> None:
    """Test set stores value using default TTL."""

    async def run_test() -> bool:
      cache = MemoryCache(default_ttl=300)
      await cache.set('key', 'value')
      return await cache.exists('key')

    result = asyncio.run(run_test())
    assert result is True

  def test_set_with_ttl(self) -> None:
    """Test set stores value with custom TTL."""

    async def run_test() -> bool:
      cache = MemoryCache(default_ttl=300)
      await cache.set('key', 'value', ttl=60)
      return await cache.exists('key')

    result = asyncio.run(run_test())
    assert result is True

  def test_set_overwrite_existing(self) -> None:
    """Test set overwrites existing value."""

    async def run_test() -> str | None:
      cache = MemoryCache()
      await cache.set('key', 'old_value')
      await cache.set('key', 'new_value')
      return await cache.get('key')

    result = asyncio.run(run_test())
    assert result == 'new_value'


class TestMemoryCacheDelete:
  """Test suite for MemoryCache.delete() method."""

  def test_delete_existing_key(self) -> None:
    """Test delete removes existing key."""

    async def run_test() -> bool:
      cache = MemoryCache()
      await cache.set('key', 'value')
      await cache.delete('key')
      return await cache.exists('key')

    result = asyncio.run(run_test())
    assert result is False

  def test_delete_non_existing_key(self) -> None:
    """Test delete handles non-existing key gracefully."""

    async def run_test() -> None:
      cache = MemoryCache()
      # Should not raise
      await cache.delete('nonexistent')

    asyncio.run(run_test())  # Should complete without error


class TestMemoryCacheClear:
  """Test suite for MemoryCache.clear() method."""

  def test_clear_all(self) -> None:
    """Test clear removes all entries."""

    async def run_test() -> int:
      cache = MemoryCache()
      await cache.set('key1', 'value1')
      await cache.set('key2', 'value2')
      await cache.set('key3', 'value3')
      count = await cache.clear()
      return count

    result = asyncio.run(run_test())
    assert result == 3

  def test_clear_with_pattern(self) -> None:
    """Test clear removes only matching keys."""

    async def run_test() -> tuple[int, bool, bool]:
      cache = MemoryCache()
      await cache.set('user:1', 'alice')
      await cache.set('user:2', 'bob')
      await cache.set('post:1', 'hello')
      count = await cache.clear('user:*')
      user_exists = await cache.exists('user:1')
      post_exists = await cache.exists('post:1')
      return count, user_exists, post_exists

    count, user_exists, post_exists = asyncio.run(run_test())
    assert count == 2
    assert user_exists is False
    assert post_exists is True

  def test_clear_empty_cache(self) -> None:
    """Test clear on empty cache returns 0."""

    async def run_test() -> int:
      cache = MemoryCache()
      return await cache.clear()

    result = asyncio.run(run_test())
    assert result == 0


class TestMemoryCacheExists:
  """Test suite for MemoryCache.exists() method."""

  def test_exists_for_existing_key(self) -> None:
    """Test exists returns True for existing key."""

    async def run_test() -> bool:
      cache = MemoryCache()
      await cache.set('key', 'value')
      return await cache.exists('key')

    result = asyncio.run(run_test())
    assert result is True

  def test_exists_for_non_existing_key(self) -> None:
    """Test exists returns False for non-existing key."""

    async def run_test() -> bool:
      cache = MemoryCache()
      return await cache.exists('nonexistent')

    result = asyncio.run(run_test())
    assert result is False


class TestMemoryCacheTTLExpiration:
  """Test suite for MemoryCache TTL expiration behavior."""

  def test_custom_ttl_expiration(self) -> None:
    """Test that custom TTL causes expiration."""

    async def run_test() -> tuple[str | None, str | None]:
      cache = MemoryCache(default_ttl=300)

      # Mock time.monotonic to simulate time passing
      original_monotonic = time.monotonic

      with patch('time.monotonic') as mock_monotonic:
        # Set initial time
        current_time = original_monotonic()
        mock_monotonic.return_value = current_time

        # Set with short TTL
        await cache.set('key', 'value', ttl=1)

        # Get immediately - should exist
        value_before = await cache.get('key')

        # Advance time past TTL
        mock_monotonic.return_value = current_time + 2

        # Get after expiration - should be None
        value_after = await cache.get('key')

        return value_before, value_after

    before, after = asyncio.run(run_test())
    assert before == 'value'
    assert after is None

  def test_exists_respects_custom_ttl(self) -> None:
    """Test that exists() respects custom TTL."""

    async def run_test() -> tuple[bool, bool]:
      cache = MemoryCache(default_ttl=300)
      original_monotonic = time.monotonic

      with patch('time.monotonic') as mock_monotonic:
        current_time = original_monotonic()
        mock_monotonic.return_value = current_time

        await cache.set('key', 'value', ttl=1)
        exists_before = await cache.exists('key')

        mock_monotonic.return_value = current_time + 2
        exists_after = await cache.exists('key')

        return exists_before, exists_after

    before, after = asyncio.run(run_test())
    assert before is True
    assert after is False


class TestMemoryCacheLRUEviction:
  """Test suite for MemoryCache LRU eviction behavior."""

  def test_max_size_respected(self) -> None:
    """Test that max_size is respected by TTLCache."""

    async def run_test() -> int:
      cache = MemoryCache(max_size=3, default_ttl=300)
      await cache.set('key1', 'value1')
      await cache.set('key2', 'value2')
      await cache.set('key3', 'value3')
      await cache.set('key4', 'value4')  # Should evict oldest
      return cache.size

    result = asyncio.run(run_test())
    # TTLCache may have 3 or fewer entries
    assert result <= 3

  def test_size_property(self) -> None:
    """Test size property returns current cache size."""

    async def run_test() -> int:
      cache = MemoryCache()
      await cache.set('key1', 'value1')
      await cache.set('key2', 'value2')
      return cache.size

    result = asyncio.run(run_test())
    assert result == 2


# RedisCache Backend Tests (with mocking)

# Check if redis is available for conditional test skipping
try:
  import redis.asyncio  # noqa: F401

  REDIS_AVAILABLE = True
except ImportError:
  REDIS_AVAILABLE = False

redis_skip = pytest.mark.skipif(not REDIS_AVAILABLE, reason='redis package not installed')


@redis_skip
class TestRedisCacheGet:
  """Test suite for RedisCache.get() with mocked redis client."""

  def test_get_returns_value(self) -> None:
    """Test get returns deserialized value from redis."""

    async def run_test() -> dict | None:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_client.get.return_value = '{"name": "Alice", "age": 30}'
        mock_from_url.return_value = mock_client

        cache = RedisCache(url='redis://localhost:6379', prefix='test:')
        result = await cache.get('user:123')

        mock_client.get.assert_called_once_with('test:user:123')
        return result

    result = asyncio.run(run_test())
    assert result == {'name': 'Alice', 'age': 30}

  def test_get_returns_none_for_missing_key(self) -> None:
    """Test get returns None when key not found."""

    async def run_test() -> str | None:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_client.get.return_value = None
        mock_from_url.return_value = mock_client

        cache = RedisCache()
        return await cache.get('nonexistent')

    result = asyncio.run(run_test())
    assert result is None

  def test_get_handles_non_json_value(self) -> None:
    """Test get handles non-JSON values gracefully."""

    async def run_test() -> str | None:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_client.get.return_value = 'plain string value'
        mock_from_url.return_value = mock_client

        cache = RedisCache()
        return await cache.get('key')

    result = asyncio.run(run_test())
    # Should return raw value when JSON decode fails
    assert result == 'plain string value'


@redis_skip
class TestRedisCacheSet:
  """Test suite for RedisCache.set() with mocked redis client."""

  def test_set_with_default_ttl(self) -> None:
    """Test set stores value with default TTL."""

    async def run_test() -> None:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_from_url.return_value = mock_client

        cache = RedisCache(default_ttl=300, prefix='test:')
        await cache.set('key', {'data': 'value'})

        mock_client.set.assert_called_once()
        call_args = mock_client.set.call_args
        assert call_args[0][0] == 'test:key'
        assert '"data": "value"' in call_args[0][1]
        assert call_args[1]['ex'] == 300

    asyncio.run(run_test())

  def test_set_with_custom_ttl(self) -> None:
    """Test set stores value with custom TTL."""

    async def run_test() -> None:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_from_url.return_value = mock_client

        cache = RedisCache(default_ttl=300)
        await cache.set('key', 'value', ttl=60)

        call_args = mock_client.set.call_args
        assert call_args[1]['ex'] == 60

    asyncio.run(run_test())


@redis_skip
class TestRedisCacheDelete:
  """Test suite for RedisCache.delete() with mocked redis client."""

  def test_delete_calls_redis_delete(self) -> None:
    """Test delete calls redis client delete."""

    async def run_test() -> None:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_from_url.return_value = mock_client

        cache = RedisCache(prefix='test:')
        await cache.delete('key')

        mock_client.delete.assert_called_once_with('test:key')

    asyncio.run(run_test())


@redis_skip
class TestRedisCacheClear:
  """Test suite for RedisCache.clear() with mocked redis client."""

  def test_clear_uses_scan(self) -> None:
    """Test clear uses SCAN to find and delete keys."""

    async def run_test() -> int:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        # Simulate SCAN returning keys then stopping
        mock_client.scan.side_effect = [
          (0, ['test:key1', 'test:key2']),
        ]
        mock_from_url.return_value = mock_client

        cache = RedisCache(prefix='test:')
        count = await cache.clear()

        return count

    result = asyncio.run(run_test())
    assert result == 2

  def test_clear_with_pattern(self) -> None:
    """Test clear with pattern uses correct redis pattern."""

    async def run_test() -> None:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_client.scan.return_value = (0, [])
        mock_from_url.return_value = mock_client

        cache = RedisCache(prefix='prefix:')
        await cache.clear('user:*')

        call_args = mock_client.scan.call_args
        assert call_args[1]['match'] == 'prefix:user:*'

    asyncio.run(run_test())


class TestRedisCacheInitialization:
  """Test suite for RedisCache initialization."""

  def test_init_without_redis_raises_import_error(self) -> None:
    """Test RedisCache raises ImportError when redis not installed."""
    with (
      patch.dict('sys.modules', {'redis.asyncio': None, 'redis': None}),
      patch('builtins.__import__', side_effect=ImportError('No module named redis')),
      pytest.raises(ImportError, match="requires the 'redis' package"),
    ):
      RedisCache()

  @redis_skip
  def test_close_closes_client(self) -> None:
    """Test close method closes the redis client."""

    async def run_test() -> None:
      with patch('redis.asyncio.from_url') as mock_from_url:
        mock_client = AsyncMock()
        mock_from_url.return_value = mock_client

        cache = RedisCache()
        # Initialize client
        await cache.get('key')
        # Close
        await cache.close()

        mock_client.close.assert_called_once()

    asyncio.run(run_test())


# CacheManager Tests


class TestCacheManagerGetOrSet:
  """Test suite for CacheManager.get_or_set() method."""

  def test_get_or_set_cache_hit(self) -> None:
    """Test get_or_set returns cached value on cache hit."""

    async def run_test() -> tuple[str, int]:
      config = CacheConfig(backend='memory')
      manager = CacheManager(config)

      call_count = 0

      async def factory() -> str:
        nonlocal call_count
        call_count += 1
        return 'fresh_value'

      # First call - miss
      await manager.get_or_set('key', factory)
      # Second call - hit
      result = await manager.get_or_set('key', factory)

      return result, call_count

    result, calls = asyncio.run(run_test())
    assert result == 'fresh_value'
    assert calls == 1  # Factory only called once

  def test_get_or_set_cache_miss(self) -> None:
    """Test get_or_set calls factory on cache miss."""

    async def run_test() -> str:
      config = CacheConfig(backend='memory')
      manager = CacheManager(config)

      async def factory() -> str:
        return 'new_value'

      result = await manager.get_or_set('new_key', factory)
      return result

    result = asyncio.run(run_test())
    assert result == 'new_value'

  def test_get_or_set_with_disabled_cache(self) -> None:
    """Test get_or_set always calls factory when cache disabled."""

    async def run_test() -> int:
      config = CacheConfig(enabled=False)
      manager = CacheManager(config)

      call_count = 0

      async def factory() -> str:
        nonlocal call_count
        call_count += 1
        return 'value'

      await manager.get_or_set('key', factory)
      await manager.get_or_set('key', factory)

      return call_count

    calls = asyncio.run(run_test())
    assert calls == 2  # Factory called every time

  def test_get_or_set_tracks_tables(self) -> None:
    """Test get_or_set tracks table associations."""

    async def run_test() -> set[str]:
      config = CacheConfig(backend='memory')
      manager = CacheManager(config)

      async def factory() -> list[str]:
        return ['user1', 'user2']

      await manager.get_or_set('users', factory, tables=['user'])

      return manager.get_table_keys('user')

    keys = asyncio.run(run_test())
    assert len(keys) == 1
    assert 'reverie:users' in keys


class TestCacheManagerInvalidate:
  """Test suite for CacheManager.invalidate() method."""

  def test_invalidate_by_key(self) -> None:
    """Test invalidate removes entry by key."""

    async def run_test() -> bool:
      config = CacheConfig(backend='memory')
      manager = CacheManager(config)

      await manager.set('key', 'value')
      await manager.invalidate(key='key')

      return await manager.exists('key')

    exists = asyncio.run(run_test())
    assert exists is False

  def test_invalidate_by_table(self) -> None:
    """Test invalidate removes entries by table."""

    async def run_test() -> tuple[bool, bool]:
      config = CacheConfig(backend='memory')
      manager = CacheManager(config)

      await manager.set('user:1', 'alice', tables=['user'])
      await manager.set('user:2', 'bob', tables=['user'])
      await manager.set('post:1', 'hello', tables=['post'])

      await manager.invalidate(table='user')

      user_exists = await manager.exists('user:1')
      post_exists = await manager.exists('post:1')

      return user_exists, post_exists

    user_exists, post_exists = asyncio.run(run_test())
    assert user_exists is False
    assert post_exists is True

  def test_invalidate_by_pattern(self) -> None:
    """Test invalidate removes entries by pattern."""

    async def run_test() -> int:
      config = CacheConfig(backend='memory')
      manager = CacheManager(config)

      await manager.set('user:1', 'alice')
      await manager.set('user:2', 'bob')
      await manager.set('post:1', 'hello')

      count = await manager.invalidate(pattern='user:*')
      return count

    count = asyncio.run(run_test())
    assert count >= 2

  def test_invalidate_returns_zero_when_disabled(self) -> None:
    """Test invalidate returns 0 when cache disabled."""

    async def run_test() -> int:
      config = CacheConfig(enabled=False)
      manager = CacheManager(config)

      return await manager.invalidate(key='any')

    count = asyncio.run(run_test())
    assert count == 0


class TestCacheManagerBuildKey:
  """Test suite for CacheManager.build_key() method."""

  def test_build_key_single_part(self) -> None:
    """Test build_key with single part."""
    config = CacheConfig(key_prefix='cache:')
    manager = CacheManager(config)

    key = manager.build_key('users')
    assert key == 'cache:users'

  def test_build_key_multiple_parts(self) -> None:
    """Test build_key with multiple parts."""
    config = CacheConfig(key_prefix='reverie:')
    manager = CacheManager(config)

    key = manager.build_key('user', '123', 'profile')
    assert key == 'reverie:user:123:profile'

  def test_build_key_no_double_prefix(self) -> None:
    """Test build_key doesn't double-prefix."""
    config = CacheConfig(key_prefix='reverie:')
    manager = CacheManager(config)

    key = manager.build_key('reverie:already_prefixed')
    assert key == 'reverie:already_prefixed'


class TestCacheManagerTableTracking:
  """Test suite for CacheManager table tracking."""

  def test_track_table(self) -> None:
    """Test track_table associates key with table."""
    config = CacheConfig()
    manager = CacheManager(config)

    manager.track_table('user', 'user:123')

    keys = manager.get_table_keys('user')
    assert 'reverie:user:123' in keys

  def test_get_table_keys_returns_copy(self) -> None:
    """Test get_table_keys returns a copy of the set."""
    config = CacheConfig()
    manager = CacheManager(config)

    manager.track_table('user', 'user:1')
    keys = manager.get_table_keys('user')
    keys.add('should_not_affect_original')

    original_keys = manager.get_table_keys('user')
    assert 'should_not_affect_original' not in original_keys

  def test_get_table_keys_empty_table(self) -> None:
    """Test get_table_keys returns empty set for unknown table."""
    config = CacheConfig()
    manager = CacheManager(config)

    keys = manager.get_table_keys('nonexistent')
    assert keys == set()


class TestCacheManagerProperties:
  """Test suite for CacheManager properties."""

  def test_config_property(self) -> None:
    """Test config property returns configuration."""
    config = CacheConfig(default_ttl=600)
    manager = CacheManager(config)

    assert manager.config.default_ttl == 600

  def test_stats_property(self) -> None:
    """Test stats property returns statistics."""
    config = CacheConfig()
    manager = CacheManager(config)

    assert isinstance(manager.stats, CacheStats)
    assert manager.stats.hits == 0

  def test_is_enabled_property(self) -> None:
    """Test is_enabled property."""
    enabled_manager = CacheManager(CacheConfig(enabled=True))
    disabled_manager = CacheManager(CacheConfig(enabled=False))

    assert enabled_manager.is_enabled is True
    assert disabled_manager.is_enabled is False

  def test_is_initialized_property(self) -> None:
    """Test is_initialized property."""

    async def run_test() -> tuple[bool, bool]:
      config = CacheConfig()
      manager = CacheManager(config)

      before = manager.is_initialized
      await manager.get('key')  # Triggers initialization
      after = manager.is_initialized

      return before, after

    before, after = asyncio.run(run_test())
    assert before is False
    assert after is True


class TestCacheManagerClose:
  """Test suite for CacheManager.close() method."""

  def test_close_resets_state(self) -> None:
    """Test close resets manager state."""

    async def run_test() -> bool:
      config = CacheConfig()
      manager = CacheManager(config)

      await manager.set('key', 'value')
      assert manager.is_initialized is True

      await manager.close()
      return manager.is_initialized

    is_initialized = asyncio.run(run_test())
    assert is_initialized is False


# Cache Decorator Tests


class TestCacheQueryDecorator:
  """Test suite for cache_query decorator."""

  def test_decorator_caches_results(self) -> None:
    """Test cache_query decorator caches function results."""

    async def run_test() -> int:
      # Configure global cache
      config = CacheConfig(backend='memory')
      configure_cache(config)

      call_count = 0

      @cache_query
      async def fetch_data() -> str:
        nonlocal call_count
        call_count += 1
        return 'data'

      await fetch_data()
      await fetch_data()

      # Cleanup
      await close_cache()

      return call_count

    calls = asyncio.run(run_test())
    assert calls == 1

  def test_decorator_with_ttl(self) -> None:
    """Test cache_query decorator with custom TTL."""

    async def run_test() -> bool:
      config = CacheConfig(backend='memory')
      configure_cache(config)

      @cache_query(ttl=60)
      async def fetch_data() -> str:
        return 'data'

      # Check that decorator stores TTL metadata
      has_ttl = hasattr(fetch_data, '_cache_ttl')

      await close_cache()
      return has_ttl

    has_ttl = asyncio.run(run_test())
    assert has_ttl is True

  def test_decorator_with_custom_key(self) -> None:
    """Test cache_query decorator with custom key."""

    async def run_test() -> str | None:
      config = CacheConfig(backend='memory')
      manager = configure_cache(config)

      @cache_query(key='my_custom_key')
      async def fetch_data() -> str:
        return 'data'

      await fetch_data()

      # Check value is cached under custom key
      value = await manager.get('my_custom_key')

      await close_cache()
      return value

    value = asyncio.run(run_test())
    assert value == 'data'

  def test_decorator_with_key_builder(self) -> None:
    """Test cache_query decorator with custom key builder."""

    async def run_test() -> str | None:
      config = CacheConfig(backend='memory')
      manager = configure_cache(config)

      @cache_query(key_builder=lambda user_id: f'user:{user_id}')
      async def get_user(user_id: str) -> dict:
        return {'id': user_id, 'name': 'Alice'}

      await get_user('123')

      value = await manager.get('user:123')

      await close_cache()
      return value

    value = asyncio.run(run_test())
    assert value == {'id': '123', 'name': 'Alice'}

  def test_decorator_without_cache_configured(self) -> None:
    """Test cache_query decorator works without cache configured."""

    async def run_test() -> str:
      # Reset global cache manager
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      call_count = 0

      @cache_query
      async def fetch_data() -> str:
        nonlocal call_count
        call_count += 1
        return 'data'

      result = await fetch_data()
      return result

    result = asyncio.run(run_test())
    assert result == 'data'

  def test_decorator_requires_async_function(self) -> None:
    """Test cache_query raises TypeError for non-async functions."""
    with pytest.raises(TypeError, match='can only decorate async functions'):

      @cache_query
      def sync_func() -> str:  # type: ignore[arg-type]
        return 'data'


class TestCacheKeyFor:
  """Test suite for cache_key_for function."""

  def test_cache_key_for_with_custom_builder(self) -> None:
    """Test cache_key_for uses custom key builder."""

    @cache_query(key_builder=lambda x, y: f'{x}:{y}')
    async def func(x: str, y: str) -> str:
      return f'{x}-{y}'

    key = cache_key_for(func, 'a', 'b')
    assert key == 'a:b'

  def test_cache_key_for_with_static_key(self) -> None:
    """Test cache_key_for returns static key."""

    @cache_query(key='static_key')
    async def func() -> str:
      return 'data'

    key = cache_key_for(func)
    assert key == 'static_key'

  def test_cache_key_for_generated_key(self) -> None:
    """Test cache_key_for generates key from function and args."""

    @cache_query
    async def func(x: int) -> int:
      return x * 2

    key = cache_key_for(func, 42)
    assert 'func' in key


class TestIsCached:
  """Test suite for is_cached function."""

  def test_is_cached_returns_true_for_decorated(self) -> None:
    """Test is_cached returns True for decorated function."""

    @cache_query
    async def cached_func() -> str:
      return 'data'

    assert is_cached(cached_func) is True

  def test_is_cached_returns_false_for_undecorated(self) -> None:
    """Test is_cached returns False for undecorated function."""

    async def uncached_func() -> str:
      return 'data'

    assert is_cached(uncached_func) is False


# Global Cache Functions Tests


class TestConfigureCache:
  """Test suite for configure_cache global function."""

  def test_configure_cache_creates_manager(self) -> None:
    """Test configure_cache creates and returns CacheManager."""

    async def run_test() -> bool:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      config = CacheConfig(backend='memory')
      manager = configure_cache(config)

      result = isinstance(manager, CacheManager)

      await close_cache()
      return result

    is_manager = asyncio.run(run_test())
    assert is_manager is True

  def test_configure_cache_replaces_existing(self) -> None:
    """Test configure_cache replaces existing manager."""

    async def run_test() -> tuple[int, int]:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      config1 = CacheConfig(default_ttl=100)
      manager1 = configure_cache(config1)
      ttl1 = manager1.config.default_ttl

      config2 = CacheConfig(default_ttl=200)
      manager2 = configure_cache(config2)
      ttl2 = manager2.config.default_ttl

      await close_cache()
      return ttl1, ttl2

    ttl1, ttl2 = asyncio.run(run_test())
    assert ttl1 == 100
    assert ttl2 == 200


class TestGetCacheManager:
  """Test suite for get_cache_manager global function."""

  def test_get_cache_manager_returns_manager(self) -> None:
    """Test get_cache_manager returns configured manager."""

    async def run_test() -> bool:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      config = CacheConfig()
      configure_cache(config)

      manager = get_cache_manager()

      await close_cache()
      return manager is not None

    has_manager = asyncio.run(run_test())
    assert has_manager is True

  def test_get_cache_manager_returns_none_when_not_configured(self) -> None:
    """Test get_cache_manager returns None when not configured."""
    import reverie.cache as cache_module

    cache_module._cache_manager = None

    manager = get_cache_manager()
    assert manager is None


class TestInvalidateGlobal:
  """Test suite for invalidate global function."""

  def test_invalidate_by_key(self) -> None:
    """Test global invalidate by key."""

    async def run_test() -> int:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      config = CacheConfig(backend='memory')
      manager = configure_cache(config)

      await manager.set('key', 'value')
      count = await invalidate(key='key')

      await close_cache()
      return count

    count = asyncio.run(run_test())
    assert count == 1

  def test_invalidate_returns_zero_when_not_configured(self) -> None:
    """Test global invalidate returns 0 when cache not configured."""

    async def run_test() -> int:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      return await invalidate(key='any')

    count = asyncio.run(run_test())
    assert count == 0


class TestClearCacheGlobal:
  """Test suite for clear_cache global function."""

  def test_clear_cache_removes_all(self) -> None:
    """Test global clear_cache removes all entries."""

    async def run_test() -> int:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      config = CacheConfig(backend='memory')
      manager = configure_cache(config)

      await manager.set('key1', 'value1')
      await manager.set('key2', 'value2')

      count = await clear_cache()

      await close_cache()
      return count

    count = asyncio.run(run_test())
    assert count == 2

  def test_clear_cache_returns_zero_when_not_configured(self) -> None:
    """Test global clear_cache returns 0 when not configured."""

    async def run_test() -> int:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      return await clear_cache()

    count = asyncio.run(run_test())
    assert count == 0


class TestCloseCacheGlobal:
  """Test suite for close_cache global function."""

  def test_close_cache_cleans_up(self) -> None:
    """Test close_cache cleans up resources."""

    async def run_test() -> bool:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      config = CacheConfig()
      configure_cache(config)

      await close_cache()

      return cache_module._cache_manager is None

    is_none = asyncio.run(run_test())
    assert is_none is True

  def test_close_cache_handles_not_configured(self) -> None:
    """Test close_cache handles case when not configured."""

    async def run_test() -> None:
      import reverie.cache as cache_module

      cache_module._cache_manager = None

      # Should not raise
      await close_cache()

    asyncio.run(run_test())  # Should complete without error


# CacheBackend Abstract Class Tests


class TestCacheBackendAbstract:
  """Test suite for CacheBackend abstract class."""

  def test_cannot_instantiate_directly(self) -> None:
    """Test CacheBackend cannot be instantiated directly."""
    with pytest.raises(TypeError):
      CacheBackend()  # type: ignore[abstract]

  def test_concrete_implementation_works(self) -> None:
    """Test concrete implementations are valid."""
    # MemoryCache is a valid concrete implementation
    cache = MemoryCache()
    assert isinstance(cache, CacheBackend)


# Edge Cases and Integration Tests


class TestCacheEdgeCases:
  """Test suite for edge cases and integration scenarios."""

  def test_cache_none_value(self) -> None:
    """Test caching None value."""

    async def run_test() -> tuple[bool, None | str]:
      cache = MemoryCache()
      await cache.set('key', None)

      # Note: get() returns None for both missing keys and None values
      # exist() should distinguish
      exists = await cache.exists('key')
      value = await cache.get('key')

      return exists, value

    exists, value = asyncio.run(run_test())
    assert exists is True
    assert value is None

  def test_cache_empty_string(self) -> None:
    """Test caching empty string."""

    async def run_test() -> str | None:
      cache = MemoryCache()
      await cache.set('key', '')
      return await cache.get('key')

    value = asyncio.run(run_test())
    assert value == ''

  def test_cache_large_value(self) -> None:
    """Test caching large value."""

    async def run_test() -> int:
      cache = MemoryCache()
      large_data = {'items': list(range(10000))}
      await cache.set('large', large_data)
      result = await cache.get('large')
      return len(result['items']) if result else 0

    length = asyncio.run(run_test())
    assert length == 10000

  def test_cache_special_characters_in_key(self) -> None:
    """Test cache handles special characters in keys."""

    async def run_test() -> str | None:
      cache = MemoryCache()
      await cache.set('key:with:colons', 'value')
      await cache.set('key/with/slashes', 'value2')
      await cache.set('key with spaces', 'value3')

      return await cache.get('key:with:colons')

    value = asyncio.run(run_test())
    assert value == 'value'

  def test_concurrent_access(self) -> None:
    """Test concurrent cache access."""

    async def run_test() -> int:
      cache = MemoryCache()

      async def writer(i: int) -> None:
        await cache.set(f'key{i}', f'value{i}')

      async def reader(i: int) -> str | None:
        return await cache.get(f'key{i}')

      # Concurrent writes
      await asyncio.gather(*[writer(i) for i in range(100)])

      # Check all values
      results = await asyncio.gather(*[reader(i) for i in range(100)])

      return sum(1 for r in results if r is not None)

    count = asyncio.run(run_test())
    assert count == 100

  def test_stats_update_correctly(self) -> None:
    """Test cache stats update correctly."""

    async def run_test() -> CacheStats:
      config = CacheConfig(backend='memory')
      manager = CacheManager(config)

      # Generate some hits and misses
      await manager.get('nonexistent1')  # miss
      await manager.get('nonexistent2')  # miss
      await manager.set('key', 'value')
      await manager.get('key')  # hit
      await manager.get('key')  # hit
      await manager.get('key')  # hit

      return manager.stats

    stats = asyncio.run(run_test())
    assert stats.hits == 3
    assert stats.misses == 2
    assert stats.hit_ratio == 0.6
