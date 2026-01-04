"""Cache decorator for query functions.

This module provides the cache_query decorator for caching
query results with configurable TTL and key generation.
"""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast, overload

P = ParamSpec('P')
T = TypeVar('T')


def _serialize_arg(arg: Any) -> str:
  """Serialize an argument to a string for cache key generation.

  Args:
      arg: The argument to serialize.

  Returns:
      A string representation suitable for hashing.
  """
  if arg is None:
    return 'None'
  if isinstance(arg, (str, int, float, bool)):
    return repr(arg)
  if isinstance(arg, (list, tuple)):
    return f'[{",".join(_serialize_arg(a) for a in arg)}]'
  if isinstance(arg, dict):
    items = sorted((k, _serialize_arg(v)) for k, v in arg.items())
    return f'{{{",".join(f"{k}:{v}" for k, v in items)}}}'
  if hasattr(arg, '__dict__'):
    # For objects, use their dict representation
    return _serialize_arg(vars(arg))
  # Fallback to string representation
  return str(arg)


def _generate_cache_key(
  func: Callable[..., Any],
  args: tuple[Any, ...],
  kwargs: dict[str, Any],
) -> str:
  """Generate a cache key from function and arguments.

  Args:
      func: The function being cached.
      args: Positional arguments.
      kwargs: Keyword arguments.

  Returns:
      A unique cache key string.
  """
  # Get function identifier
  module = func.__module__ or ''
  name = func.__qualname__

  # Serialize arguments
  args_str = ','.join(_serialize_arg(a) for a in args)
  kwargs_str = ','.join(f'{k}={_serialize_arg(v)}' for k, v in sorted(kwargs.items()))

  # Create a hash of the arguments for shorter keys
  combined = f'{module}.{name}({args_str},{kwargs_str})'
  key_hash = hashlib.sha256(combined.encode()).hexdigest()[:16]

  return f'{module}.{name}:{key_hash}'


@overload
def cache_query(
  __func: Callable[P, Awaitable[T]],
) -> Callable[P, Awaitable[T]]: ...


@overload
def cache_query(
  *,
  ttl: int | None = None,
  key: str | None = None,
  key_builder: Callable[..., str] | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]: ...


def cache_query(
  __func: Callable[P, Awaitable[T]] | None = None,
  *,
  ttl: int | None = None,
  key: str | None = None,
  key_builder: Callable[..., str] | None = None,
) -> Callable[P, Awaitable[T]] | Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
  """Decorator to cache query results.

  This decorator caches the results of async query functions,
  automatically managing cache keys and expiration.

  Args:
      ttl: Cache TTL in seconds. Uses global default if None.
      key: Static cache key. Generates from args if None.
      key_builder: Custom function to build cache key from arguments.
                   Receives the same arguments as the decorated function.

  Returns:
      The decorated function with caching enabled.

  Example:
      Basic usage with default settings::

          @cache_query
          async def get_users() -> list[User]:
              return await crud.find_all(User)

      With custom TTL::

          @cache_query(ttl=60)
          async def get_active_users() -> list[User]:
              return await crud.find_all(User, where="active = true")

      With static key::

          @cache_query(ttl=300, key="all_products")
          async def get_all_products() -> list[Product]:
              return await crud.find_all(Product)

      With custom key builder::

          @cache_query(key_builder=lambda user_id: f"user:{user_id}")
          async def get_user(user_id: str) -> User | None:
              return await crud.find_one(User, user_id)

  Note:
      The cache manager must be configured before using this decorator.
      Use configure_cache() to set up the global cache manager.
  """

  def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    # Ensure the function is async
    if not inspect.iscoroutinefunction(func):
      raise TypeError('cache_query can only decorate async functions')

    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
      # Import here to avoid circular imports
      from reverie.cache import get_cache_manager

      manager = get_cache_manager()

      # If cache is not configured, just call the function
      if manager is None:
        return await func(*args, **kwargs)

      # Determine cache key
      if key is not None:
        cache_key = key
      elif key_builder is not None:
        cache_key = key_builder(*args, **kwargs)
      else:
        cache_key = _generate_cache_key(func, args, kwargs)

      # Use cache manager to get or set
      async def factory() -> T:
        return await func(*args, **kwargs)

      return cast(T, await manager.get_or_set(cache_key, factory, ttl))

    # Store metadata on the wrapper for introspection
    wrapper._cache_ttl = ttl  # type: ignore[attr-defined]
    wrapper._cache_key = key  # type: ignore[attr-defined]
    wrapper._cache_key_builder = key_builder  # type: ignore[attr-defined]
    wrapper._cached = True  # type: ignore[attr-defined]

    return wrapper

  if __func is not None:
    # Called without arguments: @cache_query
    return decorator(__func)

  # Called with arguments: @cache_query(ttl=60)
  return decorator


def cache_key_for(
  func: Callable[..., Any],
  *args: Any,
  **kwargs: Any,
) -> str:
  """Generate the cache key that would be used for a function call.

  This is useful for manually invalidating specific cache entries.

  Args:
      func: The cached function.
      *args: Positional arguments.
      **kwargs: Keyword arguments.

  Returns:
      The cache key string.

  Example:
      >>> key = cache_key_for(get_user, user_id="123")
      >>> await invalidate(key=key)
  """
  # Check if the function has a custom key builder
  if hasattr(func, '_cache_key_builder') and func._cache_key_builder is not None:
    result: str = func._cache_key_builder(*args, **kwargs)
    return result

  # Check if the function has a static key
  if hasattr(func, '_cache_key') and func._cache_key is not None:
    key_value: str = func._cache_key
    return key_value

  # Generate key from arguments
  # Get the original function if wrapped
  original = func
  while hasattr(original, '__wrapped__'):
    original = original.__wrapped__

  return _generate_cache_key(original, args, kwargs)


def is_cached(func: Callable[..., Any]) -> bool:
  """Check if a function is decorated with cache_query.

  Args:
      func: The function to check.

  Returns:
      True if the function is cached, False otherwise.
  """
  return getattr(func, '_cached', False)
