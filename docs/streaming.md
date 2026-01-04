# Live Queries & Streaming Guide

This guide covers reverie's live query and real-time streaming system for building reactive applications with SurrealDB.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Live Query API](#live-query-api)
- [Event Handling Patterns](#event-handling-patterns)
- [Diff Mode](#diff-mode)
- [Connection Requirements](#connection-requirements)
- [Error Handling for Streams](#error-handling-for-streams)
- [Cleanup and Resource Management](#cleanup-and-resource-management)
- [Best Practices](#best-practices)
- [Complete Examples](#complete-examples)
- [API Reference](#api-reference)

## Overview

reverie provides a streaming system for subscribing to real-time data changes in SurrealDB. Live queries allow your application to receive instant notifications when records are created, updated, or deleted.

### Key Features

- **Real-time notifications** - Receive immediate updates on data changes
- **Async iterator support** - Consume events with `async for` loops
- **Callback-based subscriptions** - Register functions to handle events
- **Diff mode** - Efficient JSON Patch format for change tracking
- **Multiple subscriptions** - Monitor multiple tables simultaneously
- **Automatic cleanup** - Resource management with context managers

### Use Cases

- Real-time dashboards and analytics
- Live chat and messaging applications
- Collaborative editing tools
- Notification systems
- Activity feeds and timelines
- IoT data monitoring
- Live inventory tracking

## Prerequisites

### WebSocket Connection Required

Live queries require a WebSocket connection to SurrealDB. HTTP connections do not support real-time streaming.

```python
from reverie.connection.config import ConnectionConfig

# WebSocket connection (required for live queries)
config = ConnectionConfig(
  db_url='ws://localhost:8000/rpc',  # WebSocket protocol
  db_ns='development',
  db='main',
  db_user='root',
  db_pass='root',
  enable_live_queries=True,  # Enable streaming support
)
```

### Configuration Validation

The [`ConnectionConfig`](src/reverie/connection/config.py:9) automatically validates that live queries are only enabled with WebSocket connections:

```python
# This will raise a validation error
config = ConnectionConfig(
  db_url='http://localhost:8000',  # HTTP protocol
  enable_live_queries=True,  # Cannot use with HTTP
)
# ValueError: Live queries require WebSocket connection (ws:// or wss://)
```

## Quick Start

### Basic Live Query Subscription

```python
import asyncio
from reverie.connection.client import DatabaseClient
from reverie.connection.config import ConnectionConfig
from reverie.connection.streaming import StreamingManager

async def watch_users():
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='development',
    db='main',
    enable_live_queries=True,
  )

  async with DatabaseClient(config) as client:
    # Access the streaming manager
    streaming = client._streaming

    # Start a live query on the 'user' table
    query = await streaming.live('user')

    print(f"Watching for changes on 'user' table...")

    # Subscribe and process events
    async for notification in streaming.subscribe(query):
      action = notification.get('action')
      result = notification.get('result')
      print(f"{action}: {result}")

if __name__ == '__main__':
  asyncio.run(watch_users())
```

### Triggering Events

In another terminal or process, make changes to see real-time notifications:

```python
# Create a user - triggers CREATE event
await client.create('user', {'name': 'Alice', 'email': 'alice@example.com'})

# Update a user - triggers UPDATE event
await client.update('user:alice', {'email': 'alice.new@example.com'})

# Delete a user - triggers DELETE event
await client.delete('user:alice')
```

## Live Query API

### StreamingManager

The [`StreamingManager`](src/reverie/connection/streaming.py:95) class handles all live query operations. It is automatically initialized when you connect with `enable_live_queries=True`.

```python
from reverie.connection.streaming import StreamingManager

# Access via DatabaseClient
async with DatabaseClient(config) as client:
  streaming: StreamingManager = client._streaming
```

### Starting a Live Query

Use the [`live()`](src/reverie/connection/streaming.py:109) method to start watching a table:

```python
# Watch all changes on a table
query = await streaming.live('user')

# Watch with diff mode for efficient change tracking
query = await streaming.live('user', diff=True)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table` | `str` | required | Table name to watch |
| `diff` | `bool` | `False` | Return JSON Patch diffs instead of full records |

### LiveQuery Object

The [`LiveQuery`](src/reverie/connection/streaming.py:20) object represents an active subscription:

```python
query = await streaming.live('user')

# Properties
print(query.query_uuid)  # UUID identifying this subscription
print(query.table)       # Table being watched
print(query.diff)        # Whether diff mode is enabled
print(query.is_active)   # Whether query is still active
```

### Subscription Methods

#### Async Iterator Subscription

Use [`subscribe()`](src/reverie/connection/streaming.py:155) for async iteration:

```python
query = await streaming.live('user')

async for notification in streaming.subscribe(query):
  handle_notification(notification)
```

#### Callback-Based Subscription

Use [`subscribe_with_callback()`](src/reverie/connection/streaming.py:198) for event-driven handling:

```python
def on_change(notification):
  print(f"Change detected: {notification}")

query = await streaming.live('user')
await streaming.subscribe_with_callback(query, on_change)

# Subscription runs in background task
# Do other work while receiving events...
```

### Stopping Live Queries

#### Kill Single Query

Use [`kill()`](src/reverie/connection/streaming.py:227) to stop a specific subscription:

```python
await streaming.kill(query)
```

#### Kill All Queries

Use [`kill_all()`](src/reverie/connection/streaming.py:263) to stop all active subscriptions:

```python
await streaming.kill_all()
```

### Managing Active Queries

Get a list of all active subscriptions:

```python
active = streaming.get_active_queries()
print(f"Active subscriptions: {len(active)}")

for query in active:
  print(f"  - {query.table} (UUID: {query.query_uuid})")
```

## Event Handling Patterns

### Notification Structure

Each notification contains:

```python
{
  'action': 'CREATE' | 'UPDATE' | 'DELETE' | 'CLOSE',
  'result': {...}  # The affected record data
}
```

### Handling Different Event Types

```python
async for notification in streaming.subscribe(query):
  action = notification.get('action')
  result = notification.get('result')

  if action == 'CREATE':
    handle_create(result)
  elif action == 'UPDATE':
    handle_update(result)
  elif action == 'DELETE':
    handle_delete(result)
  elif action == 'CLOSE':
    # Query was closed by the server
    break
```

### Using Async Callbacks

The streaming system supports both synchronous and asynchronous callbacks:

```python
# Synchronous callback
def sync_handler(notification):
  print(f"Sync: {notification}")

# Asynchronous callback
async def async_handler(notification):
  await process_notification(notification)
  await update_cache(notification)

query = await streaming.live('user')

# Add multiple callbacks
query.add_callback(sync_handler)
query.add_callback(async_handler)

# Start subscription (callbacks are invoked automatically)
async for _ in streaming.subscribe(query):
  pass
```

### Multiple Table Subscriptions

Subscribe to multiple tables simultaneously:

```python
async def multi_table_watch():
  async with DatabaseClient(config) as client:
    streaming = client._streaming

    # Start watching multiple tables
    user_query = await streaming.live('user')
    post_query = await streaming.live('post')
    comment_query = await streaming.live('comment')

    # Process each stream in separate tasks
    async def watch_table(query, name):
      async for notification in streaming.subscribe(query):
        print(f"[{name}] {notification}")

    await asyncio.gather(
      watch_table(user_query, 'user'),
      watch_table(post_query, 'post'),
      watch_table(comment_query, 'comment'),
    )
```

## Diff Mode

Diff mode returns JSON Patch operations instead of full records, making it more efficient for large documents with small changes.

### Enabling Diff Mode

```python
# Enable diff mode
query = await streaming.live('user', diff=True)
```

### JSON Patch Format

With diff mode enabled, notifications contain patch operations:

```python
# Without diff mode (full record)
{
  'action': 'UPDATE',
  'result': {
    'id': 'user:123',
    'name': 'Alice',
    'email': 'alice.new@example.com',
    'age': 30,
    # ... all fields
  }
}

# With diff mode (patch operations)
{
  'action': 'UPDATE',
  'result': [
    {'op': 'replace', 'path': '/email', 'value': 'alice.new@example.com'}
  ]
}
```

### Patch Operations

JSON Patch supports these operations:

| Operation | Description | Example |
|-----------|-------------|---------|
| `add` | Add a new field | `{'op': 'add', 'path': '/status', 'value': 'active'}` |
| `replace` | Replace existing value | `{'op': 'replace', 'path': '/email', 'value': 'new@example.com'}` |
| `remove` | Remove a field | `{'op': 'remove', 'path': '/temporary_field'}` |

### Applying Patches

Use a JSON Patch library to apply diffs to local state:

```python
import jsonpatch

# Local cache of records
cache = {}

async for notification in streaming.subscribe(query):
  action = notification.get('action')
  result = notification.get('result')

  if action == 'CREATE':
    # For creates, result is the full record
    cache[result['id']] = result

  elif action == 'UPDATE':
    # For updates in diff mode, result is patch operations
    record_id = notification.get('id')  # Get record ID from notification
    if record_id in cache:
      patch = jsonpatch.JsonPatch(result)
      cache[record_id] = patch.apply(cache[record_id])

  elif action == 'DELETE':
    record_id = result.get('id')
    cache.pop(record_id, None)
```

### When to Use Diff Mode

**Use diff mode when:**

- Records are large with many fields
- Changes typically affect few fields
- Bandwidth is limited
- You maintain local state that needs incremental updates

**Avoid diff mode when:**

- Records are small
- You always need full record state
- Patch application overhead exceeds bandwidth savings

## Connection Requirements

### WebSocket Protocol

Live queries require WebSocket connections:

```python
# Supported protocols for live queries
'ws://localhost:8000/rpc'   # WebSocket (development)
'wss://db.example.com/rpc'  # Secure WebSocket (production)

# NOT supported for live queries
'http://localhost:8000'     # HTTP
'https://db.example.com'    # HTTPS
```

### Connection Configuration

```python
from reverie.connection.config import ConnectionConfig

config = ConnectionConfig(
  # WebSocket URL (required)
  db_url='ws://localhost:8000/rpc',

  # Database selection
  db_ns='development',
  db='main',

  # Authentication (optional)
  db_user='root',
  db_pass='root',

  # Enable streaming (default: True)
  enable_live_queries=True,

  # Connection pool settings
  db_max_connections=10,    # Max concurrent connections
  db_timeout=30.0,          # Connection timeout in seconds

  # Retry settings for reconnection
  db_retry_max_attempts=3,  # Retry attempts on connection loss
  db_retry_min_wait=1.0,    # Minimum wait between retries
  db_retry_max_wait=10.0,   # Maximum wait between retries
  db_retry_multiplier=2.0,  # Exponential backoff multiplier
)
```

### TLS/SSL Connections

For production, use secure WebSocket connections:

```python
config = ConnectionConfig(
  db_url='wss://db.example.com/rpc',  # Secure WebSocket
  db_ns='production',
  db='main',
  db_user='app_user',
  db_pass='secure_password',
  enable_live_queries=True,
)
```

## Error Handling for Streams

### StreamingError Exception

The [`StreamingError`](src/reverie/connection/streaming.py:14) exception is raised for streaming-related failures:

```python
from reverie.connection.streaming import StreamingError

try:
  query = await streaming.live('user')
  async for notification in streaming.subscribe(query):
    process(notification)
except StreamingError as e:
  print(f"Streaming error: {e}")
  # Handle connection loss, timeout, etc.
```

### Common Error Scenarios

#### Live Query Start Failure

```python
try:
  query = await streaming.live('nonexistent_table')
except StreamingError as e:
  print(f"Failed to start live query: {e}")
```

#### Subscription Failure

```python
query = await streaming.live('user')

try:
  async for notification in streaming.subscribe(query):
    process(notification)
except StreamingError as e:
  print(f"Subscription failed: {e}")
  print(f"Query active: {query.is_active}")  # Will be False
```

#### Kill Query Failure

```python
try:
  await streaming.kill(query)
except StreamingError as e:
  print(f"Failed to kill query: {e}")
```

### Reconnection Handling

Implement reconnection logic for long-running subscriptions:

```python
async def resilient_subscription(config, table):
  max_retries = 5
  retry_count = 0

  while retry_count < max_retries:
    try:
      async with DatabaseClient(config) as client:
        streaming = client._streaming
        query = await streaming.live(table)

        print(f"Connected, watching {table}...")
        retry_count = 0  # Reset on successful connection

        async for notification in streaming.subscribe(query):
          await process_notification(notification)

    except StreamingError as e:
      retry_count += 1
      wait_time = min(2 ** retry_count, 30)  # Exponential backoff
      print(f"Connection lost: {e}. Retrying in {wait_time}s... ({retry_count}/{max_retries})")
      await asyncio.sleep(wait_time)

  print("Max retries reached, giving up.")
```

### Graceful Degradation

Handle streaming errors without crashing the application:

```python
async def watch_with_fallback(config):
  async with DatabaseClient(config) as client:
    streaming = client._streaming

    try:
      query = await streaming.live('user')

      async for notification in streaming.subscribe(query):
        await handle_realtime_update(notification)

    except StreamingError:
      # Fall back to polling
      print("Live queries unavailable, falling back to polling...")
      while True:
        users = await client.select('user')
        await handle_poll_results(users)
        await asyncio.sleep(5)  # Poll every 5 seconds
```

## Cleanup and Resource Management

### Automatic Cleanup with Context Managers

Use context managers for automatic resource cleanup:

```python
async with DatabaseClient(config) as client:
  streaming = client._streaming
  query = await streaming.live('user')

  try:
    async for notification in streaming.subscribe(query):
      if should_stop():
        break
      process(notification)
  finally:
    # Always clean up the query
    if query.is_active:
      await streaming.kill(query)
# Client disconnects automatically on context exit
```

### Manual Cleanup

For manual lifecycle management:

```python
client = DatabaseClient(config)
await client.connect()

streaming = client._streaming
query = await streaming.live('user')

try:
  # Process notifications...
  async for notification in streaming.subscribe(query):
    process(notification)
finally:
  # Clean up in reverse order
  await streaming.kill(query)
  await client.disconnect()
```

### Cleanup All Active Queries

Before application shutdown:

```python
async def shutdown(client):
  streaming = client._streaming

  # Kill all active subscriptions
  active_count = len(streaming.get_active_queries())
  await streaming.kill_all()
  print(f"Cleaned up {active_count} live queries")

  await client.disconnect()
```

### Handling Callback Tasks

When using callback subscriptions, ensure tasks are properly cancelled:

```python
async def run_with_cleanup():
  async with DatabaseClient(config) as client:
    streaming = client._streaming
    query = await streaming.live('user')

    await streaming.subscribe_with_callback(query, on_change)

    try:
      # Application runs...
      await run_main_application()
    finally:
      # kill() automatically cancels the subscription task
      await streaming.kill(query)
```

## Best Practices

### 1. Always Clean Up Subscriptions

```python
# Good - Explicit cleanup
query = await streaming.live('user')
try:
  async for notification in streaming.subscribe(query):
    process(notification)
finally:
  await streaming.kill(query)

# Avoid - Orphaned subscription
query = await streaming.live('user')
async for notification in streaming.subscribe(query):
  if error:
    return  # Query left active!
```

### 2. Use Diff Mode for Large Records

```python
# Good - Efficient for large records
query = await streaming.live('document', diff=True)

# Avoid - Full records transferred on every change
query = await streaming.live('document', diff=False)
```

### 3. Handle All Event Types

```python
# Good - Handle all cases
async for notification in streaming.subscribe(query):
  action = notification.get('action')
  if action == 'CREATE':
    handle_create(notification)
  elif action == 'UPDATE':
    handle_update(notification)
  elif action == 'DELETE':
    handle_delete(notification)
  elif action == 'CLOSE':
    break

# Avoid - Missing event types
async for notification in streaming.subscribe(query):
  if notification['action'] == 'CREATE':
    handle_create(notification)
  # Updates and deletes silently ignored!
```

### 4. Implement Reconnection Logic

```python
# Good - Handles connection issues
async def resilient_watch():
  while True:
    try:
      async with DatabaseClient(config) as client:
        await watch_table(client)
    except (StreamingError, ConnectionError):
      await asyncio.sleep(5)
      continue

# Avoid - Crashes on connection loss
async def fragile_watch():
  async with DatabaseClient(config) as client:
    await watch_table(client)  # Crash if connection drops
```

### 5. Limit Concurrent Subscriptions

```python
# Good - Controlled number of subscriptions
MAX_SUBSCRIPTIONS = 10
queries = []

for table in important_tables[:MAX_SUBSCRIPTIONS]:
  queries.append(await streaming.live(table))

# Avoid - Unlimited subscriptions
for table in all_tables:  # Could be thousands!
  await streaming.live(table)
```

### 6. Use Async Callbacks for I/O Operations

```python
# Good - Async callback for database operations
async def on_user_created(notification):
  await send_welcome_email(notification['result'])
  await update_analytics(notification)

# Avoid - Blocking sync callback
def on_user_created(notification):
  send_welcome_email_sync(notification['result'])  # Blocks event loop!
```

### 7. Log Streaming Events for Debugging

```python
import structlog

logger = structlog.get_logger()

async for notification in streaming.subscribe(query):
  logger.debug(
    'live_query_notification',
    action=notification.get('action'),
    table=query.table,
    query_uuid=str(query.query_uuid),
  )
  await process(notification)
```

## Complete Examples

### Real-Time Dashboard

```python
import asyncio
from reverie.connection.client import DatabaseClient
from reverie.connection.config import ConnectionConfig
from reverie.connection.streaming import StreamingError

class RealtimeDashboard:
  def __init__(self, config: ConnectionConfig):
    self.config = config
    self.stats = {
      'users': 0,
      'active_sessions': 0,
      'orders_today': 0,
    }

  async def run(self):
    async with DatabaseClient(self.config) as client:
      streaming = client._streaming

      # Initialize stats
      await self._load_initial_stats(client)

      # Start live queries
      user_query = await streaming.live('user')
      session_query = await streaming.live('session')
      order_query = await streaming.live('order')

      # Process all streams concurrently
      try:
        await asyncio.gather(
          self._watch_users(streaming, user_query),
          self._watch_sessions(streaming, session_query),
          self._watch_orders(streaming, order_query),
          self._display_loop(),
        )
      finally:
        await streaming.kill_all()

  async def _load_initial_stats(self, client):
    self.stats['users'] = len(await client.select('user'))
    self.stats['active_sessions'] = len(await client.select('session'))

  async def _watch_users(self, streaming, query):
    async for notification in streaming.subscribe(query):
      action = notification.get('action')
      if action == 'CREATE':
        self.stats['users'] += 1
      elif action == 'DELETE':
        self.stats['users'] -= 1

  async def _watch_sessions(self, streaming, query):
    async for notification in streaming.subscribe(query):
      action = notification.get('action')
      if action == 'CREATE':
        self.stats['active_sessions'] += 1
      elif action == 'DELETE':
        self.stats['active_sessions'] -= 1

  async def _watch_orders(self, streaming, query):
    async for notification in streaming.subscribe(query):
      if notification.get('action') == 'CREATE':
        self.stats['orders_today'] += 1

  async def _display_loop(self):
    while True:
      print(f"\rUsers: {self.stats['users']} | "
            f"Sessions: {self.stats['active_sessions']} | "
            f"Orders: {self.stats['orders_today']}", end='')
      await asyncio.sleep(1)

# Usage
if __name__ == '__main__':
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='production',
    db='analytics',
    enable_live_queries=True,
  )
  dashboard = RealtimeDashboard(config)
  asyncio.run(dashboard.run())
```

### Chat Application

```python
import asyncio
from datetime import datetime
from reverie.connection.client import DatabaseClient
from reverie.connection.config import ConnectionConfig

class ChatRoom:
  def __init__(self, config: ConnectionConfig, room_id: str, user_id: str):
    self.config = config
    self.room_id = room_id
    self.user_id = user_id

  async def join(self):
    async with DatabaseClient(self.config) as client:
      streaming = client._streaming

      # Watch the messages table for this room
      query = await streaming.live('message')

      print(f"Joined room {self.room_id}. Type 'quit' to exit.")

      # Run message receiver and sender concurrently
      try:
        await asyncio.gather(
          self._receive_messages(streaming, query),
          self._send_messages(client),
        )
      finally:
        await streaming.kill(query)

  async def _receive_messages(self, streaming, query):
    async for notification in streaming.subscribe(query):
      if notification.get('action') != 'CREATE':
        continue

      message = notification.get('result', {})

      # Filter to this room
      if message.get('room_id') != self.room_id:
        continue

      # Don't show own messages (already displayed when sent)
      if message.get('sender_id') == self.user_id:
        continue

      timestamp = message.get('created_at', '')
      sender = message.get('sender_id', 'Unknown')
      content = message.get('content', '')

      print(f"\n[{timestamp}] {sender}: {content}")

  async def _send_messages(self, client):
    while True:
      # Get user input (run in executor to not block)
      content = await asyncio.get_event_loop().run_in_executor(
        None, input, ''
      )

      if content.lower() == 'quit':
        break

      if not content.strip():
        continue

      # Send message
      await client.create('message', {
        'room_id': self.room_id,
        'sender_id': self.user_id,
        'content': content,
        'created_at': datetime.now().isoformat(),
      })

      print(f"[You]: {content}")

# Usage
if __name__ == '__main__':
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='chat',
    db='main',
    enable_live_queries=True,
  )
  chat = ChatRoom(config, room_id='general', user_id='alice')
  asyncio.run(chat.join())
```

### Inventory Sync with Diff Mode

```python
import asyncio
import jsonpatch
from reverie.connection.client import DatabaseClient
from reverie.connection.config import ConnectionConfig

class InventorySync:
  def __init__(self, config: ConnectionConfig):
    self.config = config
    self.inventory: dict[str, dict] = {}

  async def run(self):
    async with DatabaseClient(self.config) as client:
      streaming = client._streaming

      # Load initial inventory
      products = await client.select('product')
      for product in products:
        self.inventory[product['id']] = product

      print(f"Loaded {len(self.inventory)} products")

      # Watch with diff mode for efficient updates
      query = await streaming.live('product', diff=True)

      try:
        async for notification in streaming.subscribe(query):
          await self._handle_change(notification)
      finally:
        await streaming.kill(query)

  async def _handle_change(self, notification):
    action = notification.get('action')
    result = notification.get('result')

    if action == 'CREATE':
      # Full record on create
      product_id = result['id']
      self.inventory[product_id] = result
      print(f"+ Added: {result.get('name')} (Stock: {result.get('stock', 0)})")

    elif action == 'UPDATE':
      # Apply JSON patch for updates
      product_id = notification.get('id')

      if product_id in self.inventory:
        try:
          patch = jsonpatch.JsonPatch(result)
          self.inventory[product_id] = patch.apply(self.inventory[product_id])

          # Check what changed
          for op in result:
            path = op.get('path', '')
            if '/stock' in path:
              new_stock = self.inventory[product_id].get('stock')
              print(f"~ Stock updated: {product_id} -> {new_stock}")
            elif '/price' in path:
              new_price = self.inventory[product_id].get('price')
              print(f"~ Price updated: {product_id} -> ${new_price}")

        except jsonpatch.JsonPatchException as e:
          print(f"Patch error for {product_id}: {e}")

    elif action == 'DELETE':
      product_id = result.get('id')
      if product_id in self.inventory:
        name = self.inventory[product_id].get('name', 'Unknown')
        del self.inventory[product_id]
        print(f"- Removed: {name}")

  def get_low_stock(self, threshold: int = 10) -> list[dict]:
    return [
      p for p in self.inventory.values()
      if p.get('stock', 0) < threshold
    ]

# Usage
if __name__ == '__main__':
  config = ConnectionConfig(
    db_url='ws://localhost:8000/rpc',
    db_ns='ecommerce',
    db='main',
    enable_live_queries=True,
  )
  sync = InventorySync(config)
  asyncio.run(sync.run())
```

## API Reference

### StreamingManager

| Method | Description |
|--------|-------------|
| [`live(table, diff=False)`](src/reverie/connection/streaming.py:109) | Start a live query on a table |
| [`subscribe(query)`](src/reverie/connection/streaming.py:155) | Subscribe to query as async iterator |
| [`subscribe_with_callback(query, callback)`](src/reverie/connection/streaming.py:198) | Subscribe with callback function |
| [`kill(query)`](src/reverie/connection/streaming.py:227) | Stop a live query |
| [`kill_all()`](src/reverie/connection/streaming.py:263) | Stop all active queries |
| [`get_active_queries()`](src/reverie/connection/streaming.py:272) | Get list of active queries |

### LiveQuery

| Property | Type | Description |
|----------|------|-------------|
| `query_uuid` | `UUID` | Unique identifier for the subscription |
| `table` | `str` | Table being watched |
| `diff` | `bool` | Whether diff mode is enabled |
| `is_active` | `bool` | Whether query is still active |

| Method | Description |
|--------|-------------|
| [`add_callback(callback)`](src/reverie/connection/streaming.py:48) | Add notification callback |
| [`remove_callback(callback)`](src/reverie/connection/streaming.py:56) | Remove notification callback |
| [`notify(notification)`](src/reverie/connection/streaming.py:65) | Notify all callbacks (internal) |
| [`deactivate()`](src/reverie/connection/streaming.py:84) | Mark query as inactive |

### StreamingError

```python
from reverie.connection.streaming import StreamingError

# Raised when:
# - Live query fails to start
# - Subscription encounters an error
# - Kill operation fails
```

### Notification Format

```python
{
  'action': str,  # 'CREATE', 'UPDATE', 'DELETE', or 'CLOSE'
  'result': dict, # Record data or patch operations (diff mode)
}
```

## Next Steps

- Learn about [Query Building](queries.md) for database operations
- Explore [Caching](caching.md) for performance optimization
- Check out [Schema Definition](schema.md) for data modeling
- See [Connection Configuration](installation.md#configuration) for setup options
