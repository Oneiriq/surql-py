# Query Optimization Hints

Complete guide to using query optimization hints in reverie to improve database query performance.

## Table of Contents

- [Overview](#overview)
- [Why Use Query Hints](#why-use-query-hints)
- [Available Hint Types](#available-hint-types)
- [Using Hints with Query Builder](#using-hints-with-query-builder)
- [Hint Reference](#hint-reference)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)

## Overview

Query optimization hints allow you to guide SurrealDB's query planner to make better decisions about query execution. By providing hints, you can:

- Force or suggest the use of specific indexes
- Control query execution timeouts
- Enable parallel processing for large datasets
- Optimize fetch strategies for memory efficiency
- Analyze query execution plans

Hints are rendered as SQL comments in the generated SurrealQL, maintaining compatibility while providing optimization guidance.

## Why Use Query Hints

### Performance Optimization

Query hints help you optimize performance in specific scenarios:

- **Slow queries** - Force index usage or enable parallel execution
- **Large datasets** - Control fetch strategies to manage memory
- **Timeouts** - Prevent queries from running too long
- **Analysis** - Use EXPLAIN to understand query execution

### When to Use Hints

Use hints when:

- You know a specific index performs better for your query
- Processing large datasets that could benefit from parallelization
- You need to prevent long-running queries in production
- Debugging performance issues with execution plans

### When NOT to Use Hints

Avoid hints when:

- The query is already performing well
- You haven't profiled or measured performance
- You're unsure which optimization to apply
- The database query planner is making good decisions

## Available Hint Types

reverie supports five types of query optimization hints:

| Hint Type | Purpose | Use Case |
|-----------|---------|----------|
| **IndexHint** | Force/suggest index usage | When you know the optimal index |
| **ParallelHint** | Enable parallel execution | Large dataset processing |
| **TimeoutHint** | Set query timeout | Prevent long-running queries |
| **FetchHint** | Control fetch strategy | Optimize memory vs. latency |
| **ExplainHint** | Get execution plan | Performance analysis |

## Using Hints with Query Builder

### Basic Usage

Add hints to queries using builder methods:

```python
from reverie.query import Query

# Add a single hint
query = (
  Query()
    .select(['name', 'email'])
    .from_table('user')
    .where('age >= 18')
    .use_index('age_idx')
)

# Result: /* USE INDEX user.age_idx */ SELECT name, email FROM user WHERE age >= 18
```

### Combining Multiple Hints

Chain multiple hints together:

```python
query = (
  Query()
    .select()
    .from_table('large_table')
    .where('status = "active"')
    .use_index('status_idx')
    .with_timeout(30.0)
    .parallel(max_workers=4)
    .with_fetch('batch', batch_size=1000)
)

# Result includes all hints:
# /* USE INDEX large_table.status_idx */
# /* TIMEOUT 30.0s */
# /* PARALLEL 4 */
# /* FETCH BATCH 1000 */
# SELECT * FROM large_table WHERE status = "active"
```

### Using with_hints() Method

Add multiple hints at once:

```python
from reverie.query.hints import IndexHint, TimeoutHint, ParallelHint

query = (
  Query()
    .select()
    .from_table('analytics_data')
    .where('date >= $start_date')
    .with_hints(
      IndexHint(table='analytics_data', index='date_idx'),
      TimeoutHint(seconds=60.0),
      ParallelHint(enabled=True, max_workers=6),
    )
)
```

## Hint Reference

### IndexHint

Force or suggest the use of a specific index.

**Constructor:**

```python
from reverie.query.hints import IndexHint

hint = IndexHint(
  table='user',      # Table name
  index='email_idx', # Index name
  force=False,       # Force vs. suggest (default: False)
)
```

**Query Builder Methods:**

```python
# Suggest index usage
query.use_index('email_idx')

# Force index usage
query.force_index('email_idx')
```

**Output:**

```sql
/* USE INDEX user.email_idx */    -- Suggestion
/* FORCE INDEX user.email_idx */  -- Forced
```

**When to Use:**

- You've profiled queries and know which index performs best
- The query planner is choosing a suboptimal index
- You want to ensure a specific access path

**Example:**

```python
# Force use of email index for email lookups
query = (
  Query()
    .select(['id', 'name', 'email'])
    .from_table('user')
    .where('email = $email')
    .force_index('email_idx')
)
```

### ParallelHint

Enable or configure parallel query execution.

**Constructor:**

```python
from reverie.query.hints import ParallelHint

# Enable with default workers
hint = ParallelHint(enabled=True)

# Enable with specific worker count
hint = ParallelHint(enabled=True, max_workers=8)

# Disable parallel execution
hint = ParallelHint(enabled=False)
```

**Query Builder Method:**

```python
# Enable with default workers
query.parallel()

# Enable with specific worker count
query.parallel(max_workers=8)
```

**Output:**

```sql
/* PARALLEL ON */   -- Default workers
/* PARALLEL 8 */    -- 8 workers
/* PARALLEL OFF */  -- Disabled
```

**When to Use:**

- Processing large datasets (millions of records)
- Complex aggregations across many rows
- Queries that can be parallelized without conflicts

**Performance Considerations:**

- More workers ≠ better performance (diminishing returns)
- Sweet spot is typically 4-8 workers
- Consider server CPU count
- Monitor resource usage

**Example:**

```python
# Process large dataset with parallel execution
query = (
  Query()
    .select(['category', 'count() as total'])
    .from_table('events')
    .where('date >= $start_date')
    .group_by(['category'])
    .parallel(max_workers=6)
)
```

### TimeoutHint

Set a custom timeout for query execution.

**Constructor:**

```python
from reverie.query.hints import TimeoutHint

hint = TimeoutHint(seconds=30.0)
```

**Query Builder Method:**

```python
query.with_timeout(30.0)
```

**Output:**

```sql
/* TIMEOUT 30.0s */
```

**When to Use:**

- Prevent queries from running indefinitely
- Set stricter timeouts for user-facing queries
- Allow longer timeouts for batch processing

**Common Timeout Values:**

- **Interactive queries**: 5-10 seconds
- **Reports/dashboards**: 30-60 seconds
- **Batch processing**: 300+ seconds
- **Real-time APIs**: 1-3 seconds

**Example:**

```python
# User search with strict timeout
query = (
  Query()
    .select()
    .from_table('user')
    .where('username CONTAINS $search')
    .with_timeout(5.0)
    .limit(50)
)
```

### FetchHint

Control how records are fetched from the database.

**Constructor:**

```python
from reverie.query.hints import FetchHint

# Eager - load all results immediately
hint = FetchHint(strategy='eager')

# Lazy - fetch on demand
hint = FetchHint(strategy='lazy')

# Batch - fetch in batches
hint = FetchHint(strategy='batch', batch_size=1000)
```

**Query Builder Method:**

```python
# Eager fetch
query.with_fetch('eager')

# Batch fetch
query.with_fetch('batch', batch_size=1000)

# Lazy fetch
query.with_fetch('lazy')
```

**Output:**

```sql
/* FETCH EAGER */
/* FETCH BATCH 1000 */
/* FETCH LAZY */
```

**Strategy Comparison:**

| Strategy | Memory Usage | Latency | Best For |
|----------|--------------|---------|----------|
| **eager** | High | Low | Small result sets (<1000 rows) |
| **batch** | Medium | Medium | Medium to large datasets |
| **lazy** | Low | High | Streaming, large datasets |

**When to Use Each:**

- **Eager**: Small result sets, all data needed immediately
- **Batch**: Large datasets, processing in chunks
- **Lazy**: Very large datasets, streaming scenarios

**Example:**

```python
# Fetch large result set in batches
query = (
  Query()
    .select()
    .from_table('log_entries')
    .where('created_at > $cutoff')
    .with_fetch('batch', batch_size=5000)
    .parallel(max_workers=4)
)
```

### ExplainHint

Request query execution plan for performance analysis.

**Constructor:**

```python
from reverie.query.hints import ExplainHint

# Basic explain
hint = ExplainHint()

# Full execution plan
hint = ExplainHint(full=True)
```

**Query Builder Method:**

```python
# Basic explain
query.explain()

# Full explain
query.explain(full=True)
```

**Output:**

```sql
/* EXPLAIN */
/* EXPLAIN FULL */
```

**When to Use:**

- Debugging slow queries
- Understanding index usage
- Verifying optimization effectiveness
- Learning query planner behavior

**Example:**

```python
# Analyze query execution plan
query = (
  Query()
    .select()
    .from_table('orders')
    .where('total > 1000 AND status = "pending"')
    .order_by('created_at', 'DESC')
    .explain(full=True)
)

# Execute and examine the plan
async with get_client(config) as client:
  result = await query.execute(client)
  print(result)  # Contains execution plan
```

## Best Practices

### 1. Profile Before Optimizing

Always measure performance before adding hints:

```python
import time

async def measure_query_performance():
  query = Query().select().from_table('user').where('age > 18')
  
  start = time.time()
  async with get_client(config) as client:
    results = await query.execute(client)
  duration = time.time() - start
  
  print(f'Query took {duration:.2f}s')
  
  # Only add hints if performance is unacceptable
```

### 2. Start with EXPLAIN

Use EXPLAIN to understand current behavior:

```python
# First, analyze the query
analysis_query = query.explain(full=True)

# Then optimize based on findings
optimized_query = (
  query
    .use_index('optimal_idx')
    .parallel(max_workers=4)
)
```

### 3. Test Hint Combinations

Different hint combinations can have varying effects:

```python
# Test different configurations
configs = [
  {'workers': 2, 'batch': 500},
  {'workers': 4, 'batch': 1000},
  {'workers': 8, 'batch': 2000},
]

for config in configs:
  query = (
    base_query
      .parallel(max_workers=config['workers'])
      .with_fetch('batch', batch_size=config['batch'])
  )
  
  # Measure and compare
  duration = await measure_performance(query)
  print(f"Workers: {config['workers']}, Batch: {config['batch']} -> {duration:.2f}s")
```

### 4. Be Conservative with Timeouts

Set timeouts that allow legitimate queries to complete:

```python
# Too strict - may fail valid queries
query.with_timeout(1.0)  # ❌

# Reasonable for user queries
query.with_timeout(10.0)  # ✓

# Generous for batch operations
query.with_timeout(300.0)  # ✓
```

### 5. Document Why Hints Are Used

Add comments explaining optimization choices:

```python
# This query scans millions of records, so we:
# - Force the date index for efficient filtering
# - Enable parallel processing for speed
# - Use batch fetching to manage memory
# - Set 5-minute timeout for large batches
query = (
  Query()
    .select()
    .from_table('events')
    .where('date >= $start_date')
    .force_index('date_idx')
    .parallel(max_workers=6)
    .with_fetch('batch', batch_size=10000)
    .with_timeout(300.0)
)
```

### 6. Hint Override Behavior

Later hints override earlier hints of the same type:

```python
query = (
  Query()
    .select()
    .from_table('user')
    .with_timeout(10.0)  # Initial timeout
    .parallel(max_workers=4)
    .with_timeout(30.0)  # Overrides to 30s
)

# Final query has 30s timeout, not 10s
```

### 7. Validate Hints Match Query

Ensure IndexHint table matches the query table:

```python
from reverie.query.hints import validate_hint, IndexHint

hint = IndexHint(table='user', index='email_idx')
errors = validate_hint(hint, table='user')  # Returns []

errors = validate_hint(hint, table='post')  # Returns error
# ['Index hint table "user" does not match query table "post"']
```

## Troubleshooting

### Hints Not Applied

**Problem**: Query runs without hints being applied.

**Solutions**:

1. Verify hint syntax:
   ```python
   query = Query().select().from_table('user').use_index('idx')
   print(query.to_surql())  # Check output includes hint
   ```

2. Ensure hints are supported by SurrealDB version
3. Check server logs for hint processing

### Index Hint Ignored

**Problem**: FORCE INDEX hint is ignored.

**Possible Causes**:

- Index doesn't exist on the table
- Index name is misspelled
- Query structure prevents index usage (e.g., function on indexed column)

**Solution**:

```python
# Verify index exists
async with get_client(config) as client:
  result = await client.execute('INFO FOR TABLE user')
  print(result)  # Check indexes

# Use EXPLAIN to see actual index usage
query = (
  Query()
    .select()
    .from_table('user')
    .where('email = $email')
    .force_index('email_idx')
    .explain(full=True)
)
```

### Parallel Execution Not Faster

**Problem**: Adding parallel hint doesn't improve performance.

**Reasons**:

- Dataset is too small (overhead > benefit)
- Bottleneck is I/O, not CPU
- Too many workers causing contention
- Sequential dependencies in query

**Solution**:

```python
# Try different worker counts
for workers in [2, 4, 8, 16]:
  query = base_query.parallel(max_workers=workers)
  duration = await measure_performance(query)
  print(f'{workers} workers: {duration:.2f}s')

# Use the optimal configuration
```

### Memory Issues with Eager Fetch

**Problem**: Out of memory errors with large result sets.

**Solution**:

```python
# Switch from eager to batch
query = (
  Query()
    .select()
    .from_table('large_table')
    .with_fetch('batch', batch_size=1000)  # Not 'eager'
)
```

### Timeout Too Short

**Problem**: Legitimate queries timing out.

**Solution**:

```python
# Increase timeout for complex queries
query = (
  Query()
    .select()
    .from_table('analytics')
    .where('complex_calculation(data) > threshold')
    .with_timeout(120.0)  # Increased from 30s
)
```

## Examples

### Example 1: User Search Optimization

```python
from reverie.query import Query

async def search_users_optimized(search_term: str):
  """Optimized user search with hints."""
  query = (
    Query()
      .select(['id', 'username', 'email', 'created_at'])
      .from_table('user')
      .where('username CONTAINS $search OR email CONTAINS $search')
      .force_index('username_email_idx')  # Force composite index
      .with_timeout(5.0)  # Quick timeout for user-facing search
      .with_fetch('batch', batch_size=100)
      .limit(100)
  )
  
  async with get_client(config) as client:
    results = await query.execute(client, {'search': search_term})
    return results
```

### Example 2: Large Dataset Processing

```python
async def process_event_logs(start_date: str):
  """Process millions of event logs efficiently."""
  query = (
    Query()
      .select(['event_type', 'count() as total', 'avg(duration) as avg_duration'])
      .from_table('events')
      .where('created_at >= $start_date')
      .group_by(['event_type'])
      .use_index('created_at_idx')
      .parallel(max_workers=8)  # Utilize multiple cores
      .with_fetch('batch', batch_size=10000)  # Large batches for efficiency
      .with_timeout(600.0)  # 10-minute timeout
  )
  
  async with get_client(config) as client:
    results = await query.execute(client, {'start_date': start_date})
    return results
```

### Example 3: Query Performance Analysis

```python
async def analyze_slow_query():
  """Analyze and optimize a slow query."""
  
  # Step 1: Run with EXPLAIN to see current plan
  analysis = (
    Query()
      .select()
      .from_table('orders')
      .where('total > 1000 AND status = "pending"')
      .order_by('created_at', 'DESC')
      .explain(full=True)
  )
  
  async with get_client(config) as client:
    plan = await analysis.execute(client)
    print('Execution Plan:', plan)
    
    # Step 2: Apply optimizations based on plan
    optimized = (
      Query()
        .select()
        .from_table('orders')
        .where('total > 1000 AND status = "pending"')
        .order_by('created_at', 'DESC')
        .use_index('total_status_idx')  # Based on EXPLAIN output
        .with_timeout(15.0)
        .parallel(max_workers=4)
    )
    
    # Step 3: Verify improvement
    optimized_plan = optimized.explain(full=True)
    optimized_result = await optimized_plan.execute(client)
    print('Optimized Plan:', optimized_result)
```

### Example 4: Dynamic Hint Selection

```python
async def adaptive_query(table: str, filters: dict, row_estimate: int):
  """Adapt hints based on estimated result size."""
  query = Query().select().from_table(table)
  
  # Add filters
  for field, value in filters.items():
    query = query.where(f'{field} = ${field}')
  
  # Adapt hints based on data volume
  if row_estimate < 1000:
    # Small dataset - eager fetch
    query = query.with_fetch('eager')
  elif row_estimate < 100000:
    # Medium dataset - batch with moderate workers
    query = (
      query
        .with_fetch('batch', batch_size=1000)
        .parallel(max_workers=4)
    )
  else:
    # Large dataset - aggressive optimization
    query = (
      query
        .with_fetch('batch', batch_size=5000)
        .parallel(max_workers=8)
        .with_timeout(120.0)
    )
  
  async with get_client(config) as client:
    return await query.execute(client, filters)
```

### Example 5: Production Query Template

```python
from reverie.query import Query
from reverie.query.hints import IndexHint, TimeoutHint, FetchHint

class QueryTemplates:
  """Production-ready query templates with optimizations."""
  
  @staticmethod
  def user_lookup(index_name: str = 'email_idx'):
    """Optimized user lookup query."""
    return (
      Query()
        .select()
        .from_table('user')
        .force_index(index_name)
        .with_timeout(3.0)
        .with_fetch('eager')
    )
  
  @staticmethod
  def bulk_analytics():
    """Heavy analytics query."""
    return (
      Query()
        .select()
        .from_table('analytics_events')
        .parallel(max_workers=6)
        .with_fetch('batch', batch_size=10000)
        .with_timeout(300.0)
    )
  
  @staticmethod
  def real_time_dashboard():
    """Fast queries for dashboards."""
    return (
      Query()
        .with_timeout(5.0)
        .with_fetch('batch', batch_size=500)
    )
```

## Additional Resources

- [Query Builder Documentation](queries.md) - Complete query building reference
- [Query Hints Example Code](examples/query_hints_example.py) - More code examples
- [Performance Best Practices](queries.md#best-practices) - General query optimization

## Summary

Query optimization hints provide fine-grained control over query execution:

- **IndexHint** - Guide index selection
- **ParallelHint** - Enable parallel processing
- **TimeoutHint** - Control query duration
- **FetchHint** - Optimize memory usage
- **ExplainHint** - Analyze execution plans

Use hints judiciously, always profile first, and document your optimization decisions.
