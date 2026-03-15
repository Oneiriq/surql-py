"""Query optimization hints examples.

This example demonstrates how to use query optimization hints to guide
SurrealDB query execution for better performance.
"""

import asyncio

from surql.query import Query
from surql.query.hints import (
    FetchHint,
    IndexHint,
    ParallelHint,
    TimeoutHint,
)


async def basic_index_hint() -> None:
    """Example: Using index hints to force specific index usage."""
    print('\n=== Basic Index Hint ===')

    # Suggest using a specific index
    query = (
        Query()
        .select(['name', 'email'])
        .from_table('user')
        .where('email LIKE "%@example.com"')
        .use_index('email_idx')
    )

    print('USE INDEX Hint:')
    print(query.to_surql())
    print()

    # Force using a specific index (even if query planner thinks otherwise)
    query = (
        Query()
        .select(['name', 'email'])
        .from_table('user')
        .where('email = "alice@example.com"')
        .force_index('email_idx')
    )

    print('FORCE INDEX Hint:')
    print(query.to_surql())


async def timeout_hint() -> None:
    """Example: Setting query timeout to prevent long-running queries."""
    print('\n=== Timeout Hint ===')

    # Set a 30-second timeout for an expensive query
    query = (
        Query()
        .select()
        .from_table('large_table')
        .where('complex_calculation(data) > threshold')
        .with_timeout(30.0)
    )

    print('Query with 30-second timeout:')
    print(query.to_surql())


async def parallel_execution() -> None:
    """Example: Enabling parallel execution for large datasets."""
    print('\n=== Parallel Execution Hint ===')

    # Enable parallel execution with default worker count
    query = (
        Query()
        .select()
        .from_table('large_dataset')
        .parallel()
    )

    print('Parallel execution (default workers):')
    print(query.to_surql())
    print()

    # Enable parallel execution with specific worker count
    query = (
        Query()
        .select()
        .from_table('large_dataset')
        .parallel(max_workers=8)
    )

    print('Parallel execution (8 workers):')
    print(query.to_surql())


async def fetch_strategies() -> None:
    """Example: Controlling how records are fetched from the database."""
    print('\n=== Fetch Strategy Hints ===')

    # Eager fetch - load all results immediately
    query = (
        Query()
        .select()
        .from_table('small_table')
        .with_fetch('eager')
    )

    print('Eager fetch strategy:')
    print(query.to_surql())
    print()

    # Batch fetch - fetch records in batches
    query = (
        Query()
        .select()
        .from_table('large_table')
        .with_fetch('batch', batch_size=1000)
    )

    print('Batch fetch strategy (1000 records per batch):')
    print(query.to_surql())
    print()

    # Lazy fetch - fetch records on demand
    query = (
        Query()
        .select()
        .from_table('streaming_data')
        .with_fetch('lazy')
    )

    print('Lazy fetch strategy:')
    print(query.to_surql())


async def explain_query() -> None:
    """Example: Using EXPLAIN to analyze query execution plans."""
    print('\n=== EXPLAIN Hint ===')

    # Basic explain
    query = (
        Query()
        .select()
        .from_table('user')
        .where('age > 18')
        .explain()
    )

    print('Basic EXPLAIN:')
    print(query.to_surql())
    print()

    # Full execution plan
    query = (
        Query()
        .select()
        .from_table('user')
        .where('age > 18 AND status = "active"')
        .explain(full=True)
    )

    print('Full EXPLAIN:')
    print(query.to_surql())


async def combining_hints() -> None:
    """Example: Combining multiple hints for optimal performance."""
    print('\n=== Combining Multiple Hints ===')

    # Complex query with multiple optimization hints
    query = (
        Query()
        .select(['name', 'email', 'age', 'status'])
        .from_table('user')
        .where('age > 18 AND status = "active"')
        .order_by('created_at', 'DESC')
        .limit(100)
        .use_index('age_status_idx')
        .with_timeout(10.0)
        .parallel(max_workers=4)
        .with_fetch('batch', batch_size=50)
    )

    print('Query with multiple hints:')
    print(query.to_surql())


async def real_world_scenario_1() -> None:
    """Example: Optimizing a user search query."""
    print('\n=== Real-World Scenario 1: User Search ===')

    # User search with email pattern matching
    # - Force use of email index for better performance
    # - Set timeout to prevent slow searches from blocking
    # - Use batch fetching for large result sets
    query = (
        Query()
        .select(['id', 'name', 'email', 'created_at'])
        .from_table('user')
        .where('email CONTAINS $search_term')
        .force_index('email_idx')
        .with_timeout(5.0)
        .with_fetch('batch', batch_size=100)
        .limit(1000)
    )

    print('Optimized user search query:')
    print(query.to_surql())


async def real_world_scenario_2() -> None:
    """Example: Analyzing slow query for optimization."""
    print('\n=== Real-World Scenario 2: Query Analysis ===')

    # First, run with EXPLAIN to understand the execution plan
    analysis_query = (
        Query()
        .select()
        .from_table('orders')
        .where('total > 1000 AND status = "pending"')
        .order_by('created_at', 'DESC')
        .explain(full=True)
    )

    print('Step 1: Analyze query execution plan:')
    print(analysis_query.to_surql())
    print()

    # After analyzing, optimize with appropriate hints
    optimized_query = (
        Query()
        .select()
        .from_table('orders')
        .where('total > 1000 AND status = "pending"')
        .order_by('created_at', 'DESC')
        .use_index('total_status_idx')
        .with_timeout(15.0)
        .parallel(max_workers=4)
    )

    print('Step 2: Apply optimization hints:')
    print(optimized_query.to_surql())


async def real_world_scenario_3() -> None:
    """Example: Processing large dataset efficiently."""
    print('\n=== Real-World Scenario 3: Large Dataset Processing ===')

    # Process millions of records with optimal settings
    # - Enable parallel processing for speed
    # - Use batch fetching to manage memory
    # - Set generous timeout for large dataset
    query = (
        Query()
        .select(['id', 'data', 'processed_at'])
        .from_table('events')
        .where('processed_at IS NULL')
        .parallel(max_workers=8)
        .with_fetch('batch', batch_size=5000)
        .with_timeout(300.0)
        .limit(1000000)
    )

    print('Large dataset processing query:')
    print(query.to_surql())


async def hint_composition() -> None:
    """Example: Composing queries with hints programmatically."""
    print('\n=== Hint Composition ===')

    # Start with base query
    base_query = Query().select().from_table('products')

    # Add filtering
    filtered_query = base_query.where('price > 100')

    # Add optimization hints based on data volume
    data_volume = 'large'  # Could be determined dynamically

    if data_volume == 'large':
        optimized_query = (
            filtered_query
            .parallel(max_workers=4)
            .with_fetch('batch', batch_size=1000)
            .with_timeout(30.0)
        )
    else:
        optimized_query = filtered_query.with_fetch('eager')

    print(f'Query optimized for {data_volume} dataset:')
    print(optimized_query.to_surql())


async def using_with_hints_method() -> None:
    """Example: Using with_hints for bulk hint addition."""
    print('\n=== Using with_hints() Method ===')

    # Add multiple hints at once
    query = (
        Query()
        .select()
        .from_table('analytics_data')
        .where('date >= $start_date AND date <= $end_date')
        .with_hints(
            IndexHint(table='analytics_data', index='date_idx', force=False),
            TimeoutHint(seconds=60.0),
            ParallelHint(enabled=True, max_workers=6),
            FetchHint(strategy='batch', batch_size=2000),
        )
    )

    print('Query with multiple hints added via with_hints():')
    print(query.to_surql())


async def hint_override_example() -> None:
    """Example: Demonstrating hint override behavior."""
    print('\n=== Hint Override Behavior ===')

    # Later hints of the same type override earlier ones
    query = (
        Query()
        .select()
        .from_table('user')
        .with_timeout(10.0)  # First timeout
        .parallel(max_workers=2)
        .with_timeout(30.0)  # Overrides first timeout
    )

    print('Query with overridden timeout (30s wins):')
    print(query.to_surql())
    # Note: Only the last timeout (30s) will be in the output


async def main() -> None:
    """Run all query hint examples."""
    print('='*60)
    print('Query Optimization Hints Examples')
    print('='*60)

    await basic_index_hint()
    await timeout_hint()
    await parallel_execution()
    await fetch_strategies()
    await explain_query()
    await combining_hints()
    await real_world_scenario_1()
    await real_world_scenario_2()
    await real_world_scenario_3()
    await hint_composition()
    await using_with_hints_method()
    await hint_override_example()

    print('\n'+'='*60)
    print('Examples complete!')
    print('='*60)


if __name__ == '__main__':
    asyncio.run(main())
