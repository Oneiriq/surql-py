"""Advanced query patterns and techniques.

This example demonstrates:
- Aggregation queries
- Subqueries
- Transactions
- Complex filters
- Pagination
- Batch operations
"""

import asyncio

from pydantic import BaseModel

from src.connection.client import get_client
from src.connection.config import ConnectionConfig
from src.connection.transaction import transaction
from src.query.crud import count_records, create_record, query_records

# Database configuration
config = ConnectionConfig(
  url='ws://localhost:8000/rpc',
  namespace='examples',
  database='advanced',
  username='root',
  password='root',
)


class Product(BaseModel):
  """Product model."""

  name: str
  sku: str
  price: float
  category: str
  stock: int
  is_active: bool = True


async def setup_and_seed(client):
  """Create schema and seed data."""
  print('=== Setting Up Schema ===')

  await client.execute("""
    DEFINE TABLE product SCHEMAFULL;
    DEFINE FIELD name ON TABLE product TYPE string;
    DEFINE FIELD sku ON TABLE product TYPE string;
    DEFINE FIELD price ON TABLE product TYPE float;
    DEFINE FIELD category ON TABLE product TYPE string;
    DEFINE FIELD stock ON TABLE product TYPE int;
    DEFINE FIELD is_active ON TABLE product TYPE bool DEFAULT true;
    DEFINE INDEX sku_idx ON TABLE product COLUMNS sku UNIQUE;
  """)

  print('✓ Schema created\n')

  print('=== Seeding Data ===')

  products = [
    Product(name='Laptop', sku='LPT001', price=999.99, category='Electronics', stock=15),
    Product(name='Mouse', sku='MSE001', price=29.99, category='Electronics', stock=50),
    Product(name='Keyboard', sku='KBD001', price=79.99, category='Electronics', stock=30),
    Product(name='Monitor', sku='MON001', price=299.99, category='Electronics', stock=20),
    Product(name='Desk', sku='DSK001', price=199.99, category='Furniture', stock=10),
    Product(name='Chair', sku='CHR001', price=149.99, category='Furniture', stock=25),
    Product(name='Notebook', sku='NTB001', price=5.99, category='Stationery', stock=100),
    Product(name='Pen Set', sku='PEN001', price=12.99, category='Stationery', stock=75),
  ]

  for product in products:
    await create_record('product', product, client=client)
    print(f'Created: {product.name}')

  print()


async def aggregation_queries(client):
  """Demonstrate aggregation functions."""
  print('=== Aggregation Queries ===')

  # Count products by category
  result = await client.execute("""
    SELECT
      category,
      count() AS product_count,
      math::sum(stock) AS total_stock,
      math::mean(price) AS avg_price,
      math::min(price) AS min_price,
      math::max(price) AS max_price
    FROM product
    GROUP BY category
    ORDER BY product_count DESC
  """)

  print('Products by category:')
  if result and result[0].get('result'):
    for cat in result[0]['result']:
      print(f'\n{cat["category"]}:')
      print(f'  Products: {cat["product_count"]}')
      print(f'  Total stock: {cat["total_stock"]}')
      print(f'  Avg price: ${cat["avg_price"]:.2f}')
      print(f'  Price range: ${cat["min_price"]:.2f} - ${cat["max_price"]:.2f}')

  # Overall statistics
  result = await client.execute("""
    SELECT
      count() AS total_products,
      math::sum(stock) AS total_stock,
      math::sum(price * stock) AS inventory_value,
      math::mean(price) AS avg_price
    FROM product
    WHERE is_active = true
  """)

  print('\nOverall Statistics:')
  if result and result[0].get('result') and len(result[0]['result']) > 0:
    stats = result[0]['result'][0]
    print(f'  Total products: {stats["total_products"]}')
    print(f'  Total stock: {stats["total_stock"]}')
    print(f'  Inventory value: ${stats["inventory_value"]:.2f}')
    print(f'  Average price: ${stats["avg_price"]:.2f}')

  print()


async def subquery_examples(client):
  """Demonstrate subqueries."""
  print('=== Subquery Examples ===')

  # Products above average price
  result = await client.execute("""
    SELECT name, price
    FROM product
    WHERE price > (SELECT math::mean(price) FROM product)
    ORDER BY price DESC
  """)

  print('Products above average price:')
  if result and result[0].get('result'):
    for product in result[0]['result']:
      print(f'  - {product["name"]}: ${product["price"]:.2f}')

  # Products in categories with high stock
  result = await client.execute("""
    SELECT name, category, stock
    FROM product
    WHERE category IN (
      SELECT category
      FROM product
      GROUP BY category
      HAVING math::sum(stock) > 50
    )
    ORDER BY category, stock DESC
  """)

  print('\nProducts in high-stock categories:')
  if result and result[0].get('result'):
    current_cat = None
    for product in result[0]['result']:
      if product['category'] != current_cat:
        current_cat = product['category']
        print(f'\n{current_cat}:')
      print(f'  - {product["name"]}: {product["stock"]} units')

  print()


async def pagination_example(client):
  """Demonstrate pagination."""
  print('=== Pagination Example ===')

  page_size = 3
  total = await count_records('product', client=client)
  total_pages = (total + page_size - 1) // page_size

  print(f'Total products: {total}')
  print(f'Page size: {page_size}')
  print(f'Total pages: {total_pages}\n')

  for page in range(1, total_pages + 1):
    offset = (page - 1) * page_size

    products = await query_records(
      'product',
      Product,
      order_by=('name', 'ASC'),
      limit=page_size,
      offset=offset,
      client=client,
    )

    print(f'Page {page}:')
    for product in products:
      print(f'  - {product.name} (${product.price})')
    print()


async def complex_filtering(client):
  """Demonstrate complex filter conditions."""
  print('=== Complex Filtering ===')

  # Multiple conditions with AND/OR
  result = await client.execute("""
    SELECT name, category, price, stock
    FROM product
    WHERE (
      (category = "Electronics" AND price > 100)
      OR
      (category = "Furniture" AND stock < 15)
    )
    AND is_active = true
    ORDER BY price DESC
  """)

  print('Expensive electronics OR low-stock furniture:')
  if result and result[0].get('result'):
    for product in result[0]['result']:
      print(f'  - {product["name"]} ({product["category"]})')
      print(f'    Price: ${product["price"]}, Stock: {product["stock"]}')

  # Pattern matching
  result = await client.execute("""
    SELECT name, sku
    FROM product
    WHERE name ~ "(?i)book"
    ORDER BY name
  """)

  print("\nProducts matching 'book' (case-insensitive):")
  if result and result[0].get('result'):
    for product in result[0]['result']:
      print(f'  - {product["name"]} ({product["sku"]})')

  # Range queries
  result = await client.execute("""
    SELECT name, price
    FROM product
    WHERE price >= 10 AND price <= 100
    ORDER BY price
  """)

  print('\nProducts in $10-$100 range:')
  if result and result[0].get('result'):
    for product in result[0]['result']:
      print(f'  - {product["name"]}: ${product["price"]:.2f}')

  print()


async def transaction_example(client):
  """Demonstrate transactions."""
  print('=== Transaction Example ===')

  print('Creating order transaction...')

  try:
    async with transaction(client):
      # Deduct stock for multiple products
      await client.execute("""
        UPDATE product:LPT001 SET stock -= 1
      """)

      await client.execute("""
        UPDATE product:MSE001 SET stock -= 2
      """)

      # Create order record
      await client.execute("""
        CREATE order SET
          items = ["product:LPT001", "product:MSE001"],
          total = 1059.97,
          status = "pending",
          created_at = time::now()
      """)

      print('✓ Transaction committed')

  except Exception as e:
    print(f'✗ Transaction rolled back: {e}')

  # Verify stock changes
  result = await client.execute("""
    SELECT name, stock
    FROM product
    WHERE name IN ["Laptop", "Mouse"]
  """)

  print('\nUpdated stock levels:')
  if result and result[0].get('result'):
    for product in result[0]['result']:
      print(f'  - {product["name"]}: {product["stock"]} units')

  print()


async def batch_operations(client):
  """Demonstrate batch operations."""
  print('=== Batch Operations ===')

  # Batch update - apply discount to category
  result = await client.execute("""
    UPDATE product
    SET price = price * 0.9
    WHERE category = "Stationery"
    RETURN BEFORE, AFTER
  """)

  print('Applied 10% discount to Stationery:')
  if result and result[0].get('result'):
    for item in result[0]['result']:
      if isinstance(item, dict):
        before = item.get('BEFORE', {})
        after = item.get('AFTER', {})
        if before and after:
          print(f'  - {before.get("name")}: ${before.get("price"):.2f} → ${after.get("price"):.2f}')

  # Batch insert
  new_products = [
    {'name': 'USB Cable', 'sku': 'USB001', 'price': 9.99, 'category': 'Electronics', 'stock': 100},
    {
      'name': 'Phone Stand',
      'sku': 'PHS001',
      'price': 19.99,
      'category': 'Electronics',
      'stock': 50,
    },
  ]

  for prod in new_products:
    await client.execute('CREATE product CONTENT $data', {'data': prod})

  print('\nAdded new products:')
  for prod in new_products:
    print(f'  - {prod["name"]}')

  print()


async def advanced_sorting(client):
  """Demonstrate advanced sorting."""
  print('=== Advanced Sorting ===')

  # Multi-column sort
  result = await client.execute("""
    SELECT name, category, price
    FROM product
    ORDER BY category ASC, price DESC
  """)

  print('Products sorted by category (ASC) then price (DESC):')
  if result and result[0].get('result'):
    current_cat = None
    for product in result[0]['result']:
      if product['category'] != current_cat:
        current_cat = product['category']
        print(f'\n{current_cat}:')
      print(f'  - {product["name"]}: ${product["price"]:.2f}')

  print()


async def main():
  """Main example function."""

  async with get_client(config) as client:
    await setup_and_seed(client)
    await aggregation_queries(client)
    await subquery_examples(client)
    await pagination_example(client)
    await complex_filtering(client)
    await transaction_example(client)
    await batch_operations(client)
    await advanced_sorting(client)


if __name__ == '__main__':
  print('Ethereal Advanced Queries Example')
  print('=' * 60)
  print()

  try:
    asyncio.run(main())
    print('\nExample completed successfully!')

  except Exception as e:
    print(f'\nError: {e}')
    import traceback

    traceback.print_exc()
