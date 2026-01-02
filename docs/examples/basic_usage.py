"""Basic usage example for reverie ORM.

This example demonstrates:
- Connecting to SurrealDB
- Creating records
- Querying records
- Updating records
- Deleting records
"""

import asyncio

from pydantic import BaseModel, EmailStr, Field

from src.connection.client import get_client
from src.connection.config import ConnectionConfig
from src.query.crud import (
  count_records,
  create_record,
  delete_record,
  get_record,
  merge_record,
  query_records,
  update_record,
)


# Define data models
class User(BaseModel):
  """User data model."""

  username: str = Field(min_length=3, max_length=20)
  email: EmailStr
  age: int = Field(ge=0, le=150)
  bio: str | None = None


# Database configuration
config = ConnectionConfig(
  url='ws://localhost:8000/rpc',
  namespace='examples',
  database='basic_usage',
  username='root',
  password='root',
)


async def main():
  """Main example function."""

  async with get_client(config) as client:
    print('Connected to SurrealDB\n')

    # CREATE: Insert new records
    print('=== Creating Users ===')
    alice = await create_record(
      'user',
      User(
        username='alice',
        email='alice@example.com',
        age=30,
        bio='Software engineer',
      ),
      client=client,
    )
    print(f'Created user: {alice["id"]}')

    bob = await create_record(
      'user',
      User(
        username='bob',
        email='bob@example.com',
        age=25,
        bio='Designer',
      ),
      client=client,
    )
    print(f'Created user: {bob["id"]}')

    charlie = await create_record(
      'user',
      User(
        username='charlie',
        email='charlie@example.com',
        age=35,
      ),
      client=client,
    )
    print(f'Created user: {charlie["id"]}\n')

    # READ: Get single record
    print('=== Reading Single User ===')
    user = await get_record('user', 'alice', User, client=client)
    if user:
      print(f'Found user: {user.username} ({user.email})')
      print(f'Age: {user.age}')
      print(f'Bio: {user.bio}\n')

    # QUERY: Get multiple records
    print('=== Querying Users ===')
    users = await query_records(
      'user',
      User,
      order_by=('age', 'ASC'),
      client=client,
    )
    print(f'Found {len(users)} users:')
    for u in users:
      print(f'  - {u.username}: {u.age} years old')
    print()

    # QUERY with conditions
    print('=== Querying with Conditions ===')
    adults = await query_records(
      'user',
      User,
      conditions=['age >= 30'],
      order_by=('age', 'DESC'),
      client=client,
    )
    print(f'Users 30 and over: {len(adults)}')
    for u in adults:
      print(f'  - {u.username}: {u.age}')
    print()

    # COUNT
    print('=== Counting Records ===')
    total = await count_records('user', client=client)
    print(f'Total users: {total}\n')

    # UPDATE: Replace entire record
    print('=== Updating User (Full) ===')
    updated_alice = await update_record(
      'user',
      'alice',
      User(
        username='alice',
        email='alice.updated@example.com',
        age=31,
        bio='Senior software engineer',
      ),
      client=client,
    )
    print(f'Updated user: {updated_alice["id"]}')
    print(f'New email: {updated_alice["email"]}\n')

    # MERGE: Partial update
    print('=== Updating User (Partial) ===')
    merged_bob = await merge_record(
      'user',
      'bob',
      {'age': 26, 'bio': 'Senior designer'},
      client=client,
    )
    print(f"Updated Bob's age to: {merged_bob['age']}")
    print(f"Updated Bob's bio to: {merged_bob['bio']}\n")

    # Final query
    print('=== Final User List ===')
    final_users = await query_records(
      'user',
      User,
      order_by=('username', 'ASC'),
      client=client,
    )
    for u in final_users:
      print(f'{u.username}:')
      print(f'  Email: {u.email}')
      print(f'  Age: {u.age}')
      print(f'  Bio: {u.bio or "N/A"}')
    print()

    # DELETE
    print('=== Deleting User ===')
    await delete_record('user', 'charlie', client=client)
    print('Deleted charlie\n')

    # Verify deletion
    remaining = await count_records('user', client=client)
    print(f'Remaining users: {remaining}')


if __name__ == '__main__':
  print('reverie Basic Usage Example')
  print('=' * 40)
  print()

  try:
    asyncio.run(main())
    print('\nExample completed successfully!')

  except Exception as e:
    print(f'\nError: {e}')
    import traceback

    traceback.print_exc()
