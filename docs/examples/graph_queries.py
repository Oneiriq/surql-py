"""Graph traversal and relationship queries example.

This example demonstrates:
- Creating nodes and edges
- Graph traversal queries
- Multi-hop relationships
- Finding paths between nodes
- Complex graph patterns
"""

import asyncio

from pydantic import BaseModel, EmailStr

from src.connection.client import get_client
from src.connection.config import ConnectionConfig
from src.query.crud import create_record

# Database configuration
config = ConnectionConfig(
  url='ws://localhost:8000/rpc',
  namespace='examples',
  database='graph',
  username='root',
  password='root',
)


# Data models
class User(BaseModel):
  """User node."""

  username: str
  name: str
  email: EmailStr


class Post(BaseModel):
  """Post node."""

  title: str
  content: str


async def setup_schema(client):
  """Create tables and relationships."""
  print('=== Setting Up Schema ===')

  # Create user table
  await client.execute("""
    DEFINE TABLE user SCHEMAFULL;
    DEFINE FIELD username ON TABLE user TYPE string;
    DEFINE FIELD name ON TABLE user TYPE string;
    DEFINE FIELD email ON TABLE user TYPE string;
  """)

  # Create post table
  await client.execute("""
    DEFINE TABLE post SCHEMAFULL;
    DEFINE FIELD title ON TABLE post TYPE string;
    DEFINE FIELD content ON TABLE post TYPE string;
    DEFINE FIELD author ON TABLE post TYPE record<user>;
  """)

  # Create edge tables
  await client.execute("""
    DEFINE TABLE follows SCHEMAFULL;
    DEFINE FIELD in ON TABLE follows TYPE record<user>;
    DEFINE FIELD out ON TABLE follows TYPE record<user>;
    DEFINE FIELD since ON TABLE follows TYPE datetime DEFAULT time::now();
  """)

  await client.execute("""
    DEFINE TABLE likes SCHEMAFULL;
    DEFINE FIELD in ON TABLE likes TYPE record<user>;
    DEFINE FIELD out ON TABLE likes TYPE record<post>;
    DEFINE FIELD reaction ON TABLE likes TYPE string DEFAULT "like";
  """)

  print('✓ Schema created\n')


async def create_sample_data(client):
  """Create sample users and posts."""
  print('=== Creating Sample Data ===')

  # Create users
  users = {}
  for username, name, email in [
    ('alice', 'Alice Johnson', 'alice@example.com'),
    ('bob', 'Bob Smith', 'bob@example.com'),
    ('charlie', 'Charlie Brown', 'charlie@example.com'),
    ('diana', 'Diana Prince', 'diana@example.com'),
    ('eve', 'Eve Adams', 'eve@example.com'),
  ]:
    user = await create_record(
      'user',
      User(username=username, name=name, email=email),
      client=client,
    )
    users[username] = user['id']
    print(f'Created user: {username}')

  # Create posts
  posts = []
  for title, content, author in [
    ('Hello World', 'My first post!', users['alice']),
    ('Python Tips', 'Here are some Python tricks...', users['alice']),
    ('Database Design', 'Best practices for databases...', users['bob']),
    ('Graph Databases', 'Why graphs are awesome...', users['charlie']),
    ('Web Development', 'Building modern web apps...', users['diana']),
  ]:
    post = await create_record(
      'post',
      {'title': title, 'content': content, 'author': author},
      client=client,
    )
    posts.append(post['id'])
    print(f'Created post: {title}')

  print()
  return users, posts


async def create_relationships(client, users, posts):
  """Create follow and like relationships."""
  print('=== Creating Relationships ===')

  # Follow relationships
  follows = [
    ('alice', 'bob'),
    ('alice', 'charlie'),
    ('bob', 'charlie'),
    ('bob', 'diana'),
    ('charlie', 'diana'),
    ('charlie', 'eve'),
    ('diana', 'alice'),
    ('eve', 'alice'),
    ('eve', 'bob'),
  ]

  for follower, followed in follows:
    await client.execute(f'RELATE {users[follower]}->follows->{users[followed]}')
    print(f'{follower} follows {followed}')

  print()

  # Like relationships
  likes = [
    ('bob', posts[0]),  # Bob likes Alice's "Hello World"
    ('charlie', posts[0]),
    ('diana', posts[0]),
    ('charlie', posts[1]),  # Charlie likes Alice's "Python Tips"
    ('alice', posts[2]),  # Alice likes Bob's post
    ('diana', posts[2]),
    ('bob', posts[3]),  # Bob likes Charlie's post
    ('alice', posts[4]),  # Alice likes Diana's post
  ]

  for user, post in likes:
    await client.execute(f'RELATE {users[user]}->likes->{post}')
    print(f'{user} likes post {post}')

  print()


async def query_direct_relationships(client, users):
  """Query direct (one-hop) relationships."""
  print('=== Direct Relationships ===')

  # Who does Alice follow?
  result = await client.execute(f'SELECT ->follows->user.* AS followed FROM {users["alice"]}')
  print('Alice follows:')
  if result and result[0].get('result'):
    for item in result[0]['result']:
      if item.get('followed'):
        for user in item['followed']:
          print(f'  - {user.get("username", "Unknown")}')

  # Who follows Alice?
  result = await client.execute(f'SELECT <-follows<-user.* AS followers FROM {users["alice"]}')
  print("\nAlice's followers:")
  if result and result[0].get('result'):
    for item in result[0]['result']:
      if item.get('followers'):
        for user in item['followers']:
          print(f'  - {user.get("username", "Unknown")}')

  print()


async def query_multi_hop(client, users):
  """Query multi-hop relationships."""
  print('=== Multi-Hop Traversal ===')

  # Followers of followers (2 hops)
  result = await client.execute(
    f'SELECT <-follows<-user<-follows<-user.* AS users FROM {users["alice"]}'
  )
  print("Followers of Alice's followers:")
  if result and result[0].get('result'):
    for item in result[0]['result']:
      if item.get('users'):
        for user in item['users']:
          username = user.get('username', 'Unknown')
          if username != 'alice':  # Exclude self
            print(f'  - {username}')

  # Friend suggestions (friends of friends who you don't follow)
  result = await client.execute(f"""
    SELECT VALUE ->follows->user->follows->user
    FROM {users['alice']}
    WHERE id != {users['alice']}
  """)
  print('\nFriend suggestions for Alice (friends of friends):')
  if result and result[0].get('result'):
    suggestions = set()
    for users_list in result[0]['result']:
      if isinstance(users_list, list):
        for user in users_list:
          if isinstance(user, dict):
            username = user.get('username')
            if username and username != 'alice':
              suggestions.add(username)
    for username in suggestions:
      print(f'  - {username}')

  print()


async def query_post_interactions(client, users):
  """Query post likes and authors."""
  print('=== Post Interactions ===')

  # Posts liked by Alice
  result = await client.execute(f'SELECT ->likes->post.* AS posts FROM {users["alice"]}')
  print('Posts liked by Alice:')
  if result and result[0].get('result'):
    for item in result[0]['result']:
      if item.get('posts'):
        for post in item['posts']:
          print(f'  - {post.get("title", "Unknown")}')

  # Users who liked the same posts as Alice
  result = await client.execute(f"""
    SELECT VALUE ->likes->post<-likes<-user
    FROM {users['alice']}
    WHERE id != {users['alice']}
  """)
  print('\nUsers with similar interests (liked same posts as Alice):')
  if result and result[0].get('result'):
    similar_users = set()
    for users_list in result[0]['result']:
      if isinstance(users_list, list):
        for user in users_list:
          if isinstance(user, dict):
            username = user.get('username')
            if username and username != 'alice':
              similar_users.add(username)
    for username in similar_users:
      print(f'  - {username}')

  print()


async def query_complex_patterns(client, users):
  """Query complex graph patterns."""
  print('=== Complex Patterns ===')

  # Find influential users (high follower count)
  result = await client.execute("""
    SELECT
      username,
      count(<-follows) AS follower_count
    FROM user
    ORDER BY follower_count DESC
    LIMIT 3
  """)
  print('Most followed users:')
  if result and result[0].get('result'):
    for user in result[0]['result']:
      print(f'  - {user.get("username")}: {user.get("follower_count")} followers')

  # Find posts with most likes
  result = await client.execute("""
    SELECT
      title,
      count(<-likes) AS like_count
    FROM post
    ORDER BY like_count DESC
    LIMIT 3
  """)
  print('\nMost liked posts:')
  if result and result[0].get('result'):
    for post in result[0]['result']:
      print(f'  - {post.get("title")}: {post.get("like_count")} likes')

  # Find users who follow each other (mutual follows)
  result = await client.execute(f"""
    SELECT VALUE ->follows->user
    FROM {users['alice']}
    WHERE ->follows->user<-follows<-user.id CONTAINS {users['alice']}
  """)
  print('\nMutual follows with Alice:')
  if result and result[0].get('result'):
    for users_list in result[0]['result']:
      if isinstance(users_list, list):
        for user in users_list:
          if isinstance(user, dict):
            print(f'  - {user.get("username", "Unknown")}')

  print()


async def main():
  """Main example function."""

  async with get_client(config) as client:
    # Setup
    await setup_schema(client)
    users, posts = await create_sample_data(client)
    await create_relationships(client, users, posts)

    # Query examples
    await query_direct_relationships(client, users)
    await query_multi_hop(client, users)
    await query_post_interactions(client, users)
    await query_complex_patterns(client, users)


if __name__ == '__main__':
  print('Ethereal Graph Queries Example')
  print('=' * 60)
  print()

  try:
    asyncio.run(main())
    print('\nExample completed successfully!')

  except Exception as e:
    print(f'\nError: {e}')
    import traceback

    traceback.print_exc()
