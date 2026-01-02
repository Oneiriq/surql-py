"""Schema definition example for Ethereal.

This example demonstrates:
- Defining table schemas
- Field types and validations
- Indexes
- Events/triggers
- Permissions
- Edge schemas for relationships
"""

from src.schema.fields import (
  string_field,
  int_field,
  float_field,
  bool_field,
  datetime_field,
  record_field,
  array_field,
  object_field,
  computed_field,
)
from src.schema.table import (
  table_schema,
  unique_index,
  search_index,
  index,
  event,
  with_fields,
  with_indexes,
  TableMode,
)
from src.schema.edge import edge_schema


# User schema with comprehensive features
user_schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[
    # Basic string fields
    string_field('username', assertion='string::len($value) >= 3'),
    string_field('email', assertion='string::is::email($value)'),
    string_field('password_hash'),
    
    # Nested object fields
    string_field('name.first'),
    string_field('name.last'),
    
    # Profile fields
    string_field('bio', default='""'),
    string_field('avatar_url', default='NONE'),
    string_field('location', default='NONE'),
    
    # Numbers
    int_field('age', assertion='$value >= 13 AND $value <= 120'),
    int_field('follower_count', default='0'),
    int_field('following_count', default='0'),
    
    # Boolean flags
    bool_field('is_verified', default='false'),
    bool_field('is_active', default='true'),
    bool_field('is_private', default='false'),
    
    # Timestamps
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
    datetime_field('last_login', default='NONE'),
    
    # Collections
    array_field('roles', default='["user"]'),
    array_field('interests', default='[]'),
    
    # Flexible metadata
    object_field('metadata', default='{}', flexible=True),
    
    # Computed field
    computed_field(
      'full_name',
      'string::concat(name.first, " ", name.last)',
    ),
  ],
  indexes=[
    unique_index('username_idx', ['username']),
    unique_index('email_idx', ['email']),
    search_index('bio_search', ['bio']),
    index('location_idx', ['location']),
  ],
  events=[
    # Update timestamp on any change
    event(
      'update_timestamp',
      '$event = "UPDATE"',
      'UPDATE $value SET updated_at = time::now()',
    ),
    
    # Log email changes
    event(
      'email_changed',
      '$before.email != $after.email',
      '''
        CREATE audit_log SET
          table = 'user',
          record = $value.id,
          field = 'email',
          old_value = $before.email,
          new_value = $after.email,
          changed_at = time::now()
      ''',
    ),
  ],
  permissions={
    'select': 'is_active = true AND (is_private = false OR id = $auth.id OR $auth.admin = true)',
    'create': 'true',
    'update': 'id = $auth.id OR $auth.admin = true',
    'delete': '$auth.admin = true',
  },
)


# Post schema
post_schema = table_schema(
  'post',
  mode=TableMode.SCHEMAFULL,
  fields=[
    # Content
    string_field('title', assertion='string::len($value) > 0'),
    string_field('content'),
    string_field('slug', assertion='string::len($value) > 0'),
    
    # Metadata
    record_field('author', table='user'),
    array_field('tags', default='[]'),
    string_field('status', default='"draft"'),
    
    # Stats
    int_field('view_count', default='0'),
    int_field('like_count', default='0'),
    int_field('comment_count', default='0'),
    
    # Publishing
    bool_field('published', default='false'),
    datetime_field('published_at', default='NONE'),
    
    # Timestamps
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
  ],
  indexes=[
    unique_index('slug_idx', ['slug']),
    search_index('content_search', ['title', 'content']),
    index('author_idx', ['author']),
    index('status_idx', ['status']),
    index('published_idx', ['published', 'published_at']),
  ],
  events=[
    # Auto-set published_at when status changes to published
    event(
      'auto_publish',
      '$before.status != "published" AND $after.status = "published"',
      'UPDATE $value SET published_at = time::now(), published = true',
    ),
  ],
  permissions={
    'select': 'published = true OR author = $auth.id OR $auth.admin = true',
    'create': '$auth != NONE',
    'update': 'author = $auth.id OR $auth.admin = true',
    'delete': 'author = $auth.id OR $auth.admin = true',
  },
)


# Comment schema
comment_schema = table_schema(
  'comment',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('content', assertion='string::len($value) > 0'),
    record_field('author', table='user'),
    record_field('post', table='post'),
    record_field('parent', default='NONE'),  # For nested comments
    
    int_field('like_count', default='0'),
    bool_field('is_edited', default='false'),
    
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
  ],
  indexes=[
    index('post_idx', ['post', 'created_at']),
    index('author_idx', ['author']),
  ],
)


# Category schema
category_schema = table_schema(
  'category',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('name'),
    string_field('slug'),
    string_field('description', default='""'),
    record_field('parent', default='NONE'),
    int_field('post_count', default='0'),
  ],
  indexes=[
    unique_index('slug_idx', ['slug']),
  ],
)


# Edge schemas for relationships

# User follows user
follows_edge = edge_schema(
  'follows',
  from_table='user',
  to_table='user',
  fields=[
    datetime_field('followed_at', default='time::now()', readonly=True),
  ],
)

# User likes post
likes_edge = edge_schema(
  'likes',
  from_table='user',
  to_table='post',
  fields=[
    datetime_field('liked_at', default='time::now()', readonly=True),
    string_field('reaction', default='"like"'),  # like, love, wow, etc.
  ],
)

# Post belongs to category
categorized_edge = edge_schema(
  'categorized',
  from_table='post',
  to_table='category',
  fields=[
    datetime_field('assigned_at', default='time::now()'),
  ],
)


# Example of composing schemas functionally

def with_timestamps(schema):
  """Add timestamp fields to a schema."""
  return with_fields(
    schema,
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
  )

def with_soft_delete(schema):
  """Add soft delete fields to a schema."""
  return with_fields(
    schema,
    bool_field('is_deleted', default='false'),
    datetime_field('deleted_at', default='NONE'),
  )

def with_audit_fields(schema):
  """Add audit fields to a schema."""
  return with_fields(
    schema,
    record_field('created_by', table='user'),
    record_field('updated_by', table='user'),
  )


# Create a product schema using composition
base_product = table_schema('product', mode=TableMode.SCHEMAFULL)

product_schema = (
  base_product
  |> lambda s: with_fields(
    s,
    string_field('name'),
    string_field('sku'),
    float_field('price', assertion='$value > 0'),
  )
  |> with_timestamps
  |> with_soft_delete
  |> with_audit_fields
  |> lambda s: with_indexes(
    s,
    unique_index('sku_idx', ['sku']),
  )
)


def print_schema_info(schema, schema_name):
  """Print schema information."""
  print(f"\n{'=' * 60}")
  print(f"Schema: {schema_name}")
  print('=' * 60)
  print(f"Table: {schema.name}")
  print(f"Mode: {schema.mode.value}")
  print(f"\nFields ({len(schema.fields)}):")
  for field in schema.fields:
    constraints = []
    if field.assertion:
      constraints.append(f"assert: {field.assertion}")
    if field.default:
      constraints.append(f"default: {field.default}")
    if field.readonly:
      constraints.append("readonly")
    
    constraint_str = f" ({', '.join(constraints)})" if constraints else ""
    print(f"  - {field.name}: {field.type.value}{constraint_str}")
  
  print(f"\nIndexes ({len(schema.indexes)}):")
  for idx in schema.indexes:
    print(f"  - {idx.name}: {idx.type.value} on {', '.join(idx.columns)}")
  
  print(f"\nEvents ({len(schema.events)}):")
  for evt in schema.events:
    print(f"  - {evt.name}")
    print(f"    Condition: {evt.condition}")
  
  if schema.permissions:
    print("\nPermissions:")
    for op, rule in schema.permissions.items():
      print(f"  - {op}: {rule}")


if __name__ == '__main__':
  print("Ethereal Schema Definition Example")
  print("=" * 60)
  
  # Display schema information
  print_schema_info(user_schema, "User Schema")
  print_schema_info(post_schema, "Post Schema")
  print_schema_info(comment_schema, "Comment Schema")
  print_schema_info(category_schema, "Category Schema")
  
  print("\n" + "=" * 60)
  print("Edge Schemas")
  print("=" * 60)
  print(f"\n{follows_edge.name}: {follows_edge.from_table} -> {follows_edge.to_table}")
  print(f"{likes_edge.name}: {likes_edge.from_table} -> {likes_edge.to_table}")
  print(f"{categorized_edge.name}: {categorized_edge.from_table} -> {categorized_edge.to_table}")
  
  print("\n" + "=" * 60)
  print("Composition Example")
  print("=" * 60)
  print_schema_info(product_schema, "Product Schema (Composed)")
  
  print("\nSchema definitions completed!")
