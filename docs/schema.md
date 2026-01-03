# Schema Definition Guide

This guide covers defining type-safe database schemas using reverie's functional composition API.

## Table of Contents

- [Overview](#overview)
- [Field Types](#field-types)
- [Table Schemas](#table-schemas)
- [Edge Schemas](#edge-schemas)
- [Indexes](#indexes)
- [Events and Triggers](#events-and-triggers)
- [Permissions](#permissions)
- [Functional Composition](#functional-composition)
- [Best Practices](#best-practices)

## Overview

reverie provides a code-first approach to schema definition using:

- **Pure functions** - All schema builders return immutable data structures
- **Pydantic models** - Type-safe schema definitions with validation
- **Functional composition** - Compose schemas using pure functions
- **SurrealQL mapping** - Schemas map directly to SurrealQL statements

### Key Concepts

```python
from reverie.schema.fields import string_field, int_field
from reverie.schema.table import table_schema, unique_index, TableMode

# Define a schema using pure functions
schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('name'),
    int_field('age'),
  ],
  indexes=[
    unique_index('name_idx', ['name']),
  ],
)
```

## Field Types

### Basic Field Types

reverie supports all SurrealDB field types:

```python
from reverie.schema.fields import (
  FieldType,
  string_field,
  int_field,
  float_field,
  bool_field,
  datetime_field,
  duration_field,
  decimal_field,
  number_field,
  array_field,
  object_field,
  record_field,
  geometry_field,
  computed_field,
)
```

### String Fields

```python
# Basic string
string_field('name')

# With length validation
string_field('username', assertion='string::len($value) >= 3 AND string::len($value) <= 20')

# Email validation
string_field('email', assertion='string::is::email($value)')

# Default value
string_field('status', default='"active"')

# Read-only
string_field('id', readonly=True)
```

### Numeric Fields

```python
# Integer
int_field('age', assertion='$value >= 0 AND $value <= 150')

# Float
float_field('price', assertion='$value > 0')

# Decimal
decimal_field('balance', assertion='$value >= 0')

# Number (int or float)
number_field('quantity')

# With default
int_field('count', default='0')
```

### Boolean Fields

```python
# Basic boolean
bool_field('is_active')

# With default
bool_field('is_verified', default='false')

# Read-only
bool_field('is_deleted', default='false', readonly=True)
```

### Datetime Fields

```python
# Basic datetime
datetime_field('birthday')

# Auto-set on creation
datetime_field('created_at', default='time::now()', readonly=True)

# Auto-update
datetime_field('updated_at', default='time::now()')

# With validation
datetime_field('expires_at', assertion='$value > time::now()')
```

### Record Fields (Foreign Keys)

```python
# Link to any record
record_field('owner')

# Link to specific table
record_field('author', table='user')

# With custom assertion
record_field('category', assertion='$value.table = "category" OR $value.table = "tag"')

# Optional
record_field('parent', default='NONE')
```

### Array Fields

```python
# Basic array
array_field('tags')

# With default
array_field('roles', default='[]')

# With validation
array_field('scores', assertion='array::len($value) <= 10')
```

### Object Fields

```python
# Flexible object
object_field('metadata', flexible=True)

# With default
object_field('settings', default='{}')

# Strict object (define nested fields separately)
object_field('address', flexible=False)
# Then define nested fields:
# field('address.street', FieldType.STRING)
# field('address.city', FieldType.STRING)
```

### Computed Fields

```python
# Computed from other fields
computed_field(
  'full_name',
  'string::concat(name.first, " ", name.last)',
  FieldType.STRING,
)

# Computed with function
computed_field(
  'age_years',
  'math::floor(time::now() - birthday)',
  FieldType.INT,
)
```

### Nested Fields

Use dot notation for nested structure:

```python
from reverie.schema.fields import field, FieldType

fields = [
  field('name.first', FieldType.STRING),
  field('name.last', FieldType.STRING),
  field('address.street', FieldType.STRING),
  field('address.city', FieldType.STRING),
  field('address.zip', FieldType.STRING),
]
```

## Table Schemas

### Basic Table Definition

```python
from reverie.schema.table import table_schema, TableMode

# Schemafull (strict)
user_table = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[...],
)

# Schemaless (flexible)
log_table = table_schema(
  'log',
  mode=TableMode.SCHEMALESS,
)

# Drop (marks for deletion)
old_table = table_schema(
  'old_table',
  mode=TableMode.DROP,
  drop=True,
)
```

### Complete Table Example

```python
from reverie.schema.fields import (
  string_field,
  int_field,
  datetime_field,
  record_field,
  array_field,
  bool_field,
)
from reverie.schema.table import (
  table_schema,
  unique_index,
  search_index,
  event,
  TableMode,
)

user_table = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[
    # Basic fields
    string_field('username', assertion='string::len($value) >= 3'),
    string_field('email', assertion='string::is::email($value)'),
    string_field('password_hash'),

    # Nested fields
    string_field('name.first'),
    string_field('name.last'),

    # Optional fields
    string_field('bio', default='""'),
    string_field('avatar_url', default='NONE'),

    # Numbers
    int_field('age', assertion='$value >= 13'),
    int_field('follower_count', default='0'),

    # Boolean
    bool_field('is_verified', default='false'),
    bool_field('is_active', default='true'),

    # Timestamps
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
    datetime_field('last_login', default='NONE'),

    # Collections
    array_field('roles', default='["user"]'),
    array_field('tags', default='[]'),
  ],
  indexes=[
    unique_index('username_idx', ['username']),
    unique_index('email_idx', ['email']),
    search_index('bio_search', ['bio']),
  ],
  events=[
    event(
      'update_timestamp',
      '$event = "UPDATE"',
      'UPDATE $value SET updated_at = time::now()',
    ),
  ],
  permissions={
    'select': '$auth.id = id OR $auth.admin = true',
    'update': '$auth.id = id',
    'delete': '$auth.admin = true',
  },
)
```

## Edge Schemas

Edges represent relationships between records in SurrealDB's graph model.

reverie supports two edge table modes:

1. **TYPE RELATION (default)**: Modern SurrealDB graph edges with automatic in/out fields
2. **SCHEMAFULL**: Traditional tables with explicit in/out fields (driftnet-compatible)

### TYPE RELATION Edges (Default)

The modern approach uses `TYPE RELATION` syntax where SurrealDB automatically manages in/out fields.

```python
from reverie.schema.edge import edge_schema
from reverie.schema.fields import datetime_field

follows_edge = edge_schema(
  'follows',
  from_table='user',
  to_table='user',
  fields=[
    datetime_field('followed_at', default='time::now()', readonly=True),
  ],
)
```

Generates:

```surql
DEFINE TABLE follows TYPE RELATION FROM user TO user;
DEFINE FIELD followed_at ON TABLE follows TYPE datetime DEFAULT time::now() READONLY;
```

### Edge with Properties

```python
likes_edge = edge_schema(
  'likes',
  from_table='user',
  to_table='post',
  fields=[
    datetime_field('liked_at', default='time::now()', readonly=True),
    string_field('reaction', default='"like"'),  # like, love, wow, etc.
  ],
)
```

### SCHEMAFULL Edges (Driftnet-Compatible)

For compatibility with traditional schemas (like driftnet), use SCHEMAFULL mode with explicit in/out fields:

```python
from reverie.schema.edge import EdgeMode, schemafull_edge
from reverie.schema.fields import record_field, string_field, float_field, array_field

# Driftnet-compatible entity_relation edge
entity_relation = schemafull_edge(
  'entity_relation',
  fields=[
    record_field('in', table='entity'),
    record_field('out', table='entity'),
    string_field('relation_type'),
    float_field('confidence'),
    array_field('source_documents', default='[]'),
  ],
)
```

Generates:

```surql
DEFINE TABLE entity_relation SCHEMAFULL;
DEFINE FIELD in ON TABLE entity_relation TYPE record<entity>;
DEFINE FIELD out ON TABLE entity_relation TYPE record<entity>;
DEFINE FIELD relation_type ON TABLE entity_relation TYPE string;
DEFINE FIELD confidence ON TABLE entity_relation TYPE float;
DEFINE FIELD source_documents ON TABLE entity_relation TYPE array DEFAULT [];
```

Alternative syntax using `edge_schema`:

```python
entity_relation = edge_schema(
  'entity_relation',
  mode=EdgeMode.SCHEMAFULL,
  fields=[
    record_field('in', table='entity'),
    record_field('out', table='entity'),
    string_field('relation_type'),
    float_field('confidence'),
  ],
)
```

### Weighted Edges

```python
from reverie.schema.fields import float_field

similarity_edge = edge_schema(
  'similar_to',
  from_table='post',
  to_table='post',
  fields=[
    float_field('score', assertion='$value >= 0 AND $value <= 1'),
    string_field('algorithm'),
  ],
)
```

### Multi-Type Edges

```python
# Edges can connect different table types
tagged_edge = edge_schema(
  'tagged',
  from_table='post',  # Posts are tagged
  to_table='tag',     # With tags
  fields=[
    datetime_field('tagged_at', default='time::now()'),
  ],
)
```

### Choosing Edge Mode

**Use TYPE RELATION (default) when:**

- Building new applications with SurrealDB
- You want SurrealDB to manage edge structure automatically
- You need constrained edge endpoints (FROM/TO tables)

**Use SCHEMAFULL when:**

- Migrating from traditional graph databases
- Compatibility with existing schemas (e.g., driftnet)
- You need full control over edge field definitions
- Working with legacy SurrealDB schemas

## Indexes

### Index Types

```python
from reverie.schema.table import index, unique_index, search_index, IndexType

# Standard index
index('name_idx', ['name'], IndexType.STANDARD)

# Unique index
unique_index('email_idx', ['email'])

# Full-text search index
search_index('content_search', ['title', 'description', 'content'])
```

### Composite Indexes

```python
# Index on multiple columns
unique_index('user_post_idx', ['user_id', 'slug'])

# Order matters for range queries
index('date_user_idx', ['created_at', 'user_id'])
```

### Index Examples

```python
from reverie.schema.table import table_schema, unique_index, search_index, index

product_table = table_schema(
  'product',
  fields=[
    string_field('sku'),
    string_field('name'),
    string_field('description'),
    float_field('price'),
    string_field('category'),
    datetime_field('created_at', default='time::now()'),
  ],
  indexes=[
    # Unique constraint
    unique_index('sku_idx', ['sku']),

    # Full-text search
    search_index('product_search', ['name', 'description']),

    # Filtering/sorting
    index('category_price_idx', ['category', 'price']),
    index('created_idx', ['created_at']),
  ],
)
```

## Events and Triggers

Events are database triggers that execute when conditions are met.

### Basic Event

```python
from reverie.schema.table import event

email_change_event = event(
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
  '''
)
```

### Event Types

```python
# On INSERT
event(
  'new_user',
  '$event = "CREATE"',
  'CREATE notification SET type = "new_user", user = $value.id',
)

# On UPDATE
event(
  'user_updated',
  '$event = "UPDATE"',
  'UPDATE $value SET updated_at = time::now()',
)

# On DELETE
event(
  'user_deleted',
  '$event = "DELETE"',
  'CREATE audit_log SET action = "delete", user = $before.id',
)

# Conditional
event(
  'verify_email',
  '$before.is_verified = false AND $after.is_verified = true',
  'CREATE email_queue SET type = "welcome", user = $value.id',
)
```

### Complex Event Logic

```python
event(
  'auto_publish',
  '''
    $event = "UPDATE" AND
    $before.status = "draft" AND
    $after.status = "published" AND
    $after.published_at = NONE
  ''',
  '''
    UPDATE $value SET
      published_at = time::now(),
      updated_at = time::now()
  '''
)
```

## Permissions

Define row-level security with permissions.

### Basic Permissions

```python
permissions = {
  'select': 'true',  # Anyone can read
  'create': '$auth != NONE',  # Must be authenticated
  'update': '$auth.id = id',  # Can only update own records
  'delete': '$auth.admin = true',  # Only admins can delete
}

user_table = table_schema(
  'user',
  fields=[...],
  permissions=permissions,
)
```

### Complex Permission Rules

```python
post_permissions = {
  # Anyone can read published posts, author can read drafts
  'select': 'published = true OR author = $auth.id',

  # Only authenticated users can create
  'create': '$auth != NONE',

  # Only author can update
  'update': 'author = $auth.id',

  # Author or admin can delete
  'delete': 'author = $auth.id OR $auth.admin = true',
}
```

### Field-Level Permissions

```python
from reverie.schema.fields import string_field

# Field with custom permissions
email_field = string_field(
  'email',
  permissions={
    'select': '$auth.id = $parent.id OR $auth.admin = true',
    'update': '$auth.id = $parent.id',
  }
)
```

## Functional Composition

reverie emphasizes functional composition for building schemas.

### Composing Tables

```python
from reverie.schema.table import (
  table_schema,
  with_fields,
  with_indexes,
  with_events,
  with_permissions,
)
from reverie.schema.fields import string_field, datetime_field

# Start with base table
base_table = table_schema('user', mode=TableMode.SCHEMAFULL)

# Add fields
table_with_fields = with_fields(
  base_table,
  string_field('username'),
  string_field('email'),
)

# Add indexes
table_with_indexes = with_indexes(
  table_with_fields,
  unique_index('username_idx', ['username']),
  unique_index('email_idx', ['email']),
)

# Add timestamps
final_table = with_fields(
  table_with_indexes,
  datetime_field('created_at', default='time::now()'),
  datetime_field('updated_at', default='time::now()'),
)
```

### Reusable Components

```python
# Define reusable field sets
def timestamp_fields():
  return [
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
  ]

def soft_delete_fields():
  return [
    bool_field('is_deleted', default='false'),
    datetime_field('deleted_at', default='NONE'),
  ]

# Compose tables
user_table = table_schema(
  'user',
  fields=[
    string_field('username'),
    string_field('email'),
    *timestamp_fields(),
    *soft_delete_fields(),
  ],
)
```

### Schema Builders

```python
def auditable_table(name: str, fields: list):
  """Create a table with audit fields."""
  return table_schema(
    name,
    mode=TableMode.SCHEMAFULL,
    fields=[
      *fields,
      *timestamp_fields(),
      record_field('created_by', table='user'),
      record_field('updated_by', table='user'),
    ],
  )

# Use the builder
product_table = auditable_table(
  'product',
  [
    string_field('name'),
    float_field('price'),
  ],
)
```

## Best Practices

### 1. Use Type-Safe Helpers

```python
# Good - Type-safe helpers
string_field('email', assertion='string::is::email($value)')

# Avoid - Raw field definition
field('email', FieldType.STRING, assertion='string::is::email($value)')
```

### 2. Validate Data at Schema Level

```python
# Add assertions for data integrity
int_field('age', assertion='$value >= 0 AND $value <= 150')
string_field('status', assertion='$value INSIDE ["active", "inactive", "pending"]')
float_field('price', assertion='$value > 0')
```

### 3. Use Readonly for Immutable Fields

```python
# Prevent modification of critical fields
datetime_field('created_at', default='time::now()', readonly=True)
string_field('id', readonly=True)
```

### 4. Provide Sensible Defaults

```python
# Make optional fields clear with defaults
array_field('tags', default='[]')
bool_field('is_active', default='true')
string_field('status', default='"pending"')
```

### 5. Organize Related Fields

```python
# Group related fields together
fields = [
  # Identity
  string_field('username'),
  string_field('email'),

  # Profile
  string_field('name.first'),
  string_field('name.last'),
  string_field('bio'),

  # Metadata
  datetime_field('created_at', default='time::now()'),
  datetime_field('updated_at', default='time::now()'),
]
```

### 6. Use Meaningful Index Names

```python
# Good - Clear purpose
unique_index('email_unique', ['email'])
index('user_created_idx', ['created_at'])

# Avoid - Unclear names
index('idx1', ['email'])
```

### 7. Document Complex Assertions

```python
# Document complex validation
string_field(
  'phone',
  assertion='''
    # US phone number format: (XXX) XXX-XXXX
    string::len($value) = 14 AND
    string::slice($value, 0, 1) = "(" AND
    string::slice($value, 4, 5) = ")"
  '''
)
```

### 8. Use Events for Automation

```python
# Auto-update timestamps
event(
  'auto_update_timestamp',
  '$event = "UPDATE"',
  'UPDATE $value SET updated_at = time::now()',
)

# Auto-compute fields
event(
  'compute_full_name',
  '$event = "CREATE" OR $event = "UPDATE"',
  'UPDATE $value SET full_name = string::concat(name.first, " ", name.last)',
)
```

### 9. Implement Soft Deletes

```python
# Use soft deletes instead of hard deletes
table_schema(
  'user',
  fields=[
    # ... other fields
    bool_field('is_deleted', default='false'),
    datetime_field('deleted_at', default='NONE'),
  ],
  permissions={
    'select': 'is_deleted = false OR $auth.admin = true',
    'delete': '$auth.admin = true',
  },
  events=[
    event(
      'soft_delete',
      '$event = "DELETE"',
      'UPDATE $value SET is_deleted = true, deleted_at = time::now()',
    ),
  ],
)
```

### 10. Version Your Schemas

```python
# Include version in schema
table_schema(
  'user',
  fields=[
    # ... other fields
    int_field('schema_version', default='1', readonly=True),
  ],
)
```

## Complete Examples

### E-commerce Product Schema

```python
from reverie.schema.fields import *
from reverie.schema.table import *

product_schema = table_schema(
  'product',
  mode=TableMode.SCHEMAFULL,
  fields=[
    # Identity
    string_field('sku', assertion='string::len($value) > 0'),
    string_field('name', assertion='string::len($value) > 0'),
    string_field('slug', assertion='string::len($value) > 0'),

    # Details
    string_field('description'),
    array_field('images', default='[]'),
    array_field('tags', default='[]'),

    # Pricing
    decimal_field('price', assertion='$value > 0'),
    decimal_field('cost', assertion='$value >= 0'),
    string_field('currency', default='"USD"'),

    # Inventory
    int_field('stock', default='0', assertion='$value >= 0'),
    bool_field('in_stock', default='true'),
    int_field('low_stock_threshold', default='10'),

    # Organization
    record_field('category', table='category'),
    record_field('brand', table='brand'),

    # Status
    bool_field('is_active', default='true'),
    bool_field('is_featured', default='false'),

    # Timestamps
    datetime_field('created_at', default='time::now()', readonly=True),
    datetime_field('updated_at', default='time::now()'),
  ],
  indexes=[
    unique_index('sku_idx', ['sku']),
    unique_index('slug_idx', ['slug']),
    search_index('product_search', ['name', 'description']),
    index('category_idx', ['category']),
    index('price_idx', ['price']),
  ],
  events=[
    event(
      'update_in_stock',
      '$event = "UPDATE"',
      'UPDATE $value SET in_stock = (stock > 0)',
    ),
  ],
)
```

### Social Media Schema

```python
# User schema
user_schema = table_schema(
  'user',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('username', assertion='string::len($value) >= 3'),
    string_field('email', assertion='string::is::email($value)'),
    string_field('bio', default='""'),
    array_field('interests', default='[]'),
    datetime_field('created_at', default='time::now()'),
  ],
  indexes=[
    unique_index('username_idx', ['username']),
    unique_index('email_idx', ['email']),
  ],
)

# Post schema
post_schema = table_schema(
  'post',
  mode=TableMode.SCHEMAFULL,
  fields=[
    string_field('content'),
    record_field('author', table='user'),
    array_field('media', default='[]'),
    int_field('like_count', default='0'),
    datetime_field('created_at', default='time::now()'),
  ],
  indexes=[
    index('author_created_idx', ['author', 'created_at']),
  ],
)

# Relationships
follows_edge = edge_schema(
  'follows',
  from_table='user',
  to_table='user',
  fields=[
    datetime_field('followed_at', default='time::now()'),
  ],
)

likes_edge = edge_schema(
  'likes',
  from_table='user',
  to_table='post',
  fields=[
    datetime_field('liked_at', default='time::now()'),
  ],
)
```

## Next Steps

- Learn about [Migrations](migrations.md) to apply your schemas to the database
- Explore [Query Building](queries.md) to work with your schema data
- Check out [Examples](examples/) for more complex schema patterns
