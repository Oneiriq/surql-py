"""Tests for the schema module (fields, tables, and edges)."""

import pytest
from pydantic import ValidationError

from src.schema.edge import (
  EdgeDefinition,
  bidirectional_edge,
  edge_schema,
  typed_edge,
  with_edge_events,
  with_edge_fields,
  with_edge_indexes,
  with_edge_permissions,
  with_from_table,
  with_to_table,
)
from src.schema.fields import (
  FieldDefinition,
  FieldType,
  array_field,
  bool_field,
  computed_field,
  datetime_field,
  field,
  float_field,
  int_field,
  object_field,
  record_field,
  string_field,
)
from src.schema.table import (
  EventDefinition,
  IndexDefinition,
  IndexType,
  MTreeDistanceType,
  MTreeVectorType,
  TableDefinition,
  TableMode,
  event,
  index,
  search_index,
  set_mode,
  table_schema,
  unique_index,
  with_events,
  with_fields,
  with_indexes,
  with_permissions,
)


class TestFieldType:
  """Test suite for FieldType enum."""

  def test_field_type_values(self) -> None:
    """Test FieldType enum values."""
    assert FieldType.STRING.value == 'string'
    assert FieldType.INT.value == 'int'
    assert FieldType.FLOAT.value == 'float'
    assert FieldType.BOOL.value == 'bool'
    assert FieldType.DATETIME.value == 'datetime'
    assert FieldType.DURATION.value == 'duration'
    assert FieldType.DECIMAL.value == 'decimal'
    assert FieldType.NUMBER.value == 'number'
    assert FieldType.OBJECT.value == 'object'
    assert FieldType.ARRAY.value == 'array'
    assert FieldType.RECORD.value == 'record'
    assert FieldType.GEOMETRY.value == 'geometry'
    assert FieldType.ANY.value == 'any'


class TestFieldDefinition:
  """Test suite for FieldDefinition class."""

  def test_field_definition_basic(self) -> None:
    """Test basic field definition creation."""
    field_def = FieldDefinition(name='email', type=FieldType.STRING)

    assert field_def.name == 'email'
    assert field_def.type == FieldType.STRING
    assert field_def.assertion is None
    assert field_def.default is None
    assert field_def.value is None
    assert field_def.permissions is None
    assert field_def.readonly is False
    assert field_def.flexible is False

  def test_field_definition_with_assertion(self) -> None:
    """Test field definition with assertion."""
    field_def = FieldDefinition(
      name='email',
      type=FieldType.STRING,
      assertion='string::is::email($value)',
    )

    assert field_def.assertion == 'string::is::email($value)'

  def test_field_definition_with_default(self) -> None:
    """Test field definition with default value."""
    field_def = FieldDefinition(
      name='created_at',
      type=FieldType.DATETIME,
      default='time::now()',
    )

    assert field_def.default == 'time::now()'

  def test_field_definition_readonly(self) -> None:
    """Test readonly field definition."""
    field_def = FieldDefinition(
      name='id',
      type=FieldType.STRING,
      readonly=True,
    )

    assert field_def.readonly is True

  def test_field_definition_immutability(self) -> None:
    """Test that FieldDefinition is immutable."""
    field_def = FieldDefinition(name='email', type=FieldType.STRING)

    with pytest.raises((ValidationError, AttributeError)):
      field_def.name = 'username'  # type: ignore[misc]


class TestFieldBuilders:
  """Test suite for field builder functions."""

  def test_field_builder(self) -> None:
    """Test generic field builder."""
    field_def = field('name', FieldType.STRING)

    assert field_def.name == 'name'
    assert field_def.type == FieldType.STRING

  def test_field_builder_with_all_params(self) -> None:
    """Test field builder with all parameters."""
    field_def = field(
      'age',
      FieldType.INT,
      assertion='$value >= 0',
      default='0',
      readonly=True,
      permissions={'select': 'true'},
    )

    assert field_def.assertion == '$value >= 0'
    assert field_def.default == '0'
    assert field_def.readonly is True
    assert field_def.permissions == {'select': 'true'}

  def test_string_field_builder(self) -> None:
    """Test string field builder."""
    field_def = string_field('email', assertion='string::is::email($value)')

    assert field_def.type == FieldType.STRING
    assert field_def.assertion == 'string::is::email($value)'

  def test_int_field_builder(self) -> None:
    """Test integer field builder."""
    field_def = int_field('age', assertion='$value >= 0')

    assert field_def.type == FieldType.INT
    assert field_def.assertion == '$value >= 0'

  def test_float_field_builder(self) -> None:
    """Test float field builder."""
    field_def = float_field('price', assertion='$value > 0')

    assert field_def.type == FieldType.FLOAT
    assert field_def.assertion == '$value > 0'

  def test_bool_field_builder(self) -> None:
    """Test boolean field builder."""
    field_def = bool_field('is_active', default='true')

    assert field_def.type == FieldType.BOOL
    assert field_def.default == 'true'

  def test_datetime_field_builder(self) -> None:
    """Test datetime field builder."""
    field_def = datetime_field('created_at', default='time::now()', readonly=True)

    assert field_def.type == FieldType.DATETIME
    assert field_def.default == 'time::now()'
    assert field_def.readonly is True

  def test_record_field_builder_with_table(self) -> None:
    """Test record field builder with table constraint."""
    field_def = record_field('author', table='user')

    assert field_def.type == FieldType.RECORD
    assert '$value.table = "user"' in field_def.assertion

  def test_record_field_builder_with_custom_assertion(self) -> None:
    """Test record field builder with custom assertion and table."""
    field_def = record_field('author', table='user', assertion='$value != NONE')

    assert field_def.type == FieldType.RECORD
    assert '$value.table = "user"' in field_def.assertion
    assert '$value != NONE' in field_def.assertion

  def test_array_field_builder(self) -> None:
    """Test array field builder."""
    field_def = array_field('tags', default='[]')

    assert field_def.type == FieldType.ARRAY
    assert field_def.default == '[]'

  def test_object_field_builder(self) -> None:
    """Test object field builder."""
    field_def = object_field('metadata', flexible=True)

    assert field_def.type == FieldType.OBJECT
    assert field_def.flexible is True

  def test_computed_field_builder(self) -> None:
    """Test computed field builder."""
    field_def = computed_field(
      'full_name',
      'string::concat(first_name, " ", last_name)',
      FieldType.STRING,
    )

    assert field_def.type == FieldType.STRING
    assert field_def.value == 'string::concat(first_name, " ", last_name)'
    assert field_def.readonly is True


class TestTableMode:
  """Test suite for TableMode enum."""

  def test_table_mode_values(self) -> None:
    """Test TableMode enum values."""
    assert TableMode.SCHEMAFULL.value == 'SCHEMAFULL'
    assert TableMode.SCHEMALESS.value == 'SCHEMALESS'
    assert TableMode.DROP.value == 'DROP'


class TestIndexType:
  """Test suite for IndexType enum."""

  def test_index_type_values(self) -> None:
    """Test IndexType enum values."""
    assert IndexType.UNIQUE.value == 'UNIQUE'
    assert IndexType.SEARCH.value == 'SEARCH'
    assert IndexType.STANDARD.value == 'INDEX'
    assert IndexType.MTREE.value == 'MTREE'


class TestMTreeDistanceType:
  """Test suite for MTreeDistanceType enum."""

  def test_mtree_distance_type_values(self) -> None:
    """Test MTreeDistanceType enum values."""
    assert MTreeDistanceType.COSINE.value == 'COSINE'
    assert MTreeDistanceType.EUCLIDEAN.value == 'EUCLIDEAN'
    assert MTreeDistanceType.MANHATTAN.value == 'MANHATTAN'
    assert MTreeDistanceType.MINKOWSKI.value == 'MINKOWSKI'


class TestMTreeVectorType:
  """Test suite for MTreeVectorType enum."""

  def test_mtree_vector_type_values(self) -> None:
    """Test MTreeVectorType enum values."""
    assert MTreeVectorType.F64.value == 'F64'
    assert MTreeVectorType.F32.value == 'F32'
    assert MTreeVectorType.I64.value == 'I64'
    assert MTreeVectorType.I32.value == 'I32'
    assert MTreeVectorType.I16.value == 'I16'


class TestIndexDefinition:
  """Test suite for IndexDefinition class."""

  def test_index_definition_basic(self) -> None:
    """Test basic index definition."""
    index_def = IndexDefinition(name='email_idx', columns=['email'])

    assert index_def.name == 'email_idx'
    assert index_def.columns == ['email']
    assert index_def.type == IndexType.STANDARD

  def test_index_definition_unique(self) -> None:
    """Test unique index definition."""
    index_def = IndexDefinition(
      name='email_idx',
      columns=['email'],
      type=IndexType.UNIQUE,
    )

    assert index_def.type == IndexType.UNIQUE

  def test_index_definition_multi_column(self) -> None:
    """Test multi-column index definition."""
    index_def = IndexDefinition(
      name='name_idx',
      columns=['first_name', 'last_name'],
    )

    assert len(index_def.columns) == 2
    assert 'first_name' in index_def.columns
    assert 'last_name' in index_def.columns

  def test_index_definition_immutability(self) -> None:
    """Test that IndexDefinition is immutable."""
    index_def = IndexDefinition(name='email_idx', columns=['email'])

    with pytest.raises((ValidationError, AttributeError)):
      index_def.name = 'new_idx'  # type: ignore[misc]


class TestEventDefinition:
  """Test suite for EventDefinition class."""

  def test_event_definition_basic(self) -> None:
    """Test basic event definition."""
    event_def = EventDefinition(
      name='email_changed',
      condition='$before.email != $after.email',
      action='CREATE audit_log SET ...',
    )

    assert event_def.name == 'email_changed'
    assert event_def.condition == '$before.email != $after.email'
    assert event_def.action == 'CREATE audit_log SET ...'

  def test_event_definition_immutability(self) -> None:
    """Test that EventDefinition is immutable."""
    event_def = EventDefinition(
      name='test_event',
      condition='true',
      action='RETURN',
    )

    with pytest.raises((ValidationError, AttributeError)):
      event_def.name = 'new_event'  # type: ignore[misc]


class TestTableDefinition:
  """Test suite for TableDefinition class."""

  def test_table_definition_basic(self) -> None:
    """Test basic table definition."""
    table_def = TableDefinition(name='user')

    assert table_def.name == 'user'
    assert table_def.mode == TableMode.SCHEMAFULL
    assert len(table_def.fields) == 0
    assert len(table_def.indexes) == 0
    assert len(table_def.events) == 0
    assert table_def.permissions is None
    assert table_def.drop is False

  def test_table_definition_with_fields(self) -> None:
    """Test table definition with fields."""
    fields = [
      FieldDefinition(name='name', type=FieldType.STRING),
      FieldDefinition(name='age', type=FieldType.INT),
    ]
    table_def = TableDefinition(name='user', fields=fields)

    assert len(table_def.fields) == 2
    assert table_def.fields[0].name == 'name'

  def test_table_definition_immutability(self) -> None:
    """Test that TableDefinition is immutable."""
    table_def = TableDefinition(name='user')

    with pytest.raises((ValidationError, AttributeError)):
      table_def.name = 'post'  # type: ignore[misc]


class TestTableBuilders:
  """Test suite for table builder functions."""

  def test_table_schema_builder(self) -> None:
    """Test table_schema builder."""
    table = table_schema('user')

    assert table.name == 'user'
    assert table.mode == TableMode.SCHEMAFULL

  def test_table_schema_with_all_params(self) -> None:
    """Test table_schema with all parameters."""
    table = table_schema(
      'user',
      mode=TableMode.SCHEMALESS,
      fields=[string_field('name')],
      indexes=[index('name_idx', ['name'])],
      permissions={'select': 'true'},
    )

    assert table.mode == TableMode.SCHEMALESS
    assert len(table.fields) == 1
    assert len(table.indexes) == 1
    assert table.permissions == {'select': 'true'}

  def test_index_builder(self) -> None:
    """Test index builder."""
    idx = index('email_idx', ['email'], IndexType.UNIQUE)

    assert idx.name == 'email_idx'
    assert idx.columns == ['email']
    assert idx.type == IndexType.UNIQUE

  def test_unique_index_builder(self) -> None:
    """Test unique_index convenience builder."""
    idx = unique_index('email_idx', ['email'])

    assert idx.type == IndexType.UNIQUE

  def test_search_index_builder(self) -> None:
    """Test search_index convenience builder."""
    idx = search_index('content_search', ['title', 'content'])

    assert idx.type == IndexType.SEARCH
    assert len(idx.columns) == 2

  def test_event_builder(self) -> None:
    """Test event builder."""
    evt = event(
      'email_changed',
      '$before.email != $after.email',
      'CREATE audit_log',
    )

    assert evt.name == 'email_changed'
    assert evt.condition == '$before.email != $after.email'


class TestTableCompositionHelpers:
  """Test suite for table composition helper functions."""

  def test_with_fields(self) -> None:
    """Test with_fields composition helper."""
    table = table_schema('user')
    table = with_fields(
      table,
      string_field('name'),
      int_field('age'),
    )

    assert len(table.fields) == 2
    assert table.fields[0].name == 'name'
    assert table.fields[1].name == 'age'

  def test_with_fields_preserves_existing(self) -> None:
    """Test that with_fields preserves existing fields."""
    table = table_schema('user', fields=[string_field('email')])
    table = with_fields(table, int_field('age'))

    assert len(table.fields) == 2
    assert table.fields[0].name == 'email'
    assert table.fields[1].name == 'age'

  def test_with_indexes(self) -> None:
    """Test with_indexes composition helper."""
    table = table_schema('user')
    table = with_indexes(
      table,
      unique_index('email_idx', ['email']),
      index('name_idx', ['name']),
    )

    assert len(table.indexes) == 2

  def test_with_events(self) -> None:
    """Test with_events composition helper."""
    table = table_schema('user')
    table = with_events(
      table,
      event('email_changed', 'true', 'RETURN'),
    )

    assert len(table.events) == 1

  def test_with_permissions(self) -> None:
    """Test with_permissions composition helper."""
    table = table_schema('user')
    table = with_permissions(
      table,
      {'select': 'true', 'update': '$auth.id = id'},
    )

    assert table.permissions is not None
    assert 'select' in table.permissions
    assert table.permissions['update'] == '$auth.id = id'

  def test_set_mode(self) -> None:
    """Test set_mode composition helper."""
    table = table_schema('user')
    table = set_mode(table, TableMode.SCHEMALESS)

    assert table.mode == TableMode.SCHEMALESS

  def test_composition_immutability(self) -> None:
    """Test that composition helpers return new instances."""
    original = table_schema('user')
    modified = with_fields(original, string_field('name'))

    assert len(original.fields) == 0
    assert len(modified.fields) == 1


class TestEdgeDefinition:
  """Test suite for EdgeDefinition class."""

  def test_edge_definition_basic(self) -> None:
    """Test basic edge definition."""
    edge = EdgeDefinition(name='likes')

    assert edge.name == 'likes'
    assert edge.from_table is None
    assert edge.to_table is None
    assert len(edge.fields) == 0

  def test_edge_definition_with_constraints(self) -> None:
    """Test edge definition with table constraints."""
    edge = EdgeDefinition(
      name='likes',
      from_table='user',
      to_table='post',
    )

    assert edge.from_table == 'user'
    assert edge.to_table == 'post'

  def test_edge_definition_immutability(self) -> None:
    """Test that EdgeDefinition is immutable."""
    edge = EdgeDefinition(name='likes')

    with pytest.raises((ValidationError, AttributeError)):
      edge.name = 'follows'  # type: ignore[misc]


class TestEdgeBuilders:
  """Test suite for edge builder functions."""

  def test_edge_schema_builder(self) -> None:
    """Test edge_schema builder."""
    edge = edge_schema('likes')

    assert edge.name == 'likes'
    assert edge.from_table is None
    assert edge.to_table is None

  def test_edge_schema_with_constraints(self) -> None:
    """Test edge_schema with table constraints."""
    edge = edge_schema(
      'likes',
      from_table='user',
      to_table='post',
    )

    assert edge.from_table == 'user'
    assert edge.to_table == 'post'

  def test_edge_schema_with_fields(self) -> None:
    """Test edge_schema with fields."""
    edge = edge_schema(
      'likes',
      fields=[datetime_field('created_at', default='time::now()')],
    )

    assert len(edge.fields) == 1
    assert edge.fields[0].name == 'created_at'

  def test_bidirectional_edge(self) -> None:
    """Test bidirectional_edge convenience builder."""
    edge = bidirectional_edge('follows', 'user')

    assert edge.from_table == 'user'
    assert edge.to_table == 'user'

  def test_typed_edge(self) -> None:
    """Test typed_edge convenience builder."""
    edge = typed_edge('authored', 'user', 'post')

    assert edge.from_table == 'user'
    assert edge.to_table == 'post'


class TestEdgeCompositionHelpers:
  """Test suite for edge composition helper functions."""

  def test_with_from_table(self) -> None:
    """Test with_from_table composition helper."""
    edge = edge_schema('likes')
    edge = with_from_table(edge, 'user')

    assert edge.from_table == 'user'

  def test_with_to_table(self) -> None:
    """Test with_to_table composition helper."""
    edge = edge_schema('likes')
    edge = with_to_table(edge, 'post')

    assert edge.to_table == 'post'

  def test_with_edge_fields(self) -> None:
    """Test with_edge_fields composition helper."""
    edge = edge_schema('likes')
    edge = with_edge_fields(
      edge,
      datetime_field('created_at'),
      int_field('weight'),
    )

    assert len(edge.fields) == 2

  def test_with_edge_indexes(self) -> None:
    """Test with_edge_indexes composition helper."""
    edge = edge_schema('likes')
    edge = with_edge_indexes(
      edge,
      index('created_idx', ['created_at']),
    )

    assert len(edge.indexes) == 1

  def test_with_edge_events(self) -> None:
    """Test with_edge_events composition helper."""
    edge = edge_schema('likes')
    edge = with_edge_events(
      edge,
      event('like_created', '$event = "CREATE"', 'RETURN'),
    )

    assert len(edge.events) == 1

  def test_with_edge_permissions(self) -> None:
    """Test with_edge_permissions composition helper."""
    edge = edge_schema('likes')
    edge = with_edge_permissions(
      edge,
      {'create': '$auth.id = in'},
    )

    assert edge.permissions is not None
    assert 'create' in edge.permissions

  def test_edge_composition_immutability(self) -> None:
    """Test that edge composition helpers return new instances."""
    original = edge_schema('likes')
    modified = with_from_table(original, 'user')

    assert original.from_table is None
    assert modified.from_table == 'user'


class TestSchemaIntegration:
  """Integration tests for schema components."""

  def test_build_complete_table_schema(self) -> None:
    """Test building a complete table schema with composition."""
    user_table = table_schema('user')
    user_table = with_fields(
      user_table,
      string_field('email', assertion='string::is::email($value)'),
      string_field('name'),
      int_field('age', assertion='$value >= 0'),
      datetime_field('created_at', default='time::now()', readonly=True),
    )
    user_table = with_indexes(
      user_table,
      unique_index('email_idx', ['email']),
    )
    user_table = with_permissions(
      user_table,
      {
        'select': '$auth.id = id OR $auth.admin = true',
        'update': '$auth.id = id',
      },
    )

    assert user_table.name == 'user'
    assert len(user_table.fields) == 4
    assert len(user_table.indexes) == 1
    assert user_table.permissions is not None

  def test_build_complete_edge_schema(self) -> None:
    """Test building a complete edge schema with composition."""
    likes_edge = edge_schema('likes')
    likes_edge = with_from_table(likes_edge, 'user')
    likes_edge = with_to_table(likes_edge, 'post')
    likes_edge = with_edge_fields(
      likes_edge,
      datetime_field('created_at', default='time::now()'),
      int_field('weight', default='1'),
    )

    assert likes_edge.name == 'likes'
    assert likes_edge.from_table == 'user'
    assert likes_edge.to_table == 'post'
    assert len(likes_edge.fields) == 2
