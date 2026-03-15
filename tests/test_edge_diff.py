"""Tests for edge diffing, event validation, and permission rollback."""

import pytest

from surql.migration.diff import (
  _validate_event_expression,
  diff_edges,
  diff_permissions,
)
from surql.migration.models import DiffOperation
from surql.schema.edge import EdgeDefinition, EdgeMode
from surql.schema.fields import FieldDefinition, FieldType
from surql.schema.table import (
  EventDefinition,
  IndexDefinition,
  IndexType,
  TableDefinition,
)

# --- B1: Edge diff for modified edges ---


class TestDiffEdgesModified:
  """Tests for diff_edges when both old and new edge exist."""

  def test_field_added_to_edge(self) -> None:
    """Detects a field added to an existing edge."""
    old = EdgeDefinition(name='likes', mode=EdgeMode.RELATION, from_table='user', to_table='post')
    new = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      fields=[FieldDefinition(name='weight', type=FieldType.INT)],
    )

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.ADD_FIELD
    assert diffs[0].field == 'weight'

  def test_field_removed_from_edge(self) -> None:
    """Detects a field removed from an existing edge."""
    old = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      fields=[FieldDefinition(name='weight', type=FieldType.INT)],
    )
    new = EdgeDefinition(name='likes', mode=EdgeMode.RELATION, from_table='user', to_table='post')

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.DROP_FIELD
    assert diffs[0].field == 'weight'

  def test_field_modified_on_edge(self) -> None:
    """Detects a field type change on an existing edge."""
    old = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      fields=[FieldDefinition(name='weight', type=FieldType.INT)],
    )
    new = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      fields=[FieldDefinition(name='weight', type=FieldType.FLOAT)],
    )

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.MODIFY_FIELD

  def test_index_added_to_edge(self) -> None:
    """Detects an index added to an existing edge."""
    old = EdgeDefinition(name='likes', mode=EdgeMode.RELATION, from_table='user', to_table='post')
    new = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      indexes=[IndexDefinition(name='weight_idx', columns=['weight'], type=IndexType.STANDARD)],
    )

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.ADD_INDEX
    assert diffs[0].index == 'weight_idx'

  def test_index_removed_from_edge(self) -> None:
    """Detects an index removed from an existing edge."""
    old = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      indexes=[IndexDefinition(name='weight_idx', columns=['weight'], type=IndexType.STANDARD)],
    )
    new = EdgeDefinition(name='likes', mode=EdgeMode.RELATION, from_table='user', to_table='post')

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.DROP_INDEX

  def test_event_added_to_edge(self) -> None:
    """Detects an event added to an existing edge."""
    old = EdgeDefinition(name='likes', mode=EdgeMode.RELATION, from_table='user', to_table='post')
    new = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      events=[EventDefinition(name='on_like', condition='$event = "CREATE"', action='RETURN')],
    )

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.ADD_EVENT
    assert diffs[0].event == 'on_like'

  def test_event_removed_from_edge(self) -> None:
    """Detects an event removed from an existing edge."""
    old = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      events=[EventDefinition(name='on_like', condition='$event = "CREATE"', action='RETURN')],
    )
    new = EdgeDefinition(name='likes', mode=EdgeMode.RELATION, from_table='user', to_table='post')

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.DROP_EVENT

  def test_permission_changed_on_edge(self) -> None:
    """Detects permission changes on an existing edge."""
    old = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      permissions={'select': '$auth.id = in'},
    )
    new = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      permissions={'select': 'true'},
    )

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.MODIFY_PERMISSIONS

  def test_no_changes_returns_empty(self) -> None:
    """Identical edges produce no diffs."""
    edge = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      fields=[FieldDefinition(name='weight', type=FieldType.INT)],
    )

    diffs = diff_edges(edge, edge)

    assert diffs == []

  def test_multiple_simultaneous_changes(self) -> None:
    """Detects multiple change types at once."""
    old = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      fields=[FieldDefinition(name='old_field', type=FieldType.STRING)],
    )
    new = EdgeDefinition(
      name='likes',
      mode=EdgeMode.RELATION,
      from_table='user',
      to_table='post',
      fields=[FieldDefinition(name='new_field', type=FieldType.INT)],
      indexes=[IndexDefinition(name='idx', columns=['new_field'], type=IndexType.STANDARD)],
    )

    diffs = diff_edges(old, new)

    operations = {d.operation for d in diffs}
    assert DiffOperation.ADD_FIELD in operations
    assert DiffOperation.DROP_FIELD in operations
    assert DiffOperation.ADD_INDEX in operations

  def test_schemafull_edge_field_diff(self) -> None:
    """Detects field changes on SCHEMAFULL edges."""
    old = EdgeDefinition(name='rel', mode=EdgeMode.SCHEMAFULL)
    new = EdgeDefinition(
      name='rel',
      mode=EdgeMode.SCHEMAFULL,
      fields=[FieldDefinition(name='label', type=FieldType.STRING)],
    )

    diffs = diff_edges(old, new)

    assert len(diffs) == 1
    assert diffs[0].operation == DiffOperation.ADD_FIELD


# --- B2: Event expression validation ---


class TestValidateEventExpression:
  """Tests for _validate_event_expression SQL injection prevention."""

  def test_valid_create_condition(self) -> None:
    """Allows standard event condition."""
    _validate_event_expression('$event = "CREATE"', 'condition')

  def test_valid_field_comparison(self) -> None:
    """Allows field comparison condition."""
    _validate_event_expression('$before.email != $after.email', 'condition')

  def test_valid_boolean_condition(self) -> None:
    """Allows boolean literal condition."""
    _validate_event_expression('true', 'condition')

  def test_valid_action(self) -> None:
    """Allows standard event action."""
    _validate_event_expression('CREATE audit_log SET user = $value.id', 'action')

  def test_semicolon_space_rejected(self) -> None:
    """Rejects semicolon followed by space (statement separator)."""
    with pytest.raises(ValueError, match='statement separators'):
      _validate_event_expression('$event = "CREATE"; DROP TABLE user', 'condition')

  def test_semicolon_comment_rejected(self) -> None:
    """Rejects semicolon followed by comment."""
    with pytest.raises(ValueError, match='statement separators'):
      _validate_event_expression('$event = "CREATE";--malicious', 'condition')

  def test_trailing_semicolon_rejected(self) -> None:
    """Rejects trailing semicolon."""
    with pytest.raises(ValueError, match='statement separators'):
      _validate_event_expression('$event = "CREATE";', 'condition')

  def test_sql_comment_rejected(self) -> None:
    """Rejects SQL comments."""
    with pytest.raises(ValueError, match='SQL comments'):
      _validate_event_expression('$event = "CREATE" -- comment', 'condition')

  def test_action_with_semicolon_injection_rejected(self) -> None:
    """Rejects action with statement injection."""
    with pytest.raises(ValueError, match='statement separators'):
      _validate_event_expression('RETURN; DROP TABLE user', 'action')


# --- B3: Permission rollback SQL ---


class TestPermissionRollback:
  """Tests for permission rollback SQL generation."""

  def test_rollback_generates_old_permissions(self) -> None:
    """Backward SQL contains the old permission definitions."""
    old = TableDefinition(
      name='user',
      permissions={'select': '$auth.id = id'},
    )
    new = TableDefinition(
      name='user',
      permissions={'select': 'true', 'create': 'true'},
    )

    diffs = diff_permissions(old, new)

    assert len(diffs) == 1
    assert 'FOR SELECT' in diffs[0].backward_sql
    assert '$auth.id = id' in diffs[0].backward_sql

  def test_rollback_empty_when_no_old_permissions(self) -> None:
    """Backward SQL is empty when old table had no permissions."""
    old = TableDefinition(name='user')
    new = TableDefinition(name='user', permissions={'select': 'true'})

    diffs = diff_permissions(old, new)

    assert len(diffs) == 1
    assert diffs[0].backward_sql == ''

  def test_forward_and_backward_roundtrip(self) -> None:
    """Forward SQL has new permissions, backward has old."""
    old = TableDefinition(name='user', permissions={'select': '$auth.id = id'})
    new = TableDefinition(name='user', permissions={'select': 'true'})

    diffs = diff_permissions(old, new)

    assert 'true' in diffs[0].forward_sql
    assert '$auth.id = id' in diffs[0].backward_sql
