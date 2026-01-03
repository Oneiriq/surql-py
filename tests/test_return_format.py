"""Tests for Return Format functionality."""

from pydantic import BaseModel

from reverie.query.builder import Query, ReturnFormat


class User(BaseModel):
  """Test user model."""

  name: str
  email: str
  age: int | None = None


class TestReturnFormat:
  """Test suite for Return Format functionality."""

  def test_return_format_enum_values(self) -> None:
    """Test ReturnFormat enum has correct values."""
    assert ReturnFormat.NONE.value == 'NONE'
    assert ReturnFormat.DIFF.value == 'DIFF'
    assert ReturnFormat.FULL.value == 'FULL'
    assert ReturnFormat.BEFORE.value == 'BEFORE'
    assert ReturnFormat.AFTER.value == 'AFTER'

  def test_return_none_method(self) -> None:
    """Test return_none() method."""
    query = Query[User]().update('user:alice', {'age': 30}).return_none()

    assert query.return_format == ReturnFormat.NONE

  def test_return_diff_method(self) -> None:
    """Test return_diff() method."""
    query = Query[User]().update('user:alice', {'age': 30}).return_diff()

    assert query.return_format == ReturnFormat.DIFF

  def test_return_full_method(self) -> None:
    """Test return_full() method."""
    query = (
      Query[User]().insert('user', {'name': 'Alice', 'email': 'alice@example.com'}).return_full()
    )

    assert query.return_format == ReturnFormat.FULL

  def test_return_before_method(self) -> None:
    """Test return_before() method."""
    query = Query[User]().delete('user:alice').return_before()

    assert query.return_format == ReturnFormat.BEFORE

  def test_return_after_method(self) -> None:
    """Test return_after() method."""
    query = Query[User]().update('user:alice', {'age': 30}).return_after()

    assert query.return_format == ReturnFormat.AFTER

  def test_insert_with_return_none(self) -> None:
    """Test INSERT query with RETURN NONE."""
    query = (
      Query[User]().insert('user', {'name': 'Alice', 'email': 'alice@example.com'}).return_none()
    )

    sql = query.to_surql()
    assert 'CREATE user CONTENT' in sql
    assert 'RETURN NONE' in sql

  def test_insert_with_return_full(self) -> None:
    """Test INSERT query with RETURN FULL."""
    query = (
      Query[User]().insert('user', {'name': 'Alice', 'email': 'alice@example.com'}).return_full()
    )

    sql = query.to_surql()
    assert 'CREATE user CONTENT' in sql
    assert 'RETURN FULL' in sql

  def test_update_with_return_none(self) -> None:
    """Test UPDATE query with RETURN NONE."""
    query = Query[User]().update('user:alice', {'age': 30}).return_none()

    sql = query.to_surql()
    assert sql == 'UPDATE user:alice SET age = 30 RETURN NONE'

  def test_update_with_return_diff(self) -> None:
    """Test UPDATE query with RETURN DIFF."""
    query = Query[User]().update('user:alice', {'age': 30}).return_diff()

    sql = query.to_surql()
    assert sql == 'UPDATE user:alice SET age = 30 RETURN DIFF'

  def test_update_with_return_before(self) -> None:
    """Test UPDATE query with RETURN BEFORE."""
    query = Query[User]().update('user:alice', {'age': 30}).return_before()

    sql = query.to_surql()
    assert sql == 'UPDATE user:alice SET age = 30 RETURN BEFORE'

  def test_update_with_return_after(self) -> None:
    """Test UPDATE query with RETURN AFTER."""
    query = Query[User]().update('user:alice', {'age': 30}).return_after()

    sql = query.to_surql()
    assert sql == 'UPDATE user:alice SET age = 30 RETURN AFTER'

  def test_update_with_where_and_return_diff(self) -> None:
    """Test UPDATE query with WHERE and RETURN DIFF."""
    query = Query[User]().update('user', {'status': 'active'}).where('age > 18').return_diff()

    sql = query.to_surql()
    assert 'UPDATE user SET' in sql
    assert 'WHERE (age > 18)' in sql
    assert 'RETURN DIFF' in sql

  def test_delete_with_return_none(self) -> None:
    """Test DELETE query with RETURN NONE."""
    query = Query[User]().delete('user:alice').return_none()

    sql = query.to_surql()
    assert sql == 'DELETE user:alice RETURN NONE'

  def test_delete_with_return_before(self) -> None:
    """Test DELETE query with RETURN BEFORE."""
    query = Query[User]().delete('user:alice').return_before()

    sql = query.to_surql()
    assert sql == 'DELETE user:alice RETURN BEFORE'

  def test_delete_with_where_and_return_before(self) -> None:
    """Test DELETE query with WHERE and RETURN BEFORE."""
    query = Query[User]().delete('user').where('deleted_at IS NOT NULL').return_before()

    sql = query.to_surql()
    assert 'DELETE user WHERE' in sql
    assert 'RETURN BEFORE' in sql

  def test_relate_with_return_full(self) -> None:
    """Test RELATE query with RETURN FULL."""
    query = Query[User]().relate('likes', 'user:alice', 'post:123').return_full()

    sql = query.to_surql()
    assert 'RELATE user:alice->likes->post:123' in sql
    assert 'RETURN FULL' in sql

  def test_relate_with_data_and_return_after(self) -> None:
    """Test RELATE query with data and RETURN AFTER."""
    query = Query[User]().relate('likes', 'user:alice', 'post:123', {'weight': 5}).return_after()

    sql = query.to_surql()
    assert 'RELATE user:alice->likes->post:123' in sql
    assert 'CONTENT' in sql
    assert 'RETURN AFTER' in sql

  def test_query_immutability_with_return_format(self) -> None:
    """Test that return format methods maintain immutability."""
    query1 = Query[User]().update('user:alice', {'age': 30})
    query2 = query1.return_diff()

    assert query1.return_format is None
    assert query2.return_format == ReturnFormat.DIFF
    assert query1 is not query2

  def test_chaining_return_format_replaces_previous(self) -> None:
    """Test that chaining return format methods replaces the previous value."""
    query = Query[User]().update('user:alice', {'age': 30}).return_none().return_diff()

    assert query.return_format == ReturnFormat.DIFF

  def test_insert_without_return_format(self) -> None:
    """Test INSERT query without RETURN clause."""
    query = Query[User]().insert('user', {'name': 'Alice', 'email': 'alice@example.com'})

    sql = query.to_surql()
    assert 'CREATE user CONTENT' in sql
    assert 'RETURN' not in sql

  def test_update_without_return_format(self) -> None:
    """Test UPDATE query without RETURN clause."""
    query = Query[User]().update('user:alice', {'age': 30})

    sql = query.to_surql()
    assert sql == 'UPDATE user:alice SET age = 30'
    assert 'RETURN' not in sql

  def test_delete_without_return_format(self) -> None:
    """Test DELETE query without RETURN clause."""
    query = Query[User]().delete('user:alice')

    sql = query.to_surql()
    assert sql == 'DELETE user:alice'
    assert 'RETURN' not in sql

  def test_relate_without_return_format(self) -> None:
    """Test RELATE query without RETURN clause."""
    query = Query[User]().relate('likes', 'user:alice', 'post:123')

    sql = query.to_surql()
    assert sql == 'RELATE user:alice->likes->post:123'
    assert 'RETURN' not in sql
