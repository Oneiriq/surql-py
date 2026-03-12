"""Tests for the query module (builder, crud, executor, and results)."""

import pytest
from pydantic import BaseModel

from reverie.query.builder import (
  Query,
  VectorDistanceType,
  delete,
  insert,
  relate,
  select,
  update,
  vector_search_query,
)
from reverie.query.executor import _extract_result_data
from reverie.query.helpers import similarity_search_query
from reverie.query.results import (
  ListResult,
  PageInfo,
  QueryResult,
  RecordResult,
  aggregate,
  count_result,
  extract_one,
  extract_result,
  extract_scalar,
  has_results,
  paginated,
  record,
  records,
  success,
)
from reverie.types.operators import Gt, eq
from reverie.types.record_id import RecordID


# Test models
class User(BaseModel):
  """Test user model."""

  name: str
  email: str
  age: int | None = None


class Post(BaseModel):
  """Test post model."""

  title: str
  content: str


class TestQuery:
  """Test suite for Query builder."""

  def test_query_initialization(self) -> None:
    """Test Query initialization."""
    query: Query[User] = Query()

    assert query.operation is None
    assert query.table_name is None
    assert query.fields == []
    assert query.conditions == []

  def test_select_query(self) -> None:
    """Test SELECT query building."""
    query = Query[User]().select(['name', 'email']).from_table('user')

    assert query.operation == 'SELECT'
    assert query.table_name == 'user'
    assert query.fields == ['name', 'email']

  def test_select_all_fields(self) -> None:
    """Test SELECT * query."""
    query = Query[User]().select().from_table('user')

    assert query.fields == ['*']

  def test_where_with_string(self) -> None:
    """Test WHERE clause with string condition."""
    query = Query[User]().select().from_table('user').where('age > 18')

    assert query.conditions == ['age > 18']

  def test_where_with_operator(self) -> None:
    """Test WHERE clause with Operator instance."""
    query = Query[User]().select().from_table('user').where(Gt('age', 18))

    assert query.conditions == ['age > 18']

  def test_multiple_where_clauses(self) -> None:
    """Test multiple WHERE clauses."""
    query = (
      Query[User]().select().from_table('user').where('age > 18').where(eq('status', 'active'))
    )

    assert len(query.conditions) == 2

  def test_order_by_ascending(self) -> None:
    """Test ORDER BY ascending."""
    query = Query[User]().select().from_table('user').order_by('name')

    assert query.order_fields == [('name', 'ASC')]

  def test_order_by_descending(self) -> None:
    """Test ORDER BY descending."""
    query = Query[User]().select().from_table('user').order_by('created_at', 'DESC')

    assert query.order_fields == [('created_at', 'DESC')]

  def test_order_by_invalid_direction(self) -> None:
    """Test ORDER BY with invalid direction."""
    with pytest.raises(ValueError) as exc_info:
      Query[User]().select().from_table('user').order_by('name', 'INVALID')

    assert 'Invalid direction' in str(exc_info.value)

  def test_group_by(self) -> None:
    """Test GROUP BY clause."""
    query = Query[User]().select(['status', 'COUNT(*)']).from_table('user').group_by('status')

    assert query.group_fields == ['status']

  def test_limit(self) -> None:
    """Test LIMIT clause."""
    query = Query[User]().select().from_table('user').limit(10)

    assert query.limit_value == 10

  def test_limit_negative(self) -> None:
    """Test LIMIT with negative value."""
    with pytest.raises(ValueError):
      Query[User]().select().from_table('user').limit(-1)

  def test_offset(self) -> None:
    """Test OFFSET clause."""
    query = Query[User]().select().from_table('user').offset(20)

    assert query.offset_value == 20

  def test_offset_negative(self) -> None:
    """Test OFFSET with negative value."""
    with pytest.raises(ValueError):
      Query[User]().select().from_table('user').offset(-1)

  def test_insert_query(self) -> None:
    """Test INSERT query building."""
    data = {'name': 'Alice', 'email': 'alice@example.com'}
    query = Query[User]().insert('user', data)

    assert query.operation == 'INSERT'
    assert query.table_name == 'user'
    assert query.insert_data == data

  def test_update_query(self) -> None:
    """Test UPDATE query building."""
    data = {'status': 'active'}
    query = Query[User]().update('user:alice', data)

    assert query.operation == 'UPDATE'
    assert query.table_name == 'user:alice'
    assert query.update_data == data

  def test_delete_query(self) -> None:
    """Test DELETE query building."""
    query = Query[User]().delete('user:alice')

    assert query.operation == 'DELETE'
    assert query.table_name == 'user:alice'

  def test_relate_query(self) -> None:
    """Test RELATE query building."""
    query = Query[User]().relate('likes', 'user:alice', 'post:123')

    assert query.operation == 'RELATE'
    assert query.table_name == 'likes'
    assert query.relate_from == 'user:alice'
    assert query.relate_to == 'post:123'

  def test_relate_with_record_id(self) -> None:
    """Test RELATE with RecordID instances."""
    from_id = RecordID(table='user', id='alice')
    to_id = RecordID(table='post', id=123)

    query = Query[User]().relate('likes', from_id, to_id)

    assert query.relate_from == 'user:alice'
    assert query.relate_to == 'post:123'

  def test_traverse(self) -> None:
    """Test graph traversal."""
    query = Query[User]().select().from_table('user:alice').traverse('->likes->post')

    assert query.graph_traversal == '->likes->post'

  def test_join(self) -> None:
    """Test JOIN clause."""
    query = Query[User]().select().from_table('user').join('JOIN post ON user.id = post.author')

    assert len(query.join_clauses) == 1


class TestQueryToSurQL:
  """Test suite for Query.to_surql() method."""

  def test_to_surql_simple_select(self) -> None:
    """Test SurrealQL generation for simple SELECT."""
    query = Query[User]().select(['name']).from_table('user')

    assert query.to_surql() == 'SELECT name FROM user'

  def test_to_surql_select_all(self) -> None:
    """Test SurrealQL generation for SELECT *."""
    query = Query[User]().select().from_table('user')

    assert query.to_surql() == 'SELECT * FROM user'

  def test_to_surql_with_where(self) -> None:
    """Test SurrealQL generation with WHERE."""
    query = Query[User]().select().from_table('user').where('age > 18')

    assert query.to_surql() == 'SELECT * FROM user WHERE (age > 18)'

  def test_to_surql_with_multiple_where(self) -> None:
    """Test SurrealQL generation with multiple WHERE clauses."""
    query = Query[User]().select().from_table('user').where('age > 18').where('status = "active"')

    sql = query.to_surql()
    assert 'WHERE (age > 18) AND (status = "active")' in sql

  def test_to_surql_with_order_by(self) -> None:
    """Test SurrealQL generation with ORDER BY."""
    query = Query[User]().select().from_table('user').order_by('name')

    assert query.to_surql() == 'SELECT * FROM user ORDER BY name ASC'

  def test_to_surql_with_limit(self) -> None:
    """Test SurrealQL generation with LIMIT."""
    query = Query[User]().select().from_table('user').limit(10)

    assert query.to_surql() == 'SELECT * FROM user LIMIT 10'

  def test_to_surql_with_offset(self) -> None:
    """Test SurrealQL generation with OFFSET (START)."""
    query = Query[User]().select().from_table('user').offset(20)

    assert query.to_surql() == 'SELECT * FROM user START 20'

  def test_to_surql_complex_select(self) -> None:
    """Test SurrealQL generation for complex SELECT."""
    query = (
      Query[User]()
      .select(['name', 'email'])
      .from_table('user')
      .where('age > 18')
      .order_by('name')
      .limit(10)
      .offset(5)
    )

    sql = query.to_surql()
    assert 'SELECT name, email FROM user' in sql
    assert 'WHERE (age > 18)' in sql
    assert 'ORDER BY name ASC' in sql
    assert 'LIMIT 10' in sql
    assert 'START 5' in sql

  def test_to_surql_insert(self) -> None:
    """Test SurrealQL generation for INSERT."""
    data = {'name': 'Alice', 'email': 'alice@example.com'}
    query = Query[User]().insert('user', data)

    sql = query.to_surql()
    assert sql.startswith('CREATE user CONTENT')
    assert "name: 'Alice'" in sql
    assert "email: 'alice@example.com'" in sql

  def test_to_surql_update(self) -> None:
    """Test SurrealQL generation for UPDATE."""
    data = {'status': 'active'}
    query = Query[User]().update('user:alice', data)

    sql = query.to_surql()
    assert sql == "UPDATE user:alice SET status = 'active'"

  def test_to_surql_update_with_where(self) -> None:
    """Test SurrealQL generation for UPDATE with WHERE."""
    data = {'status': 'inactive'}
    query = Query[User]().update('user', data).where('last_login < "2024-01-01"')

    sql = query.to_surql()
    assert 'UPDATE user SET' in sql
    assert 'WHERE' in sql

  def test_to_surql_delete(self) -> None:
    """Test SurrealQL generation for DELETE."""
    query = Query[User]().delete('user:alice')

    assert query.to_surql() == 'DELETE user:alice'

  def test_to_surql_delete_with_where(self) -> None:
    """Test SurrealQL generation for DELETE with WHERE."""
    query = Query[User]().delete('user').where('deleted_at IS NOT NULL')

    sql = query.to_surql()
    assert 'DELETE user WHERE' in sql

  def test_to_surql_relate(self) -> None:
    """Test SurrealQL generation for RELATE."""
    query = Query[User]().relate('likes', 'user:alice', 'post:123')

    assert query.to_surql() == 'RELATE user:alice->likes->post:123'

  def test_to_surql_relate_with_data(self) -> None:
    """Test SurrealQL generation for RELATE with data."""
    query = Query[User]().relate('likes', 'user:alice', 'post:123', {'weight': 5})

    sql = query.to_surql()
    assert 'RELATE user:alice->likes->post:123' in sql
    assert 'CONTENT' in sql
    assert 'weight: 5' in sql

  def test_to_surql_no_operation(self) -> None:
    """Test SurrealQL generation without operation."""
    query: Query[User] = Query()

    with pytest.raises(ValueError) as exc_info:
      query.to_surql()

    assert 'operation not specified' in str(exc_info.value)

  def test_to_surql_select_no_table(self) -> None:
    """Test SurrealQL generation for SELECT without table."""
    query = Query[User]().select()

    with pytest.raises(ValueError) as exc_info:
      query.to_surql()

    assert 'Table name required' in str(exc_info.value)


class TestQueryImmutability:
  """Test suite for Query immutability."""

  def test_query_immutability(self) -> None:
    """Test that Query methods return new instances."""
    query1 = Query[User]().select()
    query2 = query1.from_table('user')

    assert query1.table_name is None
    assert query2.table_name == 'user'
    assert query1 is not query2

  def test_where_immutability(self) -> None:
    """Test that where() returns new instance."""
    query1 = Query[User]().select().from_table('user')
    query2 = query1.where('age > 18')

    assert len(query1.conditions) == 0
    assert len(query2.conditions) == 1


class TestFunctionalQueryBuilders:
  """Test suite for functional query builder helpers."""

  def test_select_helper(self) -> None:
    """Test select() helper function."""
    query = select(['name', 'email'])

    assert query.operation == 'SELECT'
    assert query.fields == ['name', 'email']

  def test_insert_helper(self) -> None:
    """Test insert() helper function."""
    data = {'name': 'Alice'}
    query = insert('user', data)

    assert query.operation == 'INSERT'
    assert query.table_name == 'user'

  def test_update_helper(self) -> None:
    """Test update() helper function."""
    data = {'status': 'active'}
    query = update('user:alice', data)

    assert query.operation == 'UPDATE'

  def test_delete_helper(self) -> None:
    """Test delete() helper function."""
    query = delete('user:alice')

    assert query.operation == 'DELETE'

  def test_relate_helper(self) -> None:
    """Test relate() helper function."""
    query = relate('likes', 'user:alice', 'post:123')

    assert query.operation == 'RELATE'


class TestRecordResult:
  """Test suite for RecordResult class."""

  def test_record_result_with_data(self) -> None:
    """Test RecordResult with data."""
    user = User(name='Alice', email='alice@example.com')
    result = RecordResult(record=user, exists=True)

    assert result.record == user
    assert result.exists is True

  def test_record_result_none(self) -> None:
    """Test RecordResult with None."""
    result: RecordResult[User] = RecordResult(record=None, exists=False)

    assert result.record is None
    assert result.exists is False

  def test_unwrap_success(self) -> None:
    """Test unwrap() with valid record."""
    user = User(name='Alice', email='alice@example.com')
    result = RecordResult(record=user, exists=True)

    unwrapped = result.unwrap()
    assert unwrapped == user

  def test_unwrap_none(self) -> None:
    """Test unwrap() with None raises error."""
    result: RecordResult[User] = RecordResult(record=None, exists=False)

    with pytest.raises(ValueError) as exc_info:
      result.unwrap()

    assert 'Cannot unwrap None' in str(exc_info.value)

  def test_unwrap_or_with_data(self) -> None:
    """Test unwrap_or() with valid record."""
    user = User(name='Alice', email='alice@example.com')
    default = User(name='Default', email='default@example.com')
    result = RecordResult(record=user, exists=True)

    assert result.unwrap_or(default) == user

  def test_unwrap_or_with_none(self) -> None:
    """Test unwrap_or() with None returns default."""
    default = User(name='Default', email='default@example.com')
    result: RecordResult[User] = RecordResult(record=None, exists=False)

    assert result.unwrap_or(default) == default


class TestListResult:
  """Test suite for ListResult class."""

  def test_list_result_basic(self) -> None:
    """Test basic ListResult creation."""
    users = [
      User(name='Alice', email='alice@example.com'),
      User(name='Bob', email='bob@example.com'),
    ]
    result = ListResult(records=users)

    assert len(result) == 2
    assert result.records == users

  def test_list_result_with_pagination(self) -> None:
    """Test ListResult with pagination info."""
    users = [User(name='Alice', email='alice@example.com')]
    result = ListResult(records=users, total=100, limit=10, offset=0, has_more=True)

    assert result.total == 100
    assert result.limit == 10
    assert result.offset == 0
    assert result.has_more is True

  def test_list_result_iteration(self) -> None:
    """Test iterating over ListResult."""
    users = [
      User(name='Alice', email='alice@example.com'),
      User(name='Bob', email='bob@example.com'),
    ]
    result = ListResult(records=users)

    names = [user.name for user in result]
    assert names == ['Alice', 'Bob']

  def test_list_result_indexing(self) -> None:
    """Test indexing into ListResult."""
    users = [
      User(name='Alice', email='alice@example.com'),
      User(name='Bob', email='bob@example.com'),
    ]
    result = ListResult(records=users)

    assert result[0].name == 'Alice'
    assert result[1].name == 'Bob'

  def test_list_result_is_empty(self) -> None:
    """Test is_empty() method."""
    empty_result: ListResult[User] = ListResult(records=[])
    non_empty_result = ListResult(records=[User(name='Alice', email='alice@example.com')])

    assert empty_result.is_empty() is True
    assert non_empty_result.is_empty() is False

  def test_list_result_first(self) -> None:
    """Test first() method."""
    users = [
      User(name='Alice', email='alice@example.com'),
      User(name='Bob', email='bob@example.com'),
    ]
    result = ListResult(records=users)

    assert result.first() == users[0]

  def test_list_result_first_empty(self) -> None:
    """Test first() on empty result."""
    result: ListResult[User] = ListResult(records=[])

    assert result.first() is None

  def test_list_result_last(self) -> None:
    """Test last() method."""
    users = [
      User(name='Alice', email='alice@example.com'),
      User(name='Bob', email='bob@example.com'),
    ]
    result = ListResult(records=users)

    assert result.last() == users[1]

  def test_list_result_last_empty(self) -> None:
    """Test last() on empty result."""
    result: ListResult[User] = ListResult(records=[])

    assert result.last() is None


class TestHelperFunctions:
  """Test suite for result helper functions."""

  def test_success_helper(self) -> None:
    """Test success() helper."""
    data = {'test': 'data'}
    result = success(data, time='100ms')

    assert result.data == data
    assert result.time == '100ms'
    assert result.status == 'OK'

  def test_record_helper(self) -> None:
    """Test record() helper."""
    user = User(name='Alice', email='alice@example.com')
    result = record(user, exists=True)

    assert result.record == user
    assert result.exists is True

  def test_records_helper(self) -> None:
    """Test records() helper."""
    users = [User(name='Alice', email='alice@example.com')]
    result = records(users, total=100, limit=10, offset=0)

    assert result.records == users
    assert result.total == 100
    assert result.limit == 10
    assert result.offset == 0

  def test_records_has_more_calculation(self) -> None:
    """Test has_more calculation in records()."""
    users = [User(name='Alice', email='alice@example.com')]

    # Has more when offset + limit < total
    result1 = records(users, total=100, limit=10, offset=0)
    assert result1.has_more is True

    # No more when offset + limit >= total
    result2 = records(users, total=10, limit=10, offset=0)
    assert result2.has_more is False

  def test_count_result_helper(self) -> None:
    """Test count_result() helper."""
    result = count_result(42)

    assert result.count == 42

  def test_aggregate_helper(self) -> None:
    """Test aggregate() helper."""
    result = aggregate(42.5, operation='AVG', field='age')

    assert result.value == 42.5
    assert result.operation == 'AVG'
    assert result.field == 'age'

  def test_paginated_helper(self) -> None:
    """Test paginated() helper."""
    users = [User(name='Alice', email='alice@example.com')]
    result = paginated(users, page=1, page_size=10, total=100)

    assert result.items == users
    assert result.page_info.current_page == 1
    assert result.page_info.page_size == 10
    assert result.page_info.total_items == 100
    assert result.page_info.total_pages == 10
    assert result.page_info.has_next is True
    assert result.page_info.has_previous is False


class TestExtractResultData:
  """Test suite for _extract_result_data helper."""

  def test_extract_none(self) -> None:
    """Test extracting from None."""
    assert _extract_result_data(None) is None

  def test_extract_direct_list(self) -> None:
    """Test extracting from direct list."""
    data = [{'name': 'Alice'}, {'name': 'Bob'}]
    assert _extract_result_data(data) == data

  def test_extract_wrapped_result(self) -> None:
    """Test extracting from wrapped result."""
    wrapped = [{'result': [{'name': 'Alice'}]}]
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}]

  def test_extract_multiple_wrapped_results(self) -> None:
    """Test extracting from multiple wrapped results."""
    wrapped = [
      {'result': [{'name': 'Alice'}]},
      {'result': [{'name': 'Bob'}]},
    ]
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}, {'name': 'Bob'}]

  def test_extract_dict_with_result_key(self) -> None:
    """Test extracting from dict with 'result' key."""
    wrapped = {'result': [{'name': 'Alice'}]}
    result = _extract_result_data(wrapped)
    assert result == [{'name': 'Alice'}]


class TestPaginationCalculations:
  """Test suite for pagination calculations."""

  def test_page_info_first_page(self) -> None:
    """Test PageInfo for first page."""
    page_info = PageInfo(
      current_page=1,
      page_size=10,
      total_pages=10,
      total_items=100,
      has_previous=False,
      has_next=True,
    )

    assert page_info.current_page == 1
    assert page_info.has_previous is False
    assert page_info.has_next is True

  def test_page_info_last_page(self) -> None:
    """Test PageInfo for last page."""
    page_info = PageInfo(
      current_page=10,
      page_size=10,
      total_pages=10,
      total_items=100,
      has_previous=True,
      has_next=False,
    )

    assert page_info.current_page == 10
    assert page_info.has_previous is True
    assert page_info.has_next is False

  def test_paginated_result_total_pages(self) -> None:
    """Test total pages calculation."""
    users = [User(name='Alice', email='alice@example.com')]

    # 100 items, 10 per page = 10 pages
    result1 = paginated(users, page=1, page_size=10, total=100)
    assert result1.page_info.total_pages == 10

    # 95 items, 10 per page = 10 pages (rounded up)
    result2 = paginated(users, page=1, page_size=10, total=95)
    assert result2.page_info.total_pages == 10


class TestResultImmutability:
  """Test suite for result immutability."""

  def test_query_result_immutability(self) -> None:
    """Test that QueryResult is immutable."""
    result = QueryResult(data={'test': 'value'})

    with pytest.raises((Exception, ValueError)):
      result.data = {}  # type: ignore[misc]

  def test_record_result_immutability(self) -> None:
    """Test that RecordResult is immutable."""
    user = User(name='Alice', email='alice@example.com')
    result = RecordResult(record=user, exists=True)

    with pytest.raises((Exception, ValueError)):
      result.exists = False  # type: ignore[misc]

  def test_list_result_immutability(self) -> None:
    """Test that ListResult is immutable."""
    users = [User(name='Alice', email='alice@example.com')]
    result = ListResult(records=users)

    with pytest.raises((Exception, ValueError)):
      result.total = 100  # type: ignore[misc]


class TestResultExtraction:
  """Test suite for result extraction utilities."""

  def test_extract_result_none(self) -> None:
    """Test extract_result with None."""
    assert extract_result(None) == []

  def test_extract_result_empty_list(self) -> None:
    """Test extract_result with empty list."""
    assert extract_result([]) == []

  def test_extract_result_flat_format(self) -> None:
    """Test extract_result with flat format (db.select)."""
    result = [{'id': 'user:123', 'name': 'Alice'}]
    extracted = extract_result(result)
    assert extracted == [{'id': 'user:123', 'name': 'Alice'}]

  def test_extract_result_flat_format_multiple(self) -> None:
    """Test extract_result with multiple flat records."""
    result = [
      {'id': 'user:123', 'name': 'Alice'},
      {'id': 'user:456', 'name': 'Bob'},
    ]
    extracted = extract_result(result)
    assert extracted == result

  def test_extract_result_nested_format(self) -> None:
    """Test extract_result with nested format (db.query)."""
    result = [{'result': [{'id': 'user:123', 'name': 'Alice'}]}]
    extracted = extract_result(result)
    assert extracted == [{'id': 'user:123', 'name': 'Alice'}]

  def test_extract_result_nested_format_multiple_statements(self) -> None:
    """Test extract_result with multiple nested results."""
    result = [
      {'result': [{'id': 'user:123', 'name': 'Alice'}]},
      {'result': [{'id': 'user:456', 'name': 'Bob'}]},
    ]
    extracted = extract_result(result)
    assert extracted == [
      {'id': 'user:123', 'name': 'Alice'},
      {'id': 'user:456', 'name': 'Bob'},
    ]

  def test_extract_result_nested_empty(self) -> None:
    """Test extract_result with empty nested result."""
    result = [{'result': []}]
    extracted = extract_result(result)
    assert extracted == []

  def test_extract_result_dict_with_result_key(self) -> None:
    """Test extract_result with dict containing 'result' key."""
    result = {'result': [{'id': 'user:123', 'name': 'Alice'}]}
    extracted = extract_result(result)
    assert extracted == [{'id': 'user:123', 'name': 'Alice'}]

  def test_extract_result_aggregate(self) -> None:
    """Test extract_result with aggregate result (flat)."""
    result = [{'count': 42}]
    extracted = extract_result(result)
    assert extracted == [{'count': 42}]

  def test_extract_result_aggregate_nested(self) -> None:
    """Test extract_result with aggregate result (nested)."""
    result = [{'result': [{'count': 42}]}]
    extracted = extract_result(result)
    assert extracted == [{'count': 42}]

  def test_extract_one_with_data(self) -> None:
    """Test extract_one with data present."""
    result = [{'result': [{'id': 'user:123', 'name': 'Alice'}]}]
    extracted = extract_one(result)
    assert extracted == {'id': 'user:123', 'name': 'Alice'}

  def test_extract_one_with_flat_format(self) -> None:
    """Test extract_one with flat format."""
    result = [{'id': 'user:123', 'name': 'Alice'}]
    extracted = extract_one(result)
    assert extracted == {'id': 'user:123', 'name': 'Alice'}

  def test_extract_one_empty(self) -> None:
    """Test extract_one with empty result."""
    assert extract_one([]) is None

  def test_extract_one_none(self) -> None:
    """Test extract_one with None."""
    assert extract_one(None) is None

  def test_extract_one_nested_empty(self) -> None:
    """Test extract_one with empty nested result."""
    result = [{'result': []}]
    assert extract_one(result) is None

  def test_extract_one_multiple_records(self) -> None:
    """Test extract_one returns only first record."""
    result = [
      {'id': 'user:123', 'name': 'Alice'},
      {'id': 'user:456', 'name': 'Bob'},
    ]
    extracted = extract_one(result)
    assert extracted == {'id': 'user:123', 'name': 'Alice'}

  def test_extract_scalar_with_data(self) -> None:
    """Test extract_scalar with data present."""
    result = [{'result': [{'count': 42}]}]
    value = extract_scalar(result, 'count')
    assert value == 42

  def test_extract_scalar_with_flat_format(self) -> None:
    """Test extract_scalar with flat format."""
    result = [{'total': 100}]
    value = extract_scalar(result, 'total')
    assert value == 100

  def test_extract_scalar_missing_key(self) -> None:
    """Test extract_scalar with missing key returns default."""
    result = [{'id': 'user:123'}]
    value = extract_scalar(result, 'count', default=0)
    assert value == 0

  def test_extract_scalar_empty_result(self) -> None:
    """Test extract_scalar with empty result returns default."""
    value = extract_scalar([], 'count', default=0)
    assert value == 0

  def test_extract_scalar_none_result(self) -> None:
    """Test extract_scalar with None returns default."""
    value = extract_scalar(None, 'count', default=0)
    assert value == 0

  def test_extract_scalar_custom_default(self) -> None:
    """Test extract_scalar with custom default."""
    value = extract_scalar([], 'avg', default=-1)
    assert value == -1

  def test_extract_scalar_avg_query(self) -> None:
    """Test extract_scalar with AVG aggregate."""
    result = [{'result': [{'avg': 25.5}]}]
    value = extract_scalar(result, 'avg')
    assert value == 25.5

  def test_has_results_with_data(self) -> None:
    """Test has_results with data present."""
    result = [{'result': [{'id': 'user:123'}]}]
    assert has_results(result) is True

  def test_has_results_with_flat_format(self) -> None:
    """Test has_results with flat format."""
    result = [{'id': 'user:123'}]
    assert has_results(result) is True

  def test_has_results_empty(self) -> None:
    """Test has_results with empty result."""
    assert has_results([]) is False

  def test_has_results_none(self) -> None:
    """Test has_results with None."""
    assert has_results(None) is False

  def test_has_results_empty_nested(self) -> None:
    """Test has_results with empty nested result."""
    result = [{'result': []}]
    assert has_results(result) is False

  def test_has_results_multiple_records(self) -> None:
    """Test has_results with multiple records."""
    result = [
      {'id': 'user:123'},
      {'id': 'user:456'},
    ]
    assert has_results(result) is True


class TestVectorSearch:
  """Test suite for vector_search() method."""

  def test_vector_search_basic(self) -> None:
    """Test basic vector search with default parameters."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2, 0.3])
    )

    assert query.vector_field == 'embedding'
    assert query.vector_value == [0.1, 0.2, 0.3]
    assert query.vector_k == 10  # default
    assert query.vector_distance == 'COSINE'  # default

  def test_vector_search_custom_k(self) -> None:
    """Test vector search with custom k value."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2], k=20)
    )

    assert query.vector_k == 20

  def test_vector_search_euclidean_distance(self) -> None:
    """Test vector search with EUCLIDEAN distance."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2], distance='EUCLIDEAN')
    )

    assert query.vector_distance == 'EUCLIDEAN'

  def test_vector_search_manhattan_distance(self) -> None:
    """Test vector search with MANHATTAN distance."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2], distance='MANHATTAN')
    )

    assert query.vector_distance == 'MANHATTAN'

  def test_vector_search_all_distance_metrics(self) -> None:
    """Test vector search with all supported distance metrics."""
    metrics: list[VectorDistanceType] = [
      'COSINE',
      'EUCLIDEAN',
      'MANHATTAN',
      'CHEBYSHEV',
      'MINKOWSKI',
      'HAMMING',
      'JACCARD',
      'PEARSON',
      'MAHALANOBIS',
    ]

    for metric in metrics:
      query = (
        Query[User]()
        .select()
        .from_table('documents')
        .vector_search(field='embedding', vector=[0.1], distance=metric)
      )
      assert query.vector_distance == metric

  def test_vector_search_invalid_k(self) -> None:
    """Test vector search with k < 1 raises error."""
    with pytest.raises(ValueError) as exc_info:
      Query[User]().select().from_table('documents').vector_search(
        field='embedding', vector=[0.1, 0.2], k=0
      )

    assert 'k must be at least 1' in str(exc_info.value)

  def test_vector_search_negative_k(self) -> None:
    """Test vector search with negative k raises error."""
    with pytest.raises(ValueError) as exc_info:
      Query[User]().select().from_table('documents').vector_search(
        field='embedding', vector=[0.1, 0.2], k=-5
      )

    assert 'k must be at least 1' in str(exc_info.value)

  def test_vector_search_empty_vector(self) -> None:
    """Test vector search with empty vector raises error."""
    with pytest.raises(ValueError) as exc_info:
      Query[User]().select().from_table('documents').vector_search(field='embedding', vector=[])

    assert 'Vector cannot be empty' in str(exc_info.value)

  def test_vector_search_immutability(self) -> None:
    """Test that vector_search() returns a new instance."""
    query1 = Query[User]().select().from_table('documents')
    query2 = query1.vector_search(field='embedding', vector=[0.1, 0.2])

    assert query1.vector_field is None
    assert query1.vector_value == []
    assert query2.vector_field == 'embedding'
    assert query2.vector_value == [0.1, 0.2]
    assert query1 is not query2

  def test_vector_search_method_chaining(self) -> None:
    """Test vector search works with method chaining."""
    query = (
      Query[User]()
      .select(['id', 'text'])
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2, 0.3], k=5)
      .limit(10)
    )

    assert query.fields == ['id', 'text']
    assert query.table_name == 'documents'
    assert query.vector_field == 'embedding'
    assert query.vector_k == 5
    assert query.limit_value == 10


class TestVectorSearchSurQL:
  """Test suite for vector search SurrealQL generation."""

  def test_to_surql_basic_vector_search(self) -> None:
    """Test SurrealQL generation for basic vector search."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2, 0.3], k=10, distance='COSINE')
    )

    sql = query.to_surql()
    assert 'SELECT * FROM documents' in sql
    assert 'WHERE embedding <|10,COSINE|> [0.1, 0.2, 0.3]' in sql

  def test_to_surql_vector_search_with_euclidean(self) -> None:
    """Test SurrealQL generation with EUCLIDEAN distance."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='features', vector=[1.0, 2.0], k=5, distance='EUCLIDEAN')
    )

    sql = query.to_surql()
    assert 'WHERE features <|5,EUCLIDEAN|> [1.0, 2.0]' in sql

  def test_to_surql_vector_search_with_fields(self) -> None:
    """Test SurrealQL generation with specific fields selected."""
    query = (
      Query[User]()
      .select(['id', 'text', 'title'])
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.5, 0.5], k=20)
    )

    sql = query.to_surql()
    assert 'SELECT id, text, title FROM documents' in sql
    assert 'WHERE embedding <|20,COSINE|> [0.5, 0.5]' in sql

  def test_to_surql_vector_search_combined_with_where(self) -> None:
    """Test SurrealQL generation combining vector search with WHERE conditions."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2], k=10)
      .where('status = "published"')
    )

    sql = query.to_surql()
    assert 'WHERE embedding <|10,COSINE|> [0.1, 0.2] AND (status = "published")' in sql

  def test_to_surql_vector_search_with_multiple_where(self) -> None:
    """Test SurrealQL generation with vector search and multiple WHERE conditions."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1], k=5)
      .where('status = "active"')
      .where('category = "news"')
    )

    sql = query.to_surql()
    assert 'embedding <|5,COSINE|> [0.1]' in sql
    assert '(status = "active")' in sql
    assert '(category = "news")' in sql
    assert ' AND ' in sql

  def test_to_surql_vector_search_with_order_by(self) -> None:
    """Test SurrealQL generation with vector search and ORDER BY."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2], k=10)
      .order_by('created_at', 'DESC')
    )

    sql = query.to_surql()
    assert 'WHERE embedding <|10,COSINE|> [0.1, 0.2]' in sql
    assert 'ORDER BY created_at DESC' in sql

  def test_to_surql_vector_search_with_limit(self) -> None:
    """Test SurrealQL generation with vector search and LIMIT."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2, 0.3], k=100)
      .limit(25)
    )

    sql = query.to_surql()
    assert 'WHERE embedding <|100,COSINE|> [0.1, 0.2, 0.3]' in sql
    assert 'LIMIT 25' in sql

  def test_to_surql_vector_search_high_dimensional(self) -> None:
    """Test SurrealQL generation with high-dimensional vectors."""
    # Simulate a 1024-dimensional embedding (just a few values for testing)
    vector = [float(i) / 1000.0 for i in range(10)]  # 10 dimensions for brevity
    query = (
      Query[User]()
      .select()
      .from_table('chunks')
      .vector_search(field='embedding', vector=vector, k=50, distance='COSINE')
    )

    sql = query.to_surql()
    assert 'embedding <|50,COSINE|>' in sql


class TestVectorSearchQueryHelper:
  """Test suite for vector_search_query() helper function."""

  def test_vector_search_query_basic(self) -> None:
    """Test basic vector_search_query() helper function."""
    query = vector_search_query(
      table='documents',
      field='embedding',
      vector=[0.1, 0.2, 0.3],
    )

    assert query.operation == 'SELECT'
    assert query.table_name == 'documents'
    assert query.vector_field == 'embedding'
    assert query.vector_value == [0.1, 0.2, 0.3]
    assert query.vector_k == 10  # default
    assert query.vector_distance == 'COSINE'  # default
    assert query.fields == ['*']

  def test_vector_search_query_with_custom_params(self) -> None:
    """Test vector_search_query() with custom parameters."""
    query = vector_search_query(
      table='images',
      field='features',
      vector=[1.0, 2.0, 3.0],
      k=20,
      distance='EUCLIDEAN',
      fields=['id', 'url', 'description'],
    )

    assert query.table_name == 'images'
    assert query.vector_field == 'features'
    assert query.vector_k == 20
    assert query.vector_distance == 'EUCLIDEAN'
    assert query.fields == ['id', 'url', 'description']

  def test_vector_search_query_to_surql(self) -> None:
    """Test vector_search_query() generates correct SurrealQL."""
    query = vector_search_query(
      table='documents',
      field='embedding',
      vector=[0.1, 0.2, 0.3],
      k=10,
      distance='COSINE',
    )

    sql = query.to_surql()
    assert sql == 'SELECT * FROM documents WHERE embedding <|10,COSINE|> [0.1, 0.2, 0.3]'

  def test_vector_search_query_chainable(self) -> None:
    """Test vector_search_query() result is chainable."""
    query = (
      vector_search_query(
        table='documents',
        field='embedding',
        vector=[0.1, 0.2],
        k=10,
      )
      .where('published = true')
      .order_by('score', 'DESC')
      .limit(5)
    )

    sql = query.to_surql()
    assert 'embedding <|10,COSINE|> [0.1, 0.2]' in sql
    assert '(published = true)' in sql
    assert 'ORDER BY score DESC' in sql
    assert 'LIMIT 5' in sql


class TestVectorSearchThreshold:
  """Test suite for vector search threshold parameter."""

  def test_vector_search_with_threshold(self) -> None:
    """Test vector_search stores threshold on the model."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2, 0.3], threshold=0.7)
    )

    assert query.vector_threshold == 0.7
    assert query.vector_field == 'embedding'
    assert query.vector_k == 10

  def test_vector_search_threshold_none_by_default(self) -> None:
    """Test threshold is None by default for backward compatibility."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2])
    )

    assert query.vector_threshold is None

  def test_to_surql_vector_with_threshold(self) -> None:
    """Test SurrealQL generation includes threshold in MTREE operator."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(
        field='embedding', vector=[0.1, 0.2, 0.3], k=10, distance='COSINE', threshold=0.7
      )
    )

    sql = query.to_surql()
    assert 'embedding <|10,COSINE,0.7|> [0.1, 0.2, 0.3]' in sql

  def test_to_surql_vector_without_threshold(self) -> None:
    """Test SurrealQL generation without threshold is unchanged."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .vector_search(field='embedding', vector=[0.1, 0.2, 0.3], k=10, distance='COSINE')
    )

    sql = query.to_surql()
    assert 'embedding <|10,COSINE|> [0.1, 0.2, 0.3]' in sql

  def test_vector_search_query_helper_with_threshold(self) -> None:
    """Test vector_search_query() convenience function accepts threshold."""
    query = vector_search_query(
      table='documents',
      field='embedding',
      vector=[0.1, 0.2, 0.3],
      k=10,
      distance='COSINE',
      threshold=0.8,
    )

    sql = query.to_surql()
    assert 'embedding <|10,COSINE,0.8|> [0.1, 0.2, 0.3]' in sql


class TestSimilarityScore:
  """Test suite for similarity_score() method."""

  def test_similarity_score_cosine(self) -> None:
    """Test similarity_score adds cosine scoring to SELECT fields."""
    query = (
      Query[User]().select().from_table('documents').similarity_score('embedding', [0.1, 0.2, 0.3])
    )

    assert any('vector::similarity::cosine' in f for f in query.fields)
    assert any('AS similarity' in f for f in query.fields)

  def test_similarity_score_euclidean(self) -> None:
    """Test similarity_score with EUCLIDEAN metric."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .similarity_score('embedding', [0.1, 0.2], 'EUCLIDEAN')
    )

    assert any('vector::similarity::euclidean' in f for f in query.fields)

  def test_similarity_score_custom_alias(self) -> None:
    """Test similarity_score with a custom alias."""
    query = (
      Query[User]()
      .select()
      .from_table('documents')
      .similarity_score('embedding', [0.1, 0.2], alias='score')
    )

    assert any('AS score' in f for f in query.fields)

  def test_similarity_score_in_surql(self) -> None:
    """Test similarity_score appears in generated SurrealQL SELECT."""
    query = (
      Query[User]()
      .select(['id', 'text'])
      .from_table('chunk')
      .similarity_score('embedding', [0.1, 0.2, 0.3], 'COSINE', 'similarity')
      .vector_search('embedding', [0.1, 0.2, 0.3], k=10, distance='COSINE', threshold=0.7)
    )

    sql = query.to_surql()
    assert 'vector::similarity::cosine(embedding, [0.1, 0.2, 0.3]) AS similarity' in sql
    assert 'embedding <|10,COSINE,0.7|> [0.1, 0.2, 0.3]' in sql

  def test_similarity_score_chaining(self) -> None:
    """Test similarity_score combined with vector_search, where, order_by, limit."""
    query = (
      Query[User]()
      .select(['id', 'text'])
      .from_table('chunk')
      .similarity_score('embedding', [0.1, 0.2], 'COSINE', 'similarity')
      .vector_search('embedding', [0.1, 0.2], k=20, distance='COSINE', threshold=0.5)
      .order_by('similarity', 'DESC')
      .limit(10)
    )

    sql = query.to_surql()
    assert 'vector::similarity::cosine' in sql
    assert 'embedding <|20,COSINE,0.5|>' in sql
    assert 'ORDER BY similarity DESC' in sql
    assert 'LIMIT 10' in sql


class TestSimilaritySearchQuery:
  """Test suite for similarity_search_query() convenience function."""

  def test_similarity_search_query_basic(self) -> None:
    """Test similarity_search_query creates proper query."""
    query = similarity_search_query(
      table='chunk',
      field='embedding',
      vector=[0.1, 0.2, 0.3],
      k=10,
      distance='COSINE',
    )

    sql = query.to_surql()
    assert 'vector::similarity::cosine' in sql
    assert 'AS similarity' in sql
    assert 'embedding <|10,COSINE|>' in sql

  def test_similarity_search_query_with_threshold(self) -> None:
    """Test similarity_search_query with threshold parameter."""
    query = similarity_search_query(
      table='chunk',
      field='embedding',
      vector=[0.1, 0.2, 0.3],
      k=10,
      distance='COSINE',
      threshold=0.7,
    )

    sql = query.to_surql()
    assert 'embedding <|10,COSINE,0.7|>' in sql

  def test_similarity_search_query_custom_fields_and_alias(self) -> None:
    """Test similarity_search_query with custom fields and alias."""
    query = similarity_search_query(
      table='chunk',
      field='embedding',
      vector=[0.1, 0.2],
      fields=['id', 'text'],
      alias='score',
    )

    sql = query.to_surql()
    assert 'id' in sql
    assert 'text' in sql
    assert 'AS score' in sql

  def test_similarity_search_driftnet_pattern(self) -> None:
    """Test the exact pattern driftnet uses for vector search.

    Validates that reverie can now produce the equivalent of driftnet's
    manually constructed SurrealQL:
      SELECT id, text, vector::similarity::cosine(embedding, $embedding) AS similarity
      FROM chunk
      WHERE embedding <|1024,COSINE,{threshold}|> $embedding
    """
    vector = [0.1] * 10  # simplified embedding
    query = (
      Query()
      .select(['id', 'text'])
      .from_table('chunk')
      .similarity_score('embedding', vector, 'COSINE', 'similarity')
      .vector_search('embedding', vector, k=1024, distance='COSINE', threshold=0.7)
      .order_by('similarity', 'DESC')
      .limit(20)
    )

    sql = query.to_surql()
    assert 'id' in sql
    assert 'text' in sql
    assert 'vector::similarity::cosine(embedding,' in sql
    assert 'AS similarity' in sql
    assert 'embedding <|1024,COSINE,0.7|>' in sql
    assert 'ORDER BY similarity DESC' in sql
    assert 'LIMIT 20' in sql
