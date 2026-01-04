"""Shared pytest fixtures for reverie ORM tests.

This module provides common fixtures used across all test modules including
mock database clients, sample schemas, and test utilities.
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
import structlog
from pydantic import BaseModel

# Configure structlog BEFORE importing reverie modules to avoid warnings
# This must run before any structlog loggers are created
structlog.configure(
  processors=[
    structlog.contextvars.merge_contextvars,
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt='iso'),
    structlog.dev.ConsoleRenderer(),
  ],
  wrapper_class=structlog.make_filtering_bound_logger(20),
  context_class=dict,
  logger_factory=structlog.PrintLoggerFactory(),
  cache_logger_on_first_use=False,
)

# Import reverie modules AFTER structlog is configured
from reverie.connection.client import DatabaseClient  # noqa: E402
from reverie.connection.config import ConnectionConfig  # noqa: E402
from reverie.schema.fields import FieldDefinition, FieldType  # noqa: E402
from reverie.schema.table import (  # noqa: E402
  IndexDefinition,
  IndexType,
  TableDefinition,
  TableMode,
)
from reverie.types.record_id import RecordID  # noqa: E402

# Test data models


class User(BaseModel):
  """Test user model."""

  name: str
  email: str
  age: int | None = None


class Post(BaseModel):
  """Test post model."""

  title: str
  content: str
  author_id: str


class Product(BaseModel):
  """Test product model."""

  name: str
  price: float
  stock: int


# Database configuration fixtures


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch):
  """Clear all REVERIE_ environment variables for test isolation."""
  import os

  for key in list(os.environ.keys()):
    if key.startswith('REVERIE_'):
      monkeypatch.delenv(key, raising=False)
  yield


@pytest.fixture
def db_config(clean_env) -> ConnectionConfig:  # noqa: ARG001
  """Provide test database configuration."""
  return ConnectionConfig(
    _env_file=None,
    url='ws://localhost:8000/rpc',
    namespace='test',
    database='test_db',
    username='test_user',
    password='test_pass',
    timeout=10.0,
    max_connections=5,
    retry_max_attempts=2,
  )


@pytest.fixture
def minimal_db_config() -> ConnectionConfig:
  """Provide minimal database configuration with defaults."""
  return ConnectionConfig()


# Mock database client fixtures


@pytest.fixture
def mock_surreal_client() -> Mock:
  """Provide mock SurrealDB client."""
  client = Mock()
  client.connect = AsyncMock()
  client.signin = AsyncMock()
  client.use = AsyncMock()
  client.close = AsyncMock()
  client.query = AsyncMock(return_value=[{'result': []}])
  client.select = AsyncMock(return_value=[])
  client.create = AsyncMock(return_value={'id': 'user:123'})
  client.update = AsyncMock(return_value={'id': 'user:123'})
  client.merge = AsyncMock(return_value={'id': 'user:123'})
  client.delete = AsyncMock(return_value=None)
  client.insert_relation = AsyncMock(return_value={'id': 'likes:123'})
  return client


@pytest.fixture
def mock_db_client(db_config: ConnectionConfig, mock_surreal_client: Mock) -> DatabaseClient:
  """Provide mock DatabaseClient instance."""
  client = DatabaseClient(db_config)
  client._client = mock_surreal_client
  client._connected = True
  return client


@pytest.fixture
async def connected_mock_client(mock_db_client: DatabaseClient) -> DatabaseClient:
  """Provide connected mock database client."""
  return mock_db_client


# Schema fixtures


@pytest.fixture
def sample_field() -> FieldDefinition:
  """Provide sample field definition."""
  return FieldDefinition(
    name='email',
    type=FieldType.STRING,
    assertion='string::is::email($value)',
  )


@pytest.fixture
def sample_fields() -> list[FieldDefinition]:
  """Provide sample field definitions for a user table."""
  return [
    FieldDefinition(name='name', type=FieldType.STRING),
    FieldDefinition(name='email', type=FieldType.STRING, assertion='string::is::email($value)'),
    FieldDefinition(name='age', type=FieldType.INT, assertion='$value >= 0 AND $value <= 150'),
    FieldDefinition(
      name='created_at', type=FieldType.DATETIME, default='time::now()', readonly=True
    ),
  ]


@pytest.fixture
def sample_index() -> IndexDefinition:
  """Provide sample index definition."""
  return IndexDefinition(
    name='email_idx',
    columns=['email'],
    type=IndexType.UNIQUE,
  )


@pytest.fixture
def sample_table(
  sample_fields: list[FieldDefinition], sample_index: IndexDefinition
) -> TableDefinition:
  """Provide sample table definition."""
  return TableDefinition(
    name='user',
    mode=TableMode.SCHEMAFULL,
    fields=sample_fields,
    indexes=[sample_index],
  )


# RecordID fixtures


@pytest.fixture
def sample_record_id() -> RecordID[User]:
  """Provide sample RecordID."""
  return RecordID(table='user', id='alice')


@pytest.fixture
def sample_record_ids() -> list[RecordID[Any]]:
  """Provide list of sample RecordIDs."""
  return [
    RecordID(table='user', id='alice'),
    RecordID(table='user', id='bob'),
    RecordID(table='post', id=123),
  ]


# Temporary directory fixtures


@pytest.fixture
def temp_migration_dir(tmp_path):
  """Provide temporary directory for migration files."""
  migrations_dir = tmp_path / 'migrations'
  migrations_dir.mkdir()
  return migrations_dir


@pytest.fixture
def temp_schema_dir(tmp_path):
  """Provide temporary directory for schema files."""
  schema_dir = tmp_path / 'schemas'
  schema_dir.mkdir()
  return schema_dir


# Test utilities


@pytest.fixture
def mock_query_result():
  """Provide mock query result."""
  return [
    {
      'result': [
        {'id': 'user:alice', 'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
        {'id': 'user:bob', 'name': 'Bob', 'email': 'bob@example.com', 'age': 25},
      ]
    }
  ]


@pytest.fixture
def mock_single_result():
  """Provide mock single record result."""
  return [
    {'result': [{'id': 'user:alice', 'name': 'Alice', 'email': 'alice@example.com', 'age': 30}]}
  ]


@pytest.fixture
def mock_count_result():
  """Provide mock count query result."""
  return [{'result': [{'count': 42}]}]


# Async test helpers


@pytest.fixture(scope='session')
def event_loop():
  """Create event loop for async tests."""
  loop = asyncio.get_event_loop_policy().new_event_loop()
  yield loop
  loop.close()


# Mock CLI runner


@pytest.fixture
def mock_cli_runner():
  """Provide mock CLI runner for testing commands."""
  from typer.testing import CliRunner

  return CliRunner()


# Helper functions for tests


def assert_immutable(obj: Any, attr: str, new_value: Any) -> None:
  """Assert that object attribute is immutable.

  Args:
    obj: Object to test
    attr: Attribute name
    new_value: Value to attempt to set

  Raises:
    AssertionError: If object is mutable
  """
  try:
    setattr(obj, attr, new_value)
    raise AssertionError(f'Expected {type(obj).__name__}.{attr} to be immutable')
  except (AttributeError, TypeError):
    # Expected - object is immutable
    pass


def create_mock_migration_file(directory, name: str, content: str):
  """Create a mock migration file.

  Args:
    directory: Directory path
    name: Migration filename
    content: File content

  Returns:
    Path to created file
  """
  migration_file = directory / name
  migration_file.write_text(content)
  return migration_file
