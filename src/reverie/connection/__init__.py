"""Database connection layer for reverie ORM.

This module provides:
- Connection configuration management
- Async database client with connection pooling
- Transaction support with ACID guarantees
- Connection context management for dependency injection
"""

from reverie.connection.client import (
  ConnectionError,
  DatabaseClient,
  DatabaseError,
  QueryError,
  get_client,
)
from reverie.connection.config import ConnectionConfig
from reverie.connection.context import (
  ContextError,
  clear_db,
  connection_override,
  connection_scope,
  get_db,
  has_db,
  set_db,
)
from reverie.connection.transaction import (
  Transaction,
  TransactionError,
  TransactionState,
  transaction,
)

__all__ = [
  # Configuration
  'ConnectionConfig',
  # Client
  'DatabaseClient',
  'get_client',
  # Exceptions
  'DatabaseError',
  'ConnectionError',
  'QueryError',
  'TransactionError',
  'ContextError',
  # Transaction
  'Transaction',
  'TransactionState',
  'transaction',
  # Context
  'get_db',
  'set_db',
  'clear_db',
  'has_db',
  'connection_scope',
  'connection_override',
]
