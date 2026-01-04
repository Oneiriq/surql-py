"""Database connection layer for reverie ORM.

This module provides:
- Connection configuration management
- Async database client with connection pooling
- Transaction support with ACID guarantees
- Connection context management for dependency injection
- Multi-connection registry
- Authentication management
- Real-time streaming and live queries
"""

from reverie.connection.auth import (
  AuthManager,
  AuthType,
  DatabaseCredentials,
  NamespaceCredentials,
  RootCredentials,
  ScopeCredentials,
  TokenAuth,
)
from reverie.connection.client import (
  ConnectionError,
  DatabaseClient,
  DatabaseError,
  QueryError,
  get_client,
)
from reverie.connection.config import ConnectionConfig, NamedConnectionConfig
from reverie.connection.context import (
  ContextError,
  clear_db,
  connection_override,
  connection_scope,
  get_db,
  has_db,
  set_db,
)
from reverie.connection.registry import ConnectionRegistry, RegistryError, get_registry
from reverie.connection.streaming import LiveQuery, StreamingError, StreamingManager
from reverie.connection.transaction import (
  Transaction,
  TransactionError,
  TransactionState,
  transaction,
)

__all__ = [
  # Configuration
  'ConnectionConfig',
  'NamedConnectionConfig',
  # Client
  'DatabaseClient',
  'get_client',
  # Exceptions
  'DatabaseError',
  'ConnectionError',
  'QueryError',
  'TransactionError',
  'ContextError',
  'RegistryError',
  'StreamingError',
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
  # Registry
  'ConnectionRegistry',
  'get_registry',
  # Auth
  'AuthManager',
  'AuthType',
  'RootCredentials',
  'NamespaceCredentials',
  'DatabaseCredentials',
  'ScopeCredentials',
  'TokenAuth',
  # Streaming
  'StreamingManager',
  'LiveQuery',
]
