"""Shared fixtures for SurrealDB v3 integration tests.

These fixtures only activate when a live SurrealDB instance is reachable
at the URL in ``SURREAL_URL`` (defaults to ``ws://localhost:8000/rpc``).
Locally, run:

    docker run -d --name surrealdb -p 8000:8000 \
      surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
    uv run pytest tests/integration/ -v

CI boots the same image in the ``Integration`` workflow.

All tests are skipped when the server is not reachable so running
``pytest`` without Docker stays green.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import uuid
from collections.abc import AsyncIterator
from urllib.parse import urlparse

import pytest

from surql.connection.client import DatabaseClient
from surql.connection.config import ConnectionConfig


def _server_reachable(url: str) -> bool:
  """TCP-probe the configured SurrealDB server."""
  parsed = urlparse(url)
  host = parsed.hostname or 'localhost'
  port = parsed.port or 8000
  try:
    with socket.create_connection((host, port), timeout=1):
      return True
  except OSError:
    return False


SURREAL_URL = os.environ.get('SURREAL_URL', 'ws://localhost:8000/rpc')
SURREAL_USER = os.environ.get('SURREAL_USER', 'root')
SURREAL_PASS = os.environ.get('SURREAL_PASS', 'root')


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
  """Skip every integration test when the server is unreachable."""
  del config
  if _server_reachable(SURREAL_URL):
    return
  skip_marker = pytest.mark.skip(
    reason=f'SurrealDB not reachable at {SURREAL_URL}; start Docker image to run.'
  )
  for item in items:
    item.add_marker(skip_marker)


@pytest.fixture
def anyio_backend() -> str:
  """Use the asyncio backend for integration tests (trio not needed)."""
  return 'asyncio'


@pytest.fixture
async def integration_client() -> AsyncIterator[DatabaseClient]:
  """Connect to the live server against a fresh namespace/database.

  Each test gets an isolated ns/db pair keyed by a UUID so tests can
  CREATE/DROP tables without stepping on each other.
  """
  ns = f'test_ns_{uuid.uuid4().hex[:12]}'
  db = f'test_db_{uuid.uuid4().hex[:12]}'
  config = ConnectionConfig(
    _env_file=None,
    url=SURREAL_URL,
    namespace=ns,
    database=db,
    username=SURREAL_USER,
    password=SURREAL_PASS,
    enable_live_queries=False,
  )
  client = DatabaseClient(config)
  await client.connect()
  try:
    yield client
  finally:
    # Best-effort cleanup; ignore disconnect errors.
    with contextlib.suppress(Exception):
      await asyncio.wait_for(client.disconnect(), timeout=5.0)
