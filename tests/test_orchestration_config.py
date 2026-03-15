"""Tests for orchestration configuration."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from surql.connection.config import ConnectionConfig
from surql.orchestration.config import (
  EnvironmentConfig,
  EnvironmentRegistry,
  configure_environments,
  get_registry,
  register_environment,
  set_registry,
)


class TestEnvironmentConfig:
  """Tests for EnvironmentConfig."""

  def test_create_environment_config(self) -> None:
    """Test creating environment configuration."""
    connection = ConnectionConfig(
      db_url='ws://localhost:8000/rpc',
      db_ns='test',
      db='main',
    )
    config = EnvironmentConfig(
      name='development',
      connection=connection,
      priority=100,
      tags={'dev', 'local'},
    )

    assert config.name == 'development'
    assert config.connection == connection
    assert config.priority == 100
    assert 'dev' in config.tags
    assert config.require_approval is False
    assert config.allow_destructive is True

  def test_environment_config_immutable(self) -> None:
    """Test that environment config is immutable."""
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    config = EnvironmentConfig(name='test', connection=connection)

    with pytest.raises(ValidationError):
      config.name = 'new_name'  # type: ignore

  def test_environment_config_validation(self) -> None:
    """Test environment name validation."""
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')

    # Test empty name
    with pytest.raises(ValidationError):
      EnvironmentConfig(name='', connection=connection)

    # Test invalid characters
    with pytest.raises(ValidationError):
      EnvironmentConfig(name='prod@123', connection=connection)

  def test_environment_config_with_defaults(self) -> None:
    """Test environment config with default values."""
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    config = EnvironmentConfig(name='production', connection=connection)

    assert config.priority == 100
    assert config.tags == set()
    assert config.require_approval is False
    assert config.allow_destructive is True


class TestEnvironmentRegistry:
  """Tests for EnvironmentRegistry."""

  def test_create_registry(self) -> None:
    """Test creating environment registry."""
    registry = EnvironmentRegistry()
    assert registry.list_environments() == []

  def test_register_environment(self) -> None:
    """Test registering an environment."""
    registry = EnvironmentRegistry()
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')

    registry.register_environment(
      name='development',
      connection=connection,
      priority=100,
      tags={'dev'},
    )

    assert 'development' in registry.list_environments()
    env = registry.get_environment('development')
    assert env is not None
    assert env.name == 'development'

  def test_unregister_environment(self) -> None:
    """Test unregistering an environment."""
    registry = EnvironmentRegistry()
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')

    registry.register_environment('test', connection)
    assert 'test' in registry.list_environments()

    registry.unregister_environment('test')
    assert 'test' not in registry.list_environments()

  def test_get_environment_not_found(self) -> None:
    """Test getting non-existent environment."""
    registry = EnvironmentRegistry()
    env = registry.get_environment('nonexistent')
    assert env is None

  def test_list_environments_sorted_by_priority(self) -> None:
    """Test that environments are listed by priority."""
    registry = EnvironmentRegistry()
    conn1 = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test1', db='main')
    conn2 = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test2', db='main')
    conn3 = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test3', db='main')

    registry.register_environment('low_priority', conn1, priority=200)
    registry.register_environment('high_priority', conn2, priority=50)
    registry.register_environment('medium_priority', conn3, priority=100)

    envs = registry.list_environments()
    assert envs == ['high_priority', 'medium_priority', 'low_priority']

  def test_get_environments_by_tag(self) -> None:
    """Test getting environments by tag."""
    registry = EnvironmentRegistry()
    conn1 = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test1', db='main')
    conn2 = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test2', db='main')

    registry.register_environment('dev', conn1, tags={'development', 'local'})
    registry.register_environment('prod', conn2, tags={'production', 'critical'})

    dev_envs = registry.get_environments_by_tag('development')
    assert len(dev_envs) == 1
    assert dev_envs[0].name == 'dev'

    prod_envs = registry.get_environments_by_tag('production')
    assert len(prod_envs) == 1
    assert prod_envs[0].name == 'prod'

  def test_from_config_file(self, tmp_path: Path) -> None:
    """Test loading registry from config file."""
    config_file = tmp_path / 'environments.json'
    config_data = {
      'environments': [
        {
          'name': 'development',
          'connection': {
            'db_url': 'ws://localhost:8000/rpc',
            'db_ns': 'development',
            'db': 'main',
          },
          'priority': 100,
          'tags': ['dev', 'local'],
        },
        {
          'name': 'production',
          'connection': {
            'db_url': 'ws://prod.example.com:8000/rpc',
            'db_ns': 'production',
            'db': 'main',
          },
          'priority': 1,
          'tags': ['prod', 'critical'],
          'require_approval': True,
          'allow_destructive': False,
        },
      ]
    }

    config_file.write_text(json.dumps(config_data))

    registry = EnvironmentRegistry.from_config_file(config_file)
    assert len(registry.list_environments()) == 2

    dev_env = registry.get_environment('development')
    assert dev_env is not None
    assert dev_env.priority == 100
    assert 'dev' in dev_env.tags

    prod_env = registry.get_environment('production')
    assert prod_env is not None
    assert prod_env.priority == 1
    assert prod_env.require_approval is True
    assert prod_env.allow_destructive is False

  def test_from_config_file_not_found(self, tmp_path: Path) -> None:
    """Test loading from non-existent config file."""
    config_file = tmp_path / 'nonexistent.json'
    registry = EnvironmentRegistry.from_config_file(config_file)
    assert len(registry.list_environments()) == 0


class TestGlobalRegistry:
  """Tests for global registry functions."""

  def test_get_registry_creates_singleton(self) -> None:
    """Test that get_registry returns singleton."""
    # Reset global registry
    set_registry(EnvironmentRegistry())

    registry1 = get_registry()
    registry2 = get_registry()
    assert registry1 is registry2

  def test_set_registry(self) -> None:
    """Test setting global registry."""
    new_registry = EnvironmentRegistry()
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    new_registry.register_environment('test', connection)

    set_registry(new_registry)

    registry = get_registry()
    assert 'test' in registry.list_environments()

  def test_register_environment_global(self) -> None:
    """Test registering environment in global registry."""
    # Reset global registry
    set_registry(EnvironmentRegistry())

    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')
    register_environment('global_test', connection)

    registry = get_registry()
    assert 'global_test' in registry.list_environments()

  def test_configure_environments(self, tmp_path: Path) -> None:
    """Test configuring environments from file."""
    config_file = tmp_path / 'test_environments.json'
    config_data = {
      'environments': [
        {
          'name': 'staging',
          'connection': {
            'db_url': 'ws://staging.example.com:8000/rpc',
            'db_ns': 'staging',
            'db': 'main',
          },
          'priority': 50,
        }
      ]
    }

    config_file.write_text(json.dumps(config_data))

    configure_environments(config_file)

    registry = get_registry()
    assert 'staging' in registry.list_environments()


class TestEnvironmentConfigEdgeCases:
  """Tests for edge cases in environment configuration."""

  def test_valid_environment_names(self) -> None:
    """Test various valid environment name formats."""
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')

    # Test valid names
    valid_names = ['production', 'prod-1', 'dev_local', 'test123']

    for name in valid_names:
      config = EnvironmentConfig(name=name, connection=connection)
      assert config.name == name

  def test_priority_boundaries(self) -> None:
    """Test priority value boundaries."""
    connection = ConnectionConfig(db_url='ws://localhost:8000/rpc', db_ns='test', db='main')

    # Test zero priority
    config = EnvironmentConfig(name='test', connection=connection, priority=0)
    assert config.priority == 0

    # Test high priority
    config = EnvironmentConfig(name='test2', connection=connection, priority=1000)
    assert config.priority == 1000
