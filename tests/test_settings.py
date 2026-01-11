"""Tests for the settings module."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from reverie.settings import (
  PyProjectTomlSettingsSource,
  Settings,
  get_migration_path,
  get_settings,
)


class TestSettings:
  """Test suite for Settings class."""

  def test_settings_with_defaults(self) -> None:
    """Test settings loading with default values."""
    with patch.dict(os.environ, {}, clear=True):
      settings = Settings()

      assert settings.environment == 'development'
      assert settings.debug is True
      assert settings.log_level == 'INFO'
      assert settings.app_name == 'reverie'
      assert settings.version == '0.6.0'

  def test_settings_with_custom_values(self) -> None:
    """Test settings loading with custom environment variables."""
    env_vars = {
      'REVERIE_ENVIRONMENT': 'production',
      'REVERIE_DEBUG': 'false',
      'REVERIE_LOG_LEVEL': 'WARNING',
      'REVERIE_APP_NAME': 'custom-app',
      'REVERIE_VERSION': '1.0.0',
    }

    with patch.dict(os.environ, env_vars, clear=True):
      settings = Settings()

      assert settings.environment == 'production'
      assert settings.debug is False
      assert settings.log_level == 'WARNING'
      assert settings.app_name == 'custom-app'
      assert settings.version == '1.0.0'

  @pytest.mark.parametrize(
    'debug_value,expected',
    [
      ('true', True),
      ('True', True),
      ('1', True),
      ('yes', True),
      ('false', False),
      ('False', False),
      ('0', False),
      ('no', False),
    ],
  )
  def test_debug_parsing(self, debug_value: str, expected: bool) -> None:
    """Test various debug value formats are parsed correctly."""
    with patch.dict(os.environ, {'REVERIE_DEBUG': debug_value}, clear=True):
      settings = Settings()
      assert settings.debug is expected

  def test_get_settings_returns_cached_instance(self) -> None:
    """Test that get_settings returns a cached instance."""
    # Clear the cache first
    get_settings.cache_clear()

    with patch.dict(os.environ, {'REVERIE_APP_NAME': 'test-app'}, clear=True):
      settings1 = get_settings()
      settings2 = get_settings()

      # Should be the same instance due to lru_cache
      assert settings1 is settings2
      assert settings1.app_name == 'test-app'

  def test_invalid_environment_raises_validation_error(self) -> None:
    """Test that invalid environment value raises ValidationError."""
    # Settings uses REVERIE_ prefix for environment variables
    with patch.dict(os.environ, {'REVERIE_ENVIRONMENT': 'invalid'}, clear=True):
      with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)
      assert 'environment' in str(exc_info.value).lower()

  def test_invalid_log_level_raises_validation_error(self) -> None:
    """Test that invalid log level value raises ValidationError."""
    # Settings uses REVERIE_ prefix for environment variables
    with patch.dict(os.environ, {'REVERIE_LOG_LEVEL': 'INVALID'}, clear=True):
      with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)
      assert 'log_level' in str(exc_info.value).lower()


class TestMigrationPath:
  """Test suite for migration_path configuration."""

  def test_migration_path_default(self) -> None:
    """Test default migration_path value."""
    with patch.dict(os.environ, {}, clear=True):
      settings = Settings(_env_file=None)
      assert settings.migration_path == Path('migrations')

  def test_migration_path_from_env_var(self) -> None:
    """Test migration_path loaded from environment variable."""
    with patch.dict(os.environ, {'REVERIE_MIGRATION_PATH': 'db/migrations'}, clear=True):
      settings = Settings(_env_file=None)
      assert settings.migration_path == Path('db/migrations')

  def test_migration_path_absolute_from_env_var(self) -> None:
    """Test migration_path with absolute path from environment variable."""
    with patch.dict(os.environ, {'REVERIE_MIGRATION_PATH': '/custom/path'}, clear=True):
      settings = Settings(_env_file=None)
      assert settings.migration_path == Path('/custom/path')

  def test_get_migration_path_function(self) -> None:
    """Test get_migration_path helper function."""
    get_settings.cache_clear()
    with patch.dict(os.environ, {'REVERIE_MIGRATION_PATH': 'custom/migrations'}, clear=True):
      migration_path = get_migration_path()
      assert migration_path == Path('custom/migrations')
    get_settings.cache_clear()


class TestPyProjectTomlSettingsSource:
  """Test suite for PyProjectTomlSettingsSource."""

  def test_load_toml_nonexistent_file(
    self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """Test loading from nonexistent pyproject.toml."""
    monkeypatch.chdir(tmp_path)

    source = PyProjectTomlSettingsSource(Settings)
    assert source._toml_data == {}

  def test_load_toml_empty_tool_section(
    self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """Test loading from pyproject.toml without [tool.reverie] section."""
    monkeypatch.chdir(tmp_path)
    pyproject = tmp_path / 'pyproject.toml'
    pyproject.write_text('[project]\nname = "test"\n')

    source = PyProjectTomlSettingsSource(Settings)
    assert source._toml_data == {}

  def test_load_toml_with_reverie_section(
    self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """Test loading from pyproject.toml with [tool.reverie] section."""
    monkeypatch.chdir(tmp_path)
    pyproject = tmp_path / 'pyproject.toml'
    pyproject.write_text(
      '[project]\nname = "test"\n\n'
      '[tool.reverie]\n'
      'migration_path = "db/migrations"\n'
      'environment = "production"\n'
    )

    source = PyProjectTomlSettingsSource(Settings)
    assert source._toml_data == {
      'migration_path': 'db/migrations',
      'environment': 'production',
    }

  def test_get_field_value(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test get_field_value method."""
    monkeypatch.chdir(tmp_path)
    pyproject = tmp_path / 'pyproject.toml'
    pyproject.write_text('[tool.reverie]\nmigration_path = "custom/path"\n')

    source = PyProjectTomlSettingsSource(Settings)
    value, name, is_complex = source.get_field_value(None, 'migration_path')

    assert value == 'custom/path'
    assert name == 'migration_path'
    assert is_complex is False

  def test_get_field_value_missing_field(
    self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """Test get_field_value for missing field returns None."""
    monkeypatch.chdir(tmp_path)
    pyproject = tmp_path / 'pyproject.toml'
    pyproject.write_text('[tool.reverie]\nmigration_path = "path"\n')

    source = PyProjectTomlSettingsSource(Settings)
    value, name, is_complex = source.get_field_value(None, 'nonexistent')

    assert value is None
    assert name == 'nonexistent'
    assert is_complex is False

  def test_settings_priority_env_over_toml(
    self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """Test that environment variables take priority over pyproject.toml."""
    monkeypatch.chdir(tmp_path)
    pyproject = tmp_path / 'pyproject.toml'
    pyproject.write_text('[tool.reverie]\nmigration_path = "toml/path"\n')

    with patch.dict(os.environ, {'REVERIE_MIGRATION_PATH': 'env/path'}, clear=True):
      settings = Settings(_env_file=None)
      assert settings.migration_path == Path('env/path')

  def test_settings_loads_from_toml_when_no_env(
    self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """Test that settings loads from pyproject.toml when no env var is set."""
    monkeypatch.chdir(tmp_path)
    pyproject = tmp_path / 'pyproject.toml'
    pyproject.write_text('[tool.reverie]\nmigration_path = "toml/migrations"\n')

    with patch.dict(os.environ, {}, clear=True):
      settings = Settings(_env_file=None)
      assert settings.migration_path == Path('toml/migrations')

  def test_invalid_toml_handled_gracefully(
    self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """Test that invalid TOML file is handled gracefully."""
    monkeypatch.chdir(tmp_path)
    pyproject = tmp_path / 'pyproject.toml'
    pyproject.write_text('invalid toml content [[[')

    # Should not raise, should just use empty dict
    source = PyProjectTomlSettingsSource(Settings)
    assert source._toml_data == {}
