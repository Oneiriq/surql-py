"""Tests for the settings module."""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from src.settings import Settings, get_settings


class TestSettings:
    """Test suite for Settings class."""

    def test_settings_with_defaults(self) -> None:
        """Test settings loading with default values."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()

            assert settings.environment == 'development'
            assert settings.debug is True
            assert settings.log_level == 'INFO'
            assert settings.app_name == 'python-uv-simple'
            assert settings.version == '0.1.0'

    def test_settings_with_custom_values(self) -> None:
        """Test settings loading with custom environment variables."""
        env_vars = {
            'ENVIRONMENT': 'production',
            'DEBUG': 'false',
            'LOG_LEVEL': 'WARNING',
            'APP_NAME': 'custom-app',
            'VERSION': '1.0.0',
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
        with patch.dict(os.environ, {'DEBUG': debug_value}, clear=True):
            settings = Settings()
            assert settings.debug is expected

    def test_get_settings_returns_cached_instance(self) -> None:
        """Test that get_settings returns a cached instance."""
        # Clear the cache first
        get_settings.cache_clear()

        with patch.dict(os.environ, {'APP_NAME': 'test-app'}, clear=True):
            settings1 = get_settings()
            settings2 = get_settings()

            # Should be the same instance due to lru_cache
            assert settings1 is settings2
            assert settings1.app_name == 'test-app'

    def test_invalid_environment_raises_validation_error(self) -> None:
        """Test that invalid environment value raises ValidationError."""
        with patch.dict(os.environ, {'ENVIRONMENT': 'invalid'}, clear=True):
            with pytest.raises(ValidationError) as exc_info:
                Settings()
            assert 'environment' in str(exc_info.value)

    def test_invalid_log_level_raises_validation_error(self) -> None:
        """Test that invalid log level value raises ValidationError."""
        with patch.dict(os.environ, {'LOG_LEVEL': 'INVALID'}, clear=True):
            with pytest.raises(ValidationError) as exc_info:
                Settings()
            assert 'log_level' in str(exc_info.value)
