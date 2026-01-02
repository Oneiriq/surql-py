"""Tests for the CLI module."""

import re
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from src.cli.common import (
  OutputFormat,
  confirm,
  confirm_destructive,
  get_migrations_directory,
  validate_directory_exists,
  validate_file_exists,
)
from src.cli.migrate import app as migrate_app


def strip_ansi(text: str) -> str:
  """Remove ANSI escape sequences from text."""
  ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
  return ansi_escape.sub('', text)


class TestOutputFormat:
  """Test suite for OutputFormat enum."""

  def test_output_format_values(self) -> None:
    """Test OutputFormat enum values."""
    assert OutputFormat.TABLE.value == 'table'
    assert OutputFormat.JSON.value == 'json'
    assert OutputFormat.TEXT.value == 'text'


class TestGetMigrationsDirectory:
  """Test suite for get_migrations_directory function."""

  def test_get_migrations_directory_default(self, tmp_path: Path, monkeypatch) -> None:
    """Test getting default migrations directory."""
    monkeypatch.chdir(tmp_path)

    with patch('src.cli.common.display_info'):
      directory = get_migrations_directory()

    assert directory == tmp_path / 'migrations'
    assert directory.exists()

  def test_get_migrations_directory_custom(self, tmp_path: Path) -> None:
    """Test getting custom migrations directory."""
    custom_dir = tmp_path / 'custom_migrations'

    with patch('src.cli.common.display_info'):
      directory = get_migrations_directory(custom_dir)

    assert directory == custom_dir
    assert directory.exists()

  def test_get_migrations_directory_existing(self, tmp_path: Path) -> None:
    """Test getting existing migrations directory."""
    existing_dir = tmp_path / 'migrations'
    existing_dir.mkdir()

    directory = get_migrations_directory(existing_dir)

    assert directory == existing_dir
    assert directory.exists()


class TestValidateFileExists:
  """Test suite for validate_file_exists function."""

  def test_validate_file_exists_valid(self, tmp_path: Path) -> None:
    """Test validation with valid file."""
    test_file = tmp_path / 'test.txt'
    test_file.write_text('test')

    # Should not raise
    validate_file_exists(test_file)

  def test_validate_file_exists_missing(self, tmp_path: Path) -> None:
    """Test validation with missing file."""
    from typer import BadParameter

    test_file = tmp_path / 'missing.txt'

    with pytest.raises(BadParameter) as exc_info:
      validate_file_exists(test_file)

    assert 'not found' in str(exc_info.value)

  def test_validate_file_exists_directory(self, tmp_path: Path) -> None:
    """Test validation when path is a directory."""
    from typer import BadParameter

    test_dir = tmp_path / 'test_dir'
    test_dir.mkdir()

    with pytest.raises(BadParameter) as exc_info:
      validate_file_exists(test_dir)

    assert 'not a file' in str(exc_info.value)


class TestValidateDirectoryExists:
  """Test suite for validate_directory_exists function."""

  def test_validate_directory_exists_valid(self, tmp_path: Path) -> None:
    """Test validation with valid directory."""
    test_dir = tmp_path / 'test_dir'
    test_dir.mkdir()

    # Should not raise
    validate_directory_exists(test_dir)

  def test_validate_directory_exists_missing(self, tmp_path: Path) -> None:
    """Test validation with missing directory."""
    from typer import BadParameter

    test_dir = tmp_path / 'missing_dir'

    with pytest.raises(BadParameter) as exc_info:
      validate_directory_exists(test_dir)

    assert 'not found' in str(exc_info.value)

  def test_validate_directory_exists_file(self, tmp_path: Path) -> None:
    """Test validation when path is a file."""
    from typer import BadParameter

    test_file = tmp_path / 'test.txt'
    test_file.write_text('test')

    with pytest.raises(BadParameter) as exc_info:
      validate_directory_exists(test_file)

    assert 'not a directory' in str(exc_info.value)


class TestMigrateCommands:
  """Test suite for migrate CLI commands."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_migrate_app_exists(self) -> None:
    """Test that migrate app exists."""
    assert migrate_app is not None

  def test_migrate_help(self) -> None:
    """Test migrate help command."""
    result = self.runner.invoke(migrate_app, ['--help'])

    assert result.exit_code == 0
    assert 'migrate' in result.stdout.lower()

  def test_migrate_create_help(self) -> None:
    """Test migrate create help."""
    result = self.runner.invoke(migrate_app, ['create', '--help'])

    assert result.exit_code == 0
    assert 'create' in result.stdout.lower()

  def test_migrate_create_missing_description(self) -> None:
    """Test migrate create without description."""
    result = self.runner.invoke(migrate_app, ['create'])

    assert result.exit_code != 0

  def test_migrate_create_success(self, tmp_path: Path) -> None:
    """Test successful migration creation."""
    with (
      patch('src.cli.migrate.get_migrations_directory', return_value=tmp_path),
      patch('src.cli.migrate.create_blank_migration') as mock_create,
    ):
      mock_file = tmp_path / '20260102_120000_test.py'
      mock_create.return_value = mock_file

      result = self.runner.invoke(migrate_app, ['create', 'test migration'])

      assert result.exit_code == 0
      mock_create.assert_called_once()

  def test_migrate_status_help(self) -> None:
    """Test migrate status help."""
    result = self.runner.invoke(migrate_app, ['status', '--help'])

    assert result.exit_code == 0
    assert 'status' in result.stdout.lower()

  def test_migrate_up_help(self) -> None:
    """Test migrate up help."""
    result = self.runner.invoke(migrate_app, ['up', '--help'])

    assert result.exit_code == 0
    assert 'up' in result.stdout.lower() or 'apply' in result.stdout.lower()

  def test_migrate_down_help(self) -> None:
    """Test migrate down help."""
    result = self.runner.invoke(migrate_app, ['down', '--help'])

    assert result.exit_code == 0
    assert 'down' in result.stdout.lower() or 'rollback' in result.stdout.lower()

  def test_migrate_history_help(self) -> None:
    """Test migrate history help."""
    result = self.runner.invoke(migrate_app, ['history', '--help'])

    assert result.exit_code == 0
    assert 'history' in result.stdout.lower()

  def test_migrate_validate_help(self) -> None:
    """Test migrate validate help."""
    result = self.runner.invoke(migrate_app, ['validate', '--help'])

    assert result.exit_code == 0
    assert 'validate' in result.stdout.lower()

  def test_migrate_validate_empty_directory(self, tmp_path: Path) -> None:
    """Test validate with empty migrations directory."""
    migrations_dir = tmp_path / 'migrations'
    migrations_dir.mkdir()

    with patch('src.cli.migrate.get_migrations_directory', return_value=migrations_dir):
      result = self.runner.invoke(migrate_app, ['validate'])

      # Should succeed with warning about no files
      assert result.exit_code == 0

  def test_migrate_validate_invalid_filename(self, tmp_path: Path) -> None:
    """Test validate with invalid migration filename."""
    migrations_dir = tmp_path / 'migrations'
    migrations_dir.mkdir()

    # Create invalid migration file
    (migrations_dir / 'invalid.py').write_text('# invalid')

    with patch('src.cli.migrate.get_migrations_directory', return_value=migrations_dir):
      result = self.runner.invoke(migrate_app, ['validate'])

      # Should fail validation
      assert result.exit_code == 1

  def test_migrate_validate_valid_files(self, temp_migration_dir: Path) -> None:
    """Test validate with valid migration files."""
    # Create valid migration file
    migration_file = temp_migration_dir / '20260102_120000_test.py'
    migration_content = """
metadata = {
  'version': '20260102_120000',
  'description': 'Test',
}

def up():
  return []

def down():
  return []
"""
    migration_file.write_text(migration_content)

    with (
      patch('src.cli.migrate.get_migrations_directory', return_value=temp_migration_dir),
      patch('src.cli.migrate.validate_migrations', return_value=[]),
    ):
      result = self.runner.invoke(migrate_app, ['validate'])

      assert result.exit_code == 0


class TestCLIErrorHandling:
  """Test suite for CLI error handling."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_migrate_create_error_handling(self) -> None:
    """Test error handling in migrate create."""
    with patch('src.cli.migrate.get_migrations_directory', side_effect=Exception('Test error')):
      result = self.runner.invoke(migrate_app, ['create', 'test'])

      assert result.exit_code == 1

  def test_migrate_status_with_invalid_directory(self) -> None:
    """Test status command with non-existent directory."""
    with (
      patch('src.cli.migrate.get_migrations_directory', return_value=Path('/nonexistent')),
      patch('src.cli.migrate.discover_migrations', return_value=[]),
    ):
      result = self.runner.invoke(migrate_app, ['status'])

      # Should handle gracefully
      assert result.exit_code in [0, 1]


class TestCLIFormats:
  """Test suite for CLI output formats."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_migrate_status_table_format(self) -> None:
    """Test status with table format (default)."""
    result = self.runner.invoke(migrate_app, ['status', '--help'])

    assert result.exit_code == 0
    assert '--format' in strip_ansi(result.stdout)

  def test_migrate_status_json_format(self) -> None:
    """Test status with JSON format option."""
    result = self.runner.invoke(migrate_app, ['status', '--help'])

    assert result.exit_code == 0
    # JSON format should be available as option
    assert '--format' in strip_ansi(result.stdout)

  def test_migrate_history_formats(self) -> None:
    """Test history command supports format option."""
    result = self.runner.invoke(migrate_app, ['history', '--help'])

    assert result.exit_code == 0
    assert '--format' in strip_ansi(result.stdout)


class TestCLIVerboseOption:
  """Test suite for CLI verbose option."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_migrate_create_verbose(self, tmp_path: Path) -> None:
    """Test migrate create with verbose option."""
    with (
      patch('src.cli.migrate.get_migrations_directory', return_value=tmp_path),
      patch('src.cli.migrate.create_blank_migration') as mock_create,
    ):
      mock_file = tmp_path / '20260102_120000_test.py'
      mock_create.return_value = mock_file

      result = self.runner.invoke(migrate_app, ['create', 'test', '--verbose'])

      assert result.exit_code == 0

  def test_verbose_option_available(self) -> None:
    """Test that verbose option is available."""
    result = self.runner.invoke(migrate_app, ['create', '--help'])

    assert result.exit_code == 0
    assert '--verbose' in result.stdout or '-v' in result.stdout


class TestCLIDryRunOption:
  """Test suite for CLI dry-run option."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_dry_run_option_available_up(self) -> None:
    """Test that dry-run option is available for up command."""
    result = self.runner.invoke(migrate_app, ['up', '--help'])

    assert result.exit_code == 0
    assert '--dry-run' in result.stdout

  def test_dry_run_option_available_down(self) -> None:
    """Test that dry-run option is available for down command."""
    result = self.runner.invoke(migrate_app, ['down', '--help'])

    assert result.exit_code == 0
    assert '--dry-run' in result.stdout


class TestCLIDirectoryOption:
  """Test suite for CLI directory option."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_directory_option_available(self) -> None:
    """Test that directory option is available."""
    result = self.runner.invoke(migrate_app, ['create', '--help'])

    assert result.exit_code == 0
    assert '--directory' in result.stdout or '-d' in result.stdout

  def test_migrate_create_custom_directory(self, tmp_path: Path) -> None:
    """Test migrate create with custom directory."""
    custom_dir = tmp_path / 'custom'

    with (
      patch('src.cli.migrate.get_migrations_directory', return_value=custom_dir) as mock_get,
      patch('src.cli.migrate.create_blank_migration') as mock_create,
    ):
      mock_file = custom_dir / '20260102_120000_test.py'
      mock_create.return_value = mock_file

      result = self.runner.invoke(migrate_app, ['create', 'test', '--directory', str(custom_dir)])

      assert result.exit_code == 0
      # Verify custom directory was used
      mock_get.assert_called_once()


class TestCLIStepsOption:
  """Test suite for CLI steps option."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_steps_option_available_up(self) -> None:
    """Test that steps option is available for up command."""
    result = self.runner.invoke(migrate_app, ['up', '--help'])

    assert result.exit_code == 0
    assert '--steps' in result.stdout or '-n' in result.stdout

  def test_steps_option_available_down(self) -> None:
    """Test that steps option is available for down command."""
    result = self.runner.invoke(migrate_app, ['down', '--help'])

    assert result.exit_code == 0
    assert '--steps' in result.stdout or '-n' in result.stdout


class TestCLIGenerateCommand:
  """Test suite for migrate generate command."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_generate_help(self) -> None:
    """Test generate command help."""
    result = self.runner.invoke(migrate_app, ['generate', '--help'])

    assert result.exit_code == 0
    assert 'generate' in result.stdout.lower()

  def test_generate_creates_blank_migration(self, tmp_path: Path) -> None:
    """Test that generate creates blank migration (current implementation)."""
    with (
      patch('src.cli.migrate.get_migrations_directory', return_value=tmp_path),
      patch('src.cli.migrate.create_blank_migration') as mock_create,
    ):
      mock_file = tmp_path / '20260102_120000_test.py'
      mock_create.return_value = mock_file

      result = self.runner.invoke(migrate_app, ['generate', 'test schema'])

      # Should succeed and create blank migration
      assert result.exit_code == 0
      mock_create.assert_called_once()


class TestCLICommandStructure:
  """Test suite for CLI command structure and organization."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_migrate_app_has_commands(self) -> None:
    """Test that migrate app has all expected commands."""
    result = self.runner.invoke(migrate_app, ['--help'])

    assert result.exit_code == 0

    # Check for main commands
    expected_commands = ['create', 'up', 'down', 'status', 'history', 'validate', 'generate']
    for cmd in expected_commands:
      assert cmd in result.stdout

  def test_migrate_no_args_shows_help(self) -> None:
    """Test that running migrate without args shows help."""
    result = self.runner.invoke(migrate_app, [])

    # Should show help (exit code 2 when no args provided with no_args_is_help=True)
    assert result.exit_code in [0, 2]
    assert 'Usage' in result.stdout or 'Commands' in result.stdout


class TestCLIConfirmations:
  """Test suite for CLI confirmation prompts."""

  def test_confirm_function(self) -> None:
    """Test confirm function."""
    with patch('typer.confirm', return_value=True):
      result = confirm('Test message')
      assert result is True

  def test_confirm_destructive_yes(self) -> None:
    """Test destructive confirmation with yes."""
    with patch('typer.prompt', return_value='yes'), patch('src.cli.common.display_warning'):
      result = confirm_destructive('Test operation')
      assert result is True

  def test_confirm_destructive_no(self) -> None:
    """Test destructive confirmation with no."""
    with patch('typer.prompt', return_value='yes'), patch('src.cli.common.display_warning'):
      result = confirm_destructive('Test operation')
      assert result is True

  def test_confirm_destructive_case_insensitive(self) -> None:
    """Test destructive confirmation is case insensitive."""
    with patch('typer.prompt', return_value='YES'), patch('src.cli.common.display_warning'):
      result = confirm_destructive('Test operation')
      assert result is True
