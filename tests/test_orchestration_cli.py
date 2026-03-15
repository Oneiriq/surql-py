"""Tests for orchestration CLI commands."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from surql.cli.orchestrate import app
from surql.orchestration.strategy import DeploymentResult, DeploymentStatus


@pytest.fixture
def cli_runner() -> CliRunner:
  """Create CLI test runner."""
  return CliRunner()


@pytest.fixture
def test_config_file(tmp_path: Path) -> Path:
  """Create test configuration file."""
  config_file = tmp_path / 'environments.json'
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
      },
      {
        'name': 'production',
        'connection': {
          'db_url': 'ws://prod.example.com:8000/rpc',
          'db_ns': 'production',
          'db': 'main',
        },
        'priority': 1,
      },
    ]
  }
  config_file.write_text(json.dumps(config_data))
  return config_file


@pytest.fixture
def test_migrations_dir(tmp_path: Path) -> Path:
  """Create test migrations directory."""
  migrations_dir = tmp_path / 'migrations'
  migrations_dir.mkdir()

  # Create a sample migration file
  migration_file = migrations_dir / '20260109_120000_test_migration.py'
  migration_content = """
'''Test migration'''

metadata = {
    'version': '20260109_120000',
    'description': 'Test migration',
    'author': 'test',
}

def up():
    return ['CREATE TABLE test SCHEMAFULL;']

def down():
    return ['REMOVE TABLE test;']
"""
  migration_file.write_text(migration_content)
  return migrations_dir


class TestDeployCommand:
  """Tests for deploy command."""

  def test_deploy_help(self, cli_runner: CliRunner) -> None:
    """Test deploy command help."""
    result = cli_runner.invoke(app, ['deploy', '--help'])
    assert result.exit_code == 0
    assert 'Deploy migrations across multiple database environments' in result.stdout

  def test_deploy_missing_config(
    self,
    cli_runner: CliRunner,
    tmp_path: Path,
  ) -> None:
    """Test deploy with missing config file."""
    nonexistent = tmp_path / 'nonexistent.json'
    result = cli_runner.invoke(
      app,
      [
        'deploy',
        '--environments',
        'staging',
        '--config',
        str(nonexistent),
        '--dry-run',
      ],
    )
    assert result.exit_code == 1
    assert 'Configuration file not found' in result.output

  def test_deploy_missing_migrations_dir(
    self,
    cli_runner: CliRunner,
    test_config_file: Path,
    tmp_path: Path,
  ) -> None:
    """Test deploy with missing migrations directory."""
    nonexistent_dir = tmp_path / 'nonexistent_migrations'
    result = cli_runner.invoke(
      app,
      [
        'deploy',
        '--environments',
        'staging',
        '--config',
        str(test_config_file),
        '--migrations-dir',
        str(nonexistent_dir),
        '--dry-run',
      ],
    )
    assert result.exit_code == 1
    assert 'Migrations directory not found' in result.output


class TestStatusCommand:
  """Tests for status command."""

  def test_status_help(self, cli_runner: CliRunner) -> None:
    """Test status command help."""
    result = cli_runner.invoke(app, ['status', '--help'])
    assert result.exit_code == 0
    assert 'Check deployment status of environments' in result.stdout

  def test_status_missing_config(
    self,
    cli_runner: CliRunner,
    tmp_path: Path,
  ) -> None:
    """Test status with missing config file."""
    nonexistent = tmp_path / 'nonexistent.json'
    result = cli_runner.invoke(
      app,
      [
        'status',
        '--environments',
        'staging',
        '--config',
        str(nonexistent),
      ],
    )
    assert result.exit_code == 1
    assert 'Configuration file not found' in result.output


class TestValidateCommand:
  """Tests for validate command."""

  def test_validate_help(self, cli_runner: CliRunner) -> None:
    """Test validate command help."""
    result = cli_runner.invoke(app, ['validate', '--help'])
    assert result.exit_code == 0
    assert 'Validate environment configuration' in result.stdout

  def test_validate_missing_config(
    self,
    cli_runner: CliRunner,
    tmp_path: Path,
  ) -> None:
    """Test validate with missing config file."""
    nonexistent = tmp_path / 'nonexistent.json'
    result = cli_runner.invoke(
      app,
      [
        'validate',
        '--config',
        str(nonexistent),
      ],
    )
    assert result.exit_code == 1
    assert 'Configuration file not found' in result.output


class TestCLIOutputFormatting:
  """Tests for CLI output formatting."""

  def test_display_deployment_results(self) -> None:
    """Test deployment results display."""
    from datetime import UTC, datetime

    from surql.cli.orchestrate import _display_deployment_results

    results = {
      'env1': DeploymentResult(
        environment='env1',
        status=DeploymentStatus.SUCCESS,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        migrations_applied=2,
      ),
      'env2': DeploymentResult(
        environment='env2',
        status=DeploymentStatus.FAILED,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        error='Connection failed',
      ),
    }

    # Should not raise any errors
    _display_deployment_results(results)


class TestCLIEdgeCases:
  """Tests for CLI edge cases."""

  def test_invalid_strategy(
    self,
    cli_runner: CliRunner,
    test_config_file: Path,
    test_migrations_dir: Path,
  ) -> None:
    """Test deployment with invalid strategy."""
    result = cli_runner.invoke(
      app,
      [
        'deploy',
        '--environments',
        'staging',
        '--config',
        str(test_config_file),
        '--migrations-dir',
        str(test_migrations_dir),
        '--strategy',
        'invalid_strategy',
        '--dry-run',
      ],
    )
    assert result.exit_code == 1
    assert 'Invalid strategy' in result.output

  def test_empty_environments_list(
    self,
    cli_runner: CliRunner,
    test_config_file: Path,
    test_migrations_dir: Path,
  ) -> None:
    """Test deployment with empty environments list."""
    with patch('surql.cli.orchestrate.MigrationCoordinator') as mock_coordinator:
      mock_instance = mock_coordinator.return_value
      mock_instance.deploy_to_environments = AsyncMock(return_value={})

      result = cli_runner.invoke(
        app,
        [
          'deploy',
          '--environments',
          '',
          '--config',
          str(test_config_file),
          '--migrations-dir',
          str(test_migrations_dir),
          '--dry-run',
        ],
      )

      # Should handle gracefully
      assert result.exit_code in [0, 1]
