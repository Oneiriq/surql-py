"""Schema validation and check implementations.

Async handlers for validate, check, and hook-config commands,
plus exit code constants and display helpers.
"""

from pathlib import Path
from typing import Any

from reverie.cli.common import (
  display_code,
  display_error,
  display_info,
  display_success,
  display_warning,
)
from reverie.cli.schema_diff import _load_schemas_from_file
from reverie.connection.client import get_client
from reverie.settings import get_db_config

# Exit codes for validate command
VALIDATE_EXIT_SUCCESS = 0
VALIDATE_EXIT_ERRORS = 1
VALIDATE_EXIT_WARNINGS = 2
VALIDATE_EXIT_CONNECTION_ERROR = 3

# Exit codes for check command
CHECK_EXIT_NO_DRIFT = 0
CHECK_EXIT_DRIFT_DETECTED = 1
CHECK_EXIT_ERROR = 2


async def _validate_schema_async(
  schema_file: Path,
  strict: bool,
  strict_warnings: bool,
  output_format: str,
  output: Path | None,
  verbose: bool,
) -> int:
  """Async implementation of schema validation.

  Args:
    schema_file: Path to Python schema file
    strict: Exit with non-zero code on any drift
    strict_warnings: Also fail on warnings
    output_format: Output format (text, json)
    output: Output file path
    verbose: Enable verbose output

  Returns:
    Exit code based on validation results
  """
  from reverie.schema.registry import clear_registry
  from reverie.schema.validator import (
    validate_schema as run_validation,
  )
  from reverie.schema.validator_utils import (
    filter_warnings,
    format_validation_report,
    get_validation_summary,
    has_errors,
  )

  # Validate schema file exists
  if not schema_file.exists():
    display_error(f'Schema file not found: {schema_file}')
    return VALIDATE_EXIT_ERRORS

  display_info(f'Loading schemas from: {schema_file}')

  # Clear registry and load new schemas
  clear_registry()
  code_tables = _load_schemas_from_file(schema_file)

  if not code_tables:
    display_warning('No schemas found in the specified file')
    display_info('Make sure your file registers schemas using register_table()')
    return VALIDATE_EXIT_ERRORS

  display_success(f'Loaded {len(code_tables)} table schemas from file')

  # Fetch database schemas and run validation
  config = get_db_config()

  async with get_client(config) as client:
    display_info('Validating schema against database...')

    results = await run_validation(code_tables, client)

    # Get summary statistics
    summary = get_validation_summary(results)

    # Determine exit code
    exit_code = VALIDATE_EXIT_SUCCESS
    if strict:
      if has_errors(results):
        exit_code = VALIDATE_EXIT_ERRORS
      elif strict_warnings and filter_warnings(results):
        exit_code = VALIDATE_EXIT_WARNINGS
    elif strict_warnings and filter_warnings(results):
      exit_code = VALIDATE_EXIT_WARNINGS

    # Format output
    if output_format.lower() == 'json':
      content = _format_validation_json(results, summary)
    else:
      content = format_validation_report(results, include_info=verbose)

    # Write output
    if output:
      output.write_text(content, encoding='utf-8')
      display_success(f'Validation report written to: {output}')
    else:
      # Print to stdout
      if output_format.lower() == 'json':
        print(content)
      else:
        _display_validation_results(results, summary, verbose)

    return exit_code


def _format_validation_json(
  results: list[Any],
  summary: dict[str, Any],
) -> str:
  """Format validation results as JSON.

  Args:
    results: List of ValidationResult objects
    summary: Summary statistics dictionary

  Returns:
    JSON string
  """
  import json

  from reverie.schema.validator_utils import has_errors

  output_data = {
    'valid': not has_errors(results),
    'summary': {
      'total': summary['total'],
      'errors': summary['errors'],
      'warnings': summary['warnings'],
      'info': summary['info'],
    },
    'results': [
      {
        'severity': r.severity.value,
        'table': r.table,
        'field': r.field,
        'message': r.message,
        'code_value': r.code_value,
        'db_value': r.db_value,
      }
      for r in results
    ],
  }
  return json.dumps(output_data, indent=2)


def _display_validation_results(
  results: list[Any],
  summary: dict[str, Any],
  verbose: bool,
) -> None:
  """Display validation results to console with rich formatting.

  Args:
    results: List of ValidationResult objects
    summary: Summary statistics dictionary
    verbose: Include INFO severity results
  """
  from rich.console import Console
  from rich.text import Text

  from reverie.schema.validator import ValidationSeverity
  from reverie.schema.validator_utils import group_by_table

  console = Console()

  if not results:
    display_success('No schema validation issues found - code and database are in sync!')
    return

  # Filter results based on verbose flag
  filtered = results
  if not verbose:
    filtered = [r for r in results if r.severity != ValidationSeverity.INFO]

  if not filtered:
    display_success('No significant schema validation issues found.')
    return

  # Display summary header
  error_count = summary['errors']
  warning_count = summary['warnings']

  if error_count > 0:
    display_error(f'Schema validation found {error_count} errors, {warning_count} warnings')
  elif warning_count > 0:
    display_warning(f'Schema validation found {warning_count} warnings')
  else:
    display_info(f'Schema validation found {summary["info"]} informational items')

  # Group and display by table
  grouped = group_by_table(filtered)

  for table_name, table_results in sorted(grouped.items()):
    console.print(f'\n  [{table_name}]', style='bold')

    for result in table_results:
      # Color coding by severity
      if result.severity == ValidationSeverity.ERROR:
        style = 'bold red'
        icon = '[!]'
      elif result.severity == ValidationSeverity.WARNING:
        style = 'yellow'
        icon = '[~]'
      else:
        style = 'dim'
        icon = '[i]'

      text = Text()
      text.append(f'    {icon} ', style=style)
      text.append(result.message)
      if result.field:
        text.append(f' ({result.field})', style='cyan')
      console.print(text)

      if result.code_value or result.db_value:
        console.print(f'        code: {result.code_value}, db: {result.db_value}', style='dim')


async def _check_schema_async(
  schema_path: Path,
  migrations_dir: Path | None,
  fail_on_drift: bool,
  show_diff: bool,
  output_format: str,
  verbose: bool,
) -> int:
  """Async implementation of schema check.

  Args:
    schema_path: Path to schema files or directory
    migrations_dir: Path to migrations directory (auto-detected if None)
    fail_on_drift: Exit with non-zero code when drift detected
    show_diff: Show detailed diff information
    output_format: Output format (text, json)
    verbose: Enable verbose output

  Returns:
    Exit code based on drift detection results
  """
  from reverie.migration.hooks import check_schema_drift

  # Validate schema path exists
  if not schema_path.exists():
    display_error(f'Schema path not found: {schema_path}')
    return CHECK_EXIT_ERROR

  # Build list of schema paths
  schema_paths = [schema_path]

  if verbose:
    display_info(f'Checking schema files in: {schema_path}')
    if migrations_dir:
      display_info(f'Using migrations directory: {migrations_dir}')
    else:
      display_info('Migrations directory will be auto-detected')

  # Run drift check
  result = await check_schema_drift(
    schema_paths=schema_paths,
    migrations_dir=migrations_dir,
    fail_on_drift=fail_on_drift,
  )

  # Format and display results
  if output_format.lower() == 'json':
    _display_check_result_json(result, show_diff)
  else:
    _display_check_result_text(result, show_diff, verbose)

  # Return appropriate exit code
  if result.passed:
    return CHECK_EXIT_NO_DRIFT
  return CHECK_EXIT_DRIFT_DETECTED


def _display_check_result_text(
  result: Any,
  show_diff: bool,
  verbose: bool,
) -> None:
  """Display check result as text.

  Args:
    result: HookCheckResult object
    show_diff: Show detailed diff information
    verbose: Enable verbose output
  """
  from rich.console import Console

  console = Console()

  if result.passed:
    display_success(result.message)
    return

  # Display drift detected
  display_warning('Schema drift detected!')
  console.print()

  if result.unmigrated_files:
    console.print('  Files with unmigrated changes:', style='bold')
    for file_path in result.unmigrated_files:
      console.print(f'    - {file_path}', style='yellow')

  if show_diff and verbose:
    console.print()
    console.print('  Details:', style='bold')
    # The message contains details when drift is found
    for line in result.message.split('\n')[1:]:  # Skip first line (header)
      if line.strip():
        console.print(f'  {line}', style='dim')

  if result.suggested_action:
    console.print()
    console.print('  Suggested action:', style='bold blue')
    console.print(f'    {result.suggested_action}', style='cyan')


def _display_check_result_json(result: Any, show_diff: bool) -> None:
  """Display check result as JSON.

  Args:
    result: HookCheckResult object
    show_diff: Show detailed diff information
  """
  import json

  output_data = {
    'passed': result.passed,
    'message': result.message,
    'unmigrated_files': [str(f) for f in result.unmigrated_files],
    'suggested_action': result.suggested_action,
  }

  if show_diff:
    output_data['details'] = result.message

  print(json.dumps(output_data, indent=2))


def generate_hook_config_impl(
  schema_path: str,
  fail_on_drift: bool,
) -> None:
  """Implementation of hook-config command.

  Args:
    schema_path: Path to schema files
    fail_on_drift: Configure hook to fail on drift
  """
  from reverie.migration.hooks import generate_precommit_config

  config = generate_precommit_config(
    schema_path=schema_path,
    fail_on_drift=fail_on_drift,
  )

  display_info('Add the following to your .pre-commit-config.yaml:')
  print()
  display_code(config, language='yaml', title='Pre-commit Configuration')
