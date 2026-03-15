"""Validation result filtering, grouping, formatting, and summary utilities.

This module provides utility functions for working with schema validation results
including filtering by severity, grouping by table, and generating reports.
"""

from typing import Any

from surql.schema.validator import ValidationResult, ValidationSeverity


def filter_by_severity(
  results: list[ValidationResult],
  severity: ValidationSeverity,
) -> list[ValidationResult]:
  """Filter validation results by severity level.

  Args:
    results: List of validation results
    severity: Severity level to filter by

  Returns:
    Filtered list of results
  """
  return [r for r in results if r.severity == severity]


def filter_errors(results: list[ValidationResult]) -> list[ValidationResult]:
  """Get only ERROR severity results.

  Args:
    results: List of validation results

  Returns:
    List of ERROR severity results
  """
  return filter_by_severity(results, ValidationSeverity.ERROR)


def filter_warnings(results: list[ValidationResult]) -> list[ValidationResult]:
  """Get only WARNING severity results.

  Args:
    results: List of validation results

  Returns:
    List of WARNING severity results
  """
  return filter_by_severity(results, ValidationSeverity.WARNING)


def group_by_table(
  results: list[ValidationResult],
) -> dict[str, list[ValidationResult]]:
  """Group validation results by table name.

  Args:
    results: List of validation results

  Returns:
    Dictionary of table name to list of results
  """
  grouped: dict[str, list[ValidationResult]] = {}
  for result in results:
    if result.table not in grouped:
      grouped[result.table] = []
    grouped[result.table].append(result)
  return grouped


def has_errors(results: list[ValidationResult]) -> bool:
  """Check if any validation results have ERROR severity.

  Args:
    results: List of validation results

  Returns:
    True if any errors exist
  """
  return any(r.severity == ValidationSeverity.ERROR for r in results)


def format_validation_report(
  results: list[ValidationResult],
  *,
  include_info: bool = False,
) -> str:
  """Format validation results as a human-readable report.

  Args:
    results: List of validation results
    include_info: Whether to include INFO severity results

  Returns:
    Formatted report string
  """
  lines: list[str] = []

  if not results:
    lines.append('No schema validation issues found.')
    return '\n'.join(lines)

  # Filter results
  filtered = results
  if not include_info:
    filtered = [r for r in results if r.severity != ValidationSeverity.INFO]

  if not filtered:
    lines.append('No significant schema validation issues found.')
    return '\n'.join(lines)

  # Group by table
  grouped = group_by_table(filtered)

  # Count by severity
  error_count = len(filter_errors(filtered))
  warning_count = len(filter_warnings(filtered))

  lines.append(f'Schema Validation Report: {error_count} errors, {warning_count} warnings')
  lines.append('=' * 60)

  for table_name, table_results in sorted(grouped.items()):
    lines.append(f'\n[{table_name}]')
    for result in table_results:
      severity_icon = _get_severity_icon(result.severity)
      field_str = f'.{result.field}' if result.field else ''
      lines.append(f'  {severity_icon} {result.message}{field_str}')
      if result.code_value or result.db_value:
        lines.append(f'      code: {result.code_value}, db: {result.db_value}')

  return '\n'.join(lines)


def _get_severity_icon(severity: ValidationSeverity) -> str:
  """Get an icon character for severity level.

  Args:
    severity: Severity level

  Returns:
    Icon string
  """
  icons = {
    ValidationSeverity.ERROR: '[!]',
    ValidationSeverity.WARNING: '[~]',
    ValidationSeverity.INFO: '[i]',
  }
  return icons.get(severity, '[ ]')


def get_validation_summary(results: list[ValidationResult]) -> dict[str, Any]:
  """Get summary statistics for validation results.

  Args:
    results: List of validation results

  Returns:
    Dictionary with summary statistics
  """
  return {
    'total': len(results),
    'errors': len(filter_errors(results)),
    'warnings': len(filter_warnings(results)),
    'info': len(filter_by_severity(results, ValidationSeverity.INFO)),
    'tables_affected': len(group_by_table(results)),
    'has_errors': has_errors(results),
  }
