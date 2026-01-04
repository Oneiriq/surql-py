"""Tests for the schema validator module.

This module provides comprehensive tests for schema validation against database
schemas, including validation results, utility functions, and CLI commands.
"""

import asyncio
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
from typer.testing import CliRunner

from reverie.cli.schema import app as schema_app
from reverie.schema.edge import EdgeDefinition, EdgeMode
from reverie.schema.fields import FieldDefinition, FieldType
from reverie.schema.table import (
  IndexDefinition,
  IndexType,
  MTreeDistanceType,
  MTreeVectorType,
  TableDefinition,
  TableMode,
)
from reverie.schema.validator import (
  ValidationResult,
  ValidationSeverity,
  filter_by_severity,
  filter_errors,
  filter_warnings,
  format_validation_report,
  get_validation_summary,
  group_by_table,
  has_errors,
  validate_schema,
)


def strip_ansi(text: str) -> str:
  """Remove ANSI escape sequences from text."""
  ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
  return ansi_escape.sub('', text)


def extract_json_from_output(text: str) -> dict:
  """Extract JSON object from CLI output that may contain other text."""
  stripped = strip_ansi(text)
  # Find the JSON object in the output
  start = stripped.find('{')
  if start == -1:
    raise ValueError(f'No JSON object found in output: {stripped[:100]}...')

  # Find matching closing brace
  depth = 0
  for i, char in enumerate(stripped[start:], start):
    if char == '{':
      depth += 1
    elif char == '}':
      depth -= 1
      if depth == 0:
        return json.loads(stripped[start : i + 1])

  raise ValueError(f'Unmatched braces in JSON output: {stripped[:100]}...')


# ValidationSeverity Tests


class TestValidationSeverity:
  """Test suite for ValidationSeverity enum."""

  def test_severity_error_value(self) -> None:
    """Test ERROR severity has correct value."""
    assert ValidationSeverity.ERROR.value == 'error'

  def test_severity_warning_value(self) -> None:
    """Test WARNING severity has correct value."""
    assert ValidationSeverity.WARNING.value == 'warning'

  def test_severity_info_value(self) -> None:
    """Test INFO severity has correct value."""
    assert ValidationSeverity.INFO.value == 'info'

  def test_all_severity_values(self) -> None:
    """Test all severity values are present."""
    severities = [s.value for s in ValidationSeverity]
    assert 'error' in severities
    assert 'warning' in severities
    assert 'info' in severities


# ValidationResult Tests


class TestValidationResult:
  """Test suite for ValidationResult dataclass."""

  def test_validation_result_creation_basic(self) -> None:
    """Test basic ValidationResult creation."""
    result = ValidationResult(
      severity=ValidationSeverity.ERROR,
      table='user',
      field='email',
      message='Field type mismatch',
      code_value='string',
      db_value='int',
    )

    assert result.severity == ValidationSeverity.ERROR
    assert result.table == 'user'
    assert result.field == 'email'
    assert result.message == 'Field type mismatch'
    assert result.code_value == 'string'
    assert result.db_value == 'int'

  def test_validation_result_with_none_field(self) -> None:
    """Test ValidationResult with None field (table-level issue)."""
    result = ValidationResult(
      severity=ValidationSeverity.ERROR,
      table='user',
      field=None,
      message='Table missing from database',
      code_value='exists',
      db_value='missing',
    )

    assert result.field is None

  def test_validation_result_with_none_values(self) -> None:
    """Test ValidationResult with None code_value and db_value."""
    result = ValidationResult(
      severity=ValidationSeverity.INFO,
      table='user',
      field='name',
      message='Some info message',
      code_value=None,
      db_value=None,
    )

    assert result.code_value is None
    assert result.db_value is None

  def test_validation_result_str_with_field(self) -> None:
    """Test ValidationResult __str__ with field."""
    result = ValidationResult(
      severity=ValidationSeverity.ERROR,
      table='user',
      field='email',
      message='Field type mismatch',
      code_value='string',
      db_value='int',
    )

    str_result = str(result)
    assert '[ERROR]' in str_result
    assert 'user.email' in str_result
    assert 'Field type mismatch' in str_result
    assert 'code: string' in str_result
    assert 'db: int' in str_result

  def test_validation_result_str_without_field(self) -> None:
    """Test ValidationResult __str__ without field."""
    result = ValidationResult(
      severity=ValidationSeverity.WARNING,
      table='post',
      field=None,
      message='Table exists in database but not in code',
      code_value='missing',
      db_value='exists',
    )

    str_result = str(result)
    assert '[WARNING]' in str_result
    assert 'post' in str_result
    assert 'post.' not in str_result  # No field separator

  def test_validation_result_str_without_values(self) -> None:
    """Test ValidationResult __str__ without code/db values."""
    result = ValidationResult(
      severity=ValidationSeverity.INFO,
      table='user',
      field='name',
      message='Some info',
      code_value=None,
      db_value=None,
    )

    str_result = str(result)
    assert '[INFO]' in str_result
    assert 'code:' not in str_result

  def test_validation_result_immutability(self) -> None:
    """Test that ValidationResult is frozen (immutable)."""
    result = ValidationResult(
      severity=ValidationSeverity.ERROR,
      table='user',
      field='email',
      message='Test',
      code_value='a',
      db_value='b',
    )

    with pytest.raises(AttributeError):
      result.table = 'post'  # type: ignore[misc]


# validate_schema() Tests


class TestValidateSchemaMissingTables:
  """Test suite for validate_schema() with missing tables."""

  def test_table_missing_from_database(self) -> None:
    """Test detection of table defined in code but missing from database."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='name', type=FieldType.STRING)],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(return_value=[{'result': {'tables': {}}}])

      return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    assert len(results) == 1
    assert results[0].severity == ValidationSeverity.ERROR
    assert results[0].table == 'user'
    assert 'missing from database' in results[0].message

  def test_multiple_tables_missing(self) -> None:
    """Test detection of multiple tables missing from database."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL),
        'post': TableDefinition(name='post', mode=TableMode.SCHEMAFULL),
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(return_value=[{'result': {'tables': {}}}])

      return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    assert len(results) == 2
    table_names = {r.table for r in results}
    assert 'user' in table_names
    assert 'post' in table_names


class TestValidateSchemaExtraTables:
  """Test suite for validate_schema() with extra tables in database."""

  def test_table_in_database_not_in_code(self) -> None:
    """Test detection of table in database but not defined in code."""

    async def run_test() -> list[ValidationResult]:
      code_tables: dict[str, TableDefinition] = {}

      mock_client = Mock()
      # First call: INFO FOR DB returns table list
      # Second call: INFO FOR TABLE returns table details
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'legacy_table': 'DEFINE TABLE legacy_table'}}}],
          [{'result': {'fields': {}, 'indexes': {}, 'events': {}}}],
        ]
      )

      return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    assert len(results) == 1
    assert results[0].severity == ValidationSeverity.WARNING
    assert results[0].table == 'legacy_table'
    assert 'not defined in code' in results[0].message


class TestValidateSchemaNoIssues:
  """Test suite for validate_schema() with matching schemas."""

  def test_schemas_match_returns_empty(self) -> None:
    """Test that matching schemas return empty results list."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='name', type=FieldType.STRING)],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user SCHEMAFULL'}}}],
          [
            {
              'result': {
                'fields': {'name': 'DEFINE FIELD name ON user TYPE string'},
                'indexes': {},
                'events': {},
              }
            }
          ],
        ]
      )

      # Mock parse_table_info to return matching definition
      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='name', type=FieldType.STRING)],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    assert len(results) == 0


class TestValidateSchemaFieldMismatches:
  """Test suite for validate_schema() with field mismatches."""

  def test_field_type_mismatch(self) -> None:
    """Test detection of field type mismatch."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='age', type=FieldType.INT)],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user'}}}],
          [{'result': {'fields': {'age': 'DEFINE FIELD age TYPE string'}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='age', type=FieldType.STRING)],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    type_mismatch = [r for r in results if 'type mismatch' in r.message.lower()]
    assert len(type_mismatch) >= 1
    assert type_mismatch[0].severity == ValidationSeverity.ERROR

  def test_field_missing_from_database(self) -> None:
    """Test detection of field defined in code but missing from database."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[
            FieldDefinition(name='name', type=FieldType.STRING),
            FieldDefinition(name='email', type=FieldType.STRING),
          ],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user'}}}],
          [{'result': {'fields': {'name': 'DEFINE FIELD name TYPE string'}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='name', type=FieldType.STRING)],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    missing_field = [r for r in results if r.field == 'email']
    assert len(missing_field) == 1
    assert missing_field[0].severity == ValidationSeverity.ERROR
    assert 'missing from database' in missing_field[0].message

  def test_extra_field_in_database(self) -> None:
    """Test detection of field in database but not in code."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='name', type=FieldType.STRING)],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user'}}}],
          [
            {
              'result': {
                'fields': {
                  'name': 'DEFINE FIELD name TYPE string',
                  'legacy_field': 'DEFINE FIELD legacy_field TYPE int',
                }
              }
            }
          ],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[
            FieldDefinition(name='name', type=FieldType.STRING),
            FieldDefinition(name='legacy_field', type=FieldType.INT),
          ],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    extra_field = [r for r in results if r.field == 'legacy_field']
    assert len(extra_field) == 1
    assert extra_field[0].severity == ValidationSeverity.WARNING
    assert 'not defined in code' in extra_field[0].message

  def test_field_assertion_mismatch(self) -> None:
    """Test detection of field assertion mismatch."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[
            FieldDefinition(
              name='email', type=FieldType.STRING, assertion='string::is::email($value)'
            )
          ],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user'}}}],
          [{'result': {'fields': {'email': 'DEFINE FIELD email TYPE string'}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          fields=[FieldDefinition(name='email', type=FieldType.STRING, assertion='$value != NONE')],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    assertion_mismatch = [r for r in results if 'assertion' in r.message.lower()]
    assert len(assertion_mismatch) >= 1
    assert assertion_mismatch[0].severity == ValidationSeverity.WARNING


class TestValidateSchemaIndexMismatches:
  """Test suite for validate_schema() with index mismatches."""

  def test_index_missing_from_database(self) -> None:
    """Test detection of index defined in code but missing from database."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          indexes=[IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE)],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user'}}}],
          [{'result': {'fields': {}, 'indexes': {}, 'events': {}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          indexes=[],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    missing_idx = [r for r in results if r.field == 'index:email_idx']
    assert len(missing_idx) == 1
    assert missing_idx[0].severity == ValidationSeverity.ERROR
    assert 'missing from database' in missing_idx[0].message

  def test_extra_index_in_database(self) -> None:
    """Test detection of index in database but not in code."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          indexes=[],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user'}}}],
          [{'result': {'fields': {}, 'indexes': {'legacy_idx': 'DEFINE INDEX'}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          indexes=[IndexDefinition(name='legacy_idx', columns=['legacy'], type=IndexType.STANDARD)],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    extra_idx = [r for r in results if r.field == 'index:legacy_idx']
    assert len(extra_idx) == 1
    assert extra_idx[0].severity == ValidationSeverity.WARNING

  def test_index_type_mismatch(self) -> None:
    """Test detection of index type mismatch."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          indexes=[IndexDefinition(name='email_idx', columns=['email'], type=IndexType.UNIQUE)],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user'}}}],
          [{'result': {'indexes': {'email_idx': 'DEFINE INDEX'}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          indexes=[IndexDefinition(name='email_idx', columns=['email'], type=IndexType.STANDARD)],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    type_mismatch = [r for r in results if 'Index type mismatch' in r.message]
    assert len(type_mismatch) >= 1
    assert type_mismatch[0].severity == ValidationSeverity.ERROR

  def test_index_columns_mismatch(self) -> None:
    """Test detection of index columns mismatch."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'user': TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          indexes=[
            IndexDefinition(
              name='name_idx', columns=['first_name', 'last_name'], type=IndexType.STANDARD
            )
          ],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user'}}}],
          [{'result': {'indexes': {'name_idx': 'DEFINE INDEX'}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='user',
          mode=TableMode.SCHEMAFULL,
          indexes=[
            IndexDefinition(name='name_idx', columns=['first_name'], type=IndexType.STANDARD)
          ],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    col_mismatch = [r for r in results if 'columns mismatch' in r.message]
    assert len(col_mismatch) >= 1
    assert col_mismatch[0].severity == ValidationSeverity.ERROR

  def test_mtree_index_dimension_mismatch(self) -> None:
    """Test detection of MTREE index dimension mismatch."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {
        'document': TableDefinition(
          name='document',
          mode=TableMode.SCHEMAFULL,
          indexes=[
            IndexDefinition(
              name='vec_idx',
              columns=['embedding'],
              type=IndexType.MTREE,
              dimension=1024,
              distance=MTreeDistanceType.COSINE,
              vector_type=MTreeVectorType.F32,
            )
          ],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'document': 'DEFINE TABLE document'}}}],
          [{'result': {'indexes': {'vec_idx': 'DEFINE INDEX'}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='document',
          mode=TableMode.SCHEMAFULL,
          indexes=[
            IndexDefinition(
              name='vec_idx',
              columns=['embedding'],
              type=IndexType.MTREE,
              dimension=768,
              distance=MTreeDistanceType.COSINE,
              vector_type=MTreeVectorType.F32,
            )
          ],
        )

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    dim_mismatch = [r for r in results if 'dimension mismatch' in r.message]
    assert len(dim_mismatch) >= 1
    assert dim_mismatch[0].severity == ValidationSeverity.ERROR


class TestValidateSchemaTableModeMismatch:
  """Test suite for validate_schema() with table mode mismatches."""

  def test_table_mode_mismatch_schemafull_vs_schemaless(self) -> None:
    """Test detection of table mode mismatch."""

    async def run_test() -> list[ValidationResult]:
      code_tables = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {'user': 'DEFINE TABLE user SCHEMALESS'}}}],
          [{'result': {'fields': {}, 'indexes': {}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(name='user', mode=TableMode.SCHEMALESS)

        return await validate_schema(code_tables, mock_client)

    results = asyncio.run(run_test())

    mode_mismatch = [r for r in results if 'mode mismatch' in r.message.lower()]
    assert len(mode_mismatch) >= 1
    assert mode_mismatch[0].severity == ValidationSeverity.ERROR


class TestValidateSchemaEdgeValidation:
  """Test suite for validate_schema() with edge validation."""

  def test_edge_missing_from_database(self) -> None:
    """Test detection of edge defined in code but missing from database."""

    async def run_test() -> list[ValidationResult]:
      code_tables: dict[str, TableDefinition] = {}
      code_edges = {
        'likes': EdgeDefinition(
          name='likes',
          mode=EdgeMode.RELATION,
          from_table='user',
          to_table='post',
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {}}}],
          # Edge fetch fails (table doesn't exist)
          Exception('Table not found'),
        ]
      )

      return await validate_schema(code_tables, mock_client, code_edges=code_edges)

    results = asyncio.run(run_test())

    missing_edge = [r for r in results if r.table == 'likes' and 'missing' in r.message.lower()]
    assert len(missing_edge) >= 1
    assert missing_edge[0].severity == ValidationSeverity.ERROR

  def test_edge_field_mismatch(self) -> None:
    """Test detection of edge field mismatch."""

    async def run_test() -> list[ValidationResult]:
      code_tables: dict[str, TableDefinition] = {}
      code_edges = {
        'likes': EdgeDefinition(
          name='likes',
          mode=EdgeMode.RELATION,
          fields=[FieldDefinition(name='weight', type=FieldType.INT)],
        )
      }

      mock_client = Mock()
      mock_client.execute = AsyncMock(
        side_effect=[
          [{'result': {'tables': {}}}],
          [{'result': {'fields': {}, 'indexes': {}}}],
        ]
      )

      with patch('reverie.schema.validator.parse_table_info') as mock_parse:
        mock_parse.return_value = TableDefinition(
          name='likes',
          mode=TableMode.SCHEMAFULL,
          fields=[],
        )

        return await validate_schema(code_tables, mock_client, code_edges=code_edges)

    results = asyncio.run(run_test())

    # Should detect missing field
    field_issues = [r for r in results if r.field == 'weight']
    assert len(field_issues) >= 1


# Utility Functions Tests


class TestFilterBySeverity:
  """Test suite for filter_by_severity function."""

  def test_filter_errors_only(self) -> None:
    """Test filtering for ERROR severity only."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None),
      ValidationResult(ValidationSeverity.INFO, 'user', 'age', 'Info', None, None),
    ]

    filtered = filter_by_severity(results, ValidationSeverity.ERROR)

    assert len(filtered) == 1
    assert filtered[0].severity == ValidationSeverity.ERROR

  def test_filter_warnings_only(self) -> None:
    """Test filtering for WARNING severity only."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'post', 'title', 'Warning2', None, None),
    ]

    filtered = filter_by_severity(results, ValidationSeverity.WARNING)

    assert len(filtered) == 2
    assert all(r.severity == ValidationSeverity.WARNING for r in filtered)

  def test_filter_empty_results(self) -> None:
    """Test filtering empty results list."""
    filtered = filter_by_severity([], ValidationSeverity.ERROR)
    assert len(filtered) == 0

  def test_filter_no_matches(self) -> None:
    """Test filtering when no results match severity."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error', None, None),
    ]

    filtered = filter_by_severity(results, ValidationSeverity.INFO)

    assert len(filtered) == 0


class TestFilterErrors:
  """Test suite for filter_errors convenience function."""

  def test_filter_errors(self) -> None:
    """Test filter_errors returns only ERROR severity."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error1', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None),
      ValidationResult(ValidationSeverity.ERROR, 'post', 'title', 'Error2', None, None),
    ]

    errors = filter_errors(results)

    assert len(errors) == 2
    assert all(r.severity == ValidationSeverity.ERROR for r in errors)


class TestFilterWarnings:
  """Test suite for filter_warnings convenience function."""

  def test_filter_warnings(self) -> None:
    """Test filter_warnings returns only WARNING severity."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning1', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'post', 'title', 'Warning2', None, None),
    ]

    warnings = filter_warnings(results)

    assert len(warnings) == 2
    assert all(r.severity == ValidationSeverity.WARNING for r in warnings)


class TestGroupByTable:
  """Test suite for group_by_table function."""

  def test_group_by_table_basic(self) -> None:
    """Test basic grouping by table name."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None),
      ValidationResult(ValidationSeverity.ERROR, 'post', 'title', 'Error', None, None),
    ]

    grouped = group_by_table(results)

    assert len(grouped) == 2
    assert 'user' in grouped
    assert 'post' in grouped
    assert len(grouped['user']) == 2
    assert len(grouped['post']) == 1

  def test_group_by_table_empty(self) -> None:
    """Test grouping empty results."""
    grouped = group_by_table([])
    assert len(grouped) == 0

  def test_group_by_table_single_table(self) -> None:
    """Test grouping with single table."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error1', None, None),
      ValidationResult(ValidationSeverity.ERROR, 'user', 'name', 'Error2', None, None),
    ]

    grouped = group_by_table(results)

    assert len(grouped) == 1
    assert len(grouped['user']) == 2


class TestHasErrors:
  """Test suite for has_errors function."""

  def test_has_errors_true(self) -> None:
    """Test has_errors returns True when errors present."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None),
    ]

    assert has_errors(results) is True

  def test_has_errors_false_with_warnings(self) -> None:
    """Test has_errors returns False with only warnings."""
    results = [
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None),
      ValidationResult(ValidationSeverity.INFO, 'user', 'age', 'Info', None, None),
    ]

    assert has_errors(results) is False

  def test_has_errors_empty(self) -> None:
    """Test has_errors returns False for empty list."""
    assert has_errors([]) is False


class TestFormatValidationReport:
  """Test suite for format_validation_report function."""

  def test_format_report_no_issues(self) -> None:
    """Test formatting empty results."""
    report = format_validation_report([])

    assert 'No schema validation issues found' in report

  def test_format_report_with_errors(self) -> None:
    """Test formatting report with errors."""
    results = [
      ValidationResult(
        ValidationSeverity.ERROR, 'user', 'email', 'Field type mismatch', 'string', 'int'
      ),
    ]

    report = format_validation_report(results)

    assert 'Schema Validation Report' in report
    assert '1 errors' in report or 'errors' in report
    assert 'user' in report
    assert 'email' in report

  def test_format_report_excludes_info_by_default(self) -> None:
    """Test that INFO severity is excluded by default."""
    results = [
      ValidationResult(ValidationSeverity.INFO, 'user', 'name', 'Info message', None, None),
    ]

    report = format_validation_report(results)

    # INFO should be excluded when include_info=False (default)
    assert 'No significant schema validation issues found' in report

  def test_format_report_includes_info_when_requested(self) -> None:
    """Test that INFO severity is included when requested."""
    results = [
      ValidationResult(ValidationSeverity.INFO, 'user', 'name', 'Info message', None, None),
    ]

    report = format_validation_report(results, include_info=True)

    assert 'user' in report


class TestGetValidationSummary:
  """Test suite for get_validation_summary function."""

  def test_summary_with_mixed_results(self) -> None:
    """Test summary with mixed severity results."""
    results = [
      ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error', None, None),
      ValidationResult(ValidationSeverity.ERROR, 'post', 'title', 'Error', None, None),
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None),
      ValidationResult(ValidationSeverity.INFO, 'user', 'age', 'Info', None, None),
    ]

    summary = get_validation_summary(results)

    assert summary['total'] == 4
    assert summary['errors'] == 2
    assert summary['warnings'] == 1
    assert summary['info'] == 1
    assert summary['tables_affected'] == 2
    assert summary['has_errors'] is True

  def test_summary_empty_results(self) -> None:
    """Test summary with empty results."""
    summary = get_validation_summary([])

    assert summary['total'] == 0
    assert summary['errors'] == 0
    assert summary['warnings'] == 0
    assert summary['info'] == 0
    assert summary['tables_affected'] == 0
    assert summary['has_errors'] is False

  def test_summary_no_errors(self) -> None:
    """Test summary with no errors."""
    results = [
      ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None),
    ]

    summary = get_validation_summary(results)

    assert summary['has_errors'] is False


# CLI Command Tests


class TestValidateCLICommand:
  """Test suite for schema validate CLI command."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_validate_help(self) -> None:
    """Test validate command help."""
    result = self.runner.invoke(schema_app, ['validate', '--help'])

    assert result.exit_code == 0
    assert 'validate' in result.stdout.lower()
    assert '--strict' in result.stdout
    assert '--format' in result.stdout
    assert '--output' in result.stdout

  def test_validate_missing_schema_file(self, tmp_path: Path) -> None:
    """Test validate with missing schema file."""
    result = self.runner.invoke(
      schema_app, ['validate', '--schema', str(tmp_path / 'nonexistent.py')]
    )

    assert result.exit_code != 0

  def test_validate_strict_flag_returns_nonzero_on_errors(self, tmp_path: Path) -> None:
    """Test that --strict flag returns non-zero exit code on errors."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text("""
from reverie.schema.table import table_schema
from reverie.schema.fields import string_field
from reverie.schema.table import with_fields

user_table = table_schema('user')
user_table = with_fields(user_table, string_field('name'))
""")

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client

      # Return errors from validation
      mock_validate.return_value = [
        ValidationResult(
          ValidationSeverity.ERROR, 'user', 'name', 'Missing field', 'exists', 'missing'
        )
      ]

      result = self.runner.invoke(
        schema_app, ['validate', '--schema', str(schema_file), '--strict']
      )

      # Should exit with non-zero code due to errors
      assert result.exit_code == 1

  def test_validate_strict_warnings_flag(self, tmp_path: Path) -> None:
    """Test that --strict-warnings flag returns non-zero on warnings."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client

      # Return only warnings from validation
      mock_validate.return_value = [
        ValidationResult(
          ValidationSeverity.WARNING, 'post', None, 'Extra table', 'missing', 'exists'
        )
      ]

      result = self.runner.invoke(
        schema_app, ['validate', '--schema', str(schema_file), '--strict-warnings']
      )

      # Should exit with code 2 for warnings
      assert result.exit_code == 2

  def test_validate_json_format_output(self, tmp_path: Path) -> None:
    """Test that --format json produces valid JSON output."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client

      mock_validate.return_value = []

      result = self.runner.invoke(
        schema_app, ['validate', '--schema', str(schema_file), '--format', 'json']
      )

      assert result.exit_code == 0
      # Should be valid JSON - extract from output that may contain other text
      json_output = extract_json_from_output(result.stdout)
      assert 'valid' in json_output
      assert 'summary' in json_output
      assert json_output['valid'] is True

  def test_validate_json_format_with_errors(self, tmp_path: Path) -> None:
    """Test JSON format output with validation errors."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client

      mock_validate.return_value = [
        ValidationResult(
          ValidationSeverity.ERROR, 'user', 'email', 'Field missing', 'exists', 'missing'
        )
      ]

      result = self.runner.invoke(
        schema_app, ['validate', '--schema', str(schema_file), '--format', 'json']
      )

      json_output = extract_json_from_output(result.stdout)
      assert json_output['valid'] is False
      assert json_output['summary']['errors'] == 1
      assert len(json_output['results']) == 1

  def test_validate_text_format_output(self, tmp_path: Path) -> None:
    """Test that --format text produces human-readable output."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client

      mock_validate.return_value = []

      result = self.runner.invoke(
        schema_app, ['validate', '--schema', str(schema_file), '--format', 'text']
      )

      assert result.exit_code == 0
      # Should NOT be JSON
      stripped = strip_ansi(result.stdout)
      assert not stripped.strip().startswith('{')

  def test_validate_output_to_file(self, tmp_path: Path) -> None:
    """Test that --output writes report to file."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')
    output_file = tmp_path / 'report.txt'

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client

      mock_validate.return_value = [
        ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Some warning', None, None)
      ]

      result = self.runner.invoke(
        schema_app,
        ['validate', '--schema', str(schema_file), '--output', str(output_file)],
      )

      assert result.exit_code == 0
      assert output_file.exists()
      content = output_file.read_text()
      assert len(content) > 0

  def test_validate_output_json_to_file(self, tmp_path: Path) -> None:
    """Test that --output with --format json writes JSON to file."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')
    output_file = tmp_path / 'report.json'

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client

      mock_validate.return_value = []

      result = self.runner.invoke(
        schema_app,
        [
          'validate',
          '--schema',
          str(schema_file),
          '--format',
          'json',
          '--output',
          str(output_file),
        ],
      )

      assert result.exit_code == 0
      assert output_file.exists()
      content = json.loads(output_file.read_text())
      assert 'valid' in content

  def test_validate_connection_error_handling(self, tmp_path: Path) -> None:
    """Test validate handles database connection errors gracefully."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_get_client.return_value.__aenter__.side_effect = ConnectionRefusedError('Cannot connect')

      result = self.runner.invoke(schema_app, ['validate', '--schema', str(schema_file)])

      # Should exit with connection error code (3)
      assert result.exit_code == 3

  def test_validate_success_exit_code_zero(self, tmp_path: Path) -> None:
    """Test validate returns exit code 0 on success without --strict."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}

      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client

      mock_validate.return_value = []

      result = self.runner.invoke(schema_app, ['validate', '--schema', str(schema_file)])

      assert result.exit_code == 0


class TestValidateCLIExitCodes:
  """Test suite for validate command exit codes."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_exit_code_success(self, tmp_path: Path) -> None:
    """Test exit code 0 for valid schema."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_validate.return_value = []

      result = self.runner.invoke(schema_app, ['validate', '--schema', str(schema_file)])

      assert result.exit_code == 0

  def test_exit_code_errors_with_strict(self, tmp_path: Path) -> None:
    """Test exit code 1 for errors with --strict."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_validate.return_value = [
        ValidationResult(ValidationSeverity.ERROR, 'user', 'email', 'Error', None, None)
      ]

      result = self.runner.invoke(
        schema_app, ['validate', '--schema', str(schema_file), '--strict']
      )

      assert result.exit_code == 1

  def test_exit_code_warnings_with_strict_warnings(self, tmp_path: Path) -> None:
    """Test exit code 2 for warnings with --strict-warnings."""
    schema_file = tmp_path / 'schema.py'
    schema_file.write_text('# empty schema')

    with (
      patch('reverie.cli.schema.get_db_config'),
      patch('reverie.cli.schema.get_client') as mock_get_client,
      patch('reverie.cli.schema._load_schemas_from_file') as mock_load,
      patch('reverie.schema.validator.validate_schema') as mock_validate,
    ):
      mock_load.return_value = {'user': TableDefinition(name='user', mode=TableMode.SCHEMAFULL)}
      mock_client = AsyncMock()
      mock_get_client.return_value.__aenter__.return_value = mock_client
      mock_validate.return_value = [
        ValidationResult(ValidationSeverity.WARNING, 'user', 'name', 'Warning', None, None)
      ]

      result = self.runner.invoke(
        schema_app, ['validate', '--schema', str(schema_file), '--strict-warnings']
      )

      assert result.exit_code == 2


class TestSchemaAppStructure:
  """Test suite for schema CLI app structure."""

  def setup_method(self) -> None:
    """Set up test resources."""
    self.runner = CliRunner(env={'NO_COLOR': '1'})

  def test_schema_app_has_validate_command(self) -> None:
    """Test that schema app has validate command."""
    result = self.runner.invoke(schema_app, ['--help'])

    assert result.exit_code == 0
    assert 'validate' in result.stdout

  def test_schema_app_has_all_expected_commands(self) -> None:
    """Test that schema app has all expected commands."""
    result = self.runner.invoke(schema_app, ['--help'])

    assert result.exit_code == 0
    expected_commands = ['show', 'diff', 'generate', 'export', 'tables', 'inspect', 'validate']
    for cmd in expected_commands:
      assert cmd in result.stdout
