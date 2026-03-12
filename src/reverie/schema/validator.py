"""Schema validation module for comparing Python schemas against database schemas.

This module provides validation utilities to detect schema drift, mismatches,
and inconsistencies between code-defined schemas and the actual database state.
"""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

import structlog

from reverie.schema.edge import EdgeDefinition, EdgeMode
from reverie.schema.fields import FieldDefinition
from reverie.schema.parser import parse_table_info
from reverie.schema.table import (
  IndexDefinition,
  IndexType,
  TableDefinition,
)
from reverie.schema.utils import fetch_db_tables

if TYPE_CHECKING:
  from reverie.connection.client import DatabaseClient

logger = structlog.get_logger(__name__)


class ValidationSeverity(Enum):
  """Severity levels for validation issues.

  Defines the criticality of schema validation results.
  """

  ERROR = 'error'  # Schema drift - requires migration
  WARNING = 'warning'  # Non-critical difference
  INFO = 'info'  # Informational only


@dataclass(frozen=True)
class ValidationResult:
  """Result of a schema validation check.

  Represents a single validation finding with severity level and details
  about what differs between code and database schemas.

  Examples:
    >>> result = ValidationResult(
    ...   severity=ValidationSeverity.ERROR,
    ...   table='user',
    ...   field='email',
    ...   message='Field type mismatch',
    ...   code_value='string',
    ...   db_value='int',
    ... )
  """

  severity: ValidationSeverity
  table: str
  field: str | None
  message: str
  code_value: str | None
  db_value: str | None

  def __str__(self) -> str:
    """Human-readable representation of validation result."""
    parts = [f'[{self.severity.value.upper()}] {self.table}']
    if self.field:
      parts[0] += f'.{self.field}'
    parts.append(f': {self.message}')
    if self.code_value or self.db_value:
      parts.append(f' (code: {self.code_value}, db: {self.db_value})')
    return ''.join(parts)


# Main validation function


async def validate_schema(
  code_tables: dict[str, TableDefinition],
  client: 'DatabaseClient',
  *,
  code_edges: dict[str, EdgeDefinition] | None = None,
) -> list[ValidationResult]:
  """Compare code schemas against database and return validation results.

  Performs comprehensive validation including table existence, field matching,
  index verification, and edge validation.

  Args:
    code_tables: Dictionary of code-defined table schemas
    client: Connected database client
    code_edges: Optional dictionary of code-defined edge schemas

  Returns:
    List of ValidationResult objects describing any schema differences

  Examples:
    >>> async with get_client(config) as client:
    ...   results = await validate_schema(code_tables, client)
    ...   for r in results:
    ...     print(r)
  """
  results: list[ValidationResult] = []

  # Fetch database tables
  db_tables = await _fetch_db_tables(client)

  logger.debug(
    'validating_schemas',
    code_tables=list(code_tables.keys()),
    db_tables=list(db_tables.keys()),
  )

  # Validate tables (both regular tables and edges treated as tables)
  results.extend(_validate_tables(code_tables, db_tables))

  # Validate edges if provided
  if code_edges:
    db_edges = await _fetch_db_edges(client, code_edges)
    results.extend(_validate_edges(code_edges, db_edges))

  return results


# Database fetching helpers


async def _fetch_db_tables(client: 'DatabaseClient') -> dict[str, TableDefinition]:
  """Fetch table definitions from database.

  Delegates to the shared utility function in schema.utils.

  Args:
    client: Connected database client

  Returns:
    Dictionary of table name to TableDefinition
  """
  # Pass the local parse_table_info to maintain testability
  # (tests can patch reverie.schema.validator.parse_table_info)
  return await fetch_db_tables(client, parse_table_info=parse_table_info)


async def _fetch_db_edges(
  client: 'DatabaseClient',
  code_edges: dict[str, EdgeDefinition],
) -> dict[str, TableDefinition]:
  """Fetch edge table definitions from database.

  Edges are stored as tables in SurrealDB, so we fetch their info
  as TableDefinition objects.

  Args:
    client: Connected database client
    code_edges: Code-defined edges to look up

  Returns:
    Dictionary of edge name to TableDefinition
  """
  db_edges: dict[str, TableDefinition] = {}

  for edge_name in code_edges:
    try:
      table_info = await client.execute(f'INFO FOR TABLE {edge_name};')
      info_result = table_info
      if isinstance(table_info, list) and len(table_info) > 0:
        first_info = table_info[0]
        info_result = (
          first_info.get('result', first_info) if isinstance(first_info, dict) else table_info
        )
      if isinstance(info_result, dict):
        db_edges[edge_name] = parse_table_info(edge_name, info_result)
    except Exception as e:
      logger.debug('edge_fetch_failed', edge=edge_name, error=str(e))
      # Edge table doesn't exist in DB yet

  return db_edges


# Table validation


def _validate_tables(
  code_tables: dict[str, TableDefinition],
  db_tables: dict[str, TableDefinition],
) -> list[ValidationResult]:
  """Validate all tables between code and database.

  Args:
    code_tables: Code-defined table schemas
    db_tables: Database table schemas

  Returns:
    List of validation results
  """
  results: list[ValidationResult] = []

  # Check for tables in code but not in database
  results.extend(_validate_missing_tables(code_tables, db_tables))

  # Check for tables in database but not in code
  results.extend(_validate_extra_tables(code_tables, db_tables))

  # Validate matching tables
  common_tables = set(code_tables.keys()) & set(db_tables.keys())
  for table_name in common_tables:
    code_table = code_tables[table_name]
    db_table = db_tables[table_name]
    results.extend(_validate_table(code_table, db_table))

  return results


def _validate_missing_tables(
  code_tables: dict[str, TableDefinition],
  db_tables: dict[str, TableDefinition],
) -> list[ValidationResult]:
  """Check for tables defined in code but missing from database.

  Args:
    code_tables: Code-defined table schemas
    db_tables: Database table schemas

  Returns:
    List of validation results for missing tables
  """
  results: list[ValidationResult] = []
  missing_tables = set(code_tables.keys()) - set(db_tables.keys())

  for table_name in missing_tables:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=table_name,
        field=None,
        message='Table defined in code but missing from database',
        code_value='exists',
        db_value='missing',
      )
    )

  return results


def _validate_extra_tables(
  code_tables: dict[str, TableDefinition],
  db_tables: dict[str, TableDefinition],
) -> list[ValidationResult]:
  """Check for tables in database but not defined in code.

  Args:
    code_tables: Code-defined table schemas
    db_tables: Database table schemas

  Returns:
    List of validation results for extra tables
  """
  results: list[ValidationResult] = []
  extra_tables = set(db_tables.keys()) - set(code_tables.keys())

  for table_name in extra_tables:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=None,
        message='Table exists in database but not defined in code',
        code_value='missing',
        db_value='exists',
      )
    )

  return results


def _validate_table(
  code_table: TableDefinition,
  db_table: TableDefinition,
) -> list[ValidationResult]:
  """Validate a single table between code and database.

  Args:
    code_table: Code-defined table schema
    db_table: Database table schema

  Returns:
    List of validation results for this table
  """
  results: list[ValidationResult] = []

  # Validate table mode
  results.extend(_validate_table_mode(code_table, db_table))

  # Validate fields
  results.extend(_validate_fields(code_table, db_table))

  # Validate indexes
  results.extend(_validate_indexes(code_table, db_table))

  # Validate events
  results.extend(_validate_events(code_table, db_table))

  return results


def _validate_table_mode(
  code_table: TableDefinition,
  db_table: TableDefinition,
) -> list[ValidationResult]:
  """Validate table mode (SCHEMAFULL/SCHEMALESS) matches.

  Args:
    code_table: Code-defined table schema
    db_table: Database table schema

  Returns:
    List of validation results
  """
  results: list[ValidationResult] = []

  if code_table.mode != db_table.mode:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=code_table.name,
        field=None,
        message='Table mode mismatch',
        code_value=code_table.mode.value,
        db_value=db_table.mode.value,
      )
    )

  return results


# Field validation


def _validate_fields(
  code_table: TableDefinition,
  db_table: TableDefinition,
) -> list[ValidationResult]:
  """Validate field definitions match between code and database.

  Args:
    code_table: Code-defined table schema
    db_table: Database table schema

  Returns:
    List of validation results for fields
  """
  results: list[ValidationResult] = []
  table_name = code_table.name

  code_fields = {f.name: f for f in code_table.fields}
  db_fields = {f.name: f for f in db_table.fields}

  # Missing fields (in code but not in DB)
  missing_fields = set(code_fields.keys()) - set(db_fields.keys())
  for field_name in missing_fields:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=table_name,
        field=field_name,
        message='Field defined in code but missing from database',
        code_value='exists',
        db_value='missing',
      )
    )

  # Extra fields (in DB but not in code)
  extra_fields = set(db_fields.keys()) - set(code_fields.keys())
  for field_name in extra_fields:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=field_name,
        message='Field exists in database but not defined in code',
        code_value='missing',
        db_value='exists',
      )
    )

  # Validate matching fields
  common_fields = set(code_fields.keys()) & set(db_fields.keys())
  for field_name in common_fields:
    code_field = code_fields[field_name]
    db_field = db_fields[field_name]
    results.extend(_validate_field(table_name, code_field, db_field))

  return results


def _validate_field(
  table_name: str,
  code_field: FieldDefinition,
  db_field: FieldDefinition,
) -> list[ValidationResult]:
  """Validate a single field matches between code and database.

  Args:
    table_name: Name of the table containing the field
    code_field: Code-defined field
    db_field: Database field

  Returns:
    List of validation results for this field
  """
  results: list[ValidationResult] = []

  # Check type
  if code_field.type != db_field.type:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=table_name,
        field=code_field.name,
        message='Field type mismatch',
        code_value=code_field.type.value,
        db_value=db_field.type.value,
      )
    )

  # Check assertion
  if _normalize_expression(code_field.assertion) != _normalize_expression(db_field.assertion):
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=code_field.name,
        message='Field assertion mismatch',
        code_value=code_field.assertion,
        db_value=db_field.assertion,
      )
    )

  # Check default
  if _normalize_expression(code_field.default) != _normalize_expression(db_field.default):
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=code_field.name,
        message='Field default value mismatch',
        code_value=code_field.default,
        db_value=db_field.default,
      )
    )

  # Check value (computed field)
  if _normalize_expression(code_field.value) != _normalize_expression(db_field.value):
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=code_field.name,
        message='Field computed value mismatch',
        code_value=code_field.value,
        db_value=db_field.value,
      )
    )

  # Check readonly
  if code_field.readonly != db_field.readonly:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.INFO,
        table=table_name,
        field=code_field.name,
        message='Field readonly flag mismatch',
        code_value=str(code_field.readonly),
        db_value=str(db_field.readonly),
      )
    )

  # Check flexible
  if code_field.flexible != db_field.flexible:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.INFO,
        table=table_name,
        field=code_field.name,
        message='Field flexible flag mismatch',
        code_value=str(code_field.flexible),
        db_value=str(db_field.flexible),
      )
    )

  return results


# Index validation


def _validate_indexes(
  code_table: TableDefinition,
  db_table: TableDefinition,
) -> list[ValidationResult]:
  """Validate index definitions match between code and database.

  Args:
    code_table: Code-defined table schema
    db_table: Database table schema

  Returns:
    List of validation results for indexes
  """
  results: list[ValidationResult] = []
  table_name = code_table.name

  code_indexes = {idx.name: idx for idx in code_table.indexes}
  db_indexes = {idx.name: idx for idx in db_table.indexes}

  # Missing indexes (in code but not in DB)
  missing_indexes = set(code_indexes.keys()) - set(db_indexes.keys())
  for index_name in missing_indexes:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=table_name,
        field=f'index:{index_name}',
        message='Index defined in code but missing from database',
        code_value='exists',
        db_value='missing',
      )
    )

  # Extra indexes (in DB but not in code)
  extra_indexes = set(db_indexes.keys()) - set(code_indexes.keys())
  for index_name in extra_indexes:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=f'index:{index_name}',
        message='Index exists in database but not defined in code',
        code_value='missing',
        db_value='exists',
      )
    )

  # Validate matching indexes
  common_indexes = set(code_indexes.keys()) & set(db_indexes.keys())
  for index_name in common_indexes:
    code_index = code_indexes[index_name]
    db_index = db_indexes[index_name]
    results.extend(_validate_index(table_name, code_index, db_index))

  return results


def _validate_index(
  table_name: str,
  code_index: IndexDefinition,
  db_index: IndexDefinition,
) -> list[ValidationResult]:
  """Validate a single index matches between code and database.

  Args:
    table_name: Name of the table containing the index
    code_index: Code-defined index
    db_index: Database index

  Returns:
    List of validation results for this index
  """
  results: list[ValidationResult] = []
  index_field = f'index:{code_index.name}'

  # Check index type
  if code_index.type != db_index.type:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=table_name,
        field=index_field,
        message='Index type mismatch',
        code_value=code_index.type.value,
        db_value=db_index.type.value,
      )
    )

  # Check columns
  if sorted(code_index.columns) != sorted(db_index.columns):
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=table_name,
        field=index_field,
        message='Index columns mismatch',
        code_value=','.join(code_index.columns),
        db_value=','.join(db_index.columns),
      )
    )

  # For MTREE indexes, check additional parameters
  if code_index.type == IndexType.MTREE:
    results.extend(_validate_mtree_index(table_name, code_index, db_index))

  return results


def _validate_mtree_index(
  table_name: str,
  code_index: IndexDefinition,
  db_index: IndexDefinition,
) -> list[ValidationResult]:
  """Validate MTREE-specific index parameters.

  Args:
    table_name: Name of the table containing the index
    code_index: Code-defined MTREE index
    db_index: Database MTREE index

  Returns:
    List of validation results for MTREE parameters
  """
  results: list[ValidationResult] = []
  index_field = f'index:{code_index.name}'

  # Check dimension
  if code_index.dimension != db_index.dimension:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=table_name,
        field=index_field,
        message='MTREE index dimension mismatch',
        code_value=str(code_index.dimension),
        db_value=str(db_index.dimension),
      )
    )

  # Check distance metric
  if code_index.distance != db_index.distance:
    code_dist = code_index.distance.value if code_index.distance else None
    db_dist = db_index.distance.value if db_index.distance else None
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=index_field,
        message='MTREE index distance metric mismatch',
        code_value=code_dist,
        db_value=db_dist,
      )
    )

  # Check vector type
  if code_index.vector_type != db_index.vector_type:
    code_vt = code_index.vector_type.value if code_index.vector_type else None
    db_vt = db_index.vector_type.value if db_index.vector_type else None
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=index_field,
        message='MTREE index vector type mismatch',
        code_value=code_vt,
        db_value=db_vt,
      )
    )

  return results


# Event validation


def _validate_events(
  code_table: TableDefinition,
  db_table: TableDefinition,
) -> list[ValidationResult]:
  """Validate event definitions match between code and database.

  Args:
    code_table: Code-defined table schema
    db_table: Database table schema

  Returns:
    List of validation results for events
  """
  results: list[ValidationResult] = []
  table_name = code_table.name

  code_events = {evt.name: evt for evt in code_table.events}
  db_events = {evt.name: evt for evt in db_table.events}

  # Missing events (in code but not in DB)
  missing_events = set(code_events.keys()) - set(db_events.keys())
  for event_name in missing_events:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=table_name,
        field=f'event:{event_name}',
        message='Event defined in code but missing from database',
        code_value='exists',
        db_value='missing',
      )
    )

  # Extra events (in DB but not in code)
  extra_events = set(db_events.keys()) - set(code_events.keys())
  for event_name in extra_events:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.WARNING,
        table=table_name,
        field=f'event:{event_name}',
        message='Event exists in database but not defined in code',
        code_value='missing',
        db_value='exists',
      )
    )

  return results


# Edge validation


def _validate_edges(
  code_edges: dict[str, EdgeDefinition],
  db_edges: dict[str, TableDefinition],
) -> list[ValidationResult]:
  """Validate edge definitions between code and database.

  Args:
    code_edges: Code-defined edge schemas
    db_edges: Database edge schemas (as TableDefinition)

  Returns:
    List of validation results for edges
  """
  results: list[ValidationResult] = []

  # Check for edges in code but not in database
  missing_edges = set(code_edges.keys()) - set(db_edges.keys())
  for edge_name in missing_edges:
    results.append(
      ValidationResult(
        severity=ValidationSeverity.ERROR,
        table=edge_name,
        field=None,
        message='Edge defined in code but missing from database',
        code_value='exists',
        db_value='missing',
      )
    )

  # Validate matching edges
  common_edges = set(code_edges.keys()) & set(db_edges.keys())
  for edge_name in common_edges:
    code_edge = code_edges[edge_name]
    db_edge = db_edges[edge_name]
    results.extend(_validate_edge(code_edge, db_edge))

  return results


def _validate_edge(
  code_edge: EdgeDefinition,
  db_edge: TableDefinition,
) -> list[ValidationResult]:
  """Validate a single edge definition.

  Edges in SurrealDB are stored as tables. This function validates that
  the edge table has the correct structure based on the edge mode.

  Args:
    code_edge: Code-defined edge schema
    db_edge: Database table schema for the edge

  Returns:
    List of validation results
  """
  results: list[ValidationResult] = []
  edge_name = code_edge.name

  # Validate edge mode via table properties
  # For RELATION mode, check that the table is TYPE RELATION
  # This is typically reflected in the tb field definition
  if code_edge.mode == EdgeMode.RELATION:
    # In SurrealDB, RELATION tables should have automatic in/out fields
    # We can check if the code expects TYPE RELATION but DB might be SCHEMAFULL
    results.append(
      ValidationResult(
        severity=ValidationSeverity.INFO,
        table=edge_name,
        field=None,
        message=f'Edge mode: {code_edge.mode.value}',
        code_value=code_edge.mode.value,
        db_value=db_edge.mode.value,
      )
    )

  # Validate edge fields
  code_fields = {f.name: f for f in code_edge.fields}
  db_fields = {f.name: f for f in db_edge.fields}

  for field_name in code_fields:
    if field_name not in db_fields:
      results.append(
        ValidationResult(
          severity=ValidationSeverity.ERROR,
          table=edge_name,
          field=field_name,
          message='Edge field missing from database',
          code_value='exists',
          db_value='missing',
        )
      )
    else:
      code_field = code_fields[field_name]
      db_field = db_fields[field_name]
      results.extend(_validate_field(edge_name, code_field, db_field))

  # Validate edge indexes
  code_indexes = {idx.name: idx for idx in code_edge.indexes}
  db_indexes = {idx.name: idx for idx in db_edge.indexes}

  for index_name in code_indexes:
    if index_name not in db_indexes:
      results.append(
        ValidationResult(
          severity=ValidationSeverity.ERROR,
          table=edge_name,
          field=f'index:{index_name}',
          message='Edge index missing from database',
          code_value='exists',
          db_value='missing',
        )
      )
    else:
      code_index = code_indexes[index_name]
      db_index = db_indexes[index_name]
      results.extend(_validate_index(edge_name, code_index, db_index))

  return results


# Utility functions


def _normalize_expression(expr: str | None) -> str | None:
  """Normalize a SurrealQL expression for comparison.

  Removes extra whitespace and normalizes case for keywords.

  Args:
    expr: Expression to normalize

  Returns:
    Normalized expression or None
  """
  if expr is None:
    return None

  # Remove extra whitespace
  normalized = ' '.join(expr.split())
  return normalized.strip() if normalized else None


# Re-export utility functions for backward compatibility
from reverie.schema.validator_utils import (  # noqa: E402, F401
  filter_by_severity,
  filter_errors,
  filter_warnings,
  format_validation_report,
  get_validation_summary,
  group_by_table,
  has_errors,
)
