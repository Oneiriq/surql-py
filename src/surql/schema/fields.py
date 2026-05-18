"""Field definition functions for schema building.

This module provides field type definitions and builder functions for creating
type-safe field definitions in table schemas.
"""

import re
import warnings
from enum import Enum

from pydantic import BaseModel, ConfigDict

from surql.types.reserved import check_reserved_word

_FIELD_NAME_PART_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Recognizes the `type::record("<table>", $value)` coercion pattern in a
# `value=` arg so we can lift the target table into the field's TYPE clause
# (`record<table>`). Matches both single and double quotes, optional whitespace.
_TYPE_RECORD_COERCION_PATTERN = re.compile(
  r'type::record\s*\(\s*["\']([a-zA-Z_][a-zA-Z0-9_]*)["\']\s*,\s*\$value\s*\)\s*\Z'
)


def _detect_target_table_from_value(value: str | None) -> str | None:
  """If `value` is the canonical record-coercion expression, return the target table.

  `type::record("plan", $value)` -> `"plan"`. Returns None for anything else,
  including more complex VALUE expressions a caller deliberately wrote.
  """
  if not value:
    return None
  m = _TYPE_RECORD_COERCION_PATTERN.match(value.strip())
  return m.group(1) if m else None


class FieldType(Enum):
  """SurrealDB field types.

  Represents all supported SurrealDB field types for schema definitions.
  """

  STRING = 'string'
  INT = 'int'
  FLOAT = 'float'
  BOOL = 'bool'
  DATETIME = 'datetime'
  DURATION = 'duration'
  DECIMAL = 'decimal'
  NUMBER = 'number'
  OBJECT = 'object'
  ARRAY = 'array'
  RECORD = 'record'
  GEOMETRY = 'geometry'
  ANY = 'any'


class FieldDefinition(BaseModel):
  """Immutable field definition for table schemas.

  Represents a single field in a SurrealDB table schema with all its constraints,
  defaults, and permissions.

  Examples:
    Basic string field:
    >>> field = FieldDefinition(name='email', type=FieldType.STRING)

    Field with assertion:
    >>> field = FieldDefinition(
    ...   name='email',
    ...   type=FieldType.STRING,
    ...   assertion='string::is::email($value)'
    ... )

    Field with default value:
    >>> field = FieldDefinition(
    ...   name='created_at',
    ...   type=FieldType.DATETIME,
    ...   default='time::now()'
    ... )
  """

  name: str
  type: FieldType
  assertion: str | None = None
  default: str | None = None
  value: str | None = None  # For computed fields
  permissions: dict[str, str] | None = None
  readonly: bool = False
  flexible: bool = False
  # When True, emits `TYPE option<X>` instead of `TYPE X` so the column accepts
  # NONE in addition to values of the declared type. Required for SCHEMAFULL
  # tables whose source data may omit the field.
  nullable: bool = False
  # For RECORD fields: the target table this field links to. When set, the
  # emitter writes `TYPE record<{target_table}>` (parameterized) instead of
  # bare `TYPE record`, which is what SurrealDB's introspection (and tools
  # like Surrealist's graph designer) need to render cross-table relationships.
  # If unset on a RECORD field but `value` matches the canonical
  # `type::record("X", $value)` coercion pattern, the emitter auto-detects X
  # from the value string and treats it as `target_table=X`.
  target_table: str | None = None

  model_config = ConfigDict(frozen=True)


# Field builder functions


def field(
  name: str,
  field_type: FieldType,
  *,
  assertion: str | None = None,
  default: str | None = None,
  value: str | None = None,
  permissions: dict[str, str] | None = None,
  readonly: bool = False,
  flexible: bool = False,
  nullable: bool = False,
  target_table: str | None = None,
) -> FieldDefinition:
  """Create a field definition.

  Pure function to create an immutable field definition.

  Args:
    name: Field name (supports nested fields with dot notation)
    field_type: Field type from FieldType enum
    assertion: Optional SurrealQL assertion to validate field value
    default: Optional SurrealQL expression for default value
    value: Optional SurrealQL expression for computed value
    permissions: Optional dict of permission rules (select, create, update, delete)
    readonly: If True, field value cannot be modified after creation
    flexible: If True, field allows flexible schema
    nullable: If True, emits `TYPE option<X>` so the column accepts NONE.
    target_table: For RECORD fields, the target table — emits
      `TYPE record<{target_table}>` (parameterized) so SurrealDB introspection
      and Surrealist's graph designer can render cross-table relationships.
      If left unset on a RECORD field but `value` matches the canonical
      `type::record("X", $value)` coercion pattern, the emitter auto-detects X
      and treats it as `target_table=X` (so existing schemas get the upgrade
      with no code changes — see #92).

  Returns:
    Immutable FieldDefinition instance

  Examples:
    >>> field('name', FieldType.STRING)
    FieldDefinition(name='name', type=FieldType.STRING, ...)

    >>> field('age', FieldType.INT, assertion='$value >= 0 AND $value <= 150')
    FieldDefinition(name='age', type=FieldType.INT, assertion='$value >= 0 AND $value <= 150', ...)

    >>> field('author', FieldType.RECORD, target_table='user')  # explicit
    >>> field('author', FieldType.RECORD, value='type::record("user", $value)')  # auto-detected
  """
  _validate_field_name(name)

  reserved_warning = check_reserved_word(name)
  if reserved_warning is not None:
    warnings.warn(reserved_warning, stacklevel=2)

  # If the caller didn't pass target_table explicitly but supplied the canonical
  # `type::record("X", $value)` coercion as `value=`, lift X into target_table
  # so the emitter writes `record<X>` and (optionally) drops the redundant VALUE.
  if field_type == FieldType.RECORD and target_table is None:
    target_table = _detect_target_table_from_value(value)

  # Once `target_table` is set on a RECORD field, the canonical
  # `type::record("<target_table>", $value)` coercion is redundant — the typed
  # `record<X>` already constrains the column. The emitter drops it on write
  # (see schema/sql.py::_record_type_clause), so drop it from the in-memory
  # FieldDefinition too. Otherwise consumers built against a live DB (which
  # never stores the VALUE) see a phantom mismatch in `diff_tables` between
  # code-side `value="type::record(...)"` and live-side `value=None`.
  if (
    field_type == FieldType.RECORD
    and target_table is not None
    and value is not None
    and _detect_target_table_from_value(value) == target_table
  ):
    value = None

  return FieldDefinition(
    name=name,
    type=field_type,
    assertion=assertion,
    default=default,
    value=value,
    permissions=permissions,
    readonly=readonly,
    flexible=flexible,
    nullable=nullable,
    target_table=target_table,
  )


def _validate_field_name(name: str) -> None:
  """Validate a field name against SurrealDB identifier rules.

  Supports dot-notation for nested fields (e.g., 'address.city').
  Each part must be alphanumeric/underscore and not start with a digit.

  Args:
    name: Field name to validate

  Raises:
    ValueError: If the field name is invalid
  """
  if not name:
    raise ValueError('Field name cannot be empty')

  parts = name.split('.')
  for part in parts:
    if not part:
      raise ValueError(f'Invalid field name {name!r}: empty segment')
    if not _FIELD_NAME_PART_PATTERN.match(part):
      raise ValueError(
        f'Invalid field name {name!r}: segment {part!r} must contain only '
        'alphanumeric characters and underscores, and cannot start with a digit'
      )


def string_field(
  name: str,
  *,
  assertion: str | None = None,
  default: str | None = None,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create a string field definition.

  Args:
    name: Field name
    assertion: Optional assertion to validate string value
    default: Optional default value expression
    readonly: If True, field is read-only
    permissions: Optional permission rules

  Returns:
    FieldDefinition for a string field

  Examples:
    >>> string_field('email', assertion='string::is::email($value)')
    FieldDefinition(name='email', type=FieldType.STRING, assertion='string::is::email($value)', ...)
  """
  return field(
    name,
    FieldType.STRING,
    assertion=assertion,
    default=default,
    readonly=readonly,
    permissions=permissions,
  )


def int_field(
  name: str,
  *,
  assertion: str | None = None,
  default: str | None = None,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create an integer field definition.

  Args:
    name: Field name
    assertion: Optional assertion to validate integer value
    default: Optional default value expression
    readonly: If True, field is read-only
    permissions: Optional permission rules

  Returns:
    FieldDefinition for an integer field

  Examples:
    >>> int_field('age', assertion='$value >= 0')
    FieldDefinition(name='age', type=FieldType.INT, assertion='$value >= 0', ...)
  """
  return field(
    name,
    FieldType.INT,
    assertion=assertion,
    default=default,
    readonly=readonly,
    permissions=permissions,
  )


def float_field(
  name: str,
  *,
  assertion: str | None = None,
  default: str | None = None,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create a float field definition.

  Args:
    name: Field name
    assertion: Optional assertion to validate float value
    default: Optional default value expression
    readonly: If True, field is read-only
    permissions: Optional permission rules

  Returns:
    FieldDefinition for a float field
  """
  return field(
    name,
    FieldType.FLOAT,
    assertion=assertion,
    default=default,
    readonly=readonly,
    permissions=permissions,
  )


def bool_field(
  name: str,
  *,
  default: str | None = None,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create a boolean field definition.

  Args:
    name: Field name
    default: Optional default value expression
    readonly: If True, field is read-only
    permissions: Optional permission rules

  Returns:
    FieldDefinition for a boolean field

  Examples:
    >>> bool_field('is_active', default='true')
    FieldDefinition(name='is_active', type=FieldType.BOOL, default='true', ...)
  """
  return field(
    name,
    FieldType.BOOL,
    default=default,
    readonly=readonly,
    permissions=permissions,
  )


def datetime_field(
  name: str,
  *,
  assertion: str | None = None,
  default: str | None = None,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create a datetime field definition.

  Args:
    name: Field name
    assertion: Optional assertion to validate datetime value
    default: Optional default value expression (e.g., 'time::now()')
    readonly: If True, field is read-only
    permissions: Optional permission rules

  Returns:
    FieldDefinition for a datetime field

  Examples:
    >>> datetime_field('created_at', default='time::now()', readonly=True)
    FieldDefinition(name='created_at', type=FieldType.DATETIME, default='time::now()', readonly=True, ...)
  """
  return field(
    name,
    FieldType.DATETIME,
    assertion=assertion,
    default=default,
    readonly=readonly,
    permissions=permissions,
  )


def record_field(
  name: str,
  *,
  table: str | None = None,
  assertion: str | None = None,
  default: str | None = None,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create a record (foreign key) field definition.

  Args:
    name: Field name
    table: Optional specific table name to link to
    assertion: Optional assertion to validate record value
    default: Optional default value expression
    readonly: If True, field is read-only
    permissions: Optional permission rules

  Returns:
    FieldDefinition for a record field

  Examples:
    >>> record_field('author', table='user')
    FieldDefinition(name='author', type=FieldType.RECORD, ...)

    >>> record_field('author', assertion='$value.table = "user"')
    FieldDefinition(name='author', type=FieldType.RECORD, assertion='$value.table = "user"', ...)
  """
  # If table is specified, add it to the assertion
  if table and not assertion:
    assertion = f'$value.table = "{table}"'
  elif table and assertion:
    assertion = f'($value.table = "{table}") AND ({assertion})'

  return field(
    name,
    FieldType.RECORD,
    assertion=assertion,
    default=default,
    readonly=readonly,
    permissions=permissions,
  )


def array_field(
  name: str,
  *,
  assertion: str | None = None,
  default: str | None = None,
  readonly: bool = False,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create an array field definition.

  Args:
    name: Field name
    assertion: Optional assertion to validate array value
    default: Optional default value expression
    readonly: If True, field is read-only
    permissions: Optional permission rules

  Returns:
    FieldDefinition for an array field

  Examples:
    >>> array_field('tags', default='[]')
    FieldDefinition(name='tags', type=FieldType.ARRAY, default='[]', ...)
  """
  return field(
    name,
    FieldType.ARRAY,
    assertion=assertion,
    default=default,
    readonly=readonly,
    permissions=permissions,
  )


def object_field(
  name: str,
  *,
  assertion: str | None = None,
  default: str | None = None,
  readonly: bool = False,
  flexible: bool = True,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create an object field definition.

  Args:
    name: Field name
    assertion: Optional assertion to validate object value
    default: Optional default value expression
    readonly: If True, field is read-only
    flexible: If True, allows flexible schema for nested fields
    permissions: Optional permission rules

  Returns:
    FieldDefinition for an object field

  Examples:
    >>> object_field('metadata', flexible=True)
    FieldDefinition(name='metadata', type=FieldType.OBJECT, flexible=True, ...)
  """
  return field(
    name,
    FieldType.OBJECT,
    assertion=assertion,
    default=default,
    readonly=readonly,
    flexible=flexible,
    permissions=permissions,
  )


def computed_field(
  name: str,
  value: str,
  field_type: FieldType = FieldType.ANY,
  *,
  permissions: dict[str, str] | None = None,
) -> FieldDefinition:
  """Create a computed field definition.

  Computed fields are calculated dynamically using a SurrealQL expression.

  Args:
    name: Field name
    value: SurrealQL expression to compute the field value
    field_type: Expected type of the computed value
    permissions: Optional permission rules

  Returns:
    FieldDefinition for a computed field

  Examples:
    >>> computed_field('full_name', 'string::concat(name.first, " ", name.last)', FieldType.STRING)
    FieldDefinition(name='full_name', type=FieldType.STRING, value='string::concat(name.first, " ", name.last)', ...)
  """
  return field(
    name,
    field_type,
    value=value,
    readonly=True,  # Computed fields are always read-only
    permissions=permissions,
  )
