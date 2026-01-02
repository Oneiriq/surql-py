"""Field definition functions for schema building.

This module provides field type definitions and builder functions for creating
type-safe field definitions in table schemas.
"""

from enum import Enum

from pydantic import BaseModel


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

  class Config:
    """Pydantic configuration."""

    frozen = True  # Make immutable


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

  Returns:
    Immutable FieldDefinition instance

  Examples:
    >>> field('name', FieldType.STRING)
    FieldDefinition(name='name', type=FieldType.STRING, ...)

    >>> field('age', FieldType.INT, assertion='$value >= 0 AND $value <= 150')
    FieldDefinition(name='age', type=FieldType.INT, assertion='$value >= 0 AND $value <= 150', ...)
  """
  return FieldDefinition(
    name=name,
    type=field_type,
    assertion=assertion,
    default=default,
    value=value,
    permissions=permissions,
    readonly=readonly,
    flexible=flexible,
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
