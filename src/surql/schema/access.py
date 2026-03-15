"""Access control schema definition functions.

This module provides functions for defining DEFINE ACCESS schemas for SurrealDB
access control, supporting JWT and RECORD access types.
"""

import enum

from pydantic import BaseModel, ConfigDict, model_validator


class AccessType(str, enum.Enum):
  """Access types for SurrealDB DEFINE ACCESS.

  Defines the type of access control mechanism.
  """

  JWT = 'JWT'
  RECORD = 'RECORD'


class JwtConfig(BaseModel):
  """Immutable JWT access configuration.

  Args:
    algorithm: JWT signing algorithm (HS256, HS384, HS512, RS256, etc.)
    key: Symmetric key for HMAC algorithms
    url: JWKS endpoint URL for key discovery
    issuer: Expected token issuer claim
  """

  algorithm: str = 'HS256'
  key: str | None = None
  url: str | None = None
  issuer: str | None = None

  model_config = ConfigDict(frozen=True)


class RecordAccessConfig(BaseModel):
  """Immutable RECORD access configuration.

  Args:
    signup: SurrealQL expression for user signup
    signin: SurrealQL expression for user signin
  """

  signup: str | None = None
  signin: str | None = None

  model_config = ConfigDict(frozen=True)


class AccessDefinition(BaseModel):
  """Immutable access control schema definition.

  Represents a DEFINE ACCESS statement for SurrealDB.

  Examples:
    JWT access:
    >>> access = AccessDefinition(
    ...   name='api',
    ...   type=AccessType.JWT,
    ...   jwt=JwtConfig(algorithm='HS256', key='secret'),
    ... )

    RECORD access:
    >>> access = AccessDefinition(
    ...   name='user_auth',
    ...   type=AccessType.RECORD,
    ...   record=RecordAccessConfig(
    ...     signup='CREATE user SET email = $email, pass = crypto::argon2::generate($pass)',
    ...     signin='SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass)',
    ...   ),
    ... )
  """

  name: str
  type: AccessType
  jwt: JwtConfig | None = None
  record: RecordAccessConfig | None = None
  duration_session: str | None = None
  duration_token: str | None = None

  model_config = ConfigDict(frozen=True)

  @model_validator(mode='after')
  def _validate_config_matches_type(self) -> 'AccessDefinition':
    """Validate that config matches the access type."""
    if self.type == AccessType.JWT and self.jwt is None:
      raise ValueError('JWT access type requires jwt config')
    if self.type == AccessType.RECORD and self.record is None:
      raise ValueError('RECORD access type requires record config')
    return self


# Builder functions


def access_schema(
  name: str,
  *,
  type: AccessType,
  jwt: JwtConfig | None = None,
  record: RecordAccessConfig | None = None,
  duration_session: str | None = None,
  duration_token: str | None = None,
) -> AccessDefinition:
  """Create an access control schema definition.

  Pure function to create an immutable access definition.

  Args:
    name: Access definition name
    type: Access type (JWT or RECORD)
    jwt: JWT configuration (required when type is JWT)
    record: Record access configuration (required when type is RECORD)
    duration_session: Session duration (e.g., '24h', '7d')
    duration_token: Token duration (e.g., '15m')

  Returns:
    Immutable AccessDefinition instance
  """
  return AccessDefinition(
    name=name,
    type=type,
    jwt=jwt,
    record=record,
    duration_session=duration_session,
    duration_token=duration_token,
  )


def jwt_access(
  name: str,
  *,
  algorithm: str = 'HS256',
  key: str | None = None,
  url: str | None = None,
  issuer: str | None = None,
  duration_session: str | None = None,
  duration_token: str | None = None,
) -> AccessDefinition:
  """Create a JWT access definition.

  Convenience function for creating JWT-type access controls.

  Args:
    name: Access definition name
    algorithm: JWT signing algorithm (default HS256)
    key: Symmetric key for HMAC algorithms
    url: JWKS endpoint URL
    issuer: Expected token issuer claim
    duration_session: Session duration (e.g., '24h')
    duration_token: Token duration (e.g., '15m')

  Returns:
    Immutable AccessDefinition with JWT type
  """
  return access_schema(
    name,
    type=AccessType.JWT,
    jwt=JwtConfig(algorithm=algorithm, key=key, url=url, issuer=issuer),
    duration_session=duration_session,
    duration_token=duration_token,
  )


def record_access(
  name: str,
  *,
  signup: str | None = None,
  signin: str | None = None,
  duration_session: str | None = None,
  duration_token: str | None = None,
) -> AccessDefinition:
  """Create a RECORD access definition.

  Convenience function for creating RECORD-type access controls.

  Args:
    name: Access definition name
    signup: SurrealQL expression for user signup
    signin: SurrealQL expression for user signin
    duration_session: Session duration (e.g., '24h')
    duration_token: Token duration (e.g., '15m')

  Returns:
    Immutable AccessDefinition with RECORD type
  """
  return access_schema(
    name,
    type=AccessType.RECORD,
    record=RecordAccessConfig(signup=signup, signin=signin),
    duration_session=duration_session,
    duration_token=duration_token,
  )
