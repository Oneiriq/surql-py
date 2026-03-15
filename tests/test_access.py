"""Tests for access control schema definitions and SQL generation."""

import pytest
from pydantic import ValidationError

from surql.schema.access import (
  AccessType,
  JwtConfig,
  RecordAccessConfig,
  access_schema,
  jwt_access,
  record_access,
)
from surql.schema.sql import generate_access_sql


class TestAccessSchema:
  """Tests for access_schema builder function."""

  def test_jwt_access_generates_correct_sql(self) -> None:
    """JWT access definition produces correct DEFINE ACCESS SQL."""
    result = access_schema(
      'api',
      type=AccessType.JWT,
      jwt=JwtConfig(algorithm='HS256', key='secret'),
    )
    stmts = generate_access_sql(result)

    assert stmts[0] == "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';"

  def test_record_access_generates_correct_sql(self) -> None:
    """RECORD access definition produces correct DEFINE ACCESS SQL."""
    result = access_schema(
      'user_auth',
      type=AccessType.RECORD,
      record=RecordAccessConfig(
        signup='CREATE user SET ...', signin='SELECT * FROM user WHERE ...'
      ),
    )
    stmts = generate_access_sql(result)

    assert 'TYPE RECORD' in stmts[0]
    assert 'SIGNUP (CREATE user SET ...)' in stmts[0]
    assert 'SIGNIN (SELECT * FROM user WHERE ...)' in stmts[0]

  def test_duration_fields_appear_in_sql(self) -> None:
    """Session and token durations appear in generated SQL."""
    result = access_schema(
      'api',
      type=AccessType.JWT,
      jwt=JwtConfig(key='secret'),
      duration_session='24h',
      duration_token='15m',
    )
    stmts = generate_access_sql(result)

    assert 'DURATION FOR SESSION 24h, FOR TOKEN 15m' in stmts[0]

  def test_definition_is_immutable(self) -> None:
    """AccessDefinition is frozen and cannot be mutated."""
    result = access_schema(
      'api',
      type=AccessType.JWT,
      jwt=JwtConfig(key='secret'),
    )

    with pytest.raises(ValidationError):
      result.name = 'other'  # type: ignore[misc]


class TestJwtAccess:
  """Tests for jwt_access convenience function."""

  def test_creates_jwt_with_key(self) -> None:
    """Creates JWT access with symmetric key."""
    result = jwt_access('api', key='my-secret')

    assert result.type == AccessType.JWT
    assert result.jwt is not None
    assert result.jwt.algorithm == 'HS256'
    assert result.jwt.key == 'my-secret'

  def test_creates_jwt_with_url(self) -> None:
    """Creates JWT access with JWKS URL."""
    result = jwt_access('api', url='https://auth.example.com/.well-known/jwks.json')

    assert result.jwt is not None
    assert result.jwt.url == 'https://auth.example.com/.well-known/jwks.json'

  def test_creates_jwt_with_custom_algorithm(self) -> None:
    """Creates JWT access with non-default algorithm."""
    result = jwt_access('api', algorithm='RS256', url='https://auth.example.com/jwks')

    assert result.jwt is not None
    assert result.jwt.algorithm == 'RS256'

  def test_creates_jwt_with_issuer(self) -> None:
    """Creates JWT access with issuer claim."""
    result = jwt_access('api', key='secret', issuer='https://auth.example.com')

    assert result.jwt is not None
    assert result.jwt.issuer == 'https://auth.example.com'

  def test_creates_jwt_with_durations(self) -> None:
    """Creates JWT access with session and token durations."""
    result = jwt_access('api', key='secret', duration_session='24h', duration_token='15m')

    assert result.duration_session == '24h'
    assert result.duration_token == '15m'


class TestRecordAccess:
  """Tests for record_access convenience function."""

  def test_creates_record_with_signup_signin(self) -> None:
    """Creates RECORD access with signup and signin expressions."""
    result = record_access(
      'user_auth',
      signup='CREATE user SET email = $email',
      signin='SELECT * FROM user WHERE email = $email',
    )

    assert result.type == AccessType.RECORD
    assert result.record is not None
    assert result.record.signup == 'CREATE user SET email = $email'
    assert result.record.signin == 'SELECT * FROM user WHERE email = $email'

  def test_creates_record_with_durations(self) -> None:
    """Creates RECORD access with session and token durations."""
    result = record_access(
      'user_auth',
      signup='CREATE user SET email = $email',
      duration_session='7d',
      duration_token='1h',
    )

    assert result.duration_session == '7d'
    assert result.duration_token == '1h'


class TestAccessValidation:
  """Tests for access definition validation."""

  def test_jwt_type_without_jwt_config_raises(self) -> None:
    """Raises ValueError when JWT type has no jwt config."""
    with pytest.raises(ValueError, match='JWT access type requires jwt config'):
      access_schema('api', type=AccessType.JWT)

  def test_record_type_without_record_config_raises(self) -> None:
    """Raises ValueError when RECORD type has no record config."""
    with pytest.raises(ValueError, match='RECORD access type requires record config'):
      access_schema('user_auth', type=AccessType.RECORD)


class TestGenerateAccessSql:
  """Tests for generate_access_sql function."""

  def test_jwt_with_key(self) -> None:
    """Generates DEFINE ACCESS for JWT with symmetric key."""
    access = jwt_access('api', key='secret')

    stmts = generate_access_sql(access)

    assert len(stmts) == 1
    assert stmts[0] == "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';"

  def test_jwt_with_jwks_url(self) -> None:
    """Generates DEFINE ACCESS for JWT with JWKS URL."""
    access = jwt_access('api', algorithm='RS256', url='https://auth.example.com/jwks')

    stmts = generate_access_sql(access)

    assert stmts[0] == (
      "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM RS256 URL 'https://auth.example.com/jwks';"
    )

  def test_jwt_with_issuer(self) -> None:
    """Generates DEFINE ACCESS for JWT with issuer."""
    access = jwt_access('api', key='secret', issuer='https://auth.example.com')

    stmts = generate_access_sql(access)

    assert "WITH ISSUER 'https://auth.example.com'" in stmts[0]

  def test_record_with_signup_signin(self) -> None:
    """Generates DEFINE ACCESS for RECORD with signup and signin."""
    access = record_access(
      'user_auth',
      signup='CREATE user SET email = $email, pass = crypto::argon2::generate($pass)',
      signin='SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass)',
    )

    stmts = generate_access_sql(access)

    assert 'TYPE RECORD' in stmts[0]
    assert (
      'SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass))' in stmts[0]
    )
    assert (
      'SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass))'
      in stmts[0]
    )

  def test_duration_session_only(self) -> None:
    """Generates DURATION clause with session only."""
    access = jwt_access('api', key='secret', duration_session='24h')

    stmts = generate_access_sql(access)

    assert 'DURATION FOR SESSION 24h' in stmts[0]

  def test_duration_token_only(self) -> None:
    """Generates DURATION clause with token only."""
    access = jwt_access('api', key='secret', duration_token='15m')

    stmts = generate_access_sql(access)

    assert 'DURATION FOR TOKEN 15m' in stmts[0]

  def test_duration_session_and_token(self) -> None:
    """Generates DURATION clause with both session and token."""
    access = jwt_access('api', key='secret', duration_session='24h', duration_token='15m')

    stmts = generate_access_sql(access)

    assert 'DURATION FOR SESSION 24h, FOR TOKEN 15m' in stmts[0]

  def test_record_with_durations(self) -> None:
    """Generates full RECORD access with durations."""
    access = record_access(
      'user_auth',
      signup='CREATE user SET email = $email',
      signin='SELECT * FROM user WHERE email = $email',
      duration_session='7d',
      duration_token='1h',
    )

    stmts = generate_access_sql(access)

    expected = (
      'DEFINE ACCESS user_auth ON DATABASE TYPE RECORD'
      ' SIGNUP (CREATE user SET email = $email)'
      ' SIGNIN (SELECT * FROM user WHERE email = $email)'
      ' DURATION FOR SESSION 7d, FOR TOKEN 1h;'
    )
    assert stmts[0] == expected

  def test_no_duration_clauses(self) -> None:
    """Omits DURATION when not specified."""
    access = jwt_access('api', key='secret')

    stmts = generate_access_sql(access)

    assert 'DURATION' not in stmts[0]
