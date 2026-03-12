"""Reserved word validation for SurrealDB field names.

Provides detection and warning for field names that collide with SurrealDB
reserved words to help users avoid unexpected query behavior.
"""

SURREAL_RESERVED_WORDS: frozenset[str] = frozenset(
  {
    'select',
    'from',
    'where',
    'group',
    'order',
    'limit',
    'start',
    'fetch',
    'timeout',
    'parallel',
    'value',
    'content',
    'set',
    'create',
    'update',
    'delete',
    'relate',
    'insert',
    'define',
    'remove',
    'begin',
    'commit',
    'cancel',
    'return',
    'let',
    'if',
    'else',
    'then',
    'end',
    'for',
    'break',
    'continue',
    'throw',
    'none',
    'null',
    'true',
    'false',
    'and',
    'or',
    'not',
    'is',
    'contains',
    'inside',
    'outside',
    'intersects',
    'type',
    'table',
    'field',
    'index',
    'event',
    'namespace',
    'database',
    'scope',
    'token',
    'info',
    'live',
    'kill',
    'sleep',
    'use',
    'in',
    'out',
  }
)

EDGE_ALLOWED_RESERVED: frozenset[str] = frozenset({'in', 'out'})


def check_reserved_word(
  name: str,
  *,
  allow_edge_fields: bool = False,
) -> str | None:
  """Check if a field name collides with a SurrealDB reserved word.

  Performs case-insensitive matching against the set of known SurrealDB
  reserved words. Returns a warning message string if the name is reserved,
  or None if the name is safe to use.

  Args:
    name: Field name to check (supports dot-notation; only the leaf segment
      is checked)
    allow_edge_fields: If True, 'in' and 'out' are permitted (for edge schemas)

  Returns:
    Warning message string if the name is a reserved word, None otherwise
  """
  # For dot-notation names, check only the leaf segment
  leaf = name.split('.')[-1]
  lower = leaf.lower()

  if lower not in SURREAL_RESERVED_WORDS:
    return None

  if allow_edge_fields and lower in EDGE_ALLOWED_RESERVED:
    return None

  return (
    f'Field name {name!r} collides with SurrealDB reserved word {lower!r}. '
    'This may cause unexpected query behavior.'
  )
