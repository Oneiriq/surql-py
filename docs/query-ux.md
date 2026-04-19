# Query UX Helpers

surql 1.5.0 added a layer of first-class helpers that replace hand-written SurrealQL strings for the most common call sites. Each helper returns a `SurrealFn` wrapper or a plain function that composes with the existing `Query` builder, the CRUD helpers, and raw `client.execute()` calls.

Every example below shows the raw SurrealQL you used to have to write and the typed equivalent.

## Why bother?

- **Correctness under v3** — `type::record` vs `type::thing`, `count()` vs `count(*)`, explicit `<datetime>` casts, and `GROUP ALL` are all easy to forget. The helpers emit the correct form by default.
- **Composability** — helpers return `SurrealFn` instances, so they slot into `Query.select([...])`, `Query.set(**kwargs)`, `aggregate_records(select={...})`, and `client.execute(params=...)` without double-escaping.
- **Refactorability** — renaming a field no longer means a grep across string-concatenated SurrealQL; the helpers take parameters, not literals.

## `type_record` / `type_thing`

Build `type::record('table', id)` / `type::thing('table', id)` expressions without string concatenation.

```python
from surql import type_record, type_thing, RecordID

# Before
raw = "type::record('user', 'alice')"

# After
type_record('user', 'alice').to_surql()
# -> "type::record('user', 'alice')"

# Integers render unquoted
type_record('post', 42).to_surql()
# -> "type::record('post', 42)"

# RecordID and SurrealFn arguments render verbatim (no double-quoting)
type_record('edge', RecordID(table='user', id='alice')).to_surql()
# -> "type::record('edge', user:alice)"

# Legacy v2 form, still accepted by v3
type_thing('user', 'alice').to_surql()
# -> "type::thing('user', 'alice')"
```

Prefer `type_record` for new code (see [v3 patterns](v3-patterns.md#record-id-construction-typerecord-not-typething)); `type_thing` exists for callers that still target v2 servers.

## Function factories (`SurrealFn`)

The `surql.query.functions` module exposes factories that wrap common SurrealDB built-ins as `SurrealFn` values. Every factory is re-exported from the package root.

### Time

| Helper | Renders as |
| --- | --- |
| `time_now_fn()` | `time::now()` |

```python
from surql import Query, time_now_fn, update

# Before
await client.execute(
  'UPDATE user:alice SET last_seen = time::now()'
)

# After (builder)
sql = (
  Query()
    .update('user:alice')
    .set(last_seen=time_now_fn())
    .to_surql()
)
# -> UPDATE user:alice SET last_seen = time::now()
```

### Math

| Helper | Renders as |
| --- | --- |
| `math_mean_fn(field)` | `math::mean(<field>)` |
| `math_sum_fn(field)` | `math::sum(<field>)` |
| `math_min_fn(field)` | `math::min(<field>)` |
| `math_max_fn(field)` | `math::max(<field>)` |
| `math_ceil_fn(field)` | `math::ceil(<field>)` |
| `math_floor_fn(field)` | `math::floor(<field>)` |
| `math_round_fn(field, precision=None)` | `math::round(<field>[, <precision>])` |
| `math_abs_fn(field)` | `math::abs(<field>)` |

```python
from surql import Query, aggregate_records, math_mean_fn, math_sum_fn

# Before
await client.execute(
  'SELECT math::mean(score) AS avg_score, '
  'math::sum(score) AS total_score FROM run '
  'GROUP ALL'
)

# After
await aggregate_records(
  table='run',
  select={
    'avg_score': math_mean_fn('score'),
    'total_score': math_sum_fn('score'),
  },
  group_all=True,
)
```

### Strings

| Helper | Renders as |
| --- | --- |
| `string_len(field)` | `string::len(<field>)` |
| `string_concat(*parts)` | `string::concat(<parts...>)` |
| `string_lower(field)` | `string::lowercase(<field>)` |
| `string_upper(field)` | `string::uppercase(<field>)` |

```python
from surql import Query, string_concat, string_upper

sql = (
  Query()
    .update('user:alice')
    .set(display_name=string_concat(string_upper('first_name'), "' '", 'last_name'))
    .to_surql()
)
# -> UPDATE user:alice SET display_name = string::concat(string::uppercase(first_name), ' ', last_name)
```

### `count_if`

`count_if(predicate)` renders SurrealDB's `count(<predicate>)` aggregate. Pass `None` or omit to get bare `count()` (v3 rejects `count(*)`).

```python
from surql import aggregate_records, count_if

# Before
await client.execute(
  "SELECT count(status = 'active') AS active_count FROM user GROUP ALL"
)

# After
await aggregate_records(
  table='user',
  select={'active_count': count_if("status = 'active'")},
  group_all=True,
)
```

## Result extraction aliases

SurrealDB returns three shapes depending on the call path:

- Direct records: `[{"id": "...", ...}, ...]`
- Wrapped envelope: `[{"result": [...], "time": "..."}]`
- Scalar aggregate: `[{"count": 5}]`

The extraction helpers normalise all three.

| Helper | Description |
| --- | --- |
| `extract_result(result)` | Return list of records (all shapes). |
| `extract_many(result)` | Alias for `extract_result` that reads naturally next to `extract_one`. |
| `extract_one(result)` | Return the first record, or `None`. |
| `extract_scalar(result, key, default)` | Return the named scalar from an aggregate result. |
| `has_results(result)` | `True` if the response contains any row. |
| `has_result(result)` | Alias for `has_results`. |

```python
from surql import extract_many, extract_one, has_result

raw = await client.execute('SELECT * FROM user WHERE active = true')

if has_result(raw):
  users = extract_many(raw)
  first = extract_one(raw)
```

Prefer the aliased names (`extract_many`, `has_result`) in new code — they read more naturally next to `extract_one` and `extract_scalar`. The originals remain exported for backwards compatibility.

## `aggregate_records`

`aggregate_records(table, select, group_by=None, group_all=False, where=None)` runs a typed `SELECT ... GROUP BY | GROUP ALL` query and returns rows as plain dicts, hiding the response envelope.

```python
from surql import aggregate_records, count_if, math_sum_fn

# Before
raw = await client.execute("""
  SELECT network,
         count() AS count,
         math::sum(strength) AS total_strength
  FROM memory_entry
  GROUP BY network
""")
rows = extract_result(raw)

# After
rows = await aggregate_records(
  table='memory_entry',
  select={
    'count': count_if(),
    'total_strength': math_sum_fn('strength'),
  },
  group_by=['network'],
)
```

Rules:

- `select` is a mapping of output alias -> projection. Values may be `SurrealFn`, `FunctionExpression`, or raw SurrealQL strings.
- Exactly one of `group_by=[...]` or `group_all=True` must be provided.
- `where` accepts a `str` or an `Operator` and renders as the `WHERE` clause.
- Results come back as a list of dicts, already unwrapped from the SurrealDB envelope.

## Builder API extensions

The `Query` builder gained a small set of ergonomic affordances in 1.5.0.

### `Query.set(**fields)`

Populate the `SET` clause of `UPDATE` / `UPSERT` / `INSERT` without a dict literal. Values may be any literal, plus `SurrealFn` / `Expression` instances for raw SurrealQL.

```python
from surql import Query, time_now_fn

# Before
Query().update('user:alice', {'status': 'active', 'last_seen': time_now_fn()})

# After
(
  Query()
    .update('user:alice')
    .set(status='active', last_seen=time_now_fn())
)
```

### `Query.update(target, data=None)`

`data` is now optional. When omitted, the builder defers the `SET` payload so `.set(**fields)` can populate it incrementally.

```python
from surql import Query

(
  Query()
    .update('user:alice')         # no data argument
    .set(status='active')          # fluent set
    .where('age >= 18')            # chain additional clauses
)
```

### `Query.select(fields)` accepts expressions

Each item in the projection list may now be:

- a raw field name (`'name'`),
- a pre-rendered SurrealQL fragment (`'count()'`),
- an `Expression` instance,
- or a `SurrealFn` instance (rendered verbatim via `to_surql()`).

```python
from surql import Query, count_if, math_mean_fn, as_

sql = (
  Query()
    .select([
      as_(count_if(), 'active_count'),
      as_(math_mean_fn('score'), 'avg_score'),
    ])
    .from_table('user')
    .where("status = 'active'")
    .group_all()
    .to_surql()
)
```

## Compatibility

Everything on this page is additive — the pre-1.5 APIs (`extract_result`, `has_results`, dict-only `Query.update(target, data)`, string-only `Query.select(fields)`) continue to work unchanged. Mix raw SurrealQL and typed helpers freely; they share the same quoting pipeline.

## Further reading

- [SurrealDB v3 patterns](v3-patterns.md) — the v3 forms these helpers emit by default
- [Migration notes](migration.md) — upgrading existing call sites to the new helpers
- [Query Builder](queries.md) — the underlying `Query` API
