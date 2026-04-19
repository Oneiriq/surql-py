# SurrealDB v3 Patterns

surql 1.4.0 introduced the SurrealQL forms required by SurrealDB v3. The v3 engine is stricter than v2 about datetime coercion, count aggregates, record-ID construction, transaction batching, and DDL idempotence. This page documents the patterns the library emits and explains the forms you should reach for in any raw SurrealQL you still hand-write.

All examples run cleanly against the v3 integration CI (`surrealdb/surrealdb:v3.0.5`, see the `v3-integration` workflow).

## Datetime cast on insert

v3 no longer coerces bare ISO-8601 strings into `datetime` values. Any column typed `datetime` requires an explicit `<datetime>` cast at the call site.

**Before (v2-only):**

```surql
CREATE _migration_history SET
  version = '20260102_120000',
  applied_at = '2026-01-02T12:00:00Z';
```

**Now (v3-compatible):**

```surql
CREATE _migration_history SET
  version = '20260102_120000',
  applied_at = <datetime> $applied_at;
```

Driven from Python, this is handled automatically by the migration recorder:

```python
await client.execute(
  'CREATE _migration_history SET '
  'version = $version, applied_at = <datetime> $applied_at',
  params={'version': version, 'applied_at': '2026-01-02T12:00:00Z'},
)
```

## `count()` aggregates require `GROUP ALL`

v3 rejects `count(*)` and also rejects a bare `SELECT count() FROM table` without an explicit grouping. Always append `GROUP ALL` for full-table counts and use `count()` (no `*`).

**Before:**

```surql
SELECT count(*) AS total FROM user;
```

**Now:**

```surql
SELECT count() AS total FROM user GROUP ALL;
```

The query builder emits the correct form out of the box:

```python
from surql import Query, count_records

# CRUD helper, used by SurrealDB v3
await count_records('user')  # -> SELECT count() AS count FROM user GROUP ALL

# Builder, explicit
(
  Query()
  .select(['count()'])
  .from_table('user')
  .group_all()
)
```

`count_if(predicate)` renders `count(<predicate>)` for conditional counts (see [Query UX helpers](query-ux.md#count_if)).

## Record-ID construction: `type::record`, not `type::thing`

`type::thing('table', 'id')` was renamed to `type::record('table', 'id')` in v3. The `type::thing` name still resolves for backwards compatibility but new code should use `type::record` directly.

**Prefer:**

```python
from surql import type_record

ref = type_record('user', 'alice').to_surql()
# -> type::record('user', 'alice')
```

`type_thing()` is still exported for code that must target older servers, and generates the exact v2 form:

```python
from surql import type_thing

type_thing('user', 'alice').to_surql()
# -> type::thing('user', 'alice')
```

See [Query UX helpers](query-ux.md#type_record-type_thing) for the full helper surface, including composition with `RecordID`, integers, and nested `SurrealFn` arguments.

## Buffered `BEGIN`/`COMMIT` transactions

The v3 RPC protocol dispatches each `client.execute()` call as a separate statement. `BEGIN TRANSACTION; ...; COMMIT TRANSACTION;` split across three round trips crashes v3 because later statements run outside the transaction scope and the trailing `COMMIT` has nothing to commit.

`DatabaseClient.execute()` now batches `BEGIN`/`COMMIT` and all statements between them into a single RPC frame. Callers don't have to change anything — the `transaction()` context manager and the migration executor do the right thing automatically:

```python
from surql import get_client, transaction

async with get_client(config) as client:
  async with transaction(client):
    await client.execute('UPDATE user:alice SET credits -= 10')
    await client.execute('UPDATE user:bob   SET credits += 10')
  # Emitted as a single BEGIN; ...; COMMIT; RPC.
```

Embedded engines (`mem://`, `file://`, `surrealkv://`) remain transactional-in-process and skip the wrapper entirely (see `CHANGES` 1.3.1).

## `IF NOT EXISTS` on DDL

v3 treats repeat DDL as an error unless the statement is idempotent. surql emits `DEFINE TABLE ... IF NOT EXISTS`, `DEFINE INDEX ... IF NOT EXISTS`, and `DEFINE FIELD ... IF NOT EXISTS` whenever the generator's `if_not_exists=True` flag is set (the default for the migration history table and the recommended setting for schema generation).

```python
from surql.schema.table import generate_table_sql

sql = generate_table_sql(user_schema, if_not_exists=True)
# DEFINE TABLE IF NOT EXISTS user SCHEMAFULL;
# DEFINE FIELD IF NOT EXISTS email ON user TYPE string ...;
# DEFINE INDEX IF NOT EXISTS email_idx ON TABLE user COLUMNS email UNIQUE;
```

The `_migration_history` bootstrap in `ensure_migration_table()` uses this form unconditionally so `surql migrate up` is safe to run repeatedly on a schema-already-bootstrapped database.

## Graph-depth literals

v3 rejects grouped graph-depth syntax such as `->follows{1..3}->user`. Expand to literal hop lists:

```python
from surql import traverse

# surql unrolls depth ranges into literal hop unions that v3 accepts.
await traverse('user:alice', '->follows->user', depth=(1, 3), client=client)
```

## v3 integration CI

The `v3-integration.yml` workflow spins up `surrealdb/surrealdb:v3.0.5` and runs the integration suite on every push. The same suite runs nightly against the latest `surrealdb/surrealdb:latest` image to flag upstream drift early. Opt into the local v3 container via:

```bash
export SURQL_PRE_PUSH_INTEGRATION=1
docker run -d -p 8000:8000 --name surrealdb \
  surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
```

Wire the pre-push hook once per clone so the same checks run before every `git push`:

```bash
git config core.hooksPath .githooks
```

See [CONTRIBUTING.md](https://github.com/Oneiriq/surql-py/blob/main/CONTRIBUTING.md) for the full pre-push setup.

## Further reading

- [Query UX helpers](query-ux.md) — the typed helper surface that emits v3-correct SurrealQL without raw strings
- [Migration notes](migration.md) — upgrading existing v1.3.x code to v1.4.x / v1.5.x
- [Migrations](migrations.md) — the migration system, including the buffered-transaction wrapper
