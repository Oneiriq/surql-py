# Upgrade Notes

This page covers upgrade steps from each recent release. Each section lists the shipping changes and the call-site updates most projects will want to make. Nothing in 1.4.x or 1.5.x is a breaking API change; everything is additive or strictly a bug fix, so upgrades should be drop-in.

Releases:

- [1.5.1 -- docs refresh](#151-docs-refresh)
- [1.5.0 -- query UX](#150-query-ux)
- [1.4.0 -- SurrealDB v3 support](#140-surrealdb-v3-support)
- [1.3.1 -- embedded migration fix](#131-embedded-migration-fix)

## 1.5.1 -- docs refresh

Patch release. No code changes; docs-site only.

- New pages: [SurrealDB v3 patterns](v3-patterns.md), [Query UX helpers](query-ux.md), and this upgrade guide.
- CLI reference extended to cover `surql orchestrate` subcommands.
- README examples rewritten to use the 1.5.0 helpers.
- `__version__` aligned to `pyproject.toml`.

No action required.

## 1.5.0 -- query UX

Additive. All previous call shapes keep working.

### What's new

- **Record-ID helpers**: `type_record(table, id)` / `type_thing(table, id)` return `SurrealFn` wrappers for the two SurrealDB record-construction builtins.
- **Function factories**: `time_now_fn`, `math_mean_fn`, `math_sum_fn`, `math_min_fn`, `math_max_fn`, `math_ceil_fn`, `math_floor_fn`, `math_round_fn`, `math_abs_fn`, `string_len`, `string_concat`, `string_lower`, `string_upper`, `count_if`. Each returns a `SurrealFn` that composes with `Query.set(...)`, `Query.select([...])`, and `aggregate_records(select={...})`.
- **Result helpers**: `extract_many` (alias for `extract_result`) and `has_result` (alias for `has_results`).
- **`aggregate_records`**: typed `SELECT ... GROUP BY | GROUP ALL` helper returning list-of-dicts.
- **Builder API**: `Query.set(**fields)`, `Query.update(target, data=None)`, and `Query.select(...)` now accepts `SurrealFn` / `Expression` projection items alongside strings.

### Recommended call-site updates

Before:

```python
await client.execute(
  "SELECT count(*) AS total, math::sum(amount) AS revenue "
  "FROM order WHERE status = 'paid' GROUP ALL"
)
```

After:

```python
from surql import aggregate_records, count_if, math_sum_fn

rows = await aggregate_records(
  table='order',
  select={
    'total': count_if(),
    'revenue': math_sum_fn('amount'),
  },
  where="status = 'paid'",
  group_all=True,
)
```

Before:

```python
await client.execute(
  "UPDATE user:alice SET status = 'active', last_seen = time::now()"
)
```

After:

```python
from surql import Query, time_now_fn

sql = (
  Query()
    .update('user:alice')
    .set(status='active', last_seen=time_now_fn())
    .to_surql()
)
await client.execute(sql)
```

See [Query UX helpers](query-ux.md) for the full surface with before/after side-by-side examples.

## 1.4.0 -- SurrealDB v3 support

Additive at the Python API level. The SurrealQL emitted by the library changed to forms that both v2 and v3 accept. No user-visible code changes are required; the behaviour differences below are worth being aware of.

### What's new

- **Datetime cast on `_migration_history`**: `record_migration()` now writes `applied_at = <datetime> $applied_at`.
- **Buffered `BEGIN`/`COMMIT`**: `DatabaseClient.execute()` batches transaction-scoped statements into a single RPC frame so v3 honours the commit.
- **`GROUP ALL` on `count_records`**: `count_records()` now appends `GROUP ALL` so v3 accepts the aggregate. The helper still accepts both the envelope shape and a bare scalar list from the SDK.
- **`type::record` over `type::thing`**: select record-ID targets route through `type::record(...)` on v3. `type_record` is now preferred over `type_thing` in new code.
- **Idempotent DDL**: `DEFINE TABLE _migration_history IF NOT EXISTS` (and the `if_not_exists` flag across the schema generator) so `surql migrate up` is safe to re-run.
- **Graph depth unrolling**: `traverse(...)` unrolls `{min..max}` depth ranges into literal hop unions that v3 accepts.
- **SDK pin**: minimum `surrealdb` SDK bumped to v2.0.0a1, which speaks v3's RPC protocol.
- **CI**: new `v3-integration` workflow runs the integration suite against `surrealdb/surrealdb:v3.0.5`; nightly matrix runs against `surrealdb/surrealdb:latest`.

### Recommended call-site updates

Nothing is required to keep 1.3.x code running. If you hand-write SurrealQL anywhere (raw `client.execute()` calls), audit for the v3-required forms:

- Replace `count(*)` with `count()` and add `GROUP ALL` to any full-table aggregate. See [v3 patterns: count() aggregates](v3-patterns.md#count-aggregates-require-group-all).
- Cast datetime literals explicitly: `<datetime> $value` or `<datetime> '2026-...'`. See [v3 patterns: datetime cast](v3-patterns.md#datetime-cast-on-insert).
- Prefer `type::record('table', id)` over `type::thing('table', id)` for new expressions. See [v3 patterns: record-ID construction](v3-patterns.md#record-id-construction-typerecord-not-typething).
- Ensure `BEGIN TRANSACTION; ...; COMMIT TRANSACTION;` is issued in a single `client.execute(...)` call, or use the `transaction()` context manager which already does so.

## 1.3.1 -- embedded migration fix

Bug-fix release for embedded engines (`mem://`, `memory://`, `file://`, `surrealkv://`).

- `execute_migration()` now detects embedded URL schemes and skips the `BEGIN TRANSACTION` / `COMMIT TRANSACTION` wrapper; the upstream SDK's `query()` returns an empty list for transaction-control statements in embedded mode, which previously surfaced as `IndexError: list index out of range`.
- Migrations remain effectively atomic in embedded mode because the engine lives in the host process; a crash during migration takes the process with it rather than leaving a partial schema.

No action required. Existing remote (`ws://`, `http://`) connections retain the transactional wrapper.
