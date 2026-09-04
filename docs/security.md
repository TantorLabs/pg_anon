# Security
> [🏠 Home](../README.md#-documentation-index) | [💾 Dump](operations/dump.md) | [📂 Restore](operations/restore.md) | [⚙️ How it works](how-it-works.md) | [💬 FAQ](faq.md)


pg_anon masks the table *data* you select, but a dump also carries schema objects that can
leak secrets or personal data by themselves — foreign-server credentials, function bodies,
planner statistics. This page explains what pg_anon handles by default, what it only warns
about, and how to clean a dump yourself. For the limits of masking itself, see
[Security & limitations](../README.md#-security--limitations) in the README.

---

## What else can leak

Besides table data, a dump carries schema objects. Some can hold credentials or personal
data that masking does not change. pg_anon removes what it safely can without breaking the
restore, warns about the rest, and leaves the final call to you.

| Object | Risk | pg_anon default | What you can do |
|---|---|---|---|
| FDW user mapping (`USER MAPPING`) | Remote-server user/password | Dump **blocked** when credentials are visible; mappings **stripped** on restore | [`--allow-fdw-credentials`](#foreign-data-wrappers-fdw) / [`--keep-fdw-user-mappings`](#foreign-data-wrappers-fdw) |
| FDW server (`SERVER OPTIONS`) | Internal host/port addresses | **Warned** at dump time; kept in the dump | Strip manually — see [Maximum cleanup](#maximum-cleanup-manual) |
| Subscriptions | Publisher password in `CONNECTION` string | Excluded via `pg_dump --no-subscriptions` (pg_dump ≥ 10) | — |
| Planner statistics (PG 18+) | Real (unmasked) column values | Excluded via `pg_dump --no-statistics` (pg_dump ≥ 18) | — |
| Function / procedure / trigger bodies | Hard-coded secrets or personal data | **Warned** at dump time; restored as-is | Review bodies; exclude their schema if needed |

pg_anon does not inspect other objects — security labels, GUC settings (`ALTER
ROLE/DATABASE ... SET`), comments, or extension table data. If any of them may hold
secrets, review them yourself: exclude suspicious schemas with a
[tables dictionary](dicts/tables-dictionary.md) or `--exclude-schema`, and restore the dump
structure onto a throwaway instance to check what it contains before you share it.

---

## Foreign Data Wrappers (FDW)

FDW `CREATE USER MAPPING` statements must carry `OPTIONS` (for `oracle_fdw`, a `user` and
usually a `password`). What ends up in the dump depends on whether the **dumping role can
see** those options:

| Dumping role | In the dump | Restore | Side effect |
|---|---|---|---|
| superuser / mapping owner | `USER MAPPING ... OPTIONS (user '…', password '…')` | works | **remote-server credentials leak into the dump** |
| unprivileged | `USER MAPPING ... ;` (no `OPTIONS`) | **fails** with `missing required option "user"` | no credentials, but the restore breaks |

This is `pg_dump` behavior, not a pg_anon bug — pg_anon inherits it. Its defaults cover both
cases:

**At dump time — block credential leaks.** If any FDW user mapping exposes `OPTIONS` visible
to the dumping role, the dump is **refused** (error `CREDENTIALS_LEAK`): pg_dump would
otherwise write the remote credentials into the archive. Two ways forward:

- Dump with a **less-privileged role** that owns the foreign server (so the dump stays
  self-contained, keeping `CREATE SERVER`) but cannot read the mapping `OPTIONS`. The
  minimal grants for such a role: `USAGE` on the dumped schemas, `SELECT` on their tables
  and sequences, and `USAGE` + `EXECUTE` on the `anon_funcs` schema after `init`.
- Or pass **`--allow-fdw-credentials`** to include them intentionally (an informed
  decision; the dump logs a warning that credentials are inside).

Independently, whenever a foreign server exists pg_anon warns that its `SERVER OPTIONS`
(remote host/port of your infrastructure) go into the dump. Those are not stripped
automatically — removing a `SERVER` would break the `FOREIGN TABLE`s that depend on it.
Strip them manually if they matter ([Maximum cleanup](#maximum-cleanup-manual)).

**At restore time — strip mappings.** By default pg_anon removes `USER MAPPING` entries from
the restore. This fixes the unprivileged-dump failure automatically, and it also means that
even a dump taken with `--allow-fdw-credentials` does not recreate the credentials in the
target. Pass **`--keep-fdw-user-mappings`** to restore them (a mapping without `OPTIONS`
still fails on wrappers that require them — that is the original `pg_dump` behavior).

Foreign servers (`SERVER`) and foreign tables (`FOREIGN TABLE`) are always kept, so querying
a foreign table in the restored copy needs its mapping recreated by hand (or a restore with
`--keep-fdw-user-mappings`).

---

## Maximum cleanup (manual)

To fully remove infrastructure objects and keep the dump reliable, do it by hand:

1. `pg_anon dump` — a full dump.
2. `pg_anon sync-struct-restore` — restore **only the structure** into a throwaway database.
3. Clean the throwaway database: drop FDW objects, functions, triggers, extensions, GUC
   settings — anything that must not be shared.
4. `pg_anon sync-struct-dump` — dump the cleaned structure.
5. Replace `pre_data.backup` / `post_data.backup` in the original dump with the cleaned ones.

This cannot be automated in general (the objects are not known in advance), but it can be
scripted in CI/CD for a specific database.
