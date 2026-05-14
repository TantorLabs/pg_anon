"""Shared assertion helpers for pg_anon test suites."""
from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.infrastructure.db import DBManager


def load_expected(test_file: str, name: str) -> dict:
    """Load expected result (Python dict literal) from expected/ dir next to test file."""
    path = Path(test_file).parent / "expected" / name
    return ast.literal_eval(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# Row-level checks
# --------------------------------------------------------------------------

async def check_rows(
    db_manager: DBManager,
    db_name: str,
    schema: str,
    table: str,
    fields: list[str] | None,
    expected_rows: list,
) -> bool:
    """Assert each expected_row has a matching row in the table. `"*"` matches any value."""
    if fields is None:
        query = f'SELECT * FROM "{schema}"."{table}" LIMIT 10000'
    else:
        fields_str = ", ".join(f'"{f}"' for f in fields)
        query = f'SELECT {fields_str} FROM "{schema}"."{table}" LIMIT 10000'

    db_rows = await db_manager.fetch(db_name, query)
    db_rows_prepared = [list(dict(row).values()) for row in db_rows]

    def cmp_two_rows(row_a: list, row_b: list) -> bool:
        if len(row_a) != len(row_b):
            return False
        return all(a == b or a == "*" for a, b in zip(row_a, row_b, strict=True))

    missing = [r for r in expected_rows if not any(cmp_two_rows(r, db_r) for db_r in db_rows_prepared)]
    if missing:
        for row in missing:
            print(f"check_rows: expected row {row} not found")
        print("-- first 10 actual rows --")
        for row in db_rows_prepared[:10]:
            print(row)
    return not missing


async def check_rows_count(
    db_manager: DBManager,
    db_name: str,
    objs: list[tuple[str, str, int]] | list[list],
) -> bool:
    """Assert exact row counts. `objs` items are `(schema, table, expected_count)`."""
    failed = []
    for schema, table, expected in objs:
        rows = await db_manager.fetch(db_name, f'SELECT count(1) FROM "{schema}"."{table}"')
        actual = rows[0][0]
        if actual != expected:
            failed.append((schema, table, expected, actual))
            print(f"check_rows_count: {schema}.{table} expected={expected} actual={actual}")
    return not failed


# --------------------------------------------------------------------------
# Schema-level comparisons
# --------------------------------------------------------------------------

async def list_tables(db_manager: DBManager, db_name: str) -> list[tuple[str, str]]:
    """Return sorted list of (schema, table) for every user table/partition."""
    rows = await db_manager.fetch(db_name, """
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2
    """)
    return [(r["nspname"], r["relname"]) for r in rows]


async def check_list_tables(
    db_manager: DBManager,
    db_name: str,
    expected: list[tuple[str, str]] | list[list[str]],
) -> bool:
    """Assert the target DB contains exactly the expected tables (no more, no less)."""
    actual = await list_tables(db_manager, db_name)
    expected_set: set[tuple[str, str]] = {(t[0], t[1]) for t in expected}
    actual_set = set(actual)
    missing = expected_set - actual_set
    extra = actual_set - expected_set
    for t in missing:
        print(f"check_list_tables: missing {t}")
    for t in extra:
        print(f"check_list_tables: unexpected {t}")
    return not missing and not extra


async def diff_schema(db_manager: DBManager, source: str, target: str) -> list[list]:
    """Return (schema, table, column) tuples that exist in `source` but not in `target`."""
    query = """
        SELECT n.nspname, c.relname, a.attname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE a.attnum > 0 AND NOT a.attisdropped
          AND c.relkind IN ('r', 'p')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2, a.attnum
    """
    src = [list(r.values()) for r in await db_manager.fetch(source, query)]
    tgt = [list(r.values()) for r in await db_manager.fetch(target, query)]
    return [row for row in src if row not in tgt]


async def diff_table_sample(db_manager: DBManager, source: str, target: str) -> tuple[list, list]:
    """For each column, pull up to 5 sample values from both DBs and diff.

    Returns (source_only, target_only) — rows present on one side but not the other.
    """
    query = """
        SELECT n.nspname, c.relname, a.attname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE a.attnum > 0 AND NOT a.attisdropped
          AND c.relkind IN ('r', 'p')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2, a.attnum
    """

    async def enrich(db: str) -> list:
        out = []
        for r in await db_manager.fetch(db, query):
            schema, table, col = r["nspname"], r["relname"], r["attname"]
            vals = await db_manager.fetch(db, f'SELECT "{col}" FROM "{schema}"."{table}" LIMIT 5')
            out.append([schema, table, col, [list(v.values()) for v in vals]])
        return out

    src = await enrich(source)
    tgt = await enrich(target)
    source_only = [r for r in src if r not in tgt]
    target_only = [r for r in tgt if r not in src]
    return source_only, target_only


# --------------------------------------------------------------------------
# Full-clone round-trip helpers (used by test_full_clone)
# --------------------------------------------------------------------------

_CATALOG_QUERIES: dict[str, str] = {
    "schemas": """
        SELECT nspname FROM pg_namespace
        WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast')
          AND nspname NOT LIKE 'pg_temp%' AND nspname NOT LIKE 'pg_toast_temp%'
        ORDER BY nspname
    """,
    "tables": """
        SELECT n.nspname, c.relname, c.relkind
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind IN ('r','p','v','m','f','S')
          AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        ORDER BY 1, 2
    """,
    "columns": """
        SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE a.attnum > 0 AND NOT a.attisdropped
          AND c.relkind IN ('r','p','v','m','f')
          AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        ORDER BY 1, 2, a.attnum
    """,
    "constraints": """
        SELECT n.nspname, cl.relname, co.conname, co.contype,
               pg_get_constraintdef(co.oid)
        FROM pg_constraint co
        JOIN pg_class cl ON co.conrelid = cl.oid
        JOIN pg_namespace n ON cl.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        ORDER BY 1, 2, 3
    """,
    "indexes": """
        SELECT schemaname, tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname NOT IN ('pg_catalog','information_schema','pg_toast')
        ORDER BY 1, 2, 3
    """,
    "functions": """
        SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        ORDER BY 1, 2, 3
    """,
    "types": """
        SELECT n.nspname, t.typname, t.typtype
        FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
          AND t.typtype IN ('e','c','d','r')
        ORDER BY 1, 2
    """,
    "triggers": """
        SELECT n.nspname, c.relname, t.tgname
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE NOT t.tgisinternal
        ORDER BY 1, 2, 3
    """,
    "policies": """
        SELECT schemaname, tablename, policyname, permissive, roles, cmd
        FROM pg_policies
        ORDER BY 1, 2, 3
    """,
    "sequences": """
        SELECT sequence_schema, sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema NOT IN ('pg_catalog','information_schema')
        ORDER BY 1, 2
    """,
}


async def diff_catalog(db_manager: DBManager, source: str, target: str) -> dict[str, list]:
    """Diff catalog objects between source and target DB.

    Returns a mapping { category -> [rows present in source but missing in target] }.
    Only reports the source→target direction: items unique to target indicate a restore
    bug too, but pg_anon's job is to make target contain at least what source had.
    """
    def _row_key(row) -> tuple:
        # Some catalog columns are arrays (e.g. pg_policies.roles -> name[]),
        # which asyncpg returns as Python lists. Coerce them to tuples so the
        # row can be hashed for set membership.
        return tuple(tuple(v) if isinstance(v, list) else v for v in row.values())

    diffs: dict[str, list] = {}
    for category, query in _CATALOG_QUERIES.items():
        src = [_row_key(r) for r in await db_manager.fetch(source, query)]
        tgt = {_row_key(r) for r in await db_manager.fetch(target, query)}
        missing = [r for r in src if r not in tgt]
        if missing:
            diffs[category] = missing
    return diffs


async def checksum_tables(
    db_manager: DBManager,
    db_name: str,
    tables: list[tuple[str, str]],
) -> dict[tuple[str, str], str]:
    """Compute an md5 checksum of each table's contents.

    We cast every row to text (stable under `ORDER BY ctid::text` would be non-deterministic,
    so callers should pass tables with a known PK and we use that implicitly via `ORDER BY 1`).
    """
    out: dict[tuple[str, str], str] = {}
    for schema, table in tables:
        rows = await db_manager.fetch(db_name, f"""
            SELECT coalesce(md5(string_agg(t::text, '|' ORDER BY t::text)), 'EMPTY') AS cs
            FROM "{schema}"."{table}" t
        """)
        out[(schema, table)] = rows[0]["cs"]
    return out


# --------------------------------------------------------------------------
# Dictionary file comparisons
# --------------------------------------------------------------------------

def assert_sens_dicts(actual_path: str | Path, expected_path: str | Path) -> None:
    """Compare sensitive dictionaries field by field."""
    with (
        Path(actual_path).open(encoding="utf-8") as f1,
        Path(expected_path).open(encoding="utf-8") as f2,
    ):
        actual = json.load(f1)["dictionary"]
        expected = json.load(f2)["dictionary"]

    def fields(data: list) -> list[dict]:
        out = []
        for item in data:
            f = item.get("fields", item)
            for k, v in f.items():
                out.append({k: v})
        return out

    a, e = fields(actual), fields(expected)
    missing = [f for f in e if f not in a]
    extra = [f for f in a if f not in e]
    for f in missing:
        print(f"assert_sens_dicts: missing {f}")
    for f in extra:
        print(f"assert_sens_dicts: unexpected {f}")
    assert not missing and not extra


def assert_no_sens_dicts(actual_path: str | Path, expected_path: str | Path) -> None:
    """Compare non-sensitive dictionaries."""
    with (
        Path(actual_path).open(encoding="utf-8") as f1,
        Path(expected_path).open(encoding="utf-8") as f2,
    ):
        a = json.load(f1)
        e = json.load(f2)

    assert len(a["no_sens_dictionary"]) == len(e["no_sens_dictionary"])
    a_sorted = sorted(a["no_sens_dictionary"], key=lambda x: (x["schema"], x["table"]))
    e_sorted = sorted(e["no_sens_dictionary"], key=lambda x: (x["schema"], x["table"]))
    for ai, ei in zip(a_sorted, e_sorted, strict=True):
        assert ai["schema"] == ei["schema"]
        assert ai["table"] == ei["table"]
        assert set(ai["fields"]) == set(ei["fields"])
