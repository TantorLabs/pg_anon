"""Shared assertion helpers for pg_anon test suites."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import TYPE_CHECKING

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME

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
    rows = await db_manager.fetch(
        db_name,
        """
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2
    """,
    )
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


# pg_policies has no "permissive" column before PG 10
_POLICIES_BEFORE_PG10 = "SELECT schemaname, tablename, policyname, roles, cmd FROM pg_policies ORDER BY 1, 2, 3"


async def sequence_params(db_manager: DBManager, db: str, schema: str, name: str) -> list[dict]:
    """Sequence parameters: `pg_sequences` is PG 10+, before it the sequence is a relation with these columns."""
    if await db_manager.server_version_num(db) >= 100000:
        query = (
            "SELECT increment_by, min_value, max_value, cache_size "
            f"FROM pg_sequences WHERE schemaname='{schema}' AND sequencename='{name}'"
        )
    else:
        query = f"SELECT increment_by, min_value, max_value, cache_value AS cache_size FROM {schema}.{name}"
    return [dict(r) for r in await db_manager.fetch(db, query)]


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

    queries = dict(_CATALOG_QUERIES)
    if await db_manager.server_version_num(source) < 100000:
        queries["policies"] = _POLICIES_BEFORE_PG10
    diffs: dict[str, list] = {}
    for category, query in queries.items():
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
        rows = await db_manager.fetch(
            db_name,
            f"""
            SELECT coalesce(md5(string_agg(t::text, '|' ORDER BY t::text)), 'EMPTY') AS cs
            FROM "{schema}"."{table}" t
        """,
        )
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


# --------------------------------------------------------------------------
# Two-way catalog snapshot (used by partial dump/restore tests)
# --------------------------------------------------------------------------

_SYSTEM_SCHEMAS = "('pg_catalog','information_schema','pg_toast')"

_SNAPSHOT_QUERIES: dict[str, str] = {
    "domains": f"""
        SELECT n.nspname, t.typname, format_type(t.typbasetype, t.typtypmod), t.typnotnull::text,
               coalesce(cn.nspname || '.' || c.collname, ''), coalesce(t.typdefault, '')
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        LEFT JOIN pg_collation c ON c.oid = t.typcollation AND t.typcollation <> 0
        LEFT JOIN pg_namespace cn ON cn.oid = c.collnamespace
        WHERE t.typtype = 'd' AND n.nspname NOT IN {_SYSTEM_SCHEMAS}
    """,
    "domain_constraints": f"""
        SELECT n.nspname, t.typname, co.conname, pg_get_constraintdef(co.oid), co.convalidated::text
        FROM pg_constraint co
        JOIN pg_type t ON t.oid = co.contypid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE co.contypid <> 0 AND co.contype <> 'n' AND n.nspname NOT IN {_SYSTEM_SCHEMAS}
    """,
    "composite_fields": f"""
        SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
               coalesce(cn.nspname || '.' || co.collname, '')
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_collation co ON co.oid = a.attcollation AND a.attcollation <> 0
        LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
        WHERE c.relkind = 'c' AND n.nspname NOT IN {_SYSTEM_SCHEMAS}
    """,
    "enum_labels": f"""
        SELECT n.nspname, t.typname,
               coalesce((SELECT string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
                         FROM pg_enum e WHERE e.enumtypid = t.oid), '')
        FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typtype = 'e' AND n.nspname NOT IN {_SYSTEM_SCHEMAS}
    """,
    "casts": """
        SELECT format_type(c.castsource, NULL), format_type(c.casttarget, NULL), c.castfunc::regprocedure::text,
               c.castcontext::text, c.castmethod::text
        FROM pg_cast c
        WHERE c.oid >= 16384
    """,
    "operators": """
        SELECT n.nspname, o.oprname, coalesce(format_type(NULLIF(o.oprleft, 0), NULL), ''),
               coalesce(format_type(NULLIF(o.oprright, 0), NULL), ''), coalesce(format_type(o.oprresult, NULL), ''),
               o.oprcode::text, o.oprcanhash::text, o.oprcanmerge::text,
               o.oprcom::regoperator::text, o.oprnegate::regoperator::text,
               o.oprrest::text, o.oprjoin::text
        FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace
        WHERE o.oid >= 16384
    """,
    "operator_classes": """
        SELECT n.nspname, c.opcname, am.amname, format_type(c.opcintype, NULL), c.opcdefault::text,
               fn.nspname || '.' || f.opfname,
               coalesce(format_type(NULLIF(c.opckeytype, 0), NULL), '')
        FROM pg_opclass c
        JOIN pg_namespace n ON n.oid = c.opcnamespace
        JOIN pg_am am ON am.oid = c.opcmethod
        JOIN pg_opfamily f ON f.oid = c.opcfamily
        JOIN pg_namespace fn ON fn.oid = f.opfnamespace
        WHERE c.oid >= 16384
    """,
    "operator_families": """
        SELECT n.nspname, f.opfname, am.amname
        FROM pg_opfamily f
        JOIN pg_namespace n ON n.oid = f.opfnamespace
        JOIN pg_am am ON am.oid = f.opfmethod
        WHERE f.oid >= 16384
    """,
    "operator_family_members": """
        SELECT n.nspname || '.' || f.opfname, 'operator', m.amopstrategy::text,
               format_type(m.amoplefttype, NULL), format_type(m.amoprighttype, NULL),
               m.amopopr::regoperator::text, m.amoppurpose::text
        FROM pg_amop m
        JOIN pg_opfamily f ON f.oid = m.amopfamily
        JOIN pg_namespace n ON n.oid = f.opfnamespace
        WHERE f.oid >= 16384
        UNION ALL
        SELECT n.nspname || '.' || f.opfname, 'function', m.amprocnum::text,
               format_type(m.amproclefttype, NULL), format_type(m.amprocrighttype, NULL),
               m.amproc::regprocedure::text, ''
        FROM pg_amproc m
        JOIN pg_opfamily f ON f.oid = m.amprocfamily
        JOIN pg_namespace n ON n.oid = f.opfnamespace
        WHERE f.oid >= 16384
    """,
    "views": f"""
        SELECT n.nspname, c.relname, c.relkind::text, pg_get_viewdef(c.oid, true)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('v', 'm') AND n.nspname NOT IN {_SYSTEM_SCHEMAS}
    """,
    "column_defaults": f"""
        SELECT n.nspname, c.relname, a.attname, pg_get_expr(d.adbin, d.adrelid)
        FROM pg_attrdef d
        JOIN pg_class c ON c.oid = d.adrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
        WHERE n.nspname NOT IN {_SYSTEM_SCHEMAS}
    """,
    "inheritance": """
        SELECT i.inhrelid::regclass::text, i.inhparent::regclass::text
        FROM pg_inherits i
        WHERE i.inhrelid >= 16384
    """,
    "comments": """
        SELECT 'relation', n.nspname, c.relname, d.objsubid::text, d.description
        FROM pg_description d
        JOIN pg_class c ON c.oid = d.objoid AND d.classoid = 'pg_class'::regclass
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE d.objoid >= 16384
        UNION ALL
        SELECT 'type', n.nspname, t.typname, '', d.description
        FROM pg_description d
        JOIN pg_type t ON t.oid = d.objoid AND d.classoid = 'pg_type'::regclass
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE d.objoid >= 16384
        UNION ALL
        SELECT 'function', n.nspname, p.oid::regprocedure::text, '', d.description
        FROM pg_description d
        JOIN pg_proc p ON p.oid = d.objoid AND d.classoid = 'pg_proc'::regclass
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE d.objoid >= 16384
    """,
}

# Queries that need a newer server; the value is (minimum server_version_num, query).
_SNAPSHOT_QUERIES_BY_VERSION: dict[str, tuple[int, str]] = {
    "range_types": (
        90600,
        """
        SELECT n.nspname, t.typname, format_type(r.rngsubtype, NULL), r.rngsubdiff::text,
               on_.nspname || '.' || oc.opcname, coalesce(cn.nspname || '.' || co.collname, ''),
               r.rngcanonical::text
        FROM pg_range r
        JOIN pg_type t ON t.oid = r.rngtypid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_opclass oc ON oc.oid = r.rngsubopc
        JOIN pg_namespace on_ ON on_.oid = oc.opcnamespace
        LEFT JOIN pg_collation co ON co.oid = r.rngcollation AND r.rngcollation <> 0
        LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
        WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        """,
    ),
    "multirange_types": (
        140000,
        """
        SELECT n.nspname, t.typname, mn.nspname || '.' || mt.typname
        FROM pg_range r
        JOIN pg_type t ON t.oid = r.rngtypid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_type mt ON mt.oid = r.rngmultitypid
        JOIN pg_namespace mn ON mn.oid = mt.typnamespace
        WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        """,
    ),
    "function_definitions": (
        90600,
        """
        SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), pg_get_function_result(p.oid),
               l.lanname, p.prosrc, p.provolatile::text, p.proparallel::text, p.proisstrict::text,
               p.prosecdef::text
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_language l ON l.oid = p.prolang
        WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass
                          AND d.objid = p.oid AND d.deptype = 'e')
        """,
    ),
    "function_sql_bodies": (
        140000,
        """
        SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), pg_get_function_sqlbody(p.oid)
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') AND p.prosqlbody IS NOT NULL
        """,
    ),
    "column_extras": (
        100000,
        """
        SELECT n.nspname, c.relname, a.attname, a.attidentity::text, {generated}
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
          AND a.attnum > 0 AND NOT a.attisdropped AND ({condition})
        """,
    ),
    "statistics": (
        100000,
        """
        SELECT n.nspname, s.stxname, s.stxrelid::regclass::text, s.stxkind::text
        FROM pg_statistic_ext s JOIN pg_namespace n ON n.oid = s.stxnamespace
        """,
    ),
    "publications": (
        100000,
        """
        SELECT p.pubname, p.puballtables::text, p.pubinsert::text, p.pubupdate::text, p.pubdelete::text
        FROM pg_publication p
        """,
    ),
    "publication_tables": (
        100000,
        """
        SELECT p.pubname, r.prrelid::regclass::text
        FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid
        """,
    ),
}


def _text_row(row) -> tuple[str, ...]:
    return tuple("" if v is None else (v.decode() if isinstance(v, bytes) else str(v)) for v in row.values())


def _is_anon_utils_row(row: tuple[str, ...]) -> bool:
    """Rows of the pg_anon utility schema exist in every database that was initialised."""
    return any(v == ANON_UTILS_DB_SCHEMA_NAME or f"{ANON_UTILS_DB_SCHEMA_NAME}." in v for v in row)


async def _aggregates_query(db_manager: DBManager, db: str) -> str:
    """Build the pg_aggregate query: every column in text form, no OIDs."""
    cols = await db_manager.fetch(
        db,
        """
        SELECT a.attname, a.atttypid::regtype::text AS typ
        FROM pg_attribute a
        WHERE a.attrelid = 'pg_aggregate'::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
    )
    parts = []
    for col in cols:
        name, typ = col["attname"], col["typ"]
        if name == "aggfnoid":
            continue
        if name in ("aggtranstype", "aggmtranstype"):
            parts.append(f"coalesce(format_type(NULLIF(g.{name}, 0), NULL), '')")
        elif name == "aggsortop":
            parts.append(f"g.{name}::regoperator::text")
        elif typ == "regproc":
            parts.append(f"g.{name}::text")
        else:
            parts.append(f"coalesce(g.{name}::text, '')")
    return f"""
        SELECT n.nspname, g.aggfnoid::regprocedure::text, p.proparallel::text, {", ".join(parts)}
        FROM pg_aggregate g
        JOIN pg_proc p ON p.oid = g.aggfnoid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN {_SYSTEM_SCHEMAS}
    """


async def catalog_snapshot(db_manager: DBManager, db: str) -> dict[str, set[tuple[str, ...]]]:
    """Snapshot of user objects as text rows without OIDs: `{category: {row, ...}}`."""
    version_num = await db_manager.server_version_num(db)
    queries = {**_CATALOG_QUERIES, **_SNAPSHOT_QUERIES, "aggregates": await _aggregates_query(db_manager, db)}
    if version_num < 100000:
        queries["policies"] = _POLICIES_BEFORE_PG10
    for category, (min_version, query) in _SNAPSHOT_QUERIES_BY_VERSION.items():
        if version_num < min_version:
            continue
        if category == "column_extras":
            generated = "a.attgenerated::text" if version_num >= 120000 else "''"
            condition = "a.attidentity <> ''" + (" OR a.attgenerated <> ''" if version_num >= 120000 else "")
            query = query.format(generated=generated, condition=condition)  # noqa: PLW2901
        queries[category] = query

    snapshot: dict[str, set[tuple[str, ...]]] = {}
    for category, query in queries.items():
        rows = {_text_row(r) for r in await db_manager.fetch(db, query)}
        snapshot[category] = {r for r in rows if not _is_anon_utils_row(r)}
    # PG 18 lists NOT NULL as constraints; older servers do not, so leave them out everywhere
    snapshot["constraints"] = {r for r in snapshot["constraints"] if r[3] != "n"}
    return snapshot


def snapshot_diff(
    source: dict[str, set[tuple[str, ...]]], target: dict[str, set[tuple[str, ...]]]
) -> dict[str, tuple[list[tuple[str, ...]], list[tuple[str, ...]]]]:
    """Two-way diff: `{category: (only in source, only in target)}`; equal categories are left out."""
    diff = {}
    for category in sorted(source.keys() | target.keys()):
        src, tgt = source.get(category, set()), target.get(category, set())
        if src != tgt:
            diff[category] = (sorted(src - tgt), sorted(tgt - src))
    return diff


def load_expected_missing(path: str | Path, version_num: int) -> dict[str, list[tuple[str, ...]]]:
    """Read a file of rows that must not be in the target: `{category: [row, ...]}`, a row may be limited by version."""
    raw = ast.literal_eval(Path(path).read_text(encoding="utf-8"))

    def rows(items: list) -> list[tuple[str, ...]]:
        found = []
        for item in items:
            if isinstance(item, dict):
                # {"row": tuple, "min_pg": 140000, "max_pg": 179999}: the row exists only on these servers
                if not item.get("min_pg", 0) <= version_num <= item.get("max_pg", 10**9):
                    continue
                item = item["row"]  # noqa: PLW2901
            found.append(tuple(item))
        return sorted(found)

    result = {category: rows(items) for category, items in raw.items()}
    return {category: found for category, found in result.items() if found}


def assert_target_lacks_exactly(
    diff: dict[str, tuple[list[tuple[str, ...]], list[tuple[str, ...]]]], expected_missing: dict[str, list]
) -> None:
    """The target has no rows of its own, and the rows it lacks are exactly the expected ones."""
    extra = {category: sides[1] for category, sides in diff.items() if sides[1]}
    missing = {category: sides[0] for category, sides in diff.items() if sides[0]}
    assert not extra, "the target has catalog rows that the source does not have"
    assert missing == expected_missing, "the rows missing in the target are not the expected ones"
