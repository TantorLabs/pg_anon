import ast
import json
from pathlib import Path

from tests.infrastructure.db import DBManager


def load_expected(test_file: str, name: str) -> dict:
    """Load expected result (Python dict literal) from expected/ dir next to test file."""
    path = Path(test_file).parent / "expected" / name
    return ast.literal_eval(path.read_text(encoding="utf-8"))


async def check_rows(
    db_manager: DBManager,
    db_name: str,
    schema: str,
    table: str,
    fields: list[str] | None,
    expected_rows: list,
) -> bool:
    """Check that expected rows exist in the table."""
    if fields is None:
        query = f'SELECT * FROM "{schema}"."{table}" LIMIT 10000'
    else:
        fields_str = ", ".join(fields)
        query = f'SELECT {fields_str} FROM "{schema}"."{table}" LIMIT 10000'

    db_rows = await db_manager.fetch(db_name, query)
    db_rows_prepared = [list(dict(row).values()) for row in db_rows]

    def cmp_two_rows(row_a: list, row_b: list) -> bool:
        if len(row_a) != len(row_b):
            return False
        for i in range(len(row_a)):
            if row_a[i] != row_b[i] and row_a[i] != "*":
                return False
        return True

    result = True
    for expected_row in expected_rows:
        found = any(cmp_two_rows(expected_row, db_row) for db_row in db_rows_prepared)
        if not found:
            print(f"check_rows: row {expected_row} not found")
            result = False

    if not result:
        print("========================================")
        print("Following data exists:")
        for i, row in enumerate(db_rows_prepared):
            if i < 10:
                print(str(row))

    return result


async def check_rows_count(db_manager: DBManager, db_name: str, objs: list) -> bool:
    """Check row counts. objs: list of [schema, table, expected_count]."""
    failed_objs = []
    for obj in objs:
        rows = await db_manager.fetch(db_name, f'SELECT count(1) FROM "{obj[0]}"."{obj[1]}"')
        actual_count = rows[0][0]
        if actual_count != obj[2]:
            failed_objs.append(obj)
            print(f"check_rows_count: failed check {obj}, count is {actual_count:d}")

    return len(failed_objs) == 0


async def check_list_tables(
    db_manager: DBManager,
    db_name: str,
    expected_tables_list: list,
) -> bool:
    """Check that exactly the expected tables exist."""
    query = """
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind IN ('r', 'p')
            AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2
    """
    rows = await db_manager.fetch(db_name, query)
    db_tables = [list(row.values()) for row in rows]

    not_found = [t for t in expected_tables_list if t not in db_tables]
    unexpected = [t for t in db_tables if t not in expected_tables_list]

    for t in not_found:
        print(f"check_list_tables: Table {t} not found!")
    for t in unexpected:
        print(f"check_list_tables: Found unexpected table {t}!")

    return len(not_found) == 0 and len(db_tables) == len(expected_tables_list)


async def check_list_tables_and_fields(
    db_manager: DBManager,
    source_db_name: str,
    target_db_name: str,
) -> list:
    """Compare schemas between source and target databases. Returns list of differences."""
    query = """
        SELECT n.nspname, c.relname, a.attname AS column_name
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE a.attnum > 0
            AND c.relkind IN ('r', 'p')
            AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2, a.attnum
    """

    source_rows = [list(r.values()) for r in await db_manager.fetch(source_db_name, query)]
    target_rows = [list(r.values()) for r in await db_manager.fetch(target_db_name, query)]

    return [x for x in source_rows if x not in target_rows]


async def get_list_tables_with_diff_data(
    db_manager: DBManager,
    source_db_name: str,
    target_db_name: str,
) -> tuple:
    """Get tables with differing data between source and target."""
    query = """
        SELECT n.nspname, c.relname, a.attname AS column_name
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE a.attnum > 0
            AND c.relkind IN ('r', 'p')
            AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2, a.attnum
    """

    source_rows = [list(r.values()) for r in await db_manager.fetch(source_db_name, query)]
    for row in source_rows:
        vals = [list(r.values()) for r in await db_manager.fetch(
            source_db_name, f'SELECT "{row[2]}" FROM "{row[0]}"."{row[1]}" LIMIT 5'
        )]
        row.append(vals)

    target_rows = [list(r.values()) for r in await db_manager.fetch(target_db_name, query)]
    for row in target_rows:
        vals = [list(r.values()) for r in await db_manager.fetch(
            target_db_name, f'SELECT "{row[2]}" FROM "{row[0]}"."{row[1]}" LIMIT 5'
        )]
        row.append(vals)

    source_diff = [x for x in source_rows if x not in target_rows]
    target_diff = [x for x in target_rows if x not in source_rows]

    return source_diff, target_diff


def assert_sens_dicts(actual_path: str | Path, expected_path: str | Path) -> None:
    """Compare sensitive dictionaries field by field."""
    with (
        Path(actual_path).open(encoding="utf-8") as f1,
        Path(expected_path).open(encoding="utf-8") as f2,
    ):
        actual_dict = json.load(f1)["dictionary"]
        expected_dict = json.load(f2)["dictionary"]

    def extract_fields(data: list) -> list[dict]:
        result = []
        for item in data:
            fields = item.get("fields", item)
            for k, v in fields.items():
                result.append({k: v})
        return result

    actual_fields = extract_fields(actual_dict)
    expected_fields = extract_fields(expected_dict)

    missing_in_actual = [f for f in expected_fields if f not in actual_fields]
    extra_in_actual = [f for f in actual_fields if f not in expected_fields]

    if missing_in_actual:
        for f in missing_in_actual:
            print(f"assert_sens_dicts: missing field {f}")
    if extra_in_actual:
        for f in extra_in_actual:
            print(f"assert_sens_dicts: unexpected field {f}")

    assert not missing_in_actual and not extra_in_actual


def assert_no_sens_dicts(actual_path: str | Path, expected_path: str | Path) -> None:
    """Compare non-sensitive dictionaries."""
    with (
        Path(actual_path).open(encoding="utf-8") as f1,
        Path(expected_path).open(encoding="utf-8") as f2,
    ):
        d1 = json.load(f1)
        d2 = json.load(f2)

    assert len(d1["no_sens_dictionary"]) == len(d2["no_sens_dictionary"])

    sorted_d1 = sorted(d1["no_sens_dictionary"], key=lambda x: (x["schema"], x["table"]))
    sorted_d2 = sorted(d2["no_sens_dictionary"], key=lambda x: (x["schema"], x["table"]))

    for actual, expected in zip(sorted_d1, sorted_d2, strict=True):
        assert actual["schema"] == expected["schema"]
        assert actual["table"] == expected["table"]
        assert set(actual["fields"]) == set(expected["fields"])
