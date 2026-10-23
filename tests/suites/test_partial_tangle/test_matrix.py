"""Dump x restore matrix on the "tangle" database: what is excluded must be gone, nothing else may change."""

from __future__ import annotations

import re

import pytest

from tests.infrastructure.assertions import (
    assert_target_lacks_exactly,
    catalog_snapshot,
    load_expected_missing,
    snapshot_diff,
)

from .conftest import (
    assert_dump_includes_log,
    assert_no_pg_util_commands,
    expected_path,
    input_dict,
    KNOWN_LEAK_CASES,
    KNOWN_LEAK_NAMES,
    KNOWN_LEAK_NAMES_PG14,
    leaked_lines,
    REGULAR_ROLE,
    REGULAR_ROLE_PASSWORD,
    run_dump,
    run_restore,
    unexpected_leaks,
)
from pg_anon.common.enums import ResultCode

WHITELIST = f"--partial-tables-dict-file={input_dict('include_kept.py')}"
WHITELIST_SMALL = f"--partial-tables-dict-file={input_dict('include_small.py')}"
BLACKLIST = f"--partial-tables-exclude-dict-file={input_dict('exclude_gone.py')}"

DUMP_OPTIONS = {
    "full": [],
    "whitelist": [WHITELIST],
    "blacklist": [BLACKLIST],
}

# (dump kind, restore options, file with what must not be in the target)
MATRIX = {
    "full_to_plain": ("full", [], "full.py"),
    "full_to_whitelist": ("full", [WHITELIST], "excl_gone.py"),
    "full_to_blacklist": ("full", [BLACKLIST], "excl_gone.py"),
    "whitelist_to_plain": ("whitelist", [], "excl_gone.py"),
    "whitelist_to_smaller_whitelist": ("whitelist", [WHITELIST_SMALL], "excl_gone_items.py"),
    "whitelist_to_blacklist": ("whitelist", [BLACKLIST], "excl_gone.py"),
    "blacklist_to_plain": ("blacklist", [], "excl_gone.py"),
    "blacklist_to_whitelist": ("blacklist", [WHITELIST], "excl_gone.py"),
    "blacklist_to_blacklist": ("blacklist", [BLACKLIST], "excl_gone.py"),
}


@pytest.fixture(scope="module")
async def dumps(source_db, db_params, pg_anon_runner):
    """One dump per kind; every restore case reads its dump."""
    outcomes = {}
    for kind, options in DUMP_OPTIONS.items():
        outcomes[kind] = await run_dump(pg_anon_runner, db_params, source_db, f"matrix_{kind}", options)
    return outcomes


@pytest.fixture(scope="module")
async def schema_exclude_dump(schema_source_db, db_params, pg_anon_runner):
    return await run_dump(
        pg_anon_runner, db_params, schema_source_db, "matrix_schema_exclude", ["--exclude-schema-name=tg_gone_schema"]
    )


@pytest.fixture(scope="module")
async def schema_include_whitelist_dump(schema_source_db, db_params, pg_anon_runner):
    return await run_dump(
        pg_anon_runner, db_params, schema_source_db, "matrix_schema_include", ["--schema-name=tg_core", WHITELIST]
    )


@pytest.fixture(scope="module")
async def regular_role_dump(regular_source_db, db_params, pg_anon_runner):
    return await run_dump(pg_anon_runner, db_params, regular_source_db, "matrix_regular", [WHITELIST])


async def assert_target_matches(db_manager, source_db, source_snapshot, target_db, expected_file):
    """The target lacks exactly the expected rows and has no rows of its own; kept tables hold all their rows."""
    target_snapshot = await catalog_snapshot(db_manager, target_db)
    diff = snapshot_diff(source_snapshot, target_snapshot)
    expected = load_expected_missing(expected_path(expected_file), await db_manager.server_version_num(target_db))
    assert_target_lacks_exactly(diff, expected)

    for schema, name, kind in sorted(target_snapshot["tables"]):
        if kind == "r":
            query = f'SELECT count(*) AS n FROM "{schema}"."{name}"'
            source_rows = (await db_manager.fetch(source_db, query))[0]["n"]
            target_rows = (await db_manager.fetch(target_db, query))[0]["n"]
            assert target_rows == source_rows, f"row count differs in {schema}.{name}"


def assert_partial_dump_is_done(outcome):
    assert outcome.result.result_code == ResultCode.DONE, f"dump failed: {outcome.result.error_message}"
    assert_no_pg_util_commands(outcome.messages)
    assert_dump_includes_log(outcome.messages)


def assert_dump_leaks_only(db_params, outcome, allowed):
    assert not unexpected_leaks(leaked_lines(db_params, outcome.out_dir), allowed), "dump leaks excluded names"


@pytest.mark.parametrize("case", list(MATRIX))
async def test_dump_and_restore_exclude_exactly_the_expected_objects(
    case, dumps, source_db, source_snapshot, target_db, db_manager, db_params, pg_anon_runner
):
    dump_kind, restore_options, expected_file = MATRIX[case]
    dump = dumps[dump_kind]
    if dump_kind == "full":
        assert dump.result.result_code == ResultCode.DONE
        assert_no_pg_util_commands(dump.messages)
    else:
        assert_partial_dump_is_done(dump)

    restore = await run_restore(pg_anon_runner, db_params, target_db, dump.out_dir, restore_options)

    assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"
    assert_no_pg_util_commands(restore.messages)
    await assert_target_matches(db_manager, source_db, source_snapshot, target_db, expected_file)


async def test_schema_exclusion_dump_restores_all_but_the_schema(
    schema_exclude_dump,
    schema_source_db,
    schema_source_snapshot,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    dump = schema_exclude_dump
    assert dump.result.result_code == ResultCode.DONE, f"dump failed: {dump.result.error_message}"
    assert_no_pg_util_commands(dump.messages)
    assert not unexpected_leaks(leaked_lines(db_params, dump.out_dir, "tg_gone_schema"), set()), "dump leaks the schema"

    restore = await run_restore(pg_anon_runner, db_params, target_db, dump.out_dir)

    assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"
    assert_no_pg_util_commands(restore.messages)
    await assert_target_matches(db_manager, schema_source_db, schema_source_snapshot, target_db, "excl_schema.py")


async def test_schema_inclusion_with_whitelist_dump_restores_the_rest(
    schema_include_whitelist_dump,
    schema_source_db,
    schema_source_snapshot,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    dump = schema_include_whitelist_dump
    assert_partial_dump_is_done(dump)

    restore = await run_restore(pg_anon_runner, db_params, target_db, dump.out_dir)

    assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"
    assert_no_pg_util_commands(restore.messages)
    await assert_target_matches(
        db_manager, schema_source_db, schema_source_snapshot, target_db, "excl_gone_and_schema.py"
    )


@pytest.mark.min_pg(150000)
async def test_whitelist_dump_restores_under_a_regular_role(
    regular_role_dump,
    regular_source_db,
    regular_source_snapshot,
    regular_role_target,
    db_manager,
    db_params,
    pg_anon_runner,
):
    dump = regular_role_dump
    assert_partial_dump_is_done(dump)
    # the test runs only on PostgreSQL 15 and newer, so the known names are the ones of PostgreSQL 14+
    assert_dump_leaks_only(db_params, dump, KNOWN_LEAK_NAMES | KNOWN_LEAK_NAMES_PG14)

    restore = await run_restore(
        pg_anon_runner,
        db_params,
        regular_role_target,
        dump.out_dir,
        db_user=REGULAR_ROLE,
        db_password=REGULAR_ROLE_PASSWORD,
    )

    assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"
    assert_no_pg_util_commands(restore.messages)
    await assert_target_matches(
        db_manager, regular_source_db, regular_source_snapshot, regular_role_target, "excl_gone.py"
    )


@pytest.mark.parametrize("known_names", KNOWN_LEAK_CASES)
async def test_partial_dumps_leak_only_the_known_names(dumps, schema_include_whitelist_dump, db_params, known_names):
    for dump in (dumps["whitelist"], dumps["blacklist"], schema_include_whitelist_dump):
        assert_dump_leaks_only(db_params, dump, known_names)


@pytest.mark.parametrize("known_names", KNOWN_LEAK_CASES)
async def test_known_leak_is_exactly_the_non_table_dependents(dumps, db_params, pg_anon_runner, known_names):
    """pg_dump keeps functions, types and FK of excluded tables; pg_anon cuts them at restore."""
    lines = leaked_lines(db_params, dumps["blacklist"].out_dir)
    leaked_names = {
        name
        for line in lines
        if "metadata.json" not in line and "structure.json" not in line
        for name in _gone_names(line)
    }
    assert leaked_names - {"gone_t"} == known_names, "residual leak is not the known set of non-table dependents"
    assert not [line for line in lines if line.startswith(("metadata.json", "structure.json"))], (
        "metadata.json and structure.json must not name them"
    )


def _gone_names(line: str) -> set[str]:
    if " PUBLICATION TABLE " in line:
        return set()
    return set(re.findall(r"gone_\w+", line))
