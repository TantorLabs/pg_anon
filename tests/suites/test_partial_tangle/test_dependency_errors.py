"""Kept tables that depend on excluded tables or schemas: stop with a clear error, pg_dump never starts."""

from __future__ import annotations

import pytest

from .conftest import backup_files, input_dict, run_dump, run_restore
from pg_anon.common.enums import ResultCode

# (case, exclusion option, error code, name of the kept table that must be named in the message, message)
STOP_CASES = {
    "column_with_row_of_excluded_table": (
        "row_column",
        f"--partial-tables-exclude-dict-file={input_dict('exclude_rowed.py')}",
        "EXCLUDED_TABLE_DEPENDENCY",
        "dep.holder",
        "Excluded tables are still needed by 1 table (dep.holder). Exclude these tables too or keep the excluded ones.",
    ),
}


@pytest.mark.parametrize("case", STOP_CASES)
async def test_dump_stops_before_pg_dump_when_a_kept_table_needs_an_excluded_one(
    case, db_manager, pg_anon_runner, fixtures, db_params
):
    db_case, option, error_code, dependent, message = STOP_CASES[case]
    source = f"pg_anon_tangle_dep_{db_case}"
    await db_manager.create_db(source)
    try:
        assert (await pg_anon_runner.run("init", source)).result_code == ResultCode.DONE
        await fixtures.build_partial_dependency_case(source, db_case)

        dump = await run_dump(pg_anon_runner, db_params, source, f"dep_{case}", [option])

        assert dump.result.result_code == ResultCode.FAIL
        assert str(dump.result.exception.code) == error_code
        assert dependent in str(dump.result.exception), "the message must name the kept table"
        assert str(dump.result.exception) == message
        assert not backup_files(dump.out_dir), "pg_dump must not start when the dependency is not resolved"
    finally:
        await db_manager.drop_db(source)


@pytest.mark.min_pg(100000)
async def test_whitelist_on_a_partition_includes_the_parent_without_error(
    db_manager, pg_anon_runner, fixtures, db_params
):
    source, target = "pg_anon_tangle_dep_partition", "pg_anon_tangle_dep_partition_target"
    await db_manager.create_db(source)
    await db_manager.create_db(target)
    try:
        assert (await pg_anon_runner.run("init", source)).result_code == ResultCode.DONE
        await fixtures.build_partial_dependency_case(source, "partition")

        dump = await run_dump(
            pg_anon_runner,
            db_params,
            source,
            "dep_whitelist_partition",
            [f"--partial-tables-dict-file={input_dict('include_part_1.py')}"],
        )
        assert dump.result.result_code == ResultCode.DONE, f"dump failed: {dump.result.error_message}"
        restore = await run_restore(pg_anon_runner, db_params, target, dump.out_dir)
        assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"

        tables = {
            row["relname"]
            for row in await db_manager.fetch(
                target,
                "SELECT relname FROM pg_class WHERE relnamespace = 'dep'::regnamespace AND relkind IN ('r', 'p')",
            )
        }
        assert tables == {"part_parent", "part_1", "plain"}
        rows = await db_manager.fetch(target, "SELECT count(*) AS n FROM dep.part_parent")
        assert rows[0]["n"] == 1, "only the rows of the whitelisted partition are dumped"
    finally:
        await db_manager.drop_db(source)
        await db_manager.drop_db(target)


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize("path", ["list_on_dump", "list_on_restore"])
async def test_blacklist_on_a_partitioned_table_excludes_its_partitions_without_error(
    path, db_manager, pg_anon_runner, fixtures, db_params
):
    option = f"--partial-tables-exclude-dict-file={input_dict('exclude_part_parent.py')}"
    dump_options, restore_options = ([option], None) if path == "list_on_dump" else (None, [option])
    source, target = "pg_anon_tangle_dep_partition", "pg_anon_tangle_dep_partition_target"
    await db_manager.create_db(source)
    await db_manager.create_db(target)
    try:
        assert (await pg_anon_runner.run("init", source)).result_code == ResultCode.DONE
        await fixtures.build_partial_dependency_case(source, "partition")

        dump = await run_dump(pg_anon_runner, db_params, source, f"dep_blacklist_partitioned_{path}", dump_options)
        assert dump.result.result_code == ResultCode.DONE, f"dump failed: {dump.result.error_message}"
        restore = await run_restore(pg_anon_runner, db_params, target, dump.out_dir, restore_options)
        assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"

        tables = {
            row["relname"]
            for row in await db_manager.fetch(
                target,
                "SELECT relname FROM pg_class WHERE relnamespace = 'dep'::regnamespace AND relkind IN ('r', 'p')",
            )
        }
        assert tables == {"plain"}, "the partitioned table goes away with all its partitions"
    finally:
        await db_manager.drop_db(source)
        await db_manager.drop_db(target)


@pytest.mark.parametrize("path", ["list_on_dump", "list_on_restore"])
async def test_blacklist_on_an_inherits_parent_excludes_its_children_without_error(
    path, db_manager, pg_anon_runner, fixtures, db_params
):
    option = f"--partial-tables-exclude-dict-file={input_dict('exclude_inh_parent.py')}"
    dump_options, restore_options = ([option], None) if path == "list_on_dump" else (None, [option])
    source, target = "pg_anon_tangle_dep_inheritance", "pg_anon_tangle_dep_inheritance_target"
    await db_manager.create_db(source)
    await db_manager.create_db(target)
    try:
        assert (await pg_anon_runner.run("init", source)).result_code == ResultCode.DONE
        await fixtures.build_partial_dependency_case(source, "inheritance")

        dump = await run_dump(pg_anon_runner, db_params, source, f"dep_blacklist_inherits_{path}", dump_options)
        assert dump.result.result_code == ResultCode.DONE, f"dump failed: {dump.result.error_message}"
        restore = await run_restore(pg_anon_runner, db_params, target, dump.out_dir, restore_options)
        assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"

        tables = {
            row["relname"]
            for row in await db_manager.fetch(
                target,
                "SELECT relname FROM pg_class WHERE relnamespace = 'dep'::regnamespace AND relkind = 'r'",
            )
        }
        assert tables == {"plain"}, "the parent table goes away with all its child tables"
    finally:
        await db_manager.drop_db(source)
        await db_manager.drop_db(target)
