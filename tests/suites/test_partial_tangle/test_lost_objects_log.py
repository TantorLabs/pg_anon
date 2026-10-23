"""The log names the objects that will not be restored because of excluded tables, without their own parts."""

from __future__ import annotations

import pytest

from .conftest import input_dict, run_dump, run_restore
from pg_anon.common.enums import ResultCode

EXCLUDE_GONE = f"--partial-tables-exclude-dict-file={input_dict('exclude_gone.py')}"
LOST_PREFIX = "Objects that depend on excluded tables and will not be restored: "
LOST_IN_SCHEMAS_PREFIX = "Objects that depend on excluded tables and schemas and will not be restored: "

# the trigger, the policy, the index, the statistics and the publication of gone_t go away with it and are not named
ROW_DEPENDENTS = {"function tg_core.gone_fn_setof()", "type tg_core.gone_t_row"}
VIEWS = {"view tg_core.gone_v", "materialized view tg_core.gone_mv"}
ON_DUMP = ROW_DEPENDENTS | VIEWS | {"table constraint gone_fk_out on tg_core.orders"}
# the restore names them as TOC entries; the views of a dump with the list are not in the dump at all
IN_DUMP_ON_RESTORE = ROW_DEPENDENTS | {"fk constraint gone_fk_out on tg_core.orders"}
ATOMIC = {"function tg_core.gone_fn_atomic()"}

# on dump, restore of that dump, restore of a full dump with the list; one case per range of server versions
NAMED_CASES = [
    pytest.param(ON_DUMP, IN_DUMP_ON_RESTORE, marks=pytest.mark.max_pg(140000), id="before_pg14"),
    pytest.param(ON_DUMP | ATOMIC, IN_DUMP_ON_RESTORE | ATOMIC, marks=pytest.mark.min_pg(140000), id="pg14_and_newer"),
]


@pytest.fixture(scope="module")
async def full_dump(source_db, db_params, pg_anon_runner) -> str:
    dumped = await run_dump(pg_anon_runner, db_params, source_db, "lost_objects_full")
    assert dumped.result.result_code == ResultCode.DONE, f"dump failed: {dumped.result.error_message}"
    return dumped.out_dir


def lost_names(messages: list[str], prefix: str = LOST_PREFIX) -> list[set[str]]:
    """Read the names of every line about lost objects: "N objects (a, b, c)" -> {a, b, c}."""
    lines = [message.removeprefix(prefix) for message in messages if message.startswith(prefix)]
    return [set(line[line.index("(") + 1 : -1].split(", ")) for line in lines]


@pytest.mark.parametrize(("on_dump", "in_dump"), NAMED_CASES)
async def test_lost_objects_are_named_on_dump_and_on_restore(
    on_dump, in_dump, source_db, target_db, db_params, pg_anon_runner
):
    dumped = await run_dump(pg_anon_runner, db_params, source_db, "lost_objects_log", [EXCLUDE_GONE])
    assert dumped.result.result_code == ResultCode.DONE, f"dump failed: {dumped.result.error_message}"
    assert lost_names(dumped.messages) == [on_dump]

    restored = await run_restore(pg_anon_runner, db_params, target_db, dumped.out_dir)
    assert restored.result.result_code == ResultCode.DONE, f"restore failed: {restored.result.error_message}"
    assert lost_names(restored.messages) == [in_dump]


@pytest.mark.parametrize(("on_dump", "in_dump"), NAMED_CASES)
async def test_lost_objects_are_named_on_restore_with_the_list(
    on_dump, in_dump, target_db, db_params, pg_anon_runner, full_dump
):
    restored = await run_restore(pg_anon_runner, db_params, target_db, full_dump, [EXCLUDE_GONE])

    assert restored.result.result_code == ResultCode.DONE, f"restore failed: {restored.result.error_message}"
    assert lost_names(restored.messages) == [in_dump | VIEWS]
    assert "Restore includes 3 of 4 tables" in restored.messages


async def test_view_of_an_excluded_schema_is_named(schema_source_db, db_params, pg_anon_runner):
    dumped = await run_dump(
        pg_anon_runner, db_params, schema_source_db, "lost_objects_schema", ["--exclude-schema-name=tg_gone_schema"]
    )

    assert dumped.result.result_code == ResultCode.DONE, f"dump failed: {dumped.result.error_message}"
    assert lost_names(dumped.messages, LOST_IN_SCHEMAS_PREFIX) == [{"view tg_core.gone_schema_view"}]


async def test_no_lines_about_lost_objects_and_restored_tables_without_lists(
    target_db, db_params, pg_anon_runner, full_dump
):
    restored = await run_restore(pg_anon_runner, db_params, target_db, full_dump)

    assert restored.result.result_code == ResultCode.DONE, f"restore failed: {restored.result.error_message}"
    assert not [message for message in restored.messages if "will not be restored" in message]
    assert not [message for message in restored.messages if message.startswith("Restore includes")]
