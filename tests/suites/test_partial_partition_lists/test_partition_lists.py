"""A list entry that names a partitioned table means this table with all its partitions, on dump and on restore."""

from __future__ import annotations

import logging

import pytest

from tests.infrastructure.assertions import catalog_snapshot, load_expected, snapshot_diff

from .conftest import dump, input_dict, restore, tables_with_rows
from pg_anon.common.enums import ResultCode

pytestmark = pytest.mark.min_pg(100000)

EXCLUDE = "--partial-tables-exclude-dict-file"
INCLUDE = "--partial-tables-dict-file"

# scenario: list options with their list files
SCENARIOS = {
    "exclude_parent": [(EXCLUDE, "parent.py")],
    "exclude_parent_mask": [(EXCLUDE, "parent_mask.py")],
    "include_parent": [(INCLUDE, "parent.py")],
    "exclude_nested_parent": [(EXCLUDE, "nested_parent.py")],
    "include_nested_parent": [(INCLUDE, "nested_parent.py")],
    "exclude_one_partition": [(EXCLUDE, "one_partition.py")],
    "exclude_all_partitions": [(EXCLUDE, "all_partitions.py")],
    "include_one_partition": [(INCLUDE, "one_partition.py")],
    "include_nested_partition": [(INCLUDE, "nested_partition.py")],
    "exclude_nested_partition": [(EXCLUDE, "nested_partition.py")],
    "include_parent_exclude_nested_parent": [(INCLUDE, "parent.py"), (EXCLUDE, "nested_parent.py")],
    "include_parent_exclude_one_partition": [(INCLUDE, "parent.py"), (EXCLUDE, "one_partition.py")],
    "include_partition_exclude_parent": [(INCLUDE, "one_partition_and_accounts.py"), (EXCLUDE, "parent.py")],
    "only_parent": [(INCLUDE, "parent_and_accounts.py"), (EXCLUDE, "top_partitions.py")],
}


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_list_gives_the_same_database_on_dump_and_on_restore(
    scenario, source_db, full_dump, target_dbs, db_manager, db_params, pg_anon_runner
):
    list_options = [f"{option}={input_dict(list_file)}" for option, list_file in SCENARIOS[scenario]]
    expected = load_expected(__file__, f"{scenario}.py")
    listed_on_dump, listed_on_restore = target_dbs

    out, res = await dump(pg_anon_runner, db_params, source_db, scenario, list_options)
    assert res.result_code == ResultCode.DONE, f"dump with the list failed: {res.error_message}"
    res = await restore(pg_anon_runner, db_params, listed_on_dump, out)
    assert res.result_code == ResultCode.DONE, f"restore of the dump with the list failed: {res.error_message}"

    res = await restore(pg_anon_runner, db_params, listed_on_restore, full_dump, list_options)
    assert res.result_code == ResultCode.DONE, f"restore with the list failed: {res.error_message}"

    assert await tables_with_rows(db_manager, listed_on_dump) == expected, "list on dump"
    assert await tables_with_rows(db_manager, listed_on_restore) == expected, "list on restore"
    assert not snapshot_diff(
        await catalog_snapshot(db_manager, listed_on_dump), await catalog_snapshot(db_manager, listed_on_restore)
    ), "the same list must give the same database on dump and on restore"


async def test_rule_for_a_partitioned_table_is_logged_with_the_number_of_partitions(
    source_db, full_dump, target_dbs, db_params, pg_anon_runner, caplog
):
    list_option = f"{EXCLUDE}={input_dict('nested_parent.py')}"
    expected = [
        'The rule {"schema": "plist", "table": "transactions_2026"} matches a parent table: '
        "2 child tables are excluded too."
    ]

    with caplog.at_level(logging.INFO):
        _, res = await dump(pg_anon_runner, db_params, source_db, "log_nested_parent", [list_option])
    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    assert [message for message in caplog.messages if "matches a parent table" in message] == expected

    caplog.clear()
    with caplog.at_level(logging.INFO):
        res = await restore(pg_anon_runner, db_params, target_dbs[0], full_dump, [list_option])
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert [message for message in caplog.messages if "matches a parent table" in message] == expected


async def test_mask_that_matches_the_partitions_too_is_not_logged(source_db, db_params, pg_anon_runner, caplog):
    list_option = f"{EXCLUDE}={input_dict('parent_mask.py')}"

    with caplog.at_level(logging.INFO):
        _, res = await dump(pg_anon_runner, db_params, source_db, "log_parent_mask", [list_option])

    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    assert not [message for message in caplog.messages if "matches a parent table" in message]


async def test_list_on_dump_and_on_restore_logs_the_same_number_of_tables_and_no_lost_objects(
    source_db, full_dump, target_dbs, db_params, pg_anon_runner, caplog
):
    list_option = f"{INCLUDE}={input_dict('nested_partition.py')}"

    with caplog.at_level(logging.INFO):
        out, res = await dump(pg_anon_runner, db_params, source_db, "log_nested_partition", [list_option])
    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    dump_messages = list(caplog.messages)

    caplog.clear()
    with caplog.at_level(logging.INFO):
        res = await restore(pg_anon_runner, db_params, target_dbs[0], out)
    assert res.result_code == ResultCode.DONE, f"restore of the dump with the list failed: {res.error_message}"
    restore_of_listed_messages = list(caplog.messages)

    caplog.clear()
    with caplog.at_level(logging.INFO):
        res = await restore(pg_anon_runner, db_params, target_dbs[1], full_dump, [list_option])
    assert res.result_code == ResultCode.DONE, f"restore with the list failed: {res.error_message}"

    # the partitions left out are excluded tables, not objects that depend on them
    for messages in (dump_messages, restore_of_listed_messages, caplog.messages):
        assert not [message for message in messages if "will not be restored" in message]
    assert "Dump includes 3 of 8 tables" in dump_messages
    assert "Restore includes 3 of 8 tables" in caplog.messages
