"""sync-struct-restore applies the lists of tables with the same log lines and the same errors as restore."""

from __future__ import annotations

import pytest

from .conftest import Filters, relations, restore_with_log, tables, try_restore
from pg_anon.common.enums import ResultCode
from pg_anon.common.errors import ErrorCode

# filters and the log lines about parent tables
PARENT_RULES = {
    "black_list_on_parent": (
        Filters(exclude=tables("ilist", "notifications")),
        [
            'The rule {"schema": "ilist", "table": "notifications"} matches a parent table: '
            "4 child tables are excluded too."
        ],
    ),
    "white_list_on_parent_with_one_child": (
        Filters(include=tables("ilist", "sms_notifications")),
        [
            'The rule {"schema": "ilist", "table": "sms_notifications"} matches a parent table: '
            "1 child table is included too."
        ],
    ),
    "both_lists_on_parents": (
        Filters(include=tables("ilist", "notifications"), exclude=tables("ilist", "sms_notifications")),
        [
            'The rule {"schema": "ilist", "table": "sms_notifications"} matches a parent table: '
            "1 child table is excluded too.",
            'The rule {"schema": "ilist", "table": "notifications"} matches a parent table: '
            "2 child tables are included too.",
        ],
    ),
    "lists_on_tables_without_children": (
        Filters(include=tables("ilist", "sms_urgent", "accounts"), exclude=tables("ilist", "accounts")),
        [],
    ),
}


@pytest.mark.parametrize("source", ["struct", "full"])
@pytest.mark.parametrize("name", PARENT_RULES)
async def test_structure_restore_logs_the_rules_that_match_parent_tables(
    name, source, inherits_dumps, target_db, db_params, pg_anon_runner, caplog
):
    filters, expected = PARENT_RULES[name]

    run = await restore_with_log(
        pg_anon_runner,
        db_params,
        target_db,
        caplog,
        getattr(inherits_dumps, source),
        "sync-struct-restore",
        filters.options(f"log_{name}"),
    )

    assert [message for message in run.messages if "matches a parent table" in message] == expected


@pytest.mark.parametrize("mode", ["restore", "sync-struct-restore"])
async def test_restore_stops_when_a_kept_table_needs_an_excluded_one(
    mode, row_type_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    """The table dep.holder has a column of the row type of dep.rowed."""
    options = Filters(exclude=tables("dep", "rowed")).options("row_type")

    res = await try_restore(pg_anon_runner, db_params, target_db, row_type_dumps.full, mode, options)

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.EXCLUDED_TABLE_DEPENDENCY
    assert str(res.exception) == (
        "Excluded tables are still needed by 1 table (dep.holder). Exclude these tables too or keep the excluded ones."
    )
    assert not await relations(db_manager, target_db, "dep"), "the restore must stop before any change"


@pytest.mark.parametrize("source", ["struct", "full"])
async def test_structure_restore_stops_when_the_white_list_matches_no_table(
    source, row_type_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    options = Filters(include=tables("dep", "no_such_table")).options("no_match")

    res = await try_restore(
        pg_anon_runner, db_params, target_db, getattr(row_type_dumps, source), "sync-struct-restore", options
    )

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.NO_TABLES_FOR_RESTORE
    assert not await relations(db_manager, target_db, "dep"), "the restore must stop before any change"
