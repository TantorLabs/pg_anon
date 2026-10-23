"""A rule that matches a parent table writes one log line with the number of child tables, on dump and on restore."""

from __future__ import annotations

import logging

import pytest

from .conftest import dump, list_options, restore
from pg_anon.common.enums import ResultCode

# scenario: (lists, log lines about parent tables)
SCENARIOS = {
    "black_list_on_parent": (
        {"exclude": ["notifications"]},
        [
            'The rule {"schema": "ilist", "table": "notifications"} matches a parent table: '
            "4 child tables are excluded too."
        ],
    ),
    "white_list_on_parent_with_one_child": (
        {"include": ["sms_notifications"]},
        [
            'The rule {"schema": "ilist", "table": "sms_notifications"} matches a parent table: '
            "1 child table is included too."
        ],
    ),
    "both_lists_on_parents": (
        {"include": ["notifications"], "exclude": ["sms_notifications"]},
        [
            'The rule {"schema": "ilist", "table": "sms_notifications"} matches a parent table: '
            "1 child table is excluded too.",
            'The rule {"schema": "ilist", "table": "notifications"} matches a parent table: '
            "2 child tables are included too.",
        ],
    ),
    "lists_on_tables_without_children": ({"include": ["sms_urgent", "accounts"], "exclude": ["accounts"]}, []),
}


def parent_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [message for message in caplog.messages if "matches a parent table" in message]


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_dump_logs_the_rules_that_match_parent_tables(scenario, inherits_db, db_params, pg_anon_runner, caplog):
    lists, expected = SCENARIOS[scenario]
    options = list_options(f"log_{scenario}", "ilist", lists.get("include", []), lists.get("exclude", []))

    with caplog.at_level(logging.INFO):
        _, res = await dump(pg_anon_runner, db_params, inherits_db, f"log_dump_{scenario}", options)

    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    assert parent_lines(caplog) == expected


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_restore_logs_the_rules_that_match_parent_tables(
    scenario, inherits_full_dump, target_dbs, db_params, pg_anon_runner, caplog
):
    lists, expected = SCENARIOS[scenario]
    options = list_options(f"log_{scenario}", "ilist", lists.get("include", []), lists.get("exclude", []))

    with caplog.at_level(logging.INFO):
        res = await restore(pg_anon_runner, db_params, target_dbs[0], inherits_full_dump, options)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert parent_lines(caplog) == expected
