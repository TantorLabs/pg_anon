"""A list entry that names an INHERITS parent means this table with all its child tables, on dump and on restore."""

from __future__ import annotations

import pytest

from .conftest import check_both_paths

# scenario: (lists, tables of schema ilist with their own rows)
SCENARIOS = {
    "exclude_child": (
        {"exclude": ["sms_urgent"]},
        {
            "accounts": 3,
            "reads": 4,
            "notifications": 10,
            "email_notifications": 5,
            "sms_notifications": 4,
            "marks": 2,
            "marked_email": 1,
        },
    ),
    "exclude_parent": ({"exclude": ["notifications"]}, {"accounts": 3, "reads": 4, "marks": 2}),
    "exclude_middle_parent": (
        {"exclude": ["sms_notifications"]},
        {"accounts": 3, "reads": 4, "notifications": 10, "email_notifications": 5, "marks": 2, "marked_email": 1},
    ),
    "exclude_one_of_two_parents": (
        {"exclude": ["marks"]},
        {
            "accounts": 3,
            "reads": 4,
            "notifications": 10,
            "email_notifications": 5,
            "sms_notifications": 4,
            "sms_urgent": 3,
        },
    ),
    "include_child": (
        {"include": ["sms_urgent"]},
        {"notifications": 0, "sms_notifications": 0, "sms_urgent": 3},
    ),
    "include_child_of_two_parents": (
        {"include": ["marked_email"]},
        {"notifications": 0, "email_notifications": 0, "marks": 0, "marked_email": 1},
    ),
    "include_middle_parent": (
        {"include": ["sms_notifications"]},
        {"notifications": 0, "sms_notifications": 4, "sms_urgent": 3},
    ),
    "include_parent": (
        {"include": ["notifications"]},
        {
            "notifications": 10,
            "email_notifications": 5,
            "sms_notifications": 4,
            "sms_urgent": 3,
            "marks": 0,
            "marked_email": 1,
        },
    ),
    "black_list_wins_over_white_list": (
        {"include": ["notifications"], "exclude": ["sms_notifications"]},
        {"notifications": 10, "email_notifications": 5, "marks": 0, "marked_email": 1},
    ),
    "black_list_on_child_wins_over_white_list_on_child": (
        {"include": ["sms_notifications", "sms_urgent"], "exclude": ["sms_urgent"]},
        {"notifications": 0, "sms_notifications": 4},
    ),
    "include_child_and_table_with_key_to_parent": (
        {"include": ["sms_urgent", "reads"]},
        {"notifications": 0, "sms_notifications": 0, "sms_urgent": 3, "reads": 4},
    ),
    "only_parent": (
        {"include": ["notifications"], "exclude": ["email_notifications", "sms_notifications"]},
        {"notifications": 10},
    ),
}


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_lists_give_the_same_database_on_dump_and_on_restore(
    scenario, inherits_db, inherits_full_dump, target_dbs, db_manager, db_params, pg_anon_runner
):
    lists, expected = SCENARIOS[scenario]
    await check_both_paths(
        f"inherits_{scenario}",
        "ilist",
        lists,
        expected,
        inherits_db,
        inherits_full_dump,
        target_dbs,
        db_manager,
        db_params,
        pg_anon_runner,
    )
