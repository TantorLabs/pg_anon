"""Foreign tables follow the lists as tables without rows of their own; a foreign child goes with its parent."""

from __future__ import annotations

import pytest

from .conftest import check_both_paths

# scenario: (lists, tables of schema flist: own rows of a table or the kind of a foreign table)
SCENARIOS = {
    "exclude_foreign_table": ({"exclude": ["remote_rates"]}, {"accounts": 3, "remote_spare": "foreign"}),
    "include_foreign_table": (
        {"include": ["remote_rates", "accounts"]},
        {"accounts": 3, "remote_rates": "foreign"},
    ),
    "include_plain_table": ({"include": ["accounts"]}, {"accounts": 3}),
    "black_list_wins_over_white_list": (
        {"include": ["remote_rates", "remote_spare", "accounts"], "exclude": ["remote_spare"]},
        {"accounts": 3, "remote_rates": "foreign"},
    ),
}

# scenario: (lists, tables of schema fchild)
CHILD_SCENARIOS = {
    "exclude_parent_of_foreign_child": ({"exclude": ["local_parent"]}, {"accounts": 3}),
    "exclude_foreign_child": ({"exclude": ["remote_archive"]}, {"accounts": 3, "local_parent": 2}),
    "include_foreign_child": (
        {"include": ["remote_archive", "accounts"]},
        {"accounts": 3, "local_parent": 0, "remote_archive": "foreign"},
    ),
}

# scenario: (lists, tables of schema fpart)
PARTITION_SCENARIOS = {
    "exclude_parent_with_foreign_partition": ({"exclude": ["events"]}, {"accounts": 3}),
    "exclude_foreign_partition": (
        {"exclude": ["events_remote"]},
        {"accounts": 3, "events": "partitioned", "events_local": 4},
    ),
    "include_parent_with_foreign_partition": (
        {"include": ["events"]},
        {"events": "partitioned", "events_local": 4, "events_remote": "foreign"},
    ),
    "include_foreign_partition": (
        {"include": ["events_remote", "accounts"]},
        {"accounts": 3, "events": "partitioned", "events_remote": "foreign"},
    ),
}


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_lists_on_foreign_tables_give_the_same_database_on_dump_and_on_restore(
    scenario, foreign_db, foreign_full_dump, target_dbs, db_manager, db_params, pg_anon_runner
):
    lists, expected = SCENARIOS[scenario]
    await check_both_paths(
        f"foreign_{scenario}",
        "flist",
        lists,
        expected,
        foreign_db,
        foreign_full_dump,
        target_dbs,
        db_manager,
        db_params,
        pg_anon_runner,
    )
    for target in target_dbs:
        servers = await db_manager.fetch(target, "SELECT srvname FROM pg_foreign_server")
        assert [row["srvname"] for row in servers] == ["flist_srv"], "the foreign server stays with any list"


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize("scenario", PARTITION_SCENARIOS)
async def test_lists_on_a_table_with_a_foreign_partition_give_the_same_database_on_dump_and_on_restore(
    scenario,
    foreign_partition_db,
    foreign_partition_full_dump,
    target_dbs,
    db_manager,
    db_params,
    pg_anon_runner,
):
    lists, expected = PARTITION_SCENARIOS[scenario]
    await check_both_paths(
        f"fpart_{scenario}",
        "fpart",
        lists,
        expected,
        foreign_partition_db,
        foreign_partition_full_dump,
        target_dbs,
        db_manager,
        db_params,
        pg_anon_runner,
    )


@pytest.mark.parametrize("scenario", CHILD_SCENARIOS)
async def test_lists_on_a_foreign_inherits_child_give_the_same_database_on_dump_and_on_restore(
    scenario, foreign_child_db, foreign_child_full_dump, target_dbs, db_manager, db_params, pg_anon_runner
):
    lists, expected = CHILD_SCENARIOS[scenario]
    await check_both_paths(
        f"fchild_{scenario}",
        "fchild",
        lists,
        expected,
        foreign_child_db,
        foreign_child_full_dump,
        target_dbs,
        db_manager,
        db_params,
        pg_anon_runner,
    )
