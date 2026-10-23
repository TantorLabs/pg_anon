"""sync-struct-restore and then sync-data-restore with one filter give the same database as restore with it."""

from __future__ import annotations

import pytest

from tests.infrastructure.assertions import catalog_snapshot, snapshot_diff

from .conftest import dump, Filters, relations, restore, restore_with_log, tables, tables_rows

# filters and the relations of the schema that stay: own rows of a table, or relkind for other relations
VIEWS = {
    "exclude_table_under_views": (
        Filters(exclude=tables("slink", "clients")),
        {"orders": 10, "order_notes": "v"},
    ),
    "include_table_under_views": (
        Filters(include=tables("slink", "orders")),
        {"orders": 10, "order_notes": "v"},
    ),
    "exclude_other_table": (
        Filters(exclude=tables("slink", "orders")),
        {"clients": 5, "client_names": "v"},
    ),
}

PARTITIONS = {
    "exclude_partitioned_table": (Filters(exclude=tables("plist", "transactions")), {"accounts": 3}),
    "exclude_partition": (
        Filters(exclude=tables("plist", "transactions_2025_02")),
        {
            "accounts": 3,
            "transactions": "p",
            "transactions_2025_01": 30,
            "transactions_2025_03": 32,
            "transactions_2026": "p",
            "transactions_2026_h1": 7,
            "transactions_2026_h2": 5,
        },
    ),
    "exclude_nested_partitioned_table": (
        Filters(exclude=tables("plist", "transactions_2026")),
        {
            "accounts": 3,
            "transactions": "p",
            "transactions_2025_01": 30,
            "transactions_2025_02": 28,
            "transactions_2025_03": 32,
        },
    ),
    "include_nested_partition": (
        Filters(include=tables("plist", "transactions_2026_h1", "accounts")),
        {"accounts": 3, "transactions": "p", "transactions_2026": "p", "transactions_2026_h1": 7},
    ),
    "include_partitioned_table_without_one_partition": (
        Filters(include=tables("plist", "transactions"), exclude=tables("plist", "transactions_2025_01")),
        {
            "transactions": "p",
            "transactions_2025_02": 28,
            "transactions_2025_03": 32,
            "transactions_2026": "p",
            "transactions_2026_h1": 7,
            "transactions_2026_h2": 5,
        },
    ),
}

INHERITS = {
    "exclude_parent": (Filters(exclude=tables("ilist", "notifications")), {"accounts": 3, "reads": 4, "marks": 2}),
    "exclude_child": (
        Filters(exclude=tables("ilist", "sms_urgent")),
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
    "include_child": (
        Filters(include=tables("ilist", "sms_urgent", "accounts")),
        {"accounts": 3, "notifications": 0, "sms_notifications": 0, "sms_urgent": 3},
    ),
    "include_parent_without_one_child": (
        Filters(include=tables("ilist", "sms_notifications"), exclude=tables("ilist", "sms_urgent")),
        {"notifications": 0, "sms_notifications": 4},
    ),
}

SOURCES = ["halves_of_full_dump", "separate_dumps"]


def cases(scenarios: dict) -> list:
    """Make the parameters: every scenario with both sources of the structure and the data."""
    return [pytest.param(name, source, id=f"{name}-{source}") for name in scenarios for source in SOURCES]


async def state(db_manager, db_name: str, schema: str) -> dict[str, int | str]:
    rows = await tables_rows(db_manager, db_name, [schema])
    found = await relations(db_manager, db_name, schema)
    return {name: rows[f"{schema}.{name}"] if kind == "r" else kind for name, kind in found.items()}


async def check_two_ways(
    name,
    schema,
    filters,
    expected,
    source,
    dumps,
    target_db,
    second_target_db,
    db_manager,
    db_params,
    pg_anon_runner,
    caplog,
) -> None:
    options = filters.options(f"linked_{name}")
    struct_dir, data_dir = (dumps.full, dumps.full) if source == "halves_of_full_dump" else (dumps.struct, dumps.data)

    await restore(pg_anon_runner, db_params, target_db, dumps.full, extra=options)
    await restore(pg_anon_runner, db_params, second_target_db, struct_dir, "sync-struct-restore", options)
    struct_state = await state(db_manager, second_target_db, schema)
    data_run = await restore_with_log(
        pg_anon_runner, db_params, second_target_db, caplog, data_dir, "sync-data-restore", options
    )

    assert set(struct_state) == set(expected), "sync-struct-restore must create the same relations as restore"
    assert await state(db_manager, target_db, schema) == expected, "restore"
    assert await state(db_manager, second_target_db, schema) == expected, "sync-struct-restore and sync-data-restore"
    assert not snapshot_diff(
        await catalog_snapshot(db_manager, target_db), await catalog_snapshot(db_manager, second_target_db)
    ), "one filter must give the same database in restore and in the two sync modes"
    assert not [message for message in data_run.warnings if "matches no table" in message], (
        "every rule of these filters matches a table or a parent table"
    )


@pytest.mark.parametrize(("name", "source"), cases(VIEWS))
async def test_views_follow_their_tables(
    name, source, views_dumps, target_db, second_target_db, db_manager, db_params, pg_anon_runner, caplog
):
    filters, expected = VIEWS[name]

    await check_two_ways(
        name,
        "slink",
        filters,
        expected,
        source,
        views_dumps,
        target_db,
        second_target_db,
        db_manager,
        db_params,
        pg_anon_runner,
        caplog,
    )


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(("name", "source"), cases(PARTITIONS))
async def test_partitions_follow_the_rules_for_parent_tables(
    name, source, partitions_dumps, target_db, second_target_db, db_manager, db_params, pg_anon_runner, caplog
):
    filters, expected = PARTITIONS[name]

    await check_two_ways(
        name,
        "plist",
        filters,
        expected,
        source,
        partitions_dumps,
        target_db,
        second_target_db,
        db_manager,
        db_params,
        pg_anon_runner,
        caplog,
    )


@pytest.mark.parametrize(("name", "source"), cases(INHERITS))
async def test_child_tables_follow_the_rules_for_parent_tables(
    name, source, inherits_dumps, target_db, second_target_db, db_manager, db_params, pg_anon_runner, caplog
):
    filters, expected = INHERITS[name]

    await check_two_ways(
        name,
        "ilist",
        filters,
        expected,
        source,
        inherits_dumps,
        target_db,
        second_target_db,
        db_manager,
        db_params,
        pg_anon_runner,
        caplog,
    )


def tables_with_rows(found: dict[str, int | str]) -> dict[str, int | str]:
    return {name: value for name, value in found.items() if isinstance(value, int) and value}


DATA_SOURCES = ["data_dump", "full_dump"]


async def check_rules_on_whole_structure(
    schema, filters, expected, dumps, source, target_db, db_manager, db_params, pg_anon_runner, name
) -> None:
    """The structure is restored without filters; the data restore with them leaves excluded tables empty."""
    await restore(pg_anon_runner, db_params, target_db, dumps.struct, "sync-struct-restore")
    struct_state = await state(db_manager, target_db, schema)
    data_dir = dumps.data if source == "data_dump" else dumps.full
    await restore(pg_anon_runner, db_params, target_db, data_dir, "sync-data-restore", filters.options(f"whole_{name}"))

    found = await state(db_manager, target_db, schema)
    assert set(found) == set(struct_state), "sync-data-restore must not change the structure"
    assert tables_with_rows(found) == tables_with_rows(expected)


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize("source", DATA_SOURCES)
@pytest.mark.parametrize(
    "name",
    [
        "exclude_partitioned_table",
        "exclude_nested_partitioned_table",
        "include_partitioned_table_without_one_partition",
    ],
)
async def test_data_restore_applies_rules_for_partitioned_tables_to_the_whole_structure(
    name, source, partitions_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    """A rule for a partitioned table reaches the rows of its partitions; the dump gives the parents."""
    filters, expected = PARTITIONS[name]
    await check_rules_on_whole_structure(
        "plist", filters, expected, partitions_dumps, source, target_db, db_manager, db_params, pg_anon_runner, name
    )


@pytest.mark.parametrize("source", DATA_SOURCES)
async def test_data_restore_applies_rules_for_parent_tables_to_the_whole_structure(
    source, inherits_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    """A rule for a parent table reaches the rows of its child tables; the dump gives the parents."""
    filters, expected = INHERITS["exclude_parent"]
    await check_rules_on_whole_structure(
        "ilist",
        filters,
        expected,
        inherits_dumps,
        source,
        target_db,
        db_manager,
        db_params,
        pg_anon_runner,
        "exclude_parent",
    )


async def check_filters_at_dump(
    name, schema, filters, expected, dumps, target_db, second_target_db, db_manager, db_params, pg_anon_runner
) -> None:
    """Filters given to the dump modes give what a full dump with them gives; the restore modes get no filters."""
    options = filters.options(f"at_dump_{name}")
    run = (pg_anon_runner, db_params)
    full = await dump(*run, dumps.source_db, f"at_dump_{name}_full", extra=options)
    struct = await dump(*run, dumps.source_db, f"at_dump_{name}_struct", "sync-struct-dump", options)
    data = await dump(*run, dumps.source_db, f"at_dump_{name}_data", "sync-data-dump", options)

    await restore(*run, target_db, full)
    await restore(*run, second_target_db, struct, "sync-struct-restore")
    await restore(*run, second_target_db, data, "sync-data-restore")

    assert await state(db_manager, target_db, schema) == expected, "dump and restore"
    assert await state(db_manager, second_target_db, schema) == expected, "the four sync modes"
    assert not snapshot_diff(
        await catalog_snapshot(db_manager, target_db), await catalog_snapshot(db_manager, second_target_db)
    ), "one filter at the dump must give the same database in all modes"


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    "name",
    [
        "exclude_partitioned_table",
        "exclude_nested_partitioned_table",
        "include_partitioned_table_without_one_partition",
    ],
)
async def test_dump_modes_apply_rules_for_partitioned_tables(
    name, partitions_dumps, target_db, second_target_db, db_manager, db_params, pg_anon_runner
):
    filters, expected = PARTITIONS[name]
    await check_filters_at_dump(
        name,
        "plist",
        filters,
        expected,
        partitions_dumps,
        target_db,
        second_target_db,
        db_manager,
        db_params,
        pg_anon_runner,
    )


@pytest.mark.parametrize("name", ["exclude_parent", "include_parent_without_one_child"])
async def test_dump_modes_apply_rules_for_parent_tables(
    name, inherits_dumps, target_db, second_target_db, db_manager, db_params, pg_anon_runner
):
    filters, expected = INHERITS[name]
    await check_filters_at_dump(
        name,
        "ilist",
        filters,
        expected,
        inherits_dumps,
        target_db,
        second_target_db,
        db_manager,
        db_params,
        pg_anon_runner,
    )
