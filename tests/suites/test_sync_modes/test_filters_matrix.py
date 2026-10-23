"""The same filters give the same tables and rows in all six modes of dump and restore."""

from __future__ import annotations

import logging

import pytest

from .conftest import dump, Filters, restore, tables, tables_rows

ROWS = {"s1.a": 10, "s1.b": 20, "s2.d": 5, "s2.e": 7}
SCHEMAS = ["s1", "s2"]

BLACK_B = tables("s1", "b")
WHITE_A_D = [*tables("s1", "a"), *tables("s2", "d")]
WHITE_A_B_D = [*tables("s1", "a", "b"), *tables("s2", "d")]
WHITE_A = tables("s1", "a")

# filters and the tables that stay
MATRIX = {
    "01_no_filters": (Filters(), ["s1.a", "s1.b", "s2.d", "s2.e"]),
    "02_black_list": (Filters(exclude=BLACK_B), ["s1.a", "s2.d", "s2.e"]),
    "03_white_list": (Filters(include=WHITE_A_D), ["s1.a", "s2.d"]),
    "04_white_and_black_list": (Filters(include=WHITE_A_B_D, exclude=BLACK_B), ["s1.a", "s2.d"]),
    "05_exclude_schema": (Filters(["--exclude-schema-name=s2"]), ["s1.a", "s1.b"]),
    "06_include_schema": (Filters(["--schema-name=s1"]), ["s1.a", "s1.b"]),
    "07_include_and_exclude_schema": (
        Filters(["--schema-name=s1,s2", "--exclude-schema-name=s2"]),
        ["s1.a", "s1.b"],
    ),
    "08_exclude_schema_and_black_list": (Filters(["--exclude-schema-name=s2"], exclude=BLACK_B), ["s1.a"]),
    "09_include_schema_and_white_list": (Filters(["--schema-name=s1"], include=WHITE_A), ["s1.a"]),
    "10_exclude_schema_and_white_list": (Filters(["--exclude-schema-name=s2"], include=WHITE_A_D), ["s1.a"]),
    "11_include_schema_and_black_list": (Filters(["--schema-name=s1"], exclude=BLACK_B), ["s1.a"]),
}

# the same with masks of schemas and tables
MASKS = {
    "schema_mask": (Filters(["--schema-mask=^s1$"]), ["s1.a", "s1.b"]),
    "exclude_schema_mask": (Filters(["--exclude-schema-mask=2$"]), ["s1.a", "s1.b"]),
    "black_list_table_mask": (
        Filters(exclude=[{"schema_mask": "*", "table_mask": "^[bd]$"}]),
        ["s1.a", "s2.e"],
    ),
    "white_list_table_mask": (Filters(include=[{"schema": "s1", "table_mask": "^a"}]), ["s1.a"]),
    "schema_mask_and_black_list_mask": (
        Filters(["--schema-mask=^s"], exclude=[{"schema_mask": "2$", "table_mask": "*"}]),
        ["s1.a", "s1.b"],
    ),
}

MODES = ["dump", "restore", "sync-struct-dump", "sync-struct-restore", "sync-data-dump", "sync-data-restore"]


async def created_tables(db_manager, db_name: str) -> list[str]:
    return sorted(await tables_rows(db_manager, db_name, SCHEMAS))


async def filled_tables(db_manager, db_name: str) -> dict[str, int]:
    return {table: rows for table, rows in (await tables_rows(db_manager, db_name, SCHEMAS)).items() if rows}


async def check_cell(mode, name, filters, expected, dumps, target_db, db_manager, db_params, pg_anon_runner) -> None:
    """Apply the filters in one mode, the other half of the work has no filters; compare what the mode is about."""
    options = filters.options(name)
    expected_rows = {table: ROWS[table] for table in expected}
    run = (pg_anon_runner, db_params)

    if mode == "dump":
        out = await dump(*run, dumps.source_db, f"filters_{mode}_{name}", extra=options)
        await restore(*run, target_db, out)
        assert await created_tables(db_manager, target_db) == expected
        assert await filled_tables(db_manager, target_db) == expected_rows
    elif mode == "restore":
        await restore(*run, target_db, dumps.full, extra=options)
        assert await created_tables(db_manager, target_db) == expected
        assert await filled_tables(db_manager, target_db) == expected_rows
    elif mode == "sync-struct-dump":
        out = await dump(*run, dumps.source_db, f"filters_{mode}_{name}", "sync-struct-dump", options)
        await restore(*run, target_db, out, "sync-struct-restore")
        assert await created_tables(db_manager, target_db) == expected
    elif mode == "sync-struct-restore":
        # the structure comes from a structure dump or from a full dump, the result is the same
        for in_dir in (dumps.struct, dumps.full):
            await db_manager.create_db(target_db)
            await restore(*run, target_db, in_dir, "sync-struct-restore", options)
            assert await created_tables(db_manager, target_db) == expected, in_dir
            assert not await filled_tables(db_manager, target_db), in_dir
    elif mode == "sync-data-dump":
        out = await dump(*run, dumps.source_db, f"filters_{mode}_{name}", "sync-data-dump", options)
        await restore(*run, target_db, dumps.struct, "sync-struct-restore")
        await restore(*run, target_db, out, "sync-data-restore")
        assert await filled_tables(db_manager, target_db) == expected_rows
    else:
        # the data comes from a data dump or from a full dump, the result is the same
        for in_dir in (dumps.data, dumps.full):
            await db_manager.create_db(target_db)
            await restore(*run, target_db, dumps.struct, "sync-struct-restore")
            await restore(*run, target_db, in_dir, "sync-data-restore", options)
            assert await filled_tables(db_manager, target_db) == expected_rows, in_dir


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("name", MATRIX)
async def test_filters_give_the_same_result_in_every_mode(
    name, mode, filters_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    filters, expected = MATRIX[name]

    await check_cell(mode, name, filters, expected, filters_dumps, target_db, db_manager, db_params, pg_anon_runner)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("name", MASKS)
async def test_masks_give_the_same_result_in_every_mode(
    name, mode, filters_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    filters, expected = MASKS[name]

    await check_cell(mode, name, filters, expected, filters_dumps, target_db, db_manager, db_params, pg_anon_runner)


@pytest.mark.parametrize("name", [*MATRIX, *MASKS])
async def test_both_sync_restores_with_one_filter_give_what_restore_gives(
    name, filters_dumps, target_db, second_target_db, db_manager, db_params, pg_anon_runner
):
    filters, _ = {**MATRIX, **MASKS}[name]
    options = filters.options(f"both_sync_{name}")
    run = (pg_anon_runner, db_params)

    await restore(*run, target_db, filters_dumps.full, extra=options)
    await restore(*run, second_target_db, filters_dumps.full, "sync-struct-restore", options)
    await restore(*run, second_target_db, filters_dumps.full, "sync-data-restore", options)

    assert await tables_rows(db_manager, second_target_db, SCHEMAS) == await tables_rows(db_manager, target_db, SCHEMAS)


async def test_halves_of_one_full_dump_take_different_filters(
    filters_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    """The structure comes without s1.b, the data comes without s1.b and s2.d."""
    struct_filters = Filters(exclude=tables("s1", "b"))
    data_filters = Filters(exclude=[*tables("s1", "b"), *tables("s2", "d")])

    await restore(
        pg_anon_runner,
        db_params,
        target_db,
        filters_dumps.full,
        "sync-struct-restore",
        struct_filters.options("halves_struct"),
    )
    await restore(
        pg_anon_runner,
        db_params,
        target_db,
        filters_dumps.full,
        "sync-data-restore",
        data_filters.options("halves_data"),
    )

    assert await created_tables(db_manager, target_db) == ["s1.a", "s2.d", "s2.e"]
    assert await filled_tables(db_manager, target_db) == {"s1.a": 10, "s2.e": 7}


@pytest.mark.parametrize("mode", ["dump", "sync-struct-dump", "sync-data-dump"])
@pytest.mark.parametrize("kind", ["exclude", "include"])
async def test_dump_modes_match_rules_with_tables_of_excluded_schemas(
    kind, mode, filters_dumps, db_params, pg_anon_runner, caplog
):
    """A rule for a table of an excluded schema matches a table of the database in every dump mode."""
    rules = {"exclude": {"exclude": tables("s2", "d")}, "include": {"include": WHITE_A_D}}[kind]
    name = f"excluded_schema_rule_{kind}_{mode}"

    with caplog.at_level(logging.WARNING, logger="pg_anon.logger"):
        await dump(
            pg_anon_runner,
            db_params,
            filters_dumps.source_db,
            name,
            mode,
            Filters(["--exclude-schema-name=s2"], **rules).options(name),
        )

    assert not [message for message in caplog.messages if "matches no table" in message]
