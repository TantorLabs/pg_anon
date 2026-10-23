"""Foreign keys in the sync modes: sync-struct-restore creates them in the usual way, sync-data-restore leaves them."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from .conftest import (
    dump,
    Filters,
    fk_commands,
    fk_states,
    relations,
    restore,
    restore_with_log,
    tables,
    tables_rows,
    UNCHECKED_KEYS_WARNING,
    unchecked_keys_warnings,
)

EXCLUDE_A_REF_SPARE = Filters(exclude=tables("pfk", "a_ref_spare"))
EXCLUDE_D_REF = Filters(exclude=tables("pfk", "d_ref"))


def unchecked_keys_warning(count: int) -> str:
    keys = "1 foreign key on the loaded tables was" if count == 1 else f"{count} foreign keys on the loaded tables were"
    return f"{UNCHECKED_KEYS_WARNING} {keys} not checked again."


def assert_keys_come_without_not_valid(run) -> None:
    assert not [message for message in run.warnings if "NOT VALID" in message or "is not created" in message]
    assert not [command for command in fk_commands(run) if "NOT VALID" in command or "VALIDATE" in command]


@pytest.mark.min_pg(120000)
async def test_structure_dump_with_excluded_partition_gives_the_key(
    partial_fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    out = await dump(
        pg_anon_runner,
        db_params,
        partial_fk_dumps.source_db,
        "struct_without_partition",
        "sync-struct-dump",
        EXCLUDE_A_REF_SPARE.options("struct_without_partition"),
    )
    constraints = json.loads((Path(out) / "structure.json").read_text(encoding="utf-8"))["constraints"]
    assert not [key for key, constraint in constraints.items() if "partition_excluded" in constraint], (
        "a structure dump only writes the keys down"
    )

    run = await restore_with_log(pg_anon_runner, db_params, target_db, caplog, out, "sync-struct-restore")

    assert "a_ref_spare" not in await relations(db_manager, target_db, "pfk")
    assert await fk_states(db_manager, target_db) == {
        "a_src_fk": True,
        "b_src_fk": True,
        "c_src_fk": True,
        "d_src_fk": True,
    }
    assert_keys_come_without_not_valid(run)


@pytest.mark.min_pg(120000)
@pytest.mark.parametrize("source", ["struct", "full"])
@pytest.mark.parametrize("partition", ["a_ref_spare", "b_ref_spare", "b_src_low", "c_src_low"])
async def test_structure_restore_with_excluded_partition_gives_the_key(
    partition, source, partial_fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    options = Filters(exclude=tables("pfk", partition)).options(f"restore_without_{partition}")

    run = await restore_with_log(
        pg_anon_runner, db_params, target_db, caplog, getattr(partial_fk_dumps, source), "sync-struct-restore", options
    )

    assert partition not in await relations(db_manager, target_db, "pfk")
    assert await fk_states(db_manager, target_db) == {
        "a_src_fk": True,
        "b_src_fk": True,
        "c_src_fk": True,
        "d_src_fk": True,
    }
    assert_keys_come_without_not_valid(run)


@pytest.mark.min_pg(120000)
async def test_structure_restore_of_dump_with_excluded_partition_gives_the_key(
    partial_fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """The dump mode marks the key as created by pg_anon; without rows it is still created in the usual way."""
    out = await dump(
        pg_anon_runner,
        db_params,
        partial_fk_dumps.source_db,
        "dump_without_partition",
        extra=EXCLUDE_A_REF_SPARE.options("dump_without_partition"),
    )

    run = await restore_with_log(pg_anon_runner, db_params, target_db, caplog, out, "sync-struct-restore")

    assert (await fk_states(db_manager, target_db))["a_src_fk"] is True
    assert_keys_come_without_not_valid(run)


async def test_structure_restore_does_not_look_at_data_of_a_full_dump(
    partial_fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """The dump has no rows of d_ref: restore leaves the key NOT VALID, sync-struct-restore does not."""
    out = await dump(
        pg_anon_runner, db_params, partial_fk_dumps.source_db, "dump_d_ref_no_data", dict_file="d_ref_no_data.py"
    )

    run = await restore_with_log(pg_anon_runner, db_params, target_db, caplog, out, "sync-struct-restore")

    assert (await fk_states(db_manager, target_db))["d_src_fk"] is True
    assert_keys_come_without_not_valid(run)


@pytest.mark.parametrize("source", ["struct", "full"])
async def test_structure_restore_gives_no_key_to_a_table_that_is_not_created(
    source, partial_fk_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    await restore(
        pg_anon_runner,
        db_params,
        target_db,
        getattr(partial_fk_dumps, source),
        "sync-struct-restore",
        EXCLUDE_D_REF.options("struct_without_d_ref"),
    )

    found = await relations(db_manager, target_db, "pfk")
    assert "d_ref" not in found
    assert "d_src" in found
    assert "d_src_fk" not in await fk_states(db_manager, target_db)


@pytest.mark.parametrize("mode", ["restore", "sync-struct-restore"])
async def test_black_list_removes_a_table_that_has_no_data_file(
    mode, partial_fk_dumps, target_db, db_manager, db_params, pg_anon_runner
):
    """The dump keeps d_ref without rows (dictionary_exclude); the black list of the restore still removes it."""
    out = await dump(
        pg_anon_runner, db_params, partial_fk_dumps.source_db, "dump_d_ref_no_data", dict_file="d_ref_no_data.py"
    )

    await restore(pg_anon_runner, db_params, target_db, out, mode, EXCLUDE_D_REF.options("no_data_file"))

    found = await relations(db_manager, target_db, "pfk")
    assert "d_ref" not in found
    assert "d_src" in found
    assert "d_src_fk" not in await fk_states(db_manager, target_db)


async def test_data_restore_leaves_every_key_as_it_was(
    fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """Rows of c1 and c2 refer to p, and p gets no rows: the keys are not dropped, changed or created again."""
    await restore(pg_anon_runner, db_params, target_db, fk_dumps.struct, "sync-struct-restore")
    await db_manager.execute(
        target_db,
        """
        ALTER TABLE sfk.c2 DROP CONSTRAINT c2_p_fk;
        ALTER TABLE sfk.c2 ADD CONSTRAINT c2_p_fk FOREIGN KEY (p_id) REFERENCES sfk.p (id) NOT VALID;
        ALTER TABLE sfk.r DROP CONSTRAINT r_q_fk;
        """,
    )
    keys_before = await fk_states(db_manager, target_db)
    assert keys_before == {"c1_p_fk": True, "c2_p_fk": False}

    run = await restore_with_log(
        pg_anon_runner,
        db_params,
        target_db,
        caplog,
        fk_dumps.data,
        "sync-data-restore",
        Filters(exclude=tables("sfk", "p")).options("data_without_p"),
    )

    assert await fk_states(db_manager, target_db) == keys_before
    assert not fk_commands(run)
    rows = await tables_rows(db_manager, target_db, ["sfk"])
    assert (rows["sfk.p"], rows["sfk.c1"], rows["sfk.c2"]) == (0, 5, 5)
    assert unchecked_keys_warnings(run) == [unchecked_keys_warning(2)]


@pytest.mark.parametrize(
    ("name", "filters", "count"),
    [
        ("all_tables", Filters(), 3),
        ("table_without_keys", Filters(include=tables("sfk", "lone")), None),
        ("referring_table", Filters(include=tables("sfk", "c1")), 1),
        ("referenced_table", Filters(include=tables("sfk", "p")), 2),
        ("without_p_and_its_keys", Filters(exclude=tables("sfk", "p", "c1", "c2")), 1),
    ],
)
@pytest.mark.parametrize("source", ["data", "full"])
async def test_data_restore_warns_about_keys_of_loaded_tables(
    source, name, filters, count, fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    await restore(pg_anon_runner, db_params, target_db, fk_dumps.struct, "sync-struct-restore")

    run = await restore_with_log(
        pg_anon_runner,
        db_params,
        target_db,
        caplog,
        getattr(fk_dumps, source),
        "sync-data-restore",
        filters.options(f"warning_{name}"),
    )

    assert unchecked_keys_warnings(run) == ([] if count is None else [unchecked_keys_warning(count)])


async def test_data_restore_does_not_warn_when_no_table_gets_rows(
    fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    await db_manager.create_db(fk_dumps.source_db + "_empty")
    try:
        await restore(pg_anon_runner, db_params, fk_dumps.source_db + "_empty", fk_dumps.struct, "sync-struct-restore")
        out = await dump(
            pg_anon_runner, db_params, fk_dumps.source_db + "_empty", "data_without_rows", "sync-data-dump"
        )
    finally:
        await db_manager.drop_db(fk_dumps.source_db + "_empty")
    await restore(pg_anon_runner, db_params, target_db, fk_dumps.struct, "sync-struct-restore")

    run = await restore_with_log(pg_anon_runner, db_params, target_db, caplog, out, "sync-data-restore")

    assert not unchecked_keys_warnings(run)


@pytest.mark.min_pg(120000)
async def test_data_restore_counts_a_key_of_a_partitioned_table_by_its_partition(
    partial_fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """Only the partition a_ref_old gets rows; the key a_src_fk refers to its parent table."""
    await restore(pg_anon_runner, db_params, target_db, partial_fk_dumps.full, "sync-struct-restore")

    run = await restore_with_log(
        pg_anon_runner,
        db_params,
        target_db,
        caplog,
        partial_fk_dumps.full,
        "sync-data-restore",
        Filters(include=tables("pfk", "a_ref_old")).options("data_one_partition"),
    )

    assert unchecked_keys_warnings(run) == [unchecked_keys_warning(1)]


@pytest.mark.parametrize(
    "count",
    [
        pytest.param(3, marks=pytest.mark.min_pg(120000)),
        pytest.param(1, marks=[pytest.mark.min_pg(110000), pytest.mark.max_pg(120000)]),
        pytest.param(None, marks=pytest.mark.max_pg(110000)),
    ],
)
async def test_data_restore_of_full_dump_with_marks_creates_no_keys(
    count, partial_fk_dumps, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """The dump has no rows of d_ref, so restore would create d_src_fk itself; sync-data-restore must not."""
    # pfk has four keys on PostgreSQL 12+, two on 11 and one before; one of them is dropped here
    out = await dump(
        pg_anon_runner, db_params, partial_fk_dumps.source_db, "dump_d_ref_no_data", dict_file="d_ref_no_data.py"
    )
    await restore(pg_anon_runner, db_params, target_db, out, "sync-struct-restore")
    await db_manager.execute(target_db, "ALTER TABLE pfk.d_src DROP CONSTRAINT d_src_fk")
    keys_before = await fk_states(db_manager, target_db)

    run = await restore_with_log(pg_anon_runner, db_params, target_db, caplog, out, "sync-data-restore")

    assert await fk_states(db_manager, target_db) == keys_before
    assert "d_src_fk" not in keys_before
    assert not fk_commands(run)
    assert unchecked_keys_warnings(run) == ([] if count is None else [unchecked_keys_warning(count)])
