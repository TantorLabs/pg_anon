"""End-to-end dump/restore scenarios covered by the original test_dump_restore
suite, rewritten around the domain-oriented fixtures.

Each test stands alone: it creates a fresh target_db, runs dump (if needed),
then restore, and asserts on the target. The source_db is shared (read-only).
"""
from __future__ import annotations

import filecmp
from pathlib import Path

from tests.infrastructure.assertions import check_list_tables, check_rows_count, list_tables

from .conftest import input_dict, output_path
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.constants import SAVED_DICTS_INFO_FILE_NAME
from pg_anon.common.enums import ResultCode


async def _dump(pg_anon_runner, db_params, source_db, *, out_dir, dict_file, extra=None):
    args = [
        f"--prepared-sens-dict-file={dict_file}",
        f"--output-dir={out_dir}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ]
    if extra:
        args.extend(extra)
    return await pg_anon_runner.run("dump", source_db, args)


async def _restore(pg_anon_runner, db_params, target_db, *, in_dir, extra=None):
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
        "--drop-custom-check-constr",
    ]
    if extra:
        args.extend(extra)
    return await pg_anon_runner.run("restore", target_db, args)


async def test_dump_then_restore_preserves_all_tables(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Baseline: dump with anonymization rules, restore, verify every source table
    exists in the target.
    """
    out = output_path("dump_then_restore")
    res = await _dump(pg_anon_runner, db_params, source_db,
                      out_dir=out, dict_file=input_dict("full_sens.py"))
    assert res.result_code == ResultCode.DONE

    res = await _restore(pg_anon_runner, db_params, target_db, in_dir=out)
    assert res.result_code == ResultCode.DONE

    expected = await list_tables(db_manager, source_db)
    assert await check_list_tables(db_manager, target_db, expected)


async def test_dump_with_exclude_all_keeps_validated_table(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """`dictionary_exclude: {schema:*, table:*}` should exclude everything except
    tables listed in `validate_tables` (here: hr.department).
    """
    out = output_path("exclude_all")
    res = await _dump(pg_anon_runner, db_params, source_db,
                      out_dir=out, dict_file=input_dict("exclude_all.py"))
    assert res.result_code == ResultCode.DONE

    res = await _restore(pg_anon_runner, db_params, target_db, in_dir=out)
    assert res.result_code == ResultCode.DONE

    # validate-target-tables must pass on the validated table
    options = build_run_options([
        "restore",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={target_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--prepared-sens-dict-file={input_dict('exclude_all.py')}",
        f"--input-dir={out}",
        "--debug",
    ])
    res = await PgAnonApp(options).validate_target_tables()
    assert res.result_code == ResultCode.DONE


async def test_sync_struct_restores_empty_tables(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """sync-struct-dump + sync-struct-restore must create all tables but zero rows."""
    out = output_path("sync_struct")
    res = await pg_anon_runner.run("sync-struct-dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('sync_struct.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("sync-struct-restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE

    expected_tables = await list_tables(db_manager, source_db)
    assert await check_list_tables(db_manager, target_db, expected_tables)
    zero_counts = [[s, t, 0] for s, t in expected_tables]
    assert await check_rows_count(db_manager, target_db, zero_counts)


async def test_sync_data_after_sync_struct(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """After sync-struct, sync-data populates a few tables without touching others."""
    struct_out = output_path("sync_data_struct")
    data_out = output_path("sync_data_data")

    res = await pg_anon_runner.run("sync-struct-dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('sync_struct.py')}",
        f"--output-dir={struct_out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE
    res = await pg_anon_runner.run("sync-struct-restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={struct_out}",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("sync-data-dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('sync_struct.py')}",
        f"--output-dir={data_out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE
    res = await pg_anon_runner.run("sync-data-restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={data_out}",
    ])
    assert res.result_code == ResultCode.DONE

    # employee has data in source; after sync-data must also have data in target
    counts = await db_manager.fetch(target_db, "SELECT count(1) FROM hr.employee")
    assert counts[0][0] > 0


async def test_sync_struct_restore_with_clean_db(
    target_db, db_manager, db_params, pg_anon_runner, fixtures,
):
    isolated_source = "pg_anon_dr_sync_struct_clean_src"
    await db_manager.create_db(isolated_source)
    try:
        init_res = await pg_anon_runner.run("init", isolated_source)
        assert init_res.result_code == ResultCode.DONE
        await fixtures.build_minimal_env(isolated_source)

        out = output_path("sync_struct_clean_db")
        res = await pg_anon_runner.run("sync-struct-dump", isolated_source, [
            f"--prepared-sens-dict-file={input_dict('sync_struct.py')}",
            f"--output-dir={out}",
            f"--processes={db_params.test_processes}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ])
        assert res.result_code == ResultCode.DONE

        res = await pg_anon_runner.run("sync-struct-restore", target_db, [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
        ])
        assert res.result_code == ResultCode.DONE

        first_tables = await list_tables(db_manager, target_db)
        assert first_tables, "first sync-struct-restore must create tables"

        res = await pg_anon_runner.run("sync-struct-restore", target_db, [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--clean-db",
        ])
        assert res.result_code == ResultCode.DONE

        expected_tables = await list_tables(db_manager, isolated_source)
        assert await check_list_tables(db_manager, target_db, expected_tables)
        zero_counts = [[s, t, 0] for s, t in expected_tables]
        assert await check_rows_count(db_manager, target_db, zero_counts)
    finally:
        await db_manager.drop_db(isolated_source)


async def test_restore_into_non_empty_db_fails(
    source_db, target_db, db_params, pg_anon_runner, fixtures,
):
    """Restoring into a DB that already has objects (no --drop-db/--clean-db) must fail."""
    out = output_path("nonempty_fail")
    res = await _dump(pg_anon_runner, db_params, source_db,
                      out_dir=out, dict_file=input_dict("full_sens.py"))
    assert res.result_code == ResultCode.DONE

    await fixtures.build_minimal_env(target_db)

    res = await _restore(pg_anon_runner, db_params, target_db, in_dir=out)
    assert res.result_code == ResultCode.FAIL


async def test_restore_with_drop_db_succeeds(
    source_db, target_db, db_params, pg_anon_runner, fixtures,
):
    """Same as above but with --drop-db: restore recreates the DB and succeeds."""
    out = output_path("drop_db_ok")
    res = await _dump(pg_anon_runner, db_params, source_db,
                      out_dir=out, dict_file=input_dict("full_sens.py"))
    assert res.result_code == ResultCode.DONE

    await fixtures.build_minimal_env(target_db)

    res = await _restore(pg_anon_runner, db_params, target_db,
                         in_dir=out, extra=["--drop-db"])
    assert res.result_code == ResultCode.DONE


async def test_dump_with_sql_conditions_filters_rows(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """`sql_condition` on a dict rule should limit which rows end up in the dump."""
    out = output_path("sql_conditions")
    res = await _dump(pg_anon_runner, db_params, source_db,
                      out_dir=out, dict_file=input_dict("sql_conditions.py"))
    assert res.result_code == ResultCode.DONE

    res = await _restore(pg_anon_runner, db_params, target_db,
                         in_dir=out, extra=["--drop-db"])
    assert res.result_code == ResultCode.DONE

    assert await check_rows_count(db_manager, target_db, [
        ["hr", "employee", 5],
        ["billing", "customer", 3],
    ])


async def test_dump_with_save_dicts_snapshots_inputs(
    source_db, db_params,
):
    """dump --save-dicts must materialize the run-dir with the exact input
    dicts (sens/partial-tables/partial-exclude) used for the run, so the run
    is reproducible later.
    """
    out = output_path("dump_save_dicts")
    sens = input_dict("full_sens.py")
    partial = input_dict("partial_tables.py")
    partial_exclude = input_dict("partial_exclude.py")

    options = build_run_options([
        "dump",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={sens}",
        f"--partial-tables-dict-file={partial}",
        f"--partial-tables-exclude-dict-file={partial_exclude}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--save-dicts",
        "--debug",
    ])
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE

    run_dir = Path(options.run_dir)
    assert (run_dir / SAVED_DICTS_INFO_FILE_NAME).exists()

    snapshot_sens = run_dir / "input" / Path(sens).name
    snapshot_partial = run_dir / "input" / Path(partial).name
    snapshot_exclude = run_dir / "input" / Path(partial_exclude).name

    assert snapshot_sens.exists()
    assert snapshot_partial.exists()
    assert snapshot_exclude.exists()

    assert filecmp.cmp(snapshot_sens, sens, shallow=False), \
        "sens dict snapshot differs from original"
    assert filecmp.cmp(snapshot_partial, partial, shallow=False), \
        "partial-tables-dict snapshot differs from original"
    assert filecmp.cmp(snapshot_exclude, partial_exclude, shallow=False), \
        "partial-tables-exclude-dict snapshot differs from original"


async def test_restore_with_save_dicts_snapshots_inputs(
    source_db, target_db, db_params, pg_anon_runner,
):
    """restore --save-dicts must materialize a run-dir with the partial-tables
    dicts used for the restore, proving the restore is reproducible.
    """
    out = output_path("restore_save_dicts_seed")
    partial = input_dict("partial_tables.py")
    partial_exclude = input_dict("partial_exclude.py")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    options = build_run_options([
        "restore",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={target_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        f"--partial-tables-dict-file={partial}",
        f"--partial-tables-exclude-dict-file={partial_exclude}",
        "--drop-custom-check-constr",
        "--save-dicts",
        "--drop-db",
        "--debug",
    ])
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE

    run_dir = Path(options.run_dir)
    assert (run_dir / SAVED_DICTS_INFO_FILE_NAME).exists()

    snapshot_partial = run_dir / "input" / Path(partial).name
    snapshot_exclude = run_dir / "input" / Path(partial_exclude).name

    assert snapshot_partial.exists()
    assert snapshot_exclude.exists()

    assert filecmp.cmp(snapshot_partial, partial, shallow=False), \
        "partial-tables-dict snapshot differs from original"
    assert filecmp.cmp(snapshot_exclude, partial_exclude, shallow=False), \
        "partial-tables-exclude-dict snapshot differs from original"

