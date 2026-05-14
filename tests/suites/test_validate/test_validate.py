"""Tests for --dbg-stage-{1,2,3}-validate-* debug flags that verify dictionary,
data, and full round-trip without persisting output.
"""
from __future__ import annotations

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


async def test_stage1_validate_dict(source_db, db_params, pg_anon_runner):
    """Stage 1: only validate that every rule in the dict matches something."""
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('validate.py')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--output-dir={output_path('stage1')}",
        "--clear-output-dir",
        "--dbg-stage-1-validate-dict",
    ])
    assert res.result_code == ResultCode.DONE


async def test_stage2_validate_data_then_sync_data_restore(
    source_db, target_db, db_params, pg_anon_runner,
):
    """Stage 2: dump data in validation mode, then sync-data-restore to target.

    sync-data-restore only copies data (no DDL), so the target must already
    have the table structure — we set it up via sync-struct first.
    """
    struct_out = output_path("stage2_struct")
    res = await pg_anon_runner.run("sync-struct-dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('validate.py')}",
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

    out = output_path("stage2")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('validate.py')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--output-dir={out}",
        "--clear-output-dir",
        "--dbg-stage-2-validate-data",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("sync-data-restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE


async def test_stage3_validate_full_then_restore(
    source_db, target_db, db_params, pg_anon_runner,
):
    """Stage 3: full end-to-end validation dump + normal restore."""
    out = output_path("stage3")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('validate.py')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--output-dir={out}",
        "--clear-output-dir",
        "--dbg-stage-3-validate-full",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE


async def test_sync_struct_with_stage3_flag(
    source_db, target_db, db_params, pg_anon_runner,
):
    """sync-struct-dump combined with --dbg-stage-3-validate-full + sync-struct-restore."""
    out = output_path("sync_struct_stage3")
    res = await pg_anon_runner.run("sync-struct-dump", source_db, [
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--prepared-sens-dict-file={input_dict('validate.py')}",
        f"--output-dir={out}",
        "--clear-output-dir",
        "--dbg-stage-3-validate-full",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("sync-struct-restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE
