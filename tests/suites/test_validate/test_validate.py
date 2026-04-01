from pg_anon.common.enums import ResultCode

from .conftest import TARGET_DB, TARGET_DB_2, input_dict, output_path


async def test_02_sync_struct_for_validate(source_db, target_db, db_params, pg_anon_runner):
    """sync-struct-dump with --dbg-stage-3-validate-full, then sync-struct-restore to target."""
    out = output_path("test_02_sync_struct")

    res = await pg_anon_runner.run("sync-struct-dump", source_db, [
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--prepared-sens-dict-file={input_dict('test_dbg_stages.py')}",
        "--clear-output-dir",
        f"--output-dir={out}",
        "--dbg-stage-3-validate-full",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("sync-struct-restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE


async def test_03_validate_dict(source_db, db_params, pg_anon_runner):
    """dump with --dbg-stage-1-validate-dict."""
    out = output_path("test_03_validate_dict")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test_dbg_stages.py')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--dbg-stage-1-validate-dict",
        f"--output-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE


async def test_04_validate_data(source_db, target_db, db_params, pg_anon_runner):
    """dump with --dbg-stage-2-validate-data, then sync-data-restore to target."""
    out = output_path("test_04_validate_data")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test_dbg_stages.py')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--dbg-stage-2-validate-data",
        f"--output-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("sync-data-restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE


async def test_05_validate_full(source_db, target_db_2, db_params, pg_anon_runner):
    """dump with --dbg-stage-3-validate-full, then restore to target_db_2."""
    out = output_path("test_05_validate_full")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test_dbg_stages.py')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--dbg-stage-3-validate-full",
        f"--output-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db_2, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
    ])
    assert res.result_code == ResultCode.DONE
