from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


async def test_02_dump_with_valid_pg_dump_options(source_db, db_params, pg_anon_runner):
    """Dump with valid --pg-dump-options should succeed."""
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test.py')}",
        f"--output-dir={output_path('test_02_dump')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--pg-dump-options=--no-comments --no-publications",
    ])
    assert res.result_code == ResultCode.DONE


async def test_03_restore_with_valid_pg_restore_options(
    source_db, target_db, db_params, pg_anon_runner,
):
    """Restore with valid --pg-restore-options should succeed."""
    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_02_dump')}",
        "--drop-custom-check-constr",
        "--pg-restore-options=--no-comments --no-publications",
        "--drop-db",
    ])
    assert res.result_code == ResultCode.DONE


async def test_04_dump_with_invalid_pg_dump_options(source_db, db_params, pg_anon_runner):
    """Dump with invalid --pg-dump-options should fail."""
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test.py')}",
        f"--output-dir={output_path('test_04_dump')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--pg-dump-options=--no-comments --non-existing-flag",
    ])
    assert res.result_code == ResultCode.FAIL


async def test_05_restore_with_invalid_pg_restore_options(
    source_db, target_db, db_params, pg_anon_runner,
):
    """Restore with invalid --pg-restore-options should fail."""
    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_02_dump')}",
        "--drop-custom-check-constr",
        "--pg-restore-options=--no-comments --non-existing-flag",
        "--drop-db",
    ])
    assert res.result_code == ResultCode.FAIL
