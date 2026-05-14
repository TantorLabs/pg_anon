"""Tests that `--pg-dump-options` and `--pg-restore-options` are forwarded
correctly and surface errors when given invalid flags.
"""
from __future__ import annotations

import pytest

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


async def test_dump_with_valid_pg_dump_options(source_db, db_params, pg_anon_runner):
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={output_path('valid_dump')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--pg-dump-options=--no-comments --no-publications",
    ])
    assert res.result_code == ResultCode.DONE


async def test_dump_with_invalid_pg_dump_options_fails(source_db, db_params, pg_anon_runner):
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={output_path('invalid_dump')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--pg-dump-options=--non-existing-flag",
    ])
    assert res.result_code == ResultCode.FAIL


@pytest.fixture
async def prepared_dump(source_db, db_params, pg_anon_runner):
    out = output_path("for_restore")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE
    return out


async def test_restore_with_valid_pg_restore_options(target_db, db_params, pg_anon_runner, prepared_dump):
    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={prepared_dump}",
        "--drop-custom-check-constr",
        "--pg-restore-options=--no-comments --no-publications",
        "--drop-db",
    ])
    assert res.result_code == ResultCode.DONE


async def test_restore_with_invalid_pg_restore_options_fails(target_db, db_params, pg_anon_runner, prepared_dump):
    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={prepared_dump}",
        "--drop-custom-check-constr",
        "--pg-restore-options=--non-existing-flag",
        "--drop-db",
    ])
    assert res.result_code == ResultCode.FAIL
