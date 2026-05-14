"""Tests for `restore --clean-db`.

--clean-db should DROP all objects the dump knows about and recreate them,
even if the target DB already contains those objects. It must fail, however,
if the target has extra tables not present in the dump (to avoid silent data loss).
"""
from __future__ import annotations

from tests.infrastructure.assertions import check_rows_count
from tests.infrastructure.sizes import SMALL

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


async def _init_env(fixtures, pg_anon_runner, db_name, rows):
    res = await pg_anon_runner.run("init", db_name)
    assert res.result_code == ResultCode.DONE
    await fixtures.ensure_extensions(db_name)
    await fixtures.build_hr(db_name, employees=rows)


async def _dump(pg_anon_runner, db_params, source_db, out_dir):
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out_dir}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE


async def _restore_clean(pg_anon_runner, db_params, target_db, in_dir):
    return await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
        "--drop-custom-check-constr",
        "--clean-db",
    ])


async def test_clean_db_replaces_existing_data(
    source_db, target_db, db_manager, pg_anon_runner, db_params, fixtures,
):
    """Dump small env, seed target with 5x data, --clean-db restore must reset to dumped counts."""
    out = output_path("replace_data")

    await _init_env(fixtures, pg_anon_runner, source_db, SMALL)
    await _dump(pg_anon_runner, db_params, source_db, out)

    await _init_env(fixtures, pg_anon_runner, target_db, SMALL * 5)
    res = await _restore_clean(pg_anon_runner, db_params, target_db, out)
    assert res.result_code == ResultCode.DONE

    assert await check_rows_count(db_manager, target_db, [
        ["hr", "employee", SMALL],
    ])


async def test_clean_db_fails_when_target_has_extra_tables(
    source_db, target_db, db_manager, pg_anon_runner, db_params, fixtures,
):
    """Target has tables not present in the dump → restore must FAIL to protect data."""
    out = output_path("extra_tables")

    await _init_env(fixtures, pg_anon_runner, source_db, SMALL)
    await _dump(pg_anon_runner, db_params, source_db, out)

    await _init_env(fixtures, pg_anon_runner, target_db, SMALL)
    await fixtures.build_billing(target_db, customers=SMALL)

    res = await _restore_clean(pg_anon_runner, db_params, target_db, out)
    assert res.result_code == ResultCode.FAIL
