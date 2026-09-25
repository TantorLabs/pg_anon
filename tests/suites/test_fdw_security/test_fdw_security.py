from __future__ import annotations

import pytest

from .conftest import add_user_mapping, count_user_mappings, drop_user_mapping, input_dict, output_path
from pg_anon.common.enums import ResultCode
from pg_anon.common.errors import ErrorCode


@pytest.fixture(autouse=True)
async def _clean_mapping(db_manager, source_db):
    """Keep the module-scoped source DB free of user mappings between tests."""
    await drop_user_mapping(db_manager, source_db)
    yield
    await drop_user_mapping(db_manager, source_db)


def _dump_args(out_name: str, db_params, *extra: str) -> list[str]:
    return [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={output_path(out_name)}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        *extra,
    ]


async def test_dump_blocks_visible_fdw_credentials(source_db, db_params, pg_anon_runner, db_manager):
    await add_user_mapping(db_manager, source_db, with_options=True)

    res = await pg_anon_runner.run("dump", source_db, _dump_args("blocked", db_params))

    assert res.result_code == ResultCode.FAIL
    assert res.exception is not None
    assert res.exception.code == ErrorCode.CREDENTIALS_LEAK


async def test_dump_allows_fdw_credentials_with_flag(source_db, db_params, pg_anon_runner, db_manager):
    await add_user_mapping(db_manager, source_db, with_options=True)

    res = await pg_anon_runner.run("dump", source_db, _dump_args("allowed", db_params, "--allow-fdw-credentials"))

    assert res.result_code == ResultCode.DONE


async def test_dump_not_blocked_without_visible_options(source_db, db_params, pg_anon_runner, db_manager):
    # A mapping without OPTIONS exposes no credentials -> dump must not be blocked.
    await add_user_mapping(db_manager, source_db, with_options=False)

    res = await pg_anon_runner.run("dump", source_db, _dump_args("no_options", db_params))

    assert res.result_code == ResultCode.DONE


async def test_restore_strips_user_mapping_by_default(source_db, target_db, db_params, pg_anon_runner, db_manager):
    await add_user_mapping(db_manager, source_db, with_options=True)
    out = output_path("strip_default")
    dump_res = await pg_anon_runner.run(
        "dump", source_db, _dump_args("strip_default", db_params, "--allow-fdw-credentials")
    )
    assert dump_res.result_code == ResultCode.DONE

    restore_res = await pg_anon_runner.run("restore", target_db, [f"--input-dir={out}", "--drop-db"])
    assert restore_res.result_code == ResultCode.DONE
    assert await count_user_mappings(db_manager, target_db) == 0


async def test_restore_keeps_user_mapping_with_flag(source_db, target_db, db_params, pg_anon_runner, db_manager):
    await add_user_mapping(db_manager, source_db, with_options=True)
    out = output_path("keep_flag")
    dump_res = await pg_anon_runner.run(
        "dump", source_db, _dump_args("keep_flag", db_params, "--allow-fdw-credentials")
    )
    assert dump_res.result_code == ResultCode.DONE

    restore_res = await pg_anon_runner.run(
        "restore", target_db, [f"--input-dir={out}", "--drop-db", "--keep-fdw-user-mappings"]
    )
    assert restore_res.result_code == ResultCode.DONE
    assert await count_user_mappings(db_manager, target_db) == 1
