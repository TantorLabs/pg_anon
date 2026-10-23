from __future__ import annotations

from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_clone_source"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures):
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_full_env(SOURCE_DB)
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


async def restore(pg_anon_runner, db_params, target_db: str, in_dir: str, extra: list[str] | None = None):
    return await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={in_dir}",
            "--drop-custom-check-constr",
            *(extra or []),
        ],
    )


@pytest.fixture(scope="module")
async def full_dump(source_db, db_params, pg_anon_runner) -> str:
    out = output_path("full")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, f"dump with empty dict failed: {res.error_message}"
    return out


@pytest.fixture(scope="module")
async def clone_db(full_dump, db_manager, db_params, pg_anon_runner):
    """One restore of the full dump, shared by the tests that only read it."""
    name = "pg_anon_clone_shared"
    await db_manager.create_db(name)
    res = await restore(pg_anon_runner, db_params, name, full_dump)
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    yield name
    await db_manager.drop_db(name)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_clone_tgt_{request.node.name.replace('[', '_').replace(']', '')}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
