from __future__ import annotations

from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_create_dict_source"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_dict(name: str) -> str:
    out = SUITE / "output_dict"
    out.mkdir(parents=True, exist_ok=True)
    return str(out / name)


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


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_create_dict_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


PIPELINE_SOURCE_DB = "pg_anon_create_dict_pipe_source"


@pytest.fixture(scope="module")
async def pipeline_source_db(db_manager, pg_anon_runner, fixtures):
    await db_manager.create_db(PIPELINE_SOURCE_DB)
    res = await pg_anon_runner.run("init", PIPELINE_SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_minimal_env(PIPELINE_SOURCE_DB)
    yield PIPELINE_SOURCE_DB
    await db_manager.drop_db(PIPELINE_SOURCE_DB)
