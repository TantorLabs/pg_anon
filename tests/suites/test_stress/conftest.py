from pathlib import Path

import pytest

from tests.infrastructure.data import DEFAULT_ROWS

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "test_st_source"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_dict(name: str) -> str:
    d = SUITE / "output_dict"
    d.mkdir(parents=True, exist_ok=True)
    return str(d / name)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, test_data):
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await test_data.stress_env(SOURCE_DB, count=DEFAULT_ROWS * 10)
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)
