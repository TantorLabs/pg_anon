from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "test_mk_source"
TARGET_DB = "test_mk_target"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def expected_result(name: str) -> str:
    return str(SUITE / "expected" / name)


def output_path(name: str) -> str:
    d = SUITE / "output" / name
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, test_data):
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE

    await test_data.core_tables(SOURCE_DB)
    await test_data.customer_domain(SOURCE_DB)
    await test_data.mask_include_tables(SOURCE_DB)
    await test_data.mask_exclude_tables(SOURCE_DB)
    await test_data.misc_public_tables(SOURCE_DB)
    await test_data.complex_schema_tables(SOURCE_DB)
    await test_data.schm_other_extras(SOURCE_DB)

    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture(scope="module")
async def target_db(db_manager):
    await db_manager.create_db(TARGET_DB)
    yield TARGET_DB
    await db_manager.drop_db(TARGET_DB)
