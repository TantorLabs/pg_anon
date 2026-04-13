from pathlib import Path

import pytest

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "test_rc_source"
TARGET_DB = "test_rc_target"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    d = SUITE / "output" / name
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


@pytest.fixture(scope="module")
async def source_db(db_manager):
    await db_manager.create_db(SOURCE_DB)
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture(scope="module")
async def target_db(db_manager):
    await db_manager.create_db(TARGET_DB)
    yield TARGET_DB
    await db_manager.drop_db(TARGET_DB)
