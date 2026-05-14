"""Fixtures for test_view_data. Read-only source, single module-scoped DB."""
from __future__ import annotations

from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_view_data_source"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures):
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_minimal_env(SOURCE_DB, rows=50)
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)
