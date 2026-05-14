"""Shared fixtures for test_mask.

source_db is built once (module) with the full zoo — masking rules reference
hr, billing, ecommerce tables. target_db is re-created per test for isolation.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_mask_source"


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
    # rows=1 — constant-masking rules (`email -> 'masked@example.com'`)
    # would violate UNIQUE constraints otherwise.
    await fixtures.build_minimal_env(SOURCE_DB, rows=1)
    await fixtures.build_ecommerce(SOURCE_DB, products=3)
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_mask_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
