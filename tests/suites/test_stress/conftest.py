"""Fixtures for test_stress. Uses MEDIUM fixture sizes so runs are meaningful
but still CI-friendly. Scale up via env var PG_ANON_STRESS_ROWS if desired.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.infrastructure.sizes import MEDIUM

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_stress_source"
ROWS = int(os.getenv("PG_ANON_STRESS_ROWS", str(MEDIUM)))


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_dict(name: str) -> str:
    d = SUITE / "output_dict"
    d.mkdir(parents=True, exist_ok=True)
    return str(d / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures):
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_full_env(
        SOURCE_DB,
        hr_employees=ROWS,
        billing_customers=ROWS,
        ecommerce_products=ROWS,
        audit_entries=ROWS,
        content_articles=ROWS // 10,
    )
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_stress_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
