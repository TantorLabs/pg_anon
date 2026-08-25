"""Fixtures for the demo suite.

The suite runs the very files shipped in `demo/` — the ones the Quick Start in
README.md tells the reader to use — so the guide cannot silently rot.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent
REPO_ROOT = SUITE.parents[2]
DEMO = REPO_ROOT / "demo"

DATA_SQL = DEMO / "data.sql"
META_DICT = DEMO / "meta_dict.py"

SOURCE_DB = "pg_anon_demo_source"


def output_dict(name: str) -> str:
    out = SUITE / "output_dict"
    out.mkdir(parents=True, exist_ok=True)
    return str(out / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner):
    """Source database prepared exactly as the guide tells the reader to do it."""
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    # The guide feeds this file to psql as-is; run it as-is here too.
    await db_manager.execute(SOURCE_DB, DATA_SQL.read_text(encoding="utf-8"))
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_demo_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
