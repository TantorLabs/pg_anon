from __future__ import annotations

from pathlib import Path

import pytest

SUITE = Path(__file__).resolve().parent


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


@pytest.fixture
async def source_db(db_manager, request):
    """Fresh source per test — these tests mutate it (re-init, add extras)."""
    name = f"pg_anon_clean_src_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_clean_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
