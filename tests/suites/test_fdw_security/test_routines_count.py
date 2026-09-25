from __future__ import annotations

import pytest

from pg_anon.common.db_queries import get_user_routines_and_triggers_count_query

DB = "pg_anon_fdwsec_routines"


async def _count(db_manager, db: str) -> int:
    rows = await db_manager.fetch(db, get_user_routines_and_triggers_count_query())
    return int(rows[0]["total"])


async def _ext_available(db_manager, db: str, name: str) -> bool:
    rows = await db_manager.fetch(db, f"SELECT 1 FROM pg_available_extensions WHERE name = '{name}'")
    return bool(rows)


@pytest.fixture
async def clean_db(db_manager):
    await db_manager.create_db(DB)
    yield DB
    await db_manager.drop_db(DB)


async def test_extension_objects_excluded_user_objects_counted(db_manager, clean_db):
    if not await _ext_available(db_manager, clean_db, "pgcrypto"):
        pytest.skip("pgcrypto extension is not available")

    base = await _count(db_manager, clean_db)

    # An extension adds many functions, but they must not change the count.
    await db_manager.execute(clean_db, "CREATE EXTENSION IF NOT EXISTS pgcrypto")
    assert await _count(db_manager, clean_db) == base

    # A user function is counted.
    await db_manager.execute(
        clean_db,
        "CREATE FUNCTION public.uf() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;",
    )
    assert await _count(db_manager, clean_db) == base + 1

    # A user trigger and its function add two more.
    await db_manager.execute(
        clean_db,
        """
        CREATE TABLE public.t (id int);
        CREATE FUNCTION public.trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
        CREATE TRIGGER my_trg BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION public.trg_fn();
        """,
    )
    assert await _count(db_manager, clean_db) == base + 3
