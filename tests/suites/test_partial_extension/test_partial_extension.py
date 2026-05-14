from __future__ import annotations

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


async def test_partial_restore_creates_extension_home_schema(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("ext_home_schema")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE, "full dump must succeed"

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        f"--partial-tables-dict-file={input_dict('include_users_only.py')}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE, "partial restore must succeed"

    schemas = await db_manager.fetch(target_db,
        "SELECT nspname FROM pg_namespace WHERE nspname IN ('app','trgm_home')")
    schema_names = {r["nspname"] for r in schemas}
    assert "trgm_home" in schema_names, \
        "extension home schema must be created before CREATE EXTENSION"
    assert "app" in schema_names, "user-include schema must be present"

    other_tables = await db_manager.fetch(target_db, """
        SELECT relname FROM pg_class
        WHERE relnamespace = 'other'::regnamespace AND relkind = 'r'
    """)
    assert not other_tables, (
        f"tables outside the include filter leaked through: "
        f"{[r['relname'] for r in other_tables]}"
    )

    ext_loc = await db_manager.fetch(target_db, """
        SELECT n.nspname AS extschema
        FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'pg_trgm'
    """)
    assert ext_loc, "pg_trgm must be installed on target"
    assert ext_loc[0]["extschema"] == "trgm_home", (
        f"pg_trgm should live in trgm_home, found in {ext_loc[0]['extschema']}"
    )

    sim = await db_manager.fetch(target_db,
        "SELECT trgm_home.similarity('alice', 'alise') AS s")
    assert 0 < sim[0]["s"] <= 1, "pg_trgm.similarity must be callable from trgm_home"

    rows = await db_manager.fetch(target_db,
        "SELECT count(*) AS c FROM app.users")
    src_rows = await db_manager.fetch(source_db,
        "SELECT count(*) AS c FROM app.users")
    assert rows[0]["c"] == src_rows[0]["c"]
