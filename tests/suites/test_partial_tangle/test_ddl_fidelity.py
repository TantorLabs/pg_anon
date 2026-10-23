from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

PG_11_VERSION_NUM = 110000
PG_14_VERSION_NUM = 140000

SOURCE_DB = "pg_anon_ddl_fidelity_source"

SOURCE_DDL = """
CREATE SCHEMA fid;
CREATE SCHEMA fid_app;

CREATE TABLE fid_app.items (id int PRIMARY KEY, qty int);
CREATE TABLE fid_app.skipped (id int);
INSERT INTO fid_app.items VALUES (1, 10), (2, 20);
CREATE FUNCTION fid_app.item_count() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM fid_app.items';

CREATE DOMAIN fid.d_multi AS int NOT NULL CHECK (VALUE > 0) CHECK (VALUE < 100);
CREATE DOMAIN fid.d_not_valid AS int;
ALTER DOMAIN fid.d_not_valid ADD CONSTRAINT d_not_valid_check CHECK (VALUE > 0) NOT VALID;
CREATE DOMAIN fid.d_coll AS text COLLATE "C";

CREATE TYPE fid.t_empty AS ();
CREATE TYPE fid.e_empty AS ENUM ();
CREATE TYPE fid.t_coll AS (a text COLLATE "C", b int);

CREATE TYPE fid.r_diff AS RANGE (subtype = float8, subtype_diff = float8mi);
CREATE TYPE fid.r_opc AS RANGE (subtype = text, subtype_opclass = text_pattern_ops);

CREATE FUNCTION fid.json_to_int(json) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT 1';
CREATE CAST (json AS int4) WITH FUNCTION fid.json_to_int(json);

CREATE FUNCTION fid.int_eq(int, int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT $1 = $2';
CREATE OPERATOR fid.=== (
    PROCEDURE = fid.int_eq, LEFTARG = int, RIGHTARG = int,
    COMMUTATOR = OPERATOR(fid.===), HASHES, MERGES
);
CREATE OPERATOR CLASS fid.int_hash_ops FOR TYPE int USING hash AS
    OPERATOR 1 fid.===, FUNCTION 1 hashint4(int);
CREATE INDEX items_qty_hash ON fid_app.items USING hash (qty fid.int_hash_ops);

CREATE AGGREGATE fid.agg_par(int) (
    SFUNC = int4pl, STYPE = int, SSPACE = 16, COMBINEFUNC = int4pl, PARALLEL = SAFE
);
CREATE FUNCTION fid.ffx(int, int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1';
CREATE AGGREGATE fid.agg_fx(int) (
    SFUNC = int4pl, STYPE = int, FINALFUNC = fid.ffx, FINALFUNC_EXTRA{finalfunc_modify}
);
CREATE AGGREGATE fid.agg_mv(int) (
    SFUNC = int4pl, STYPE = int, INITCOND = '0',
    MSFUNC = int4pl, MINVFUNC = int4mi, MSTYPE = int, MINITCOND = '0'
);
CREATE AGGREGATE fid.agg_max(int) (SFUNC = int4larger, STYPE = int, SORTOP = >);
CREATE FUNCTION fid.os_final(float8[], float8, float8) RETURNS float8 LANGUAGE sql IMMUTABLE AS 'SELECT $2';
CREATE AGGREGATE fid.agg_os(float8 ORDER BY float8) (
    SFUNC = array_append, STYPE = float8[], FINALFUNC = fid.os_final, FINALFUNC_EXTRA
);
"""


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner) -> AsyncIterator[str]:
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE

    version_num = await db_manager.server_version_num(SOURCE_DB)
    finalfunc_modify = ", FINALFUNC_MODIFY = SHAREABLE" if version_num >= PG_11_VERSION_NUM else ""
    await db_manager.execute(SOURCE_DB, SOURCE_DDL.replace("{finalfunc_modify}", finalfunc_modify))
    if version_num >= PG_14_VERSION_NUM:
        await db_manager.execute(
            SOURCE_DB, "CREATE TYPE fid.r_many AS RANGE (subtype = int, multirange_type_name = fid.r_many_set)"
        )

    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture(scope="module")
async def partial_dump(source_db, db_params, pg_anon_runner):
    out = output_path("ddl_fidelity")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--partial-tables-exclude-dict-file={input_dict('exclude_fid_skipped.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, "partial dump must succeed"
    return out


@pytest.fixture(scope="module")
async def restored_db(partial_dump, source_db, db_manager, db_params, pg_anon_runner) -> AsyncIterator[str]:
    name = "pg_anon_ddl_fidelity_restored"
    await db_manager.create_db(name)
    res = await pg_anon_runner.run(
        "restore",
        name,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={partial_dump}",
        ],
    )
    assert res.result_code == ResultCode.DONE, f"partial restore failed: {res.error_message}"
    yield name
    await db_manager.drop_db(name)


async def test_sql_function_reading_a_table_is_restored(restored_db, db_manager):
    rows = await db_manager.fetch(restored_db, "SELECT fid_app.item_count() AS total")
    assert rows[0]["total"] == 2, "SQL function that reads a table must be restored and work"


async def test_operator_class_and_its_index_are_restored(restored_db, db_manager):
    rows = await db_manager.fetch(
        restored_db,
        """
        SELECT
            EXISTS (SELECT 1 FROM pg_opclass WHERE opcname = 'int_hash_ops') AS has_opclass,
            EXISTS (SELECT 1 FROM pg_class WHERE relname = 'items_qty_hash') AS has_index
        """,
    )
    assert rows[0]["has_opclass"], "operator class is removed from the restore list"
    assert rows[0]["has_index"], "index that uses the operator class is not restored"
