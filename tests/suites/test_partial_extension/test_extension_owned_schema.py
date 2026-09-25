from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode

OWNED_SCHEMA_DB = "pg_anon_ext_owned_schema_source"

# like columnar_internal of citus_columnar: the schema and its table are members of the extension
OWNED_SCHEMA_DDL = """
CREATE EXTENSION hstore;
CREATE SCHEMA ext_owned;
CREATE TABLE ext_owned.catalog (id int);
ALTER EXTENSION hstore ADD SCHEMA ext_owned;
ALTER EXTENSION hstore ADD TABLE ext_owned.catalog;
-- a user table in a schema of an extension, like a TimescaleDB chunk in _timescaledb_internal
CREATE TABLE ext_owned.user_data (id int PRIMARY KEY);
INSERT INTO ext_owned.user_data VALUES (1), (2), (3);

CREATE SCHEMA app;
CREATE TABLE app.users (id int PRIMARY KEY);
INSERT INTO app.users VALUES (1), (2);
"""


@pytest.fixture(scope="module")
async def owned_schema_db(db_manager, pg_anon_runner):
    await db_manager.create_db(OWNED_SCHEMA_DB)
    res = await pg_anon_runner.run("init", OWNED_SCHEMA_DB)
    assert res.result_code == ResultCode.DONE

    await db_manager.execute(OWNED_SCHEMA_DB, OWNED_SCHEMA_DDL)
    yield OWNED_SCHEMA_DB
    await db_manager.drop_db(OWNED_SCHEMA_DB)


async def test_schema_of_an_extension_is_neither_dumped_nor_created(owned_schema_db, db_params, pg_anon_runner):
    """CREATE EXTENSION makes such a schema itself, so a partial restore must not create it first."""
    out = output_path("extension_owned_schema")
    res = await pg_anon_runner.run(
        "dump",
        owned_schema_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--partial-tables-dict-file={input_dict('include_users_only.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE

    metadata = json.loads((Path(out) / "metadata.json").read_text())
    assert "ext_owned" not in metadata["schemas"]
    assert "ext_owned" not in metadata["partial_dump_schemas"]
    assert "app" in metadata["schemas"]


@pytest.mark.parametrize("options", [[], ["--schema-name=app"], ["--exclude-schema-name=ext_owned"]])
async def test_user_tables_in_a_schema_of_an_extension_are_dumped(owned_schema_db, db_params, pg_anon_runner, options):
    """Schema options skip schemas of extensions; their tables follow the extension rules, as in pg_dump."""
    out = output_path(re.sub(r"\W", "_", f"extension_owned_schema_{'_'.join(options) or 'default'}"))
    res = await pg_anon_runner.run(
        "dump",
        owned_schema_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
            *options,
        ],
    )
    assert res.result_code == ResultCode.DONE

    metadata = json.loads((Path(out) / "metadata.json").read_text())
    dumped = {(info["schema"], info["table"]): info["rows"] for info in metadata["files"].values()}
    assert dumped.get(("ext_owned", "user_data")) == "3", "user data must not be lost"
    assert ("ext_owned", "catalog") not in dumped, "internal tables of an extension are not dumped"
    assert "ext_owned" not in metadata["schemas"]
