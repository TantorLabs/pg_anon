from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from .conftest import dump, input_dict, output_path, restore
from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

OBJECTS_DB = "pg_anon_exclude_filters_objects"

# the dump quotes "Old Types" and leaves plain_types as is, so both forms of names are covered
OBJECTS_DDL = """
CREATE SCHEMA app;
CREATE TYPE app.mood AS ENUM ('ok', 'bad');
CREATE TABLE app.t (id int PRIMARY KEY, mood app.mood);
INSERT INTO app.t VALUES (1, 'ok'), (2, 'bad');
CREATE TABLE app.skip_me (id int PRIMARY KEY);

CREATE SCHEMA "Old Types";
CREATE TYPE "Old Types".tone AS ENUM ('red');

CREATE SCHEMA plain_types;
CREATE TYPE plain_types.size AS ENUM ('s');
CREATE DOMAIN plain_types.positive AS int CHECK (VALUE > 0);

CREATE SCHEMA audit;
CREATE FUNCTION audit.on_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$;
CREATE EVENT TRIGGER audit_ddl ON ddl_command_end EXECUTE PROCEDURE audit.on_ddl();
"""


@pytest.fixture(scope="module")
async def objects_db(db_manager, pg_anon_runner) -> AsyncIterator[str]:
    await db_manager.create_db(OBJECTS_DB)
    res = await pg_anon_runner.run("init", OBJECTS_DB)
    assert res.result_code == ResultCode.DONE

    await db_manager.execute(OBJECTS_DB, OBJECTS_DDL)
    yield OBJECTS_DB
    await db_manager.drop_db(OBJECTS_DB)


async def user_types(db_manager, db_name: str) -> set[str]:
    rows = await db_manager.fetch(
        db_name,
        """
        SELECT t.typname
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typtype IN ('e', 'd') AND n.nspname IN ('app', 'Old Types', 'plain_types')
        """,
    )
    return {row["typname"] for row in rows}


async def test_partial_restore_skips_objects_of_excluded_schemas(
    objects_db, target_db, db_params, pg_anon_runner, db_manager
):
    """A partial dump keeps types and domains in metadata, the restore must not create those of excluded schemas."""
    out = output_path("partial_objects")
    res = await dump(
        pg_anon_runner,
        db_params,
        objects_db,
        out_dir=out,
        dict_file=input_dict("empty.py"),
        extra=[f"--partial-tables-exclude-dict-file={input_dict('exclude_skip_me.py')}"],
    )
    assert res.result_code == ResultCode.DONE
    metadata = json.loads((Path(out) / "metadata.json").read_text())
    assert any('"Old Types".tone' in ddl for ddl in metadata["partial_dump_types"])
    assert any("plain_types.positive" in ddl for ddl in metadata["partial_dump_domains"])

    res = await restore(
        pg_anon_runner,
        db_params,
        target_db,
        in_dir=out,
        extra=["--exclude-schema-name=Old Types,plain_types"],
    )

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await user_types(db_manager, target_db) == {"mood"}
    schemas = await db_manager.fetch(
        target_db, "SELECT nspname FROM pg_namespace WHERE nspname IN ('Old Types', 'plain_types')"
    )
    assert not schemas, "a partial restore must not create excluded schemas"
    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM app.t")
    assert rows[0]["count"] == 2


async def event_trigger_dump(pg_anon_runner, db_params, objects_db, name: str, options: list[str]) -> str:
    out = output_path(name)
    res = await dump(
        pg_anon_runner, db_params, objects_db, out_dir=out, dict_file=input_dict("empty.py"), extra=options
    )
    assert res.result_code == ResultCode.DONE
    return out


async def assert_restored_without_event_trigger(pg_anon_runner, db_params, db_manager, target_db, out, options):
    res = await restore(pg_anon_runner, db_params, target_db, in_dir=out, extra=options)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert not await db_manager.fetch(target_db, "SELECT evtname FROM pg_event_trigger")
    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM app.t")
    assert rows[0]["count"] == 2


async def test_event_trigger_of_schema_excluded_on_dump_is_left_out(
    objects_db, target_db, db_params, pg_anon_runner, db_manager
):
    """The trigger itself has no schema, so pg_dump keeps it and the restore drops it."""
    out = await event_trigger_dump(
        pg_anon_runner, db_params, objects_db, "event_trigger", ["--exclude-schema-name=audit"]
    )

    metadata = json.loads((Path(out) / "metadata.json").read_text())
    assert metadata["event_triggers"] == {
        "audit_ddl": {"trigger_name": "audit_ddl", "function_schema": "audit", "is_excluded": True}
    }
    assert metadata["excluded_event_triggers"] == ["audit_ddl"], "older versions read only this key"

    await assert_restored_without_event_trigger(pg_anon_runner, db_params, db_manager, target_db, out, [])


async def test_event_trigger_of_schema_excluded_on_restore_is_left_out(
    objects_db, target_db, db_params, pg_anon_runner, db_manager
):
    out = await event_trigger_dump(pg_anon_runner, db_params, objects_db, "event_trigger_full", [])

    metadata = json.loads((Path(out) / "metadata.json").read_text())
    assert metadata["event_triggers"]["audit_ddl"]["is_excluded"] is False
    assert "excluded_event_triggers" not in metadata

    await assert_restored_without_event_trigger(
        pg_anon_runner, db_params, db_manager, target_db, out, ["--exclude-schema-name=audit"]
    )


async def test_old_dump_drops_event_triggers_by_the_old_key(
    objects_db, target_db, db_params, pg_anon_runner, db_manager
):
    """Dumps of older versions have no "event_triggers" key, only the triggers excluded on dump."""
    out = await event_trigger_dump(
        pg_anon_runner, db_params, objects_db, "event_trigger_old", ["--exclude-schema-name=audit"]
    )
    metadata_path = Path(out) / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    del metadata["event_triggers"]
    metadata_path.write_text(json.dumps(metadata))

    await assert_restored_without_event_trigger(pg_anon_runner, db_params, db_manager, target_db, out, [])
