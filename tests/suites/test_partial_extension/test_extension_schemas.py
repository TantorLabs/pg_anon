"""Extensions are restored into the schema they had in the source database.

Covers the case that used to fail outright: an extension living in a schema excluded from the dump.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode

EXT_SOURCE_DB = "pg_anon_ext_schemas_source"


@pytest.fixture(scope="module")
async def ext_source_db(db_manager, pg_anon_runner):
    """Source database with extensions spread across dedicated schemas."""
    await db_manager.create_db(EXT_SOURCE_DB)
    res = await pg_anon_runner.run("init", EXT_SOURCE_DB)
    assert res.result_code == ResultCode.DONE

    await db_manager.execute(
        EXT_SOURCE_DB,
        """
        -- non-relocatable extension: used to make the restore fail when its schema was excluded.
        -- xml2 is the only non-relocatable extension in contrib, hence not a trusted one
        CREATE SCHEMA ext_home;
        CREATE EXTENSION xml2 SCHEMA ext_home;

        -- relocatable extension whose type is referenced by a user table
        CREATE SCHEMA types_home;
        CREATE EXTENSION hstore SCHEMA types_home;

        -- extension with a dependency, both outside the default schema
        CREATE SCHEMA geo_home;
        CREATE EXTENSION cube SCHEMA geo_home;
        CREATE EXTENSION earthdistance SCHEMA geo_home;

        CREATE SCHEMA app;
        CREATE TABLE app.users (
            id serial PRIMARY KEY,
            email text NOT NULL,
            attrs types_home.hstore
        );
        INSERT INTO app.users (email, attrs) VALUES
            ('alice@example.com', 'role=>admin'::types_home.hstore),
            ('bob@example.com',   'role=>user'::types_home.hstore);
    """,
    )

    # pg_partman requires plpgsql, which sorts after it: alphabetical ordering puts the dependency last.
    # It is not part of contrib, so where it is missing the ordering check simply has one pair less.
    if await _extension_available(db_manager, EXT_SOURCE_DB, "pg_partman"):
        await db_manager.execute(
            EXT_SOURCE_DB,
            "CREATE SCHEMA partman; CREATE EXTENSION pg_partman SCHEMA partman;",
        )

    yield EXT_SOURCE_DB
    await db_manager.drop_db(EXT_SOURCE_DB)


async def _extension_available(db_manager, db_name: str, extension: str) -> bool:
    rows = await db_manager.fetch(db_name, f"SELECT 1 FROM pg_available_extensions WHERE name = '{extension}'")
    return bool(rows)


async def _dump(
    pg_anon_runner, db_params, source_db: str, dict_name: str, out_name: str, extra: list[str] | None = None
) -> str:
    out = output_path(out_name)
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict(dict_name)}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
            *(extra or []),
        ],
    )
    assert res.result_code == ResultCode.DONE, "dump must succeed"
    return out


async def _restore(pg_anon_runner, db_params, target_db: str, out: str) -> None:
    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must succeed"


async def _extension_schema(db_manager, db_name: str, extension: str) -> str | None:
    rows = await db_manager.fetch(
        db_name,
        f"""
        SELECT n.nspname AS schema
        FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = '{extension}'
    """,
    )
    return rows[0]["schema"] if rows else None


async def test_extension_from_excluded_schema_is_restored_into_its_schema(
    ext_source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """A non-relocatable extension in an excluded schema used to abort the restore."""
    out = await _dump(
        pg_anon_runner,
        db_params,
        ext_source_db,
        "mask_email.py",
        "excluded_ext_home",
        ["--exclude-schema-name=ext_home"],
    )

    with caplog.at_level(logging.WARNING, logger="pg_anon.logger"):
        await _restore(pg_anon_runner, db_params, target_db, out)

    assert await _extension_schema(db_manager, target_db, "xml2") == "ext_home", (
        "extension must be installed into the schema it had in the source"
    )
    assert any('SCHEMA "ext_home" is excluded' in message for message in caplog.messages), (
        "restoring into an excluded schema must be reported"
    )


async def test_excluded_schema_holds_no_user_data(ext_source_db, target_db, db_manager, db_params, pg_anon_runner):
    """The excluded schema is created for the extension only, its user content is not restored."""
    out = await _dump(
        pg_anon_runner,
        db_params,
        ext_source_db,
        "mask_email.py",
        "excluded_ext_home_data",
        ["--exclude-schema-name=ext_home"],
    )
    await _restore(pg_anon_runner, db_params, target_db, out)

    rows = await db_manager.fetch(
        target_db,
        """
        SELECT c.relname FROM pg_class c
        WHERE c.relnamespace = 'ext_home'::regnamespace AND c.relkind = 'r'
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = c.oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'e'
          )
    """,
    )
    assert not rows, f"excluded schema must hold extension objects only, found {[r['relname'] for r in rows]}"


async def test_extension_type_of_excluded_schema_stays_resolvable(
    ext_source_db, target_db, db_manager, db_params, pg_anon_runner
):
    """User columns typed by an extension type break unless the extension keeps its schema."""
    out = await _dump(
        pg_anon_runner,
        db_params,
        ext_source_db,
        "mask_email.py",
        "excluded_types_home",
        ["--exclude-schema-name=types_home"],
    )
    await _restore(pg_anon_runner, db_params, target_db, out)

    assert await _extension_schema(db_manager, target_db, "hstore") == "types_home", (
        "relocating the extension would break the DDL of app.users"
    )

    rows = await db_manager.fetch(target_db, "SELECT count(*) AS c FROM app.users WHERE attrs::text LIKE '%role%'")
    source_rows = await db_manager.fetch(ext_source_db, "SELECT count(*) AS c FROM app.users")
    assert rows[0]["c"] == source_rows[0]["c"], "user rows with an extension-typed column must survive"


async def test_required_extension_is_created_in_its_source_schema(
    ext_source_db, target_db, db_manager, db_params, pg_anon_runner
):
    """A dependency must not drift into the first schema of search_path."""
    out = await _dump(pg_anon_runner, db_params, ext_source_db, "empty.py", "dependency_schema")
    await _restore(pg_anon_runner, db_params, target_db, out)

    assert await _extension_schema(db_manager, target_db, "cube") == "geo_home"
    assert await _extension_schema(db_manager, target_db, "earthdistance") == "geo_home"


async def test_metadata_lists_extensions_in_dependency_order(ext_source_db, db_manager, db_params, pg_anon_runner):
    """PostgreSQL refuses CREATE EXTENSION while a required extension is missing."""
    out = await _dump(pg_anon_runner, db_params, ext_source_db, "empty.py", "dependency_order")

    extensions = json.loads(Path(out, "metadata.json").read_text(encoding="utf-8"))["extensions"]
    names = list(extensions)

    dependencies = await db_manager.fetch(
        ext_source_db,
        """
        SELECT e.extname AS extension, r.extname AS requires
        FROM pg_extension e
        JOIN pg_depend d ON d.classid = 'pg_extension'::regclass AND d.objid = e.oid
                        AND d.refclassid = 'pg_extension'::regclass
        JOIN pg_extension r ON r.oid = d.refobjid
    """,
    )
    assert dependencies, "the source database must contain at least one extension dependency"

    for row in dependencies:
        assert names.index(row["requires"]) < names.index(row["extension"]), (
            f"{row['requires']} must precede {row['extension']}, got {names}"
        )


async def test_extension_installed_in_another_schema_is_reported(
    ext_source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """Non-empty targets may already hold the extension elsewhere; it cannot be moved, only reported."""
    out = await _dump(pg_anon_runner, db_params, ext_source_db, "empty.py", "already_installed")
    await db_manager.execute(target_db, "CREATE EXTENSION xml2 SCHEMA public")

    with caplog.at_level(logging.WARNING, logger="pg_anon.logger"):
        await _restore(pg_anon_runner, db_params, target_db, out)

    assert await _extension_schema(db_manager, target_db, "xml2") == "public", (
        "a non-relocatable extension cannot be moved, it stays where the target had it"
    )
    assert any('EXTENSION "xml2" is already installed' in message for message in caplog.messages), (
        "a schema mismatch on the target must be reported"
    )


async def test_metadata_without_relocatable_field_is_supported(
    ext_source_db, target_db, db_params, pg_anon_runner, tmp_path
):
    """The restore must not read the relocatable flag any more."""
    out = await _dump(pg_anon_runner, db_params, ext_source_db, "empty.py", "no_relocatable")

    dump_dir = tmp_path / "dump"
    dump_dir.mkdir()
    for item in Path(out).iterdir():
        (dump_dir / item.name).write_bytes(item.read_bytes())

    metadata_path = dump_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    for extension in metadata["extensions"].values():
        extension.pop("relocatable", None)
    metadata_path.write_text(json.dumps(metadata, indent=4, ensure_ascii=False), encoding="utf-8")

    await _restore(pg_anon_runner, db_params, target_db, str(dump_dir))
