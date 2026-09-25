from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
import yaml

from .conftest import dump, input_dict, output_path, restore
from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME
from pg_anon.common.enums import ResultCode

ALL_SCHEMAS = {
    ANON_UTILS_DB_SCHEMA_NAME,
    "crm",
    "my schema",
    "only_types",
    "pgq",
    "profile",
    "public",
    "s_excl",
    "s_keep",
}


def read_metadata(out_dir: str) -> dict:
    return json.loads((Path(out_dir) / "metadata.json").read_text())


def dumped_tables(out_dir: str) -> set[tuple[str, str]]:
    return {(info["schema"], info["table"]) for info in read_metadata(out_dir)["files"].values()}


async def target_schemas(db_manager, db_name: str) -> set[str]:
    rows = await db_manager.fetch(
        db_name, "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'"
    )
    return {row["nspname"] for row in rows}


async def dump_with(pg_anon_runner, db_params, source_db, name: str, options: list[str], mode: str = "dump"):
    out = output_path(name)
    res = await dump(
        pg_anon_runner, db_params, source_db, out_dir=out, dict_file=input_dict("empty.py"), mode=mode, extra=options
    )
    return res, out


@pytest.mark.parametrize(
    ("options", "expected_schemas"),
    [
        (["--exclude-schema-name=s_excl"], ALL_SCHEMAS - {"s_excl"}),
        (["--exclude-schema-mask=^s_"], ALL_SCHEMAS - {"s_excl", "s_keep"}),
        (["--exclude-schema-name=s_excl,my schema"], ALL_SCHEMAS - {"s_excl", "my schema"}),
        (["--schema-name=s_keep"], {ANON_UTILS_DB_SCHEMA_NAME, "s_keep"}),
        (["--schema-mask=^s_", "--exclude-schema-name=s_excl"], {ANON_UTILS_DB_SCHEMA_NAME, "s_keep"}),
        (["--exclude-schema-name=s_ex"], ALL_SCHEMAS),
        (["--schema-name=s_keep", "--schema-mask=^crm"], {ANON_UTILS_DB_SCHEMA_NAME, "s_keep", "crm"}),
        (["--exclude-schema-name=no_such_schema"], ALL_SCHEMAS),
        (["--exclude-schema-name=pgq"], ALL_SCHEMAS - {"pgq"}),
    ],
)
async def test_schema_options_set_dump_composition(source_db, db_params, pg_anon_runner, options, expected_schemas):
    res, out = await dump_with(pg_anon_runner, db_params, source_db, f"opts_{'_'.join(options)}", options)

    assert res.result_code == ResultCode.DONE
    metadata = read_metadata(out)
    assert set(metadata["schemas"]) == expected_schemas
    assert {schema for schema, _ in dumped_tables(out)} <= expected_schemas


async def test_excluded_schema_is_absent_after_restore(source_db, target_db, db_params, pg_anon_runner, db_manager):
    res, out = await dump_with(
        pg_anon_runner, db_params, source_db, "excluded_restore", ["--exclude-schema-name=s_excl"]
    )
    assert res.result_code == ResultCode.DONE

    res = await restore(pg_anon_runner, db_params, target_db, in_dir=out)
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert "s_excl" not in await target_schemas(db_manager, target_db)


async def test_include_schema_matching_nothing_leaves_no_objects(source_db, db_params, pg_anon_runner):
    res, _ = await dump_with(pg_anon_runner, db_params, source_db, "include_nothing", ["--schema-name=no_such"])

    assert res.result_code == ResultCode.FAIL
    assert "No objects for dump" in (res.error_message or "")


@pytest.mark.parametrize("mode", ["dump", "sync-struct-dump", "sync-data-dump"])
async def test_every_dump_mode_writes_schemas(source_db, db_params, pg_anon_runner, mode):
    res, out = await dump_with(
        pg_anon_runner, db_params, source_db, f"schemas_{mode}", ["--exclude-schema-name=s_excl"], mode
    )

    assert res.result_code == ResultCode.DONE
    assert set(read_metadata(out)["schemas"]) == ALL_SCHEMAS - {"s_excl"}


async def test_schema_count_is_logged_only_with_options(source_db, db_params, pg_anon_runner, caplog):
    with caplog.at_level(logging.INFO, logger="pg_anon.logger"):
        _, plain_out = await dump_with(pg_anon_runner, db_params, source_db, "log_plain", [])
    assert not [message for message in caplog.messages if "Dump includes" in message]

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="pg_anon.logger"):
        _, out = await dump_with(pg_anon_runner, db_params, source_db, "log_options", ["--exclude-schema-name=s_excl"])

    kept = len(read_metadata(out)["schemas"])
    assert f"Dump includes {kept} of {len(ALL_SCHEMAS)} schemas" in caplog.messages
    assert len(read_metadata(plain_out)["schemas"]) == len(ALL_SCHEMAS)


async def test_struct_and_data_dumps_with_same_options_fit(source_db, target_db, db_params, pg_anon_runner, db_manager):
    options = ["--exclude-schema-name=s_excl"]
    res, struct_out = await dump_with(pg_anon_runner, db_params, source_db, "pair_struct", options, "sync-struct-dump")
    assert res.result_code == ResultCode.DONE
    res, data_out = await dump_with(pg_anon_runner, db_params, source_db, "pair_data", options, "sync-data-dump")
    assert res.result_code == ResultCode.DONE

    res = await restore(pg_anon_runner, db_params, target_db, in_dir=struct_out, mode="sync-struct-restore")
    assert res.result_code == ResultCode.DONE, f"structure restore failed: {res.error_message}"
    res = await restore(pg_anon_runner, db_params, target_db, in_dir=data_out, mode="sync-data-restore")
    assert res.result_code == ResultCode.DONE, f"data does not fit the structure: {res.error_message}"

    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM s_keep.b")
    assert rows[0]["count"] == 5


async def test_dump_does_not_need_pg_restore(source_db, db_params, pg_anon_runner, tmp_path):
    config = yaml.safe_load(Path(db_params.test_config).read_text())
    for utils in config["pg-utils-versions"].values():
        utils["pg_restore"] = "/nonexistent/pg_restore"
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump(config))

    res, out = await dump_with(
        pg_anon_runner,
        db_params,
        source_db,
        "no_pg_restore",
        ["--exclude-schema-name=s_excl", f"--config={config_path}"],
    )

    assert res.result_code == ResultCode.DONE, f"dump must not call pg_restore: {res.error_message}"
    assert read_metadata(out)["schemas"]


async def test_restore_excludes_schema_and_what_refers_to_it(
    source_db, target_db, db_params, pg_anon_runner, db_manager
):
    """Excluding a schema on restore drops its objects and the keys and views of other schemas that use it."""
    res, out = await dump_with(pg_anon_runner, db_params, source_db, "restore_side", [])
    assert res.result_code == ResultCode.DONE

    res = await restore(pg_anon_runner, db_params, target_db, in_dir=out, extra=["--exclude-schema-name=profile"])
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"

    assert "profile" not in await target_schemas(db_manager, target_db)
    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM crm.orders")
    assert rows[0]["count"] == 3, "tables of kept schemas keep their data"
    keys = await db_manager.fetch(target_db, "SELECT conname FROM pg_constraint WHERE contype = 'f'")
    assert not keys, "a key to an excluded schema cannot be created"
    views = await db_manager.fetch(target_db, "SELECT viewname FROM pg_views WHERE schemaname = 'crm'")
    assert not views, "a view on an excluded schema cannot be created"


async def test_restore_logs_schema_count(source_db, target_db, db_params, pg_anon_runner, caplog):
    res, out = await dump_with(pg_anon_runner, db_params, source_db, "restore_log", [])
    assert res.result_code == ResultCode.DONE

    with caplog.at_level(logging.INFO, logger="pg_anon.logger"):
        await restore(pg_anon_runner, db_params, target_db, in_dir=out, extra=["--exclude-schema-name=s_excl"])

    assert f"Restore includes {len(ALL_SCHEMAS) - 1} of {len(ALL_SCHEMAS)} schemas" in caplog.messages


async def test_restore_of_old_dump_without_schemas_key(source_db, target_db, db_params, pg_anon_runner, db_manager):
    res, out = await dump_with(pg_anon_runner, db_params, source_db, "old_dump", [])
    assert res.result_code == ResultCode.DONE

    metadata_path = Path(out) / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    del metadata["schemas"]
    metadata_path.write_text(json.dumps(metadata))

    res = await restore(
        pg_anon_runner, db_params, target_db, in_dir=out, extra=["--exclude-schema-name=s_excl,only_types"]
    )
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert not {"s_excl", "only_types"} & await target_schemas(db_manager, target_db)


async def test_partial_dump_keeps_partial_schemas_key(source_db, db_params, pg_anon_runner):
    res, out = await dump_with(
        pg_anon_runner,
        db_params,
        source_db,
        "partial_key",
        [f"--partial-tables-exclude-dict-file={input_dict('exclude_my_table.py')}", "--exclude-schema-name=s_excl"],
    )

    assert res.result_code == ResultCode.DONE
    metadata = read_metadata(out)
    assert set(metadata["partial_dump_schemas"]) == set(metadata["schemas"]) == ALL_SCHEMAS - {"s_excl"}
    assert not [ddl for ddl in metadata.get("partial_dump_types") or [] if "s_excl" in ddl]


async def test_partial_restore_creates_schema_with_a_space_in_name(
    source_db, target_db, db_params, pg_anon_runner, db_manager
):
    """A partial restore of a full dump takes schema names from the TOC, where names are not quoted."""
    res, out = await dump_with(pg_anon_runner, db_params, source_db, "schema_with_space", [])
    assert res.result_code == ResultCode.DONE

    res = await restore(
        pg_anon_runner,
        db_params,
        target_db,
        in_dir=out,
        extra=[f"--partial-tables-exclude-dict-file={input_dict('exclude_my_table.py')}"],
    )
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"

    rows = await db_manager.fetch(target_db, 'SELECT count(*) AS count FROM "my schema".t')
    assert rows[0]["count"] == 4


@pytest.mark.parametrize("pair", [False, True])
async def test_dump_side_exclusion_drops_what_refers_to_the_schema(
    source_db, target_db, db_params, pg_anon_runner, db_manager, pair
):
    """Keys and views of kept schemas that use an excluded schema are left out on restore."""
    options = ["--exclude-schema-name=profile"]
    if pair:
        res, struct_out = await dump_with(
            pg_anon_runner, db_params, source_db, "ref_struct", options, "sync-struct-dump"
        )
        assert res.result_code == ResultCode.DONE
        res = await restore(pg_anon_runner, db_params, target_db, in_dir=struct_out, mode="sync-struct-restore")
        expected_orders = 0
    else:
        res, out = await dump_with(pg_anon_runner, db_params, source_db, "ref_full", options)
        assert res.result_code == ResultCode.DONE
        res = await restore(pg_anon_runner, db_params, target_db, in_dir=out)
        expected_orders = 3

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    keys = await db_manager.fetch(target_db, "SELECT conname FROM pg_constraint WHERE contype = 'f'")
    assert not keys
    views = await db_manager.fetch(target_db, "SELECT viewname FROM pg_views WHERE schemaname = 'crm'")
    assert not views
    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM crm.orders")
    assert rows[0]["count"] == expected_orders, "the schema that refers to the excluded one is kept"


@pytest.fixture
async def full_dump(source_db, db_params, pg_anon_runner) -> str:
    res, out = await dump_with(pg_anon_runner, db_params, source_db, "full_for_restore", [])
    assert res.result_code == ResultCode.DONE
    return out


@pytest.mark.parametrize(
    ("options", "expected_schemas"),
    [
        (["--schema-name=s_keep"], {"s_keep"}),
        (["--schema-name=crm,profile"], {"crm", "profile"}),
        (["--schema-mask=^s_", "--exclude-schema-name=s_excl"], {"s_keep"}),
        (["--schema-mask=^(pgq|my schema)$"], {"pgq", "my schema"}),
    ],
)
async def test_restore_keeps_only_selected_schemas(
    full_dump, target_db, db_params, pg_anon_runner, db_manager, options, expected_schemas
):
    res = await restore(pg_anon_runner, db_params, target_db, in_dir=full_dump, extra=options)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await target_schemas(db_manager, target_db) == {ANON_UTILS_DB_SCHEMA_NAME, "public", *expected_schemas}


async def test_restore_excludes_schema_with_a_space_in_name(
    full_dump, target_db, db_params, pg_anon_runner, db_manager
):
    """Names in the TOC are not quoted, so the entries of such a schema are found by pg_restore."""
    res = await restore(
        pg_anon_runner, db_params, target_db, in_dir=full_dump, extra=["--exclude-schema-name=my schema"]
    )

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert "my schema" not in await target_schemas(db_manager, target_db)
    rows = await db_manager.fetch(target_db, 'SELECT count(*) AS count FROM s_keep."MixedCase"')
    assert rows[0]["count"] == 3


async def test_restore_excludes_comment_and_grant_of_the_schema(
    full_dump, target_db, db_params, pg_anon_runner, db_manager
):
    """COMMENT and GRANT on an excluded schema would fail on restore, as the schema is not created."""
    res = await restore(pg_anon_runner, db_params, target_db, in_dir=full_dump, extra=["--exclude-schema-name=s_excl"])

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert "s_excl" not in await target_schemas(db_manager, target_db)
