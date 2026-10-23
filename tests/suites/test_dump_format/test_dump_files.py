from __future__ import annotations

from pathlib import Path

import pytest

from .conftest import copy_dump_files, count_rows, dump, read_json, restore, STRUCTURE_FILES
from pg_anon.common.dependencies import unpack_dependencies
from pg_anon.common.enums import ResultCode
from pg_anon.version import __version__, DUMP_MIN_PG_ANON_VERSION

DATA_FILES = ("metadata.json", "dumped_tables.py")

DUMP_INFO_KEYS = {
    "pg_anon_version",
    "min_pg_anon_version",
    "created",
    "pg_version",
    "pg_dump_version",
    "dbg_stage_2_validate_data",
    "dbg_stage_3_validate_full",
}
STRUCTURE_KEYS = DUMP_INFO_KEYS | {
    "schemas",
    "extensions",
    "event_triggers",
    "constraints",
    "indexes",
    "partitions",
    "inherits",
    "dependencies",
    "dump_dependents_to_skip",
}
DATA_KEYS = DUMP_INFO_KEYS | {
    "prepared_sens_dict_files",
    "seq_lastvals",
    "files",
    "total_rows",
    "db_size",
}
SOURCE_TABLES = {("fmt", "parent"), ("fmt", "child"), ("fmt", "spare")}


def file_names(dump_dir: str) -> set[str]:
    return {item.name for item in Path(dump_dir).iterdir()}


def data_file_names(dump_dir: str) -> set[str]:
    return {name for name in file_names(dump_dir) if name.endswith(".bin.gz")}


def test_dump_writes_structure_files_and_data_files(full_dump):
    names = file_names(full_dump)

    assert names - data_file_names(full_dump) == {*STRUCTURE_FILES, *DATA_FILES}
    assert data_file_names(full_dump) == set(read_json(full_dump, "metadata.json")["files"])


def test_sync_struct_dump_writes_only_structure_files(struct_dump):
    assert file_names(struct_dump) == set(STRUCTURE_FILES), "a structure dump must not write metadata.json"


def test_sync_data_dump_writes_only_data_files(data_dump):
    names = file_names(data_dump)

    assert names - data_file_names(data_dump) == set(DATA_FILES), "a data dump must not write structure files"
    assert data_file_names(data_dump) == set(read_json(data_dump, "metadata.json")["files"])


@pytest.mark.parametrize("dump_fixture", ["full_dump", "struct_dump"])
def test_structure_json_has_the_same_keys_in_both_dump_modes(dump_fixture, request):
    structure = read_json(request.getfixturevalue(dump_fixture), "structure.json")

    assert set(structure) == STRUCTURE_KEYS
    assert structure["pg_anon_version"] == __version__
    assert structure["min_pg_anon_version"] == DUMP_MIN_PG_ANON_VERSION
    assert "fmt" in structure["schemas"]
    assert structure["extensions"] is not None, "a structure dump must write extensions too"
    assert {"child_note_idx", "child_pkey"} <= set(structure["indexes"]), "a structure dump must write indexes too"


@pytest.mark.parametrize("dump_fixture", ["full_dump", "data_dump"])
def test_metadata_json_describes_only_data(dump_fixture, request):
    metadata = read_json(request.getfixturevalue(dump_fixture), "metadata.json")

    assert set(metadata) == DATA_KEYS
    assert metadata["pg_anon_version"] == __version__
    assert metadata["min_pg_anon_version"] == DUMP_MIN_PG_ANON_VERSION
    assert {(info["schema"], info["table"]) for info in metadata["files"].values()} == SOURCE_TABLES
    assert int(metadata["total_rows"]) == 13
    assert {"fmt.parent_id_seq", "fmt.child_id_seq"} <= set(metadata["seq_lastvals"])
    assert all(info["ancestors"] == [] for info in metadata["files"].values()), "a table without parents has []"


def files_ancestors(dump_dir: str) -> dict[str, list[list[str]]]:
    files = read_json(dump_dir, "metadata.json")["files"].values()
    return {f"{info['schema']}.{info['table']}": info["ancestors"] for info in files}


@pytest.mark.parametrize("mode", ["dump", "sync-data-dump"])
async def test_metadata_json_gives_parents_of_child_tables(mode, inherits_db, db_params, pg_anon_runner):
    out = await dump(pg_anon_runner, db_params, inherits_db, f"ancestors_inherits_{mode}", mode)

    assert files_ancestors(out) == {
        "ilist.notifications": [],
        "ilist.email_notifications": [["ilist", "notifications"]],
        "ilist.sms_notifications": [["ilist", "notifications"]],
        "ilist.sms_urgent": [["ilist", "sms_notifications"], ["ilist", "notifications"]],
        "ilist.marks": [],
        "ilist.marked_email": [["ilist", "email_notifications"], ["ilist", "marks"], ["ilist", "notifications"]],
        "ilist.accounts": [],
        "ilist.reads": [],
    }


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize("mode", ["dump", "sync-data-dump"])
async def test_metadata_json_gives_parents_of_partitions(mode, partitions_db, db_params, pg_anon_runner):
    out = await dump(pg_anon_runner, db_params, partitions_db, f"ancestors_partitions_{mode}", mode)

    assert files_ancestors(out) == {
        "plist.transactions_2025_01": [["plist", "transactions"]],
        "plist.transactions_2025_02": [["plist", "transactions"]],
        "plist.transactions_2025_03": [["plist", "transactions"]],
        "plist.transactions_2026_h1": [["plist", "transactions_2026"], ["plist", "transactions"]],
        "plist.transactions_2026_h2": [["plist", "transactions_2026"], ["plist", "transactions"]],
        "plist.accounts": [],
    }


@pytest.mark.min_pg(100000)
async def test_metadata_json_gives_parents_in_the_order_of_structure_json(partitions_db, db_params, pg_anon_runner):
    out = await dump(pg_anon_runner, db_params, partitions_db, "ancestors_order", "dump")
    structure = read_json(out, "structure.json")
    described = {f"{info['schema']}.{info['table']}": info["ancestors"] for info in structure["partitions"]}

    for table, ancestors in files_ancestors(out).items():
        assert ancestors == described.get(table, []), table


def test_structure_json_is_written_without_indents(full_dump):
    text = (Path(full_dump) / "structure.json").read_text(encoding="utf-8")

    assert "\n" not in text
    assert '": ' not in text, "no spaces after separators"


def test_dependencies_are_written_in_the_compact_form(full_dump):
    packed = read_json(full_dump, "structure.json")["dependencies"]

    assert set(packed) == {"tables", "groups"}
    assert {tuple(table) for table in packed["tables"]} >= SOURCE_TABLES
    assert len(packed["tables"]) == len({tuple(table) for table in packed["tables"]}), "a table is named once"

    dependencies = unpack_dependencies(packed)
    own_tables = {tuple(tables[0]) for key, tables in dependencies.items() if key.startswith("1259:")}
    assert own_tables >= SOURCE_TABLES, "every table of the dump is described"
    assert len(dependencies) == sum(len(oids) for _, objects in packed["groups"] for oids in objects.values())


def test_structure_dump_without_exclusions_writes_dependencies(struct_dump):
    """Dependencies are a regular part of every dump with structure, not only of a partial one."""
    assert read_json(struct_dump, "structure.json")["dependencies"]["groups"]


def test_constraint_key_has_schema_table_and_name(full_dump):
    constraints = read_json(full_dump, "structure.json")["constraints"]

    assert "fmt.child.child_parent_fk" in constraints
    assert constraints["fmt.child.child_parent_fk"]["constraint_name"] == "child_parent_fk"


async def test_structure_dump_and_data_dump_make_a_full_dump(
    struct_dump, data_dump, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    """The files of a structure dump and of a data dump in one directory restore as a full dump."""
    dump_dir = copy_dump_files(struct_dump, tmp_path / "dump")
    copy_dump_files(data_dump, dump_dir)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await count_rows(db_manager, target_db, "fmt.parent") == 5
    assert await count_rows(db_manager, target_db, "fmt.child") == 5
    assert await count_rows(db_manager, target_db, "fmt.spare") == 3
