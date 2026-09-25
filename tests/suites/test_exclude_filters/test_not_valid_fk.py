from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from asyncpg import ForeignKeyViolationError

from .conftest import dump, input_dict, output_path, restore
from pg_anon.common.enums import ResultCode
from pg_anon.common.errors import ErrorCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

FK_CASES_DB = "pg_anon_exclude_filters_fk_cases"

# the data of excluded_parent and events_old is left out by fk_cases.py
FK_CASES_DDL = """
CREATE SCHEMA refs;
CREATE TABLE refs.empty_parent (id int PRIMARY KEY);
CREATE TABLE refs.empty_child (id int PRIMARY KEY, parent_id int REFERENCES refs.empty_parent(id));
-- a NULL key gives the referring table a row without breaking the key
INSERT INTO refs.empty_child VALUES (1, NULL);

CREATE TABLE refs.excluded_parent (id int PRIMARY KEY);
INSERT INTO refs.excluded_parent SELECT generate_series(1, 3);
CREATE TABLE refs.idle_child (id int PRIMARY KEY, parent_id int REFERENCES refs.excluded_parent(id));

CREATE TABLE refs.events (id int, day int, PRIMARY KEY (id, day)) PARTITION BY RANGE (day);
CREATE TABLE refs.events_old PARTITION OF refs.events FOR VALUES FROM (0) TO (100);
CREATE TABLE refs.events_new PARTITION OF refs.events FOR VALUES FROM (100) TO (200);
INSERT INTO refs.events VALUES (1, 10), (2, 150);
CREATE TABLE refs.event_links (
    id int PRIMARY KEY, event_id int, event_day int, FOREIGN KEY (event_id, event_day) REFERENCES refs.events
);
INSERT INTO refs.event_links VALUES (1, 1, 10), (2, 2, 150);
"""


async def fk_state(db_manager, db_name: str) -> dict[str, bool]:
    rows = await db_manager.fetch(
        db_name,
        "SELECT conname, convalidated FROM pg_constraint WHERE contype = 'f'",
    )
    return {row["conname"]: row["convalidated"] for row in rows}


async def dump_with_conflict(pg_anon_runner, db_params, source_db, out_name: str) -> str:
    """Dump where "dictionary" keeps the schema, so one of its tables gets structure without data."""
    out = output_path(out_name)
    res = await dump(
        pg_anon_runner,
        db_params,
        source_db,
        out_dir=out,
        dict_file=input_dict("profile_conflict.py"),
    )
    assert res.result_code == ResultCode.DONE
    return out


@pytest.fixture
async def conflict_dump(source_db, db_params, pg_anon_runner) -> str:
    return await dump_with_conflict(pg_anon_runner, db_params, source_db, "dict_conflict")


async def test_dictionary_exclude_keeps_structure_of_the_schema(conflict_dump):
    """The sensitive dictionary only leaves out data, the schema stays in the dump."""
    metadata = json.loads((Path(conflict_dump) / "metadata.json").read_text())

    assert "profile" in metadata["schemas"]


async def test_dump_keeps_data_of_the_rule_table_only(conflict_dump):
    metadata = json.loads((Path(conflict_dump) / "metadata.json").read_text())
    dumped = {(info["schema"], info["table"]) for info in metadata["files"].values()}

    assert ("profile", "items") in dumped
    assert ("profile", "owners") not in dumped


async def test_restore_creates_unenforceable_fk_as_not_valid(
    conflict_dump, target_db, db_params, pg_anon_runner, db_manager, caplog
):
    """The referenced table has no data, so the key is kept but left unchecked."""
    with caplog.at_level(logging.WARNING, logger="pg_anon.logger"):
        res = await restore(pg_anon_runner, db_params, target_db, in_dir=conflict_dump)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert (await fk_state(db_manager, target_db))["items_owner_id_fkey"] is False
    assert any("created as NOT VALID" in message for message in caplog.messages), (
        "keys left unchecked must be listed at the end of the restore"
    )


async def test_not_valid_fk_still_rejects_new_rows(conflict_dump, target_db, db_params, pg_anon_runner, db_manager):
    res = await restore(pg_anon_runner, db_params, target_db, in_dir=conflict_dump)
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"

    with pytest.raises(ForeignKeyViolationError):
        await db_manager.execute(target_db, "INSERT INTO profile.items VALUES (999, 999)")


async def test_struct_only_restore_keeps_every_fk_valid(source_db, target_db, db_params, pg_anon_runner, db_manager):
    """A structure dump has no data at all, which must not turn its keys into NOT VALID ones."""
    out = output_path("struct_only_fk")
    res = await dump(
        pg_anon_runner,
        db_params,
        source_db,
        out_dir=out,
        dict_file=input_dict("empty.py"),
        mode="sync-struct-dump",
    )
    assert res.result_code == ResultCode.DONE

    res = await restore(pg_anon_runner, db_params, target_db, in_dir=out, mode="sync-struct-restore")
    assert res.result_code == ResultCode.DONE

    assert (await fk_state(db_manager, target_db))["items_owner_id_fkey"] is True


async def test_old_dump_without_constraint_definition_fails_as_before(
    conflict_dump, target_db, db_params, pg_anon_runner, db_manager
):
    """Dumps made before this change carry no definition, so the old behaviour stays."""
    metadata_path = Path(conflict_dump) / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    for constraint in metadata["constraints"].values():
        constraint.pop("definition", None)
        constraint.pop("contype", None)
    metadata_path.write_text(json.dumps(metadata))

    res = await restore(pg_anon_runner, db_params, target_db, in_dir=conflict_dump)

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.RESTORE_FAILED
    assert "items_owner_id_fkey" not in await fk_state(db_manager, target_db), "the key is not created at all"


@pytest.fixture(scope="module")
async def fk_cases_dump(db_manager, db_params, pg_anon_runner) -> AsyncIterator[str]:
    await db_manager.create_db(FK_CASES_DB)
    res = await pg_anon_runner.run("init", FK_CASES_DB)
    assert res.result_code == ResultCode.DONE
    await db_manager.execute(FK_CASES_DB, FK_CASES_DDL)

    out = output_path("fk_cases")
    res = await dump(pg_anon_runner, db_params, FK_CASES_DB, out_dir=out, dict_file=input_dict("fk_cases.py"))
    assert res.result_code == ResultCode.DONE
    yield out
    await db_manager.drop_db(FK_CASES_DB)


@pytest.fixture
async def fk_cases_restored(fk_cases_dump, target_db, db_params, pg_anon_runner, db_manager) -> dict[str, bool]:
    res = await restore(pg_anon_runner, db_params, target_db, in_dir=fk_cases_dump)
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    return await fk_state(db_manager, target_db)


async def test_fk_cases_dump_has_the_expected_data(fk_cases_dump):
    dumped = {
        info["table"]: info["rows"]
        for info in json.loads((Path(fk_cases_dump) / "metadata.json").read_text())["files"].values()
    }

    assert dumped.get("empty_parent") == "0", "a table without rows is still dumped"
    assert "excluded_parent" not in dumped
    assert "events_old" not in dumped
    assert dumped.get("events_new") == "1"


async def test_fk_to_a_dumped_table_without_rows_stays_valid(fk_cases_restored):
    """The referenced table is in the dump, so the key can be checked."""
    assert fk_cases_restored["empty_child_parent_id_fkey"] is True


async def test_fk_without_rows_to_check_stays_valid(fk_cases_restored):
    """The referenced table has no data, but the referring table has no rows to break the key."""
    assert fk_cases_restored["idle_child_parent_id_fkey"] is True


async def test_fk_to_a_partition_without_data_is_not_valid(fk_cases_restored):
    assert fk_cases_restored["event_links_event_id_event_day_fkey"] is False
