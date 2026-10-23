"""Dumps made by pg_anon 1.12.0 must still restore, and the catalog must be what 1.12.0 produced.

The dumps and the expected differences are made in stage 1b. Layout:

    legacy_1_12/full/        dump of the source below, no filters
    legacy_1_12/whitelist/   dump with --partial-tables-dict-file=input_dict/include_kept.py
    legacy_1_12/blacklist/   dump with --partial-tables-exclude-dict-file=input_dict/exclude_gone.py
    expected/legacy_<kind>.py  same format as the other expected files: what is not in the target after a plain restore

The source: `Fixtures.build_partial_tangle(db, anomalies=False, superuser_objects=False, gone_row_dependents=False)`
on the test server (PG 15).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from tests.infrastructure.assertions import (
    assert_target_lacks_exactly,
    catalog_snapshot,
    load_expected_missing,
    snapshot_diff,
)

from .conftest import build_source, expected_path, output_path, run_restore, SUITE
from pg_anon.common.enums import ResultCode

LEGACY_SOURCE_DB = "pg_anon_tangle_legacy_source"
LEGACY_BUILDER_FLAGS = {"anomalies": False, "superuser_objects": False, "gone_row_dependents": False}


# The dumps and expected catalogs were made on PG 15
pytestmark = [pytest.mark.min_pg(150000), pytest.mark.max_pg(160000)]


@pytest.fixture(scope="module")
async def legacy_source_db(db_manager, pg_anon_runner, fixtures):
    await build_source(db_manager, pg_anon_runner, fixtures, LEGACY_SOURCE_DB, **LEGACY_BUILDER_FLAGS)
    yield LEGACY_SOURCE_DB
    await db_manager.drop_db(LEGACY_SOURCE_DB)


@pytest.mark.parametrize("kind", ["full", "whitelist", "blacklist"])
async def test_dump_of_version_1_12_restores_with_the_catalog_of_that_version(
    kind, legacy_source_db, target_db, db_manager, db_params, pg_anon_runner
):
    dump_dir = SUITE / "legacy_1_12" / kind
    expected_file = expected_path(f"legacy_{kind}.py")
    if not (dump_dir / "metadata.json").exists():
        pytest.fail(f"dump of pg_anon 1.12.0 is missing: {dump_dir}/metadata.json (made in stage 1b)")
    if not Path(expected_file).exists():
        pytest.fail(f"expected file is missing: {expected_file} (made in stage 1b)")

    # restore writes its TOC lists into the input dir, so it reads a copy
    work_dir = Path(output_path(f"legacy_{kind}"))
    shutil.rmtree(work_dir)
    shutil.copytree(dump_dir, work_dir)
    restore = await run_restore(pg_anon_runner, db_params, target_db, str(work_dir))

    assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"
    diff = snapshot_diff(
        await catalog_snapshot(db_manager, legacy_source_db), await catalog_snapshot(db_manager, target_db)
    )
    # the target must lack exactly what pg_anon 1.12.0 did not restore
    assert_target_lacks_exactly(diff, load_expected_missing(expected_file, 150000))


def legacy_dump_without_data_of(table: str, name: str) -> str:
    """Copy of the full dump of 1.12.0 without the data file entry of the table, as if its data were excluded."""
    work_dir = Path(output_path(name))
    shutil.rmtree(work_dir)
    shutil.copytree(SUITE / "legacy_1_12" / "full", work_dir)
    metadata_file = work_dir / "metadata.json"
    metadata = json.loads(metadata_file.read_text())
    left_out = {
        name for name, info in metadata["files"].items() if (info["schema"], info["table"]) == ("tg_core", table)
    }
    metadata["total_rows"] -= sum(int(metadata["files"][name]["rows"]) for name in left_out)
    metadata["files"] = {name: info for name, info in metadata["files"].items() if name not in left_out}
    metadata_file.write_text(json.dumps(metadata))
    return str(work_dir)


async def key_states(db_manager, db_name: str) -> dict[str, str]:
    rows = await db_manager.fetch(
        db_name,
        "SELECT conname, convalidated FROM pg_constraint "
        "WHERE contype = 'f' AND connamespace = 'tg_core'::regnamespace",
    )
    return {row["conname"]: "valid" if row["convalidated"] else "not valid" for row in rows}


@pytest.mark.parametrize(
    ("table", "keys"),
    [
        # no row of tg_core.orders refers to gone_t: pg_anon 1.12.0 gave a NOT VALID key
        pytest.param("gone_t", {"gone_fk_out": "valid"}, id="every_reference_is_null"),
        # rows refer to customers: the same NOT VALID keys as pg_anon 1.12.0 gave
        pytest.param(
            "customers",
            {"orders_customer_id_fkey": "not valid", "gone_t_customer_id_fkey": "not valid"},
            id="rows_refer_to_the_table",
        ),
    ],
)
async def test_dump_of_version_1_12_gives_keys_to_a_table_without_data_not_worse_than_that_version(
    table, keys, legacy_source_db, target_db, db_manager, db_params, pg_anon_runner
):
    work_dir = legacy_dump_without_data_of(table, f"legacy_without_{table}")

    restore = await run_restore(pg_anon_runner, db_params, target_db, work_dir)

    assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"
    states = await key_states(db_manager, target_db)
    assert {key: states.get(key, "missing") for key in keys} == keys
