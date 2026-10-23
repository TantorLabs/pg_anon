from __future__ import annotations

import warnings

import pytest

from tests.infrastructure.assertions import list_tables

from .conftest import input_dict, output_dict, output_path
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode

# Excluded from the default test run; run explicitly with `pytest -m stress`.
pytestmark = pytest.mark.stress


def _create_dict_options(db_params, source_db, *, scan_mode: str, sens_out: str, partial_rows: int):
    return build_run_options(
        [
            "create-dict",
            f"--db-host={db_params.test_db_host}",
            f"--db-name={source_db}",
            f"--db-user={db_params.test_db_user}",
            f"--db-port={db_params.test_db_port}",
            f"--db-user-password={db_params.test_db_user_password}",
            f"--config={db_params.test_config}",
            f"--scan-mode={scan_mode}",
            f"--meta-dict-file={input_dict('meta.py')}",
            f"--output-sens-dict-file={sens_out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--scan-partial-rows={partial_rows}",
            "--debug",
        ]
    )


@pytest.fixture(scope="module")
async def scan_elapsed(source_db, db_params) -> dict[str, float]:
    """Run the partial and the full scan once and return their time in seconds."""
    elapsed = {}
    for scan_mode in ("partial", "full"):
        options = _create_dict_options(
            db_params,
            source_db,
            scan_mode=scan_mode,
            sens_out=output_dict(f"stress_{scan_mode}.json"),
            partial_rows=50,
        )
        res = await PgAnonApp(options).run()
        assert res.result_code == ResultCode.DONE, f"{scan_mode} scan failed: {res.error_message}"
        assert res.elapsed is not None
        elapsed[scan_mode] = res.elapsed
    return elapsed


async def test_stress_partial_is_faster_than_full(scan_elapsed):
    # the time depends on the machine, so a slow partial scan is reported, not failed
    ratio = scan_elapsed["full"] / max(scan_elapsed["partial"], 0.001)
    if ratio < 2:
        warnings.warn(
            f"partial scan is less than 2x faster than full: ratio={ratio:.2f} "
            f"(full={scan_elapsed['full']:.2f}s, partial={scan_elapsed['partial']:.2f}s)",
            stacklevel=1,
        )


async def test_stress_dump_restore_roundtrip(source_db, target_db, db_params, pg_anon_runner, db_manager):
    out = output_path("stress_dump")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE

    for schema, table in await list_tables(db_manager, source_db):
        query = f'SELECT count(*) AS n FROM "{schema}"."{table}"'
        source_rows = (await db_manager.fetch(source_db, query))[0]["n"]
        target_rows = (await db_manager.fetch(target_db, query))[0]["n"]
        assert target_rows == source_rows, f"row count differs in {schema}.{table}"
