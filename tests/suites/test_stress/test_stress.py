"""Stress tests — MEDIUM-sized zoo (~1k rows per table).

Two concerns:

1. Scale — all modes must complete without crashing on a non-trivial DB.
2. Performance — partial-scan must be meaningfully faster than full-scan,
   otherwise the flag has regressed.
"""
from __future__ import annotations

from .conftest import input_dict, output_dict, output_path
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode

_elapsed: dict[str, float] = {}


def _create_dict_options(db_params, source_db, *, scan_mode: str, sens_out: str, partial_rows: int):
    return build_run_options([
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
        f"--processes={db_params.test_processes}",
        f"--scan-partial-rows={partial_rows}",
        "--debug",
    ])


async def test_stress_partial_scan_completes(source_db, db_params):
    """Partial scan on MEDIUM zoo must finish successfully; record elapsed."""
    options = _create_dict_options(
        db_params, source_db,
        scan_mode="partial",
        sens_out=output_dict("stress_partial.json"),
        partial_rows=50,
    )
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE
    _elapsed["partial"] = float(res.elapsed)


async def test_stress_full_scan_completes(source_db, db_params):
    """Full scan on the same data — correctness + elapsed recording."""
    options = _create_dict_options(
        db_params, source_db,
        scan_mode="full",
        sens_out=output_dict("stress_full.json"),
        partial_rows=50,
    )
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE
    _elapsed["full"] = float(res.elapsed)


async def test_stress_partial_is_faster_than_full(source_db):
    """Partial scan must be at least 2x faster than full. Generous ratio to
    avoid flakes on slow CI, but tight enough to catch a broken partial mode.
    """
    assert "partial" in _elapsed and "full" in _elapsed, (
        "previous tests must have run to populate _elapsed"
    )
    ratio = _elapsed["full"] / max(_elapsed["partial"], 0.001)
    assert ratio >= 2, (
        f"expected partial ≥2x faster than full, got ratio={ratio:.2f} "
        f"(full={_elapsed['full']:.2f}s, partial={_elapsed['partial']:.2f}s)"
    )


async def test_stress_dump_restore_roundtrip(source_db, target_db, db_params, pg_anon_runner):
    """End-to-end dump → restore at MEDIUM scale must succeed."""
    out = output_path("stress_dump")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE
