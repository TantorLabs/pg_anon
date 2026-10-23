from __future__ import annotations

import json
from pathlib import Path

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


def backup_files(out: str) -> set[str]:
    return {item.name for item in Path(out).glob("*.backup")}


def assert_data_is_limited(out: str) -> None:
    files = json.loads((Path(out) / "metadata.json").read_text(encoding="utf-8"))["files"]
    assert files
    assert all(info.get("partial_data") for info in files.values()), "a debug stage limits the rows of every table"


async def test_stage1_validate_dict(source_db, db_params, pg_anon_runner):
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('validate.py')}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--output-dir={output_path('stage1')}",
            "--clear-output-dir",
            "--dbg-stage-1-validate-dict",
        ],
    )
    assert res.result_code == ResultCode.DONE
    assert not list(Path(output_path("stage1")).iterdir()), "stage 1 only checks the dictionary and writes nothing"


async def test_stage2_validate_data_then_sync_data_restore(
    source_db,
    target_db,
    db_params,
    pg_anon_runner,
):
    struct_out = output_path("stage2_struct")
    res = await pg_anon_runner.run(
        "sync-struct-dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('validate.py')}",
            f"--output-dir={struct_out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE
    res = await pg_anon_runner.run(
        "sync-struct-restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={struct_out}",
        ],
    )
    assert res.result_code == ResultCode.DONE

    out = output_path("stage2")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('validate.py')}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--output-dir={out}",
            "--clear-output-dir",
            "--dbg-stage-2-validate-data",
        ],
    )
    assert res.result_code == ResultCode.DONE
    assert not backup_files(out), "stage 2 dumps data only"
    assert_data_is_limited(out)

    res = await pg_anon_runner.run(
        "sync-data-restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
        ],
    )
    assert res.result_code == ResultCode.DONE


async def test_stage3_validate_full_then_restore(
    source_db,
    target_db,
    db_params,
    pg_anon_runner,
):
    out = output_path("stage3")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('validate.py')}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--output-dir={out}",
            "--clear-output-dir",
            "--dbg-stage-3-validate-full",
        ],
    )
    assert res.result_code == ResultCode.DONE
    assert backup_files(out) == {"pre_data.backup"}, "stage 3 skips the post-data section"
    assert_data_is_limited(out)

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
        ],
    )
    assert res.result_code == ResultCode.DONE


async def test_sync_struct_with_stage3_flag(
    source_db,
    target_db,
    db_params,
    pg_anon_runner,
):
    out = output_path("sync_struct_stage3")
    res = await pg_anon_runner.run(
        "sync-struct-dump",
        source_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--prepared-sens-dict-file={input_dict('validate.py')}",
            f"--output-dir={out}",
            "--clear-output-dir",
            "--dbg-stage-3-validate-full",
        ],
    )
    assert res.result_code == ResultCode.DONE
    assert backup_files(out) == {"pre_data.backup"}, "stage 3 skips the post-data section"

    res = await pg_anon_runner.run(
        "sync-struct-restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
        ],
    )
    assert res.result_code == ResultCode.DONE
