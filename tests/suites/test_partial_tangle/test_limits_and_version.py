"""Limits of the exclusion list, pg_dump lock errors and the pg_anon version stored in the dump."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from .conftest import backup_files, input_dict, run_dump, run_restore
from pg_anon.common.enums import ResultCode

BLACKLIST = f"--partial-tables-exclude-dict-file={input_dict('exclude_gone.py')}"

NEWER_VERSION = "999.0.0"

MAX_LOCKS_STDERR = """\
pg_dump: error: query failed: ERROR:  out of shared memory
HINT:  You might need to increase max_locks_per_transaction.
pg_dump: detail: Query was: LOCK TABLE "tg_core"."customers" IN ACCESS SHARE MODE
"""

MAX_LOCKS_STDERR_RU = """\
pg_dump: error: query failed: ERROR:  нехватка разделяемой памяти
HINT:  Возможно, следует увеличить параметр max_locks_per_transaction.
pg_dump: detail: Query was: LOCK TABLE "tg_core"."customers" IN ACCESS SHARE MODE
"""


async def test_dump_stops_when_the_exclusion_list_does_not_fit_into_the_command_line(
    source_db, db_params, pg_anon_runner, monkeypatch
):
    real_sysconf = os.sysconf

    def small_arg_max(name):
        return 64 if name == "SC_ARG_MAX" else real_sysconf(name)

    monkeypatch.setattr(os, "sysconf", small_arg_max)

    dump = await run_dump(pg_anon_runner, db_params, source_db, "limit_arg_max", [BLACKLIST])

    assert dump.result.result_code == ResultCode.FAIL
    assert str(dump.result.exception.code) == "TOO_MANY_EXCLUDED_TABLES"
    assert not backup_files(dump.out_dir), "pg_dump must not start with a command line that cannot be executed"


@pytest.mark.parametrize("stderr", [MAX_LOCKS_STDERR, MAX_LOCKS_STDERR_RU], ids=["english", "russian"])
def test_pg_dump_out_of_locks_is_recognized(stderr):
    from pg_anon.common.utils import is_max_locks_error  # noqa: PLC0415

    assert is_max_locks_error(stderr)


async def test_dump_stops_with_a_clear_error_when_pg_dump_runs_out_of_locks(
    source_db, db_params, pg_anon_runner, monkeypatch
):
    real_popen = subprocess.Popen

    class OutOfLocks:
        returncode = 1

        def communicate(self) -> tuple[bytes, bytes]:
            return b"", MAX_LOCKS_STDERR.encode()

    def popen(command, *args: object, **kwargs: object):
        # only the schema dump fails; other tools still run for real
        if "--section" in command:
            return OutOfLocks()
        return real_popen(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", popen)

    dump = await run_dump(pg_anon_runner, db_params, source_db, "out_of_locks")

    assert dump.result.result_code == ResultCode.FAIL
    assert str(dump.result.exception.code) == "MAX_LOCKS_EXCEEDED"
    assert "max_locks_per_transaction" in str(dump.result.exception), "the error must name the setting to change"


@pytest.mark.parametrize(
    "stderr",
    [
        "",
        "pg_dump: error: connection to server failed",
        "pg_dump: error: query failed: ERROR:  out of memory\nDETAIL:  Failed on request of size 8192.",
        'pg_dump: error: query failed: ERROR:  permission denied for table "customers"',
    ],
)
def test_other_pg_dump_errors_are_not_taken_for_lock_errors(stderr):
    from pg_anon.common.utils import is_max_locks_error  # noqa: PLC0415

    assert not is_max_locks_error(stderr)


@pytest.fixture
async def full_dump(source_db, db_params, pg_anon_runner):
    dump = await run_dump(pg_anon_runner, db_params, source_db, "version_full")
    assert dump.result.result_code == ResultCode.DONE
    return dump.out_dir


DUMP_DESCRIPTION_FILES = ("structure.json", "metadata.json")


def set_dump_version_key(out_dir: str, file_name: str, key: str, version: str) -> None:
    path = Path(out_dir) / file_name
    metadata = json.loads(path.read_text())
    assert key in metadata, f"{file_name} must store {key}"
    metadata[key] = version
    path.write_text(json.dumps(metadata))


async def test_restore_of_a_dump_from_a_newer_version_is_silent_when_the_minimal_version_fits(
    full_dump, target_db, db_params, pg_anon_runner
):
    for file_name in DUMP_DESCRIPTION_FILES:
        set_dump_version_key(full_dump, file_name, "pg_anon_version", NEWER_VERSION)

    restore = await run_restore(pg_anon_runner, db_params, target_db, full_dump)

    assert restore.result.result_code == ResultCode.DONE, f"restore failed: {restore.result.error_message}"
    assert not [warning for warning in restore.warnings if NEWER_VERSION in warning], "no warning about versions"
