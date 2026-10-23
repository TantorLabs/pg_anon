from __future__ import annotations

import json
import logging
import re
import subprocess
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import yaml

from tests.infrastructure.assertions import catalog_snapshot

from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pg_anon.common.dto import PgAnonResult

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_tangle_source"
SCHEMA_SOURCE_DB = "pg_anon_tangle_schema_source"
REGULAR_SOURCE_DB = "pg_anon_tangle_regular_source"

REGULAR_ROLE = "tg_regular_owner"
REGULAR_ROLE_PASSWORD = "tg_regular_pwd"  # noqa: S105

# Objects pg_dump cannot drop from the dump when their table is excluded; pg_anon cuts them at restore.
KNOWN_LEAK_NAMES = {"gone_fn_setof", "gone_t_row", "gone_fk_out"}
KNOWN_LEAK_NAMES_PG14 = {"gone_fn_atomic"}
# pg_dump 10 and 11 also keep extended statistics of an excluded table; 9.6 has no such statistics
KNOWN_LEAK_NAMES_PG10_PG11 = {"gone_stat"}


# one case per range of server versions; on any server exactly one of them runs
KNOWN_LEAK_CASES = [
    pytest.param(KNOWN_LEAK_NAMES, marks=pytest.mark.max_pg(100000), id="before_pg10"),
    pytest.param(
        KNOWN_LEAK_NAMES | KNOWN_LEAK_NAMES_PG10_PG11,
        marks=[pytest.mark.min_pg(100000), pytest.mark.max_pg(120000)],
        id="pg10_pg11",
    ),
    pytest.param(KNOWN_LEAK_NAMES, marks=[pytest.mark.min_pg(120000), pytest.mark.max_pg(140000)], id="pg12_pg13"),
    pytest.param(KNOWN_LEAK_NAMES | KNOWN_LEAK_NAMES_PG14, marks=pytest.mark.min_pg(140000), id="pg14_and_newer"),
]


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def expected_path(name: str) -> Path:
    return SUITE / "expected" / name


def output_path(name: str) -> str:
    out = SUITE / "output" / re.sub(r"\W", "_", name)
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


class LogCapture(logging.Handler):
    """Collect the messages of the pg_anon logger (any level) while the block runs."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.messages: list[str] = []
        self.warnings: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())
        if record.levelno == logging.WARNING:
            self.warnings.append(record.getMessage())

    def __enter__(self) -> LogCapture:
        logging.getLogger("pg_anon.logger").addHandler(self)
        return self

    def __exit__(self, *_: object) -> None:
        logging.getLogger("pg_anon.logger").removeHandler(self)


@dataclass
class RunOutcome:
    result: PgAnonResult
    out_dir: str
    messages: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


async def run_dump(pg_anon_runner, db_params, source_db, name, extra=None, mode="dump") -> RunOutcome:
    out = output_path(name)
    args = [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        *(extra or []),
    ]
    with LogCapture() as log:
        res = await pg_anon_runner.run(mode, source_db, args)
    return RunOutcome(res, out, log.messages, log.warnings)


async def run_restore(pg_anon_runner, db_params, target_db, in_dir, extra=None, **runner_kwargs: str) -> RunOutcome:
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
        *(extra or []),
    ]
    with LogCapture() as log:
        res = await pg_anon_runner.run("restore", target_db, args, **runner_kwargs)
    return RunOutcome(res, in_dir, log.messages, log.warnings)


def pg_restore_bin(db_params) -> str:
    config = yaml.safe_load(Path(db_params.test_config).read_text())
    return config["pg-utils-versions"]["default"]["pg_restore"]


def toc_lines(db_params, backup: Path) -> list[str]:
    out = subprocess.run([pg_restore_bin(db_params), "-l", str(backup)], capture_output=True, text=True, check=True)  # noqa: S603
    return [line for line in out.stdout.splitlines() if not line.startswith(";")]


def leaked_lines(db_params, out_dir: str, needle: str = "gone_") -> list[str]:
    """Lines with `needle` in both TOCs, metadata.json, structure.json and the table list of a dump."""
    found = []
    for backup in ("pre_data.backup", "post_data.backup"):
        found += [f"{backup}: {line}" for line in toc_lines(db_params, Path(out_dir) / backup) if needle in line]
    for name in ("metadata.json", "structure.json", "dumped_tables.py"):
        path = Path(out_dir) / name
        if path.exists():
            found += [f"{name}: {line.strip()}" for line in path.read_text().splitlines() if needle in line]
    return found


def unexpected_leaks(lines: list[str], allowed: set[str]) -> list[str]:
    """Leaked lines minus the allowed names of non-table objects and `PUBLICATION TABLE` entries."""
    result = []
    for line in lines:
        if line.startswith(("metadata.json", "structure.json", "dumped_tables.py")):
            result.append(line)
            continue
        names = set(re.findall(r"gone_\w+", line))
        if " PUBLICATION TABLE " in line or (names and names <= allowed):
            continue
        result.append(line)
    return result


def assert_no_pg_util_commands(messages: list[str]) -> None:
    commands = [m for m in messages if m.lstrip().startswith("['") and ("pg_dump" in m or "pg_restore" in m)]
    assert not commands, "assembled pg_dump/pg_restore commands must not be logged"


def assert_dump_includes_log(messages: list[str]) -> None:
    assert any(re.fullmatch(r"Dump includes \d+ of \d+ tables", m) for m in messages), (
        "log must say 'Dump includes N of M tables'"
    )


async def build_source(db_manager, pg_anon_runner, fixtures, name, **builder_flags: bool) -> None:
    await db_manager.create_db(name)
    res = await pg_anon_runner.run("init", name)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_partial_tangle(name, **builder_flags)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    await build_source(db_manager, pg_anon_runner, fixtures, SOURCE_DB)
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture(scope="module")
async def schema_source_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    await build_source(db_manager, pg_anon_runner, fixtures, SCHEMA_SOURCE_DB, gone_schema=True)
    yield SCHEMA_SOURCE_DB
    await db_manager.drop_db(SCHEMA_SOURCE_DB)


@pytest.fixture(scope="module")
async def regular_source_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    await build_source(db_manager, pg_anon_runner, fixtures, REGULAR_SOURCE_DB, superuser_objects=False)
    yield REGULAR_SOURCE_DB
    await db_manager.drop_db(REGULAR_SOURCE_DB)


@pytest.fixture(scope="module")
async def source_snapshot(source_db, db_manager):
    return await catalog_snapshot(db_manager, source_db)


@pytest.fixture(scope="module")
async def schema_source_snapshot(schema_source_db, db_manager):
    return await catalog_snapshot(db_manager, schema_source_db)


@pytest.fixture(scope="module")
async def regular_source_snapshot(regular_source_db, db_manager):
    return await catalog_snapshot(db_manager, regular_source_db)


@pytest.fixture
async def target_db(db_manager, request) -> AsyncIterator[str]:
    name = f"pg_anon_tangle_tgt_{zlib.crc32(request.node.name.encode())}"
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


def metadata_of(out_dir: str) -> dict:
    return json.loads((Path(out_dir) / "metadata.json").read_text())


@pytest.fixture
async def regular_role_target(db_manager) -> AsyncIterator[str]:
    """Empty database owned by an ordinary role (no superuser, no CREATEDB)."""
    name = "pg_anon_tangle_regular_target"
    await db_manager.execute(
        "postgres",
        f"""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{REGULAR_ROLE}') THEN
                CREATE ROLE {REGULAR_ROLE} LOGIN PASSWORD '{REGULAR_ROLE_PASSWORD}' NOSUPERUSER NOCREATEDB;
            END IF;
        END $$
        """,
    )
    await db_manager.create_db(name)
    await db_manager.execute("postgres", f"ALTER DATABASE {name} OWNER TO {REGULAR_ROLE}")
    # pg_anon loads data with session_replication_role = replica; PG 15+ can grant it without superuser
    await db_manager.execute("postgres", f"GRANT SET ON PARAMETER session_replication_role TO {REGULAR_ROLE}")
    yield name
    await db_manager.drop_db(name)
    try:
        await db_manager.execute("postgres", f"REVOKE SET ON PARAMETER session_replication_role FROM {REGULAR_ROLE}")
        await db_manager.execute("postgres", f"DROP ROLE IF EXISTS {REGULAR_ROLE}")
    except Exception:  # noqa: BLE001
        pass  # the database is kept (KEEP_TEST_DBS) and still needs the role


def backup_files(out_dir: str) -> list[str]:
    return sorted(p.name for p in Path(out_dir).glob("*.backup"))
