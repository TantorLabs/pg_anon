"""All modes on a 1C-like database with many tables in one schema (PG_ANON_WIDE_TABLES, 20000 by default)."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from .conftest import all_tables, input_dict, output_path, public_tables, table_name, TABLES, write_tables_dict
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode
from pg_anon.modes.view_fields import ViewFieldsMode

# Excluded from the default test run; run explicitly with `pytest -m stress`.
pytestmark = pytest.mark.stress

EMAIL_TABLE = table_name(10)


async def _restore(pg_anon_runner, db_params, target_db, dump_dir, extra=None):
    args = [
        f"--input-dir={dump_dir}",
        f"--db-connections={db_params.db_connections_per_process}",
        "--drop-custom-check-constr",
    ]
    res = await pg_anon_runner.run("restore", target_db, args + (extra or []))
    assert res.result_code == ResultCode.DONE
    return res


async def _dump(pg_anon_runner, db_params, source_db, sens_dict, name, extra=None) -> str:
    out = output_path(name)
    args = [
        f"--prepared-sens-dict-file={sens_dict}",
        f"--output-dir={out}",
        f"--db-connections={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ]
    res = await pg_anon_runner.run("dump", source_db, args + (extra or []))
    assert res.result_code == ResultCode.DONE
    return out


def _sql_args(db_params, mode: str, db_name: str, extra: list[str]) -> list[str]:
    return [
        mode,
        f"--db-host={db_params.test_db_host}",
        f"--db-name={db_name}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        *extra,
    ]


async def test_scan_finds_every_sensitive_table(sens_dict):
    dictionary = json.loads(Path(sens_dict).read_text(encoding="utf-8"))["dictionary"]
    assert {rule["table"] for rule in dictionary} == {table_name(i) for i in range(10, TABLES + 1, 10)}


async def test_view_fields_with_orm_dict(source_db, sens_dict, orm_dict, db_params, record_property):
    args = _sql_args(
        db_params,
        "view-fields",
        source_db,
        [f"--prepared-sens-dict-file={sens_dict}", f"--orm-dict-file={orm_dict}", "--schema-name=public", "--json"],
    )
    executor = ViewFieldsMode(PgAnonApp(build_run_options(args)).context)
    started = time.monotonic()
    await executor.run()
    record_property("elapsed", time.monotonic() - started)

    # 6 of 7 columns: view-fields does not show primary key columns.
    assert len(executor.fields) == TABLES * 6
    assert {f.relname for f in executor.fields} == {f"Справочник.Объект{t}" for t in all_tables()}


async def test_view_data(source_db, sens_dict, pg_anon_runner, record_property):
    res = await pg_anon_runner.run(
        "view-data",
        source_db,
        [f"--prepared-sens-dict-file={sens_dict}", "--schema-name=public", f"--table-name={EMAIL_TABLE}"],
    )
    assert res.result_code == ResultCode.DONE
    record_property("elapsed", res.elapsed)


async def test_full_dump_restore(full_dump, target_db, db_manager, pg_anon_runner, db_params, record_property):
    res = await _restore(pg_anon_runner, db_params, target_db, full_dump)
    record_property("elapsed", res.elapsed)

    assert await public_tables(db_manager, target_db) == set(all_tables())
    rows = await db_manager.fetch(target_db, f"SELECT _fld{10 * 10 + 2} AS v FROM public.{EMAIL_TABLE}")
    assert rows
    assert all("@" not in r["v"] for r in rows), "e-mails must be masked"


async def test_restore_includes_by_mask(full_dump, target_db, db_manager, pg_anon_runner, db_params):
    await _restore(
        pg_anon_runner,
        db_params,
        target_db,
        full_dump,
        [f"--partial-tables-dict-file={input_dict('include_documents.py')}"],
    )
    assert await public_tables(db_manager, target_db) == {t for t in all_tables() if t.startswith("_document")}


async def test_restore_excludes_by_name(full_dump, target_db, db_manager, pg_anon_runner, db_params):
    excluded = all_tables()[::3]
    exclude_dict = write_tables_dict("exclude_every_3rd.py", excluded)
    await _restore(
        pg_anon_runner,
        db_params,
        target_db,
        full_dump,
        [f"--partial-tables-exclude-dict-file={exclude_dict}"],
    )
    assert await public_tables(db_manager, target_db) == set(all_tables()) - set(excluded)


async def test_dump_includes_by_name(
    source_db, sens_dict, target_db, db_manager, pg_anon_runner, db_params, record_property
):
    included = all_tables()[::2]
    include_dict = write_tables_dict("include_every_2nd.py", included)
    started = time.monotonic()
    dump_dir = await _dump(
        pg_anon_runner,
        db_params,
        source_db,
        sens_dict,
        "include_dump",
        [f"--partial-tables-dict-file={include_dict}"],
    )
    record_property("elapsed", time.monotonic() - started)

    await _restore(pg_anon_runner, db_params, target_db, dump_dir)
    assert await public_tables(db_manager, target_db) == set(included)


async def test_dump_excludes_by_mask(source_db, sens_dict, target_db, db_manager, pg_anon_runner, db_params):
    dump_dir = await _dump(
        pg_anon_runner,
        db_params,
        source_db,
        sens_dict,
        "exclude_dump",
        [f"--partial-tables-exclude-dict-file={input_dict('exclude_accumrg.py')}"],
    )
    await _restore(pg_anon_runner, db_params, target_db, dump_dir)
    assert await public_tables(db_manager, target_db) == {t for t in all_tables() if not t.startswith("_accumrg")}
