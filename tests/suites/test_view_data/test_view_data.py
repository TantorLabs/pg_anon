"""Tests for `view-data` mode: preview rows of a single table with dict applied."""
from __future__ import annotations

import json

from .conftest import input_dict
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode
from pg_anon.modes.view_data import ViewDataMode


def _options(db_params, source_db, dict_file: str, extra: list[str]) -> list[str]:
    base = [
        "view-data",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={dict_file}",
        "--debug",
    ]
    base.extend(extra)
    return base


async def test_view_data_print_hr_employee(source_db, db_params):
    """Print mode on a table with masking rules in the dict."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_data.py"), [
        "--schema-name=hr",
        "--table-name=employee",
        "--limit=10",
        "--offset=0",
    ]))
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE


async def test_view_data_json_has_uniform_row_length(source_db, db_params):
    """JSON output: every column key must map to the same number of rows."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_data.py"), [
        "--json",
        "--schema-name=billing",
        "--table-name=payment_card",
        "--limit=10",
        "--offset=0",
    ]))
    executor = ViewDataMode(PgAnonApp(options).context)
    await executor.run()

    lengths = {len(col) for col in json.loads(executor.json).values()}
    assert len(lengths) == 1


async def test_view_data_offset_past_end_still_succeeds(source_db, db_params):
    """Offset past the last row should succeed with empty result — both print and json."""
    base = [
        "--schema-name=hr",
        "--table-name=employee",
        "--limit=10",
        "--offset=999999",
    ]
    for extra in ([], ["--json"]):
        options = build_run_options(
            _options(db_params, source_db, input_dict("view_data.py"), base + extra)
        )
        res = await PgAnonApp(options).run()
        assert res.result_code == ResultCode.DONE


async def test_view_data_dict_without_matching_rule_still_shows_rows(source_db, db_params):
    """If the dict has no rule for the viewed table, rows are still returned (raw)."""
    options = build_run_options(_options(db_params, source_db, input_dict("unrelated.py"), [
        "--schema-name=hr",
        "--table-name=employee",
        "--limit=10",
        "--offset=0",
    ]))
    executor = ViewDataMode(PgAnonApp(options).context)
    await executor.run()
    assert len(executor.table.rows) > 0
