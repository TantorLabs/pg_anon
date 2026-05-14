"""Tests for `view-fields` mode (scan-like preview without writing).

Each test invokes ViewFieldsMode directly against the source DB; no dump/restore
happens. We check the same invariants as the old test_view_fields suite but
against the domain-oriented fixture so expectations stay legible.
"""
from __future__ import annotations

import json
import re

import pytest

from .conftest import input_dict
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.db_utils import get_scan_fields_list
from pg_anon.common.errors import PgAnonError
from pg_anon.common.utils import get_dict_rule_for_table
from pg_anon.modes.view_fields import ViewFieldsMode


def _options(db_params, source_db, dict_file: str, extra: list[str] | None = None) -> list[str]:
    base = [
        "view-fields",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={dict_file}",
        "--debug",
    ]
    if extra:
        base.extend(extra)
    return base


def _count_sens_vs_plain(executor: ViewFieldsMode) -> tuple[int, int]:
    sens = plain = 0
    for field in executor.fields or []:
        if field.rule != "---":
            rule = get_dict_rule_for_table(
                dictionary_rules=executor.context.prepared_dictionary_obj["dictionary"],
                schema=field.nspname,
                table=field.relname,
            )
            if rule and (field.column_name in rule.get("fields", {}) or rule.get("raw_sql")):
                sens += 1
        else:
            plain += 1
    return sens, plain


async def test_view_fields_returns_every_scannable_field_minus_excluded(source_db, db_params):
    """Full view: count rows must equal scan-fields-count minus `dictionary_exclude` entries."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py")))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    scan_fields = await get_scan_fields_list(executor.context.connection_params)
    scan_total = len(scan_fields)
    excluded = sum(
        1 for f in scan_fields
        if f["nspname"] == "quirks" and f["relname"] == "with_nulls"
    )
    assert excluded > 0, "fixture invariant: quirks.with_nulls must have scannable fields"

    assert len(executor.table.rows) == scan_total - excluded
    sens, plain = _count_sens_vs_plain(executor)
    assert sens + plain == scan_total - excluded


async def test_view_fields_filter_by_schema_name(source_db, db_params):
    """--schema-name=hr filters to hr only."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py"), [
        "--schema-name=hr",
    ]))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    assert executor.fields
    assert all(f.nspname == "hr" for f in executor.fields)


async def test_view_fields_filter_by_schema_mask(source_db, db_params):
    """Regex schema mask keeps only matching schemas."""
    mask = r"^bill"
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py"), [
        f"--schema-mask={mask}",
    ]))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    assert executor.fields
    assert all(re.search(mask, f.nspname) for f in executor.fields)


async def test_view_fields_filter_by_table_name(source_db, db_params):
    """--table-name=employee limits to hr.employee rows."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py"), [
        "--table-name=employee",
    ]))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    assert executor.fields
    assert all(f.relname == "employee" for f in executor.fields)


async def test_view_fields_filter_by_table_mask(source_db, db_params):
    r"""--table-mask=^payment keeps only payment_* tables."""
    mask = r"^payment"
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py"), [
        f"--table-mask={mask}",
    ]))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    assert executor.fields
    assert all(re.search(mask, f.relname) for f in executor.fields)


async def test_view_fields_respects_fields_count_limit(source_db, db_params):
    """--fields-count=5 must truncate output to exactly 5."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py"), [
        "--fields-count=5",
    ]))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    assert len(executor.fields) == 5
    assert len(executor.table.rows) == 5
    assert executor.fields_cut_by_limits is True


async def test_view_fields_only_sensitive_equals_sensitive_subset_of_full(source_db, db_params):
    """--view-only-sensitive-fields must produce exactly the sensitive subset of the full run."""
    dict_file = input_dict("view_fields.py")

    only_sens = ViewFieldsMode(PgAnonApp(build_run_options(
        _options(db_params, source_db, dict_file, ["--view-only-sensitive-fields"])
    )).context)
    await only_sens.run()

    full = ViewFieldsMode(PgAnonApp(build_run_options(
        _options(db_params, source_db, dict_file)
    )).context)
    await full.run()

    sensitive_of_full = {str(f) for f in full.fields if f.rule != "---"}
    only_sens_set = {str(f) for f in only_sens.fields}
    assert sensitive_of_full == only_sens_set
    assert len(only_sens.fields) < len(full.fields)


async def test_view_fields_json_output_matches_field_count(source_db, db_params):
    """--json → .table is None, .json decodes to a list with one entry per field."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py"), [
        "--json",
    ]))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    assert executor.table is None
    assert executor.json is not None
    assert len(json.loads(executor.json)) == len(executor.fields)


async def test_view_fields_raises_on_zero_fields_count(source_db, db_params):
    """--fields-count=0 is invalid input → PgAnonError."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py"), [
        "--fields-count=0",
    ]))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    with pytest.raises(PgAnonError):
        await executor.run()
    assert executor.fields is None
    assert executor.table is None


async def test_view_fields_non_existent_schema_returns_zero_fields(source_db, db_params):
    """Filter to a missing schema → returns 0 fields, no exception."""
    options = build_run_options(_options(db_params, source_db, input_dict("view_fields.py"), [
        "--schema-name=does_not_exist",
    ]))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    assert executor.fields == []
    assert executor.table is not None
    assert executor.fields_cut_by_limits is False


async def test_view_fields_empty_dictionary_is_allowed(source_db, db_params):
    """Empty dictionary must not raise — every field is simply 'not in dict'."""
    options = build_run_options(_options(db_params, source_db, input_dict("empty.py")))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()
    assert executor.fields is not None
