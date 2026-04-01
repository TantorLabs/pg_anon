import json
import re

import pytest

from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.db_utils import get_scan_fields_count
from pg_anon.common.errors import PgAnonError
from pg_anon.common.utils import get_dict_rule_for_table
from pg_anon.modes.view_fields import ViewFieldsMode

from .conftest import input_dict


def _build_view_fields_options(db_params, source_db, dict_file: str, extra_args: list[str] | None = None) -> list[str]:
    """Build base view-fields CLI args, appending any extra flags."""
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
    if extra_args:
        base.extend(extra_args)
    return base


def _count_fields_by_type(executor: ViewFieldsMode) -> dict[str, int]:
    """Count fields that are in the dictionary vs not in the dictionary."""
    counters = {"in_dict": 0, "not_in_dict": 0}
    for field in executor.fields:
        if field.rule != "---":
            dict_rule = get_dict_rule_for_table(
                dictionary_rules=executor.context.prepared_dictionary_obj["dictionary"],
                schema=field.nspname,
                table=field.relname,
            )
            if dict_rule and (field.column_name in dict_rule.get("fields", {}) or dict_rule.get("raw_sql")):
                counters["in_dict"] += 1
        else:
            counters["not_in_dict"] += 1
    return counters


async def test_02_view_fields_full(source_db, db_params):
    """View all fields. Row count should equal scan fields minus excluded fields."""
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"))
    )
    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    await executor.run()

    all_rows_count = await get_scan_fields_count(context.connection_params)
    excluded_fields_count = 1
    assert len(executor.table.rows) == all_rows_count - excluded_fields_count

    fields_counters = _count_fields_by_type(executor)
    assert all_rows_count - excluded_fields_count == fields_counters["in_dict"] + fields_counters["not_in_dict"]


async def test_03_view_fields_full_by_schema(source_db, db_params):
    """View fields filtered by --schema-name=public. All fields must belong to public schema."""
    schema_name = "public"
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"), [
            f"--schema-name={schema_name}",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    await executor.run()

    for field in executor.fields:
        assert field.nspname == schema_name


async def test_04_view_fields_full_by_schema_mask(source_db, db_params):
    """View fields filtered by --schema-mask=^pub.*. All fields must match the regex."""
    schema_mask = "^pub.*"
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"), [
            f"--schema-mask={schema_mask}",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    await executor.run()

    for field in executor.fields:
        assert re.search(schema_mask, field.nspname) is not None


async def test_05_view_fields_full_by_table(source_db, db_params):
    """View fields filtered by --table-name=inn_info. All fields must belong to inn_info."""
    table_name = "inn_info"
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"), [
            f"--table-name={table_name}",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    await executor.run()

    for field in executor.fields:
        assert field.relname == table_name


async def test_06_view_fields_full_by_table_mask(source_db, db_params):
    r"""View fields filtered by --table-mask=.*\d$. All fields must match the regex."""
    table_mask = r".*\d$"
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"), [
            f"--table-mask={table_mask}",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    await executor.run()

    for field in executor.fields:
        assert re.search(table_mask, field.relname) is not None


async def test_07_view_fields_full_with_cut_output_and_notice(source_db, db_params):
    """View fields with --fields-count=5. Output should be cut to exactly 5 fields."""
    fields_scan_length = 5
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"), [
            f"--fields-count={fields_scan_length}",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    await executor.run()

    all_rows_count = await get_scan_fields_count(context.connection_params)
    assert len(executor.table.rows) != all_rows_count
    assert len(executor.table.rows) == fields_scan_length
    assert len(executor.fields) == fields_scan_length
    assert executor.fields_cut_by_limits is True


async def test_08_view_fields_with_only_sensitive_fields(source_db, db_params):
    """Compare --view-only-sensitive-fields run with full run. Sensitive sets must match."""
    dict_file = input_dict("test.py")

    options_only_sensitive = build_run_options(
        _build_view_fields_options(db_params, source_db, dict_file, [
            "--view-only-sensitive-fields",
        ])
    )
    context_only_sensitive = PgAnonApp(options_only_sensitive).context
    executor_only_sensitive = ViewFieldsMode(context_only_sensitive)
    await executor_only_sensitive.run()

    options_full = build_run_options(
        _build_view_fields_options(db_params, source_db, dict_file)
    )
    context_full = PgAnonApp(options_full).context
    executor_full = ViewFieldsMode(context_full)
    await executor_full.run()

    all_rows_count = await get_scan_fields_count(context_full.connection_params)
    excluded_fields_count = 1

    assert len(executor_full.fields) != len(executor_only_sensitive.fields)
    assert len(executor_full.table.rows) == all_rows_count - excluded_fields_count
    assert len(executor_only_sensitive.table.rows) != all_rows_count - excluded_fields_count

    sensitive_fields_in_full_executor = {
        str(field) for field in executor_full.fields if field.rule != "---"
    }
    executor_only_sensitive_fields_set = {str(field) for field in executor_only_sensitive.fields}

    assert sensitive_fields_in_full_executor == executor_only_sensitive_fields_set


async def test_09_view_filter_json_output(source_db, db_params):
    """View fields with --json. Table should be None, json output length should match field count."""
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"), [
            "--json",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    await executor.run()

    assert executor.table is None
    assert executor.json is not None

    all_rows_count = await get_scan_fields_count(context.connection_params)
    excluded_fields_count = 1
    json_data_len = len(json.loads(executor.json))
    assert json_data_len == all_rows_count - excluded_fields_count
    assert json_data_len == len(executor.fields)


async def test_10_view_fields_exception_on_zero_fields(source_db, db_params):
    """View fields with --fields-count=0 should raise PgAnonError."""
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"), [
            "--fields-count=0",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    with pytest.raises(PgAnonError):
        await executor.run()

    assert executor.fields is None
    assert executor.table is None


async def test_10_view_fields_exception_on_filter_to_zero_fields(source_db, db_params):
    """View fields with non-existent schema. Should NOT raise, but return 0 fields."""
    schema_name = "not_exists_schema_name"
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test.py"), [
            f"--schema-name={schema_name}",
        ])
    )
    executor_failed = False

    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    try:
        await executor.run()
    except ValueError:
        executor_failed = True

    assert executor_failed is False
    assert len(executor.fields) == 0
    assert executor.table is not None
    assert executor.fields_cut_by_limits is False


async def test_12_view_fields_exception_on_empty_prepared_dictionary(source_db, db_params):
    """View fields with empty dictionary. Should NOT raise ValueError."""
    options = build_run_options(
        _build_view_fields_options(db_params, source_db, input_dict("test_empty_dictionary.py"))
    )
    executor_failed = False

    context = PgAnonApp(options).context

    executor = ViewFieldsMode(context)
    try:
        await executor.run()
    except ValueError:
        executor_failed = True

    assert executor_failed is False
