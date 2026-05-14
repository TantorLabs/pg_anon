"""Tests for dictionary masking rules.

Two things are verified:

1. Masked columns in the TARGET match the constant/SQL expression from the dict.
2. Columns NOT referenced by the dict are bit-for-bit identical between SOURCE
   and TARGET (we compare row checksums so we don't care about physical order).
"""
from __future__ import annotations

from tests.infrastructure.assertions import check_rows, checksum_tables

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


async def _dump_and_restore(pg_anon_runner, db_params, source_db, target_db, *, name):
    out = output_path(name)
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('mask.py')}",
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


async def test_mask_replaces_constants_in_hr_and_billing(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """String/integer constants from the dict must appear in target rows."""
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, target_db, name="replace_constants",
    )

    employees = await db_manager.fetch(
        target_db,
        "SELECT DISTINCT email::text, phone, salary FROM hr.employee ORDER BY 1 LIMIT 3",
    )
    assert all(r["email"] == "masked@example.com" for r in employees)
    assert all(r["phone"] == "+70000000000" for r in employees)
    assert all(r["salary"] == 0 for r in employees)

    cards = await db_manager.fetch(
        target_db,
        "SELECT cardholder_name, pan_last4 FROM billing.payment_card LIMIT 3",
    )
    assert all(r["cardholder_name"] == "ANON" for r in cards)
    assert all(r["pan_last4"] == "0000" for r in cards)

    customers = await db_manager.fetch(
        target_db,
        "SELECT name, tax_id FROM billing.customer LIMIT 3",
    )
    assert all(r["name"] == "CUSTOMER" for r in customers)
    assert all(r["tax_id"] == "0000000000" for r in customers)


async def test_mask_raw_sql_overrides_entire_row(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """A dict rule with raw_sql must produce rows straight from the SELECT, not the table."""
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, target_db, name="raw_sql",
    )

    rows = await db_manager.fetch(target_db, "SELECT body FROM ecommerce.review LIMIT 20")
    assert rows, "review table should be non-empty after restore"
    assert all(r["body"] == "REDACTED" for r in rows)


async def test_mask_does_not_touch_unrelated_tables(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Tables not covered by dict rules must be identical between source and target."""
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, target_db, name="untouched",
    )

    untouched = [
        ("hr", "department"),
        ("hr", "salary_history"),
        ("billing", "invoice"),
        ("ecommerce", "product"),
        ("ecommerce", "category"),
    ]
    src = await checksum_tables(db_manager, source_db, untouched)
    tgt = await checksum_tables(db_manager, target_db, untouched)
    for key in untouched:
        assert src[key] == tgt[key], f"{key}: source {src[key]} != target {tgt[key]}"


async def test_mask_leaves_non_masked_columns_of_masked_table_intact(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Within a masked table, columns not in `fields:` must retain original values.

    hr.employee rules touch email/phone/salary; first_name, last_name, ssn, etc.
    must survive unchanged — we pick a deterministic row (id=1) and compare.
    """
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, target_db, name="partial_columns",
    )

    src_row = await db_manager.fetch(
        source_db,
        "SELECT first_name, last_name, ssn, birth_date, hire_date FROM hr.employee WHERE id = 1",
    )
    assert await check_rows(
        db_manager, target_db, "hr", "employee",
        ["first_name", "last_name", "ssn", "birth_date", "hire_date"],
        [list(dict(src_row[0]).values())],
    )


async def test_mask_with_partial_tables_whitelist(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("mask_with_whitelist")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('mask_minimal.py')}",
        f"--partial-tables-dict-file={input_dict('whitelist_hr_only.py')}",
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

    hr_emp = await db_manager.fetch(
        target_db, "SELECT salary FROM hr.employee LIMIT 5",
    )
    assert hr_emp, "hr.employee must be restored on target"
    assert all(r["salary"] == 0 for r in hr_emp), (
        "hr.employee.salary must be masked to 0 on target"
    )

    billing_cust = await db_manager.fetch(target_db, """
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'billing' AND c.relname = 'customer'
    """)
    assert not billing_cust, (
        "billing.customer is outside the whitelist — must not appear on target"
    )


async def test_mask_with_partial_tables_exclude(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("mask_with_exclude")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('mask_minimal.py')}",
        f"--partial-tables-exclude-dict-file={input_dict('exclude_billing.py')}",
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

    hr_emp = await db_manager.fetch(
        target_db, "SELECT salary FROM hr.employee LIMIT 5",
    )
    assert hr_emp, "hr.employee must be restored on target"
    assert all(r["salary"] == 0 for r in hr_emp), (
        "hr.employee.salary must be masked to 0 on target"
    )

    billing_cust = await db_manager.fetch(target_db, """
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'billing' AND c.relname = 'customer' AND c.relkind = 'r'
    """)
    assert not billing_cust, (
        "billing.customer is in the exclude list — must not appear on target"
    )
