from __future__ import annotations

import pytest

from tests.infrastructure.assertions import check_rows_count

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode

LOWERED_SEQUENCES = {
    "sp_crm.contact": "sp_crm.contact_id_seq",
    "hr.employee": "hr.employee_id_seq",
}


async def _dump(pg_anon_runner, db_params, source_db, out_dir):
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out_dir}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE


async def _restore(pg_anon_runner, db_params, target_db, in_dir, option):
    return await pg_anon_runner.run(
        "restore",
        target_db,
        [f"--db-connections-per-process={db_params.db_connections_per_process}", f"--input-dir={in_dir}", option],
    )


async def _dump_with_low_sequences(db_manager, pg_anon_runner, db_params, source_db, out_dir, sequences):
    """Dump while the sequences are set to 1, so only --seq-init-by-max-value gives ids above max(id)."""
    for sequence in sequences.values():
        await db_manager.execute(source_db, f"SELECT setval('{sequence}', 1)")
    try:
        await _dump(pg_anon_runner, db_params, source_db, out_dir)
    finally:
        for table, sequence in sequences.items():
            await db_manager.execute(source_db, f"SELECT setval('{sequence}', (SELECT max(id) FROM {table}))")


async def _constraint_owners(db_manager, db_name, constraint):
    rows = await db_manager.fetch(
        db_name,
        f"""
        SELECT n.nspname || '.' || c.relname AS table_name
        FROM pg_constraint pc
        JOIN pg_class c ON c.oid = pc.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE pc.conname = '{constraint}'
        ORDER BY 1
        """,
    )
    return [row["table_name"] for row in rows]


async def test_drop_custom_check_constr_removes_check_declared_on_inherits_parent(
    source_db, target_db, db_manager, db_params, pg_anon_runner
):
    out = output_path("check_on_inherits_parent")
    await _dump(pg_anon_runner, db_params, source_db, out)
    assert len(await _constraint_owners(db_manager, source_db, "contact_name_check")) == 4
    assert await _constraint_owners(db_manager, source_db, "vendor_code_check") == ["sp_crm.vendor"]

    res = await _restore(pg_anon_runner, db_params, target_db, out, "--drop-custom-check-constr")

    assert res.result_code == ResultCode.DONE
    assert await _constraint_owners(db_manager, target_db, "contact_name_check") == []
    assert await _constraint_owners(db_manager, target_db, "vendor_code_check") == []
    # the counts of parents include the rows of their children: contact 5 + 3 * 5, vendor 3 + partner 5
    assert await check_rows_count(
        db_manager,
        target_db,
        [
            ["sp_crm", "contact", 20],
            ["sp_crm", "person", 5],
            ["sp_crm", "company", 5],
            ["sp_crm", "partner", 5],
            ["sp_crm", "vendor", 8],
        ],
    )


@pytest.mark.min_pg(100000)
async def test_drop_custom_check_constr_removes_check_declared_on_partitioned_table(
    source_db, target_db, db_manager, db_params, pg_anon_runner
):
    out = output_path("check_on_partitioned_table")
    await _dump(pg_anon_runner, db_params, source_db, out)
    assert len(await _constraint_owners(db_manager, source_db, "orders_region_check")) == 5

    res = await _restore(pg_anon_runner, db_params, target_db, out, "--drop-custom-check-constr")

    assert res.result_code == ResultCode.DONE
    assert await _constraint_owners(db_manager, target_db, "orders_region_check") == []
    assert await check_rows_count(db_manager, target_db, [["sp", "orders", 20]])


async def test_seq_init_by_max_value_covers_inherits_parent_and_plain_table(
    source_db, target_db, db_manager, db_params, pg_anon_runner
):
    out = output_path("seq_init_inherits")
    await _dump_with_low_sequences(db_manager, pg_anon_runner, db_params, source_db, out, LOWERED_SEQUENCES)

    res = await _restore(pg_anon_runner, db_params, target_db, out, "--seq-init-by-max-value")

    assert res.result_code == ResultCode.DONE
    max_contact_id = (await db_manager.fetch(source_db, "SELECT max(id) FROM sp_crm.contact"))[0][0]
    assert max_contact_id == 20
    new_person = await db_manager.fetch(
        target_db, "INSERT INTO sp_crm.person (name, info) VALUES ('New', 'n/a') RETURNING id"
    )
    assert new_person[0]["id"] > max_contact_id

    max_employee_id = (await db_manager.fetch(source_db, "SELECT max(id) FROM hr.employee"))[0][0]
    next_employee_id = (await db_manager.fetch(target_db, "SELECT nextval('hr.employee_id_seq')"))[0][0]
    assert next_employee_id > max_employee_id


@pytest.mark.min_pg(100000)
async def test_seq_init_by_max_value_covers_partitioned_table(
    source_db, target_db, db_manager, db_params, pg_anon_runner
):
    out = output_path("seq_init_partitioned")
    await _dump_with_low_sequences(
        db_manager, pg_anon_runner, db_params, source_db, out, {"sp.orders": "sp.orders_id_seq"}
    )

    res = await _restore(pg_anon_runner, db_params, target_db, out, "--seq-init-by-max-value")

    assert res.result_code == ResultCode.DONE
    new_order = await db_manager.fetch(
        target_db,
        "INSERT INTO sp.orders (created, region, email, note, tag) "
        "VALUES ('2024-05-01', 'ru', 'new@example.com', 'ok', '-') RETURNING id",
    )
    assert new_order[0]["id"] > 20
