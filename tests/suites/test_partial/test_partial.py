from tests.infrastructure.assertions import check_list_tables

from .conftest import input_dict, output_path, TARGET_DB
from pg_anon.common.enums import ResultCode

TABLES_INCLUDE_ONLY = [
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'"],
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'2"],
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3"],
    ["public", "inn_info"],
    ["schm_customer", "customer_company"],
    ["schm_customer", "customer_contract"],
    ["schm_customer", "customer_manager"],
    ["schm_other_1", "some_tbl"],
    ["schm_other_2", "some_tbl"],
]

TABLES_EXCLUDE_ONLY = [
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'"],
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3"],
    ["public", "contracts"],
    ["public", "key_value"],
    ["public", "tbl_100"],
    ["public", "tbl_constants"],
    ["schm_customer", "customer_company"],
    ["schm_customer", "customer_contract"],
    ["schm_mask_exclude_1", "other_tbl"],
    ["schm_mask_exclude_1", "some_tbl"],
    ["schm_mask_ext_exclude_2", "card_numbers"],
    ["schm_mask_ext_exclude_2", "other_ext_tbl_2"],
    ["schm_mask_ext_exclude_2", "some_ext_tbl"],
    ["schm_mask_ext_include_2", "other_ext_tbl"],
    ["schm_mask_ext_include_2", "some_ext_tbl"],
    ["schm_mask_include_1", "other_tbl"],
    ["schm_mask_include_1", "some_tbl"],
    ["schm_mask_include_1", "tbl_123"],
    ["schm_mask_include_1", "tbl_123_456"],
    ["schm_other_2", "exclude_tbl"],
    ["schm_other_2", "some_tbl"],
    ["schm_other_2", "tbl_test_anon_functions"],
    ["schm_other_3", "data_types_test"],
    ["schm_other_4", "partitioned_table"],
    ["schm_other_4", "partitioned_table_2025_01"],
    ["schm_other_4", "partitioned_table_2025_02"],
    ["schm_other_4", "partitioned_table_2025_03"],
    ["schm_other_4", "partitioned_table_default"],
    ["schm_other_4", "goods"],
]

TABLES_COMBINED = [
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'"],
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3"],
    ["schm_customer", "customer_company"],
    ["schm_customer", "customer_contract"],
    ["schm_other_2", "some_tbl"],
]


async def _dump_and_restore(
    pg_anon_runner, db_params, source_db, out,
    dump_extra=None, restore_extra=None,
):
    """Helper: dump from source, restore to target with --drop-db."""
    dump_args = [
        f"--prepared-sens-dict-file={input_dict('test_empty_dictionary.py')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        f"--output-dir={out}",
    ]
    if dump_extra:
        dump_args.extend(dump_extra)
    res = await pg_anon_runner.run("dump", source_db, dump_args)
    assert res.result_code == ResultCode.DONE

    restore_args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
        "--drop-db",
    ]
    if restore_extra:
        restore_args.extend(restore_extra)
    res = await pg_anon_runner.run("restore", TARGET_DB, restore_args)
    assert res.result_code == ResultCode.DONE


async def test_02_partial_dump_include_restore_full(
    source_db, target_db, db_manager, pg_anon_runner, db_params,
):
    out = output_path("test_02")
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, out,
        dump_extra=[f"--partial-tables-dict-file={input_dict('test_partial_tables_dict.py')}"],
    )
    assert await check_list_tables(db_manager, TARGET_DB, TABLES_INCLUDE_ONLY)


async def test_03_partial_dump_exclude_restore_full(
    source_db, target_db, db_manager, pg_anon_runner, db_params,
):
    out = output_path("test_03")
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, out,
        dump_extra=[f"--partial-tables-exclude-dict-file={input_dict('test_partial_exclude_tables_dict.py')}"],
    )
    assert await check_list_tables(db_manager, TARGET_DB, TABLES_EXCLUDE_ONLY)


async def test_04_partial_dump_include_exclude_restore_full(
    source_db, target_db, db_manager, pg_anon_runner, db_params,
):
    out = output_path("test_04")
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, out,
        dump_extra=[
            f"--partial-tables-dict-file={input_dict('test_partial_tables_dict.py')}",
            f"--partial-tables-exclude-dict-file={input_dict('test_partial_exclude_tables_dict.py')}",
        ],
    )
    assert await check_list_tables(db_manager, TARGET_DB, TABLES_COMBINED)


async def test_05_partial_dump_include_restore_exclude(
    source_db, target_db, db_manager, pg_anon_runner, db_params,
):
    out = output_path("test_05")
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, out,
        dump_extra=[f"--partial-tables-dict-file={input_dict('test_partial_tables_dict.py')}"],
        restore_extra=[f"--partial-tables-exclude-dict-file={input_dict('test_partial_exclude_tables_dict.py')}"],
    )
    assert await check_list_tables(db_manager, TARGET_DB, TABLES_COMBINED)


async def test_06_partial_dump_exclude_restore_include(
    source_db, target_db, db_manager, pg_anon_runner, db_params,
):
    out = output_path("test_06")
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, out,
        dump_extra=[f"--partial-tables-exclude-dict-file={input_dict('test_partial_exclude_tables_dict.py')}"],
        restore_extra=[f"--partial-tables-dict-file={input_dict('test_partial_tables_dict.py')}"],
    )
    assert await check_list_tables(db_manager, TARGET_DB, TABLES_COMBINED)


async def test_07_partial_dump_full_restore_include_exclude(
    source_db, target_db, db_manager, pg_anon_runner, db_params,
):
    out = output_path("test_07")
    await _dump_and_restore(
        pg_anon_runner, db_params, source_db, out,
        dump_extra=[f"--partial-tables-exclude-dict-file={input_dict('test_partial_exclude_tables_dict.py')}"],
        restore_extra=[f"--partial-tables-dict-file={input_dict('test_partial_tables_dict.py')}"],
    )
    assert await check_list_tables(db_manager, TARGET_DB, TABLES_COMBINED)
