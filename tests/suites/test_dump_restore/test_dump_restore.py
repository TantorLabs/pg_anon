from tests.infrastructure.assertions import check_list_tables, check_rows, check_rows_count
from tests.infrastructure.data import DEFAULT_ROWS

from .conftest import input_dict, output_path
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode

# fmt: off
FULL_TABLES_LIST = [
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'"],
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'2"],
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3"],
    ["public", "contracts"],
    ["public", "inn_info"],
    ["public", "key_value"],
    ["public", "tbl_100"],
    ["public", "tbl_constants"],
    ["schm_customer", "customer_company"],
    ["schm_customer", "customer_contract"],
    ["schm_customer", "customer_manager"],
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
    ["schm_other_1", "some_tbl"],
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

ALL_ZERO_ROW_COUNTS = [
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'", 0],
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'2", 0],
    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3", 0],
    ["public", "contracts", 0],
    ["public", "inn_info", 0],
    ["public", "key_value", 0],
    ["public", "tbl_100", 0],
    ["public", "tbl_constants", 0],
    ["schm_customer", "customer_company", 0],
    ["schm_customer", "customer_contract", 0],
    ["schm_customer", "customer_manager", 0],
    ["schm_mask_exclude_1", "other_tbl", 0],
    ["schm_mask_exclude_1", "some_tbl", 0],
    ["schm_mask_ext_exclude_2", "card_numbers", 0],
    ["schm_mask_ext_exclude_2", "other_ext_tbl_2", 0],
    ["schm_mask_ext_exclude_2", "some_ext_tbl", 0],
    ["schm_mask_ext_include_2", "other_ext_tbl", 0],
    ["schm_mask_ext_include_2", "some_ext_tbl", 0],
    ["schm_mask_include_1", "other_tbl", 0],
    ["schm_mask_include_1", "some_tbl", 0],
    ["schm_mask_include_1", "tbl_123", 0],
    ["schm_mask_include_1", "tbl_123_456", 0],
    ["schm_other_1", "some_tbl", 0],
    ["schm_other_2", "exclude_tbl", 0],
    ["schm_other_2", "some_tbl", 0],
    ["schm_other_2", "tbl_test_anon_functions", 0],
    ["schm_other_3", "data_types_test", 0],
    ["schm_other_4", "partitioned_table", 0],
    ["schm_other_4", "partitioned_table_2025_01", 0],
    ["schm_other_4", "partitioned_table_2025_02", 0],
    ["schm_other_4", "partitioned_table_2025_03", 0],
    ["schm_other_4", "partitioned_table_default", 0],
    ["schm_other_4", "goods", 0],
]
# fmt: on


async def test_02_dump(source_db, db_params, pg_anon_runner):
    """Dump source DB with test.py dictionary."""
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test.py')}",
        f"--output-dir={output_path('test_02_dump')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE


async def test_03_restore(source_db, target_db, db_params, pg_anon_runner):
    """Restore dump from test_02 into target DB."""
    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_02_dump')}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE


async def test_04_dump(source_db, db_params, pg_anon_runner):
    """Dump source DB with test_exclude.py dictionary."""
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test_exclude.py')}",
        f"--output-dir={output_path('test_04_dump')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE


async def test_05_restore(source_db, target_db_2, db_params, pg_anon_runner):
    """Restore dump from test_04 into target_db_2 and validate target tables."""
    res = await pg_anon_runner.run("restore", target_db_2, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_04_dump')}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE

    options = build_run_options([
        "restore",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={target_db_2}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--prepared-sens-dict-file={input_dict('test_exclude.py')}",
        f"--input-dir={output_path('test_04_dump')}",
        "--debug",
    ])
    res = await PgAnonApp(options).validate_target_tables()
    assert res.result_code == ResultCode.DONE


async def test_06_sync_struct(source_db, target_db_3, db_params, pg_anon_runner, db_manager):
    """Sync-struct-dump from source, sync-struct-restore to target_db_3. Verify empty tables."""
    res = await pg_anon_runner.run("sync-struct-dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test_sync_struct.py')}",
        f"--output-dir={output_path('test_06_sync_struct')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("sync-struct-restore", target_db_3, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_06_sync_struct')}",
    ])
    assert res.result_code == ResultCode.DONE

    assert await check_list_tables(db_manager, target_db_3, FULL_TABLES_LIST)
    assert await check_rows_count(db_manager, target_db_3, ALL_ZERO_ROW_COUNTS)


async def test_07_sync_data(source_db, target_db_3, db_params, pg_anon_runner, db_manager):
    """Sync-data-dump from source, sync-data-restore to target_db_3. Verify data."""
    res = await pg_anon_runner.run("sync-data-dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test_sync_data.py')}",
        f"--output-dir={output_path('test_07_sync_data')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("sync-data-restore", target_db_3, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_07_sync_data')}",
    ])
    assert res.result_code == ResultCode.DONE

    assert await check_list_tables(db_manager, target_db_3, FULL_TABLES_LIST)

    expected_row_count = DEFAULT_ROWS
    assert await check_rows_count(db_manager, target_db_3, [
        ["schm_other_2", "exclude_tbl", expected_row_count],
        ["schm_other_2", "some_tbl", expected_row_count],
        ["schm_mask_include_1", "tbl_123", expected_row_count],
    ])

    assert await check_rows(
        db_manager, target_db_3,
        "schm_mask_include_1", "tbl_123",
        None,
        [[3, "t***l_3"], [4, "t***l_4"]],
    )


async def test_08_sync_data(source_db, target_db, db_params, pg_anon_runner, db_manager):
    """Sync-data-dump with test_sync_data_2.py, truncate tables, sync-data-restore to target_db."""
    res = await pg_anon_runner.run("sync-data-dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test_sync_data_2.py')}",
        f"--output-dir={output_path('test_08_sync_data')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    await db_manager.execute(target_db, "TRUNCATE TABLE schm_other_1.some_tbl")
    await db_manager.execute(target_db, "TRUNCATE TABLE schm_other_2.some_tbl")

    res = await pg_anon_runner.run("sync-data-restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_08_sync_data')}",
    ])
    assert res.result_code == ResultCode.DONE

    expected_row_count = DEFAULT_ROWS
    assert await check_rows_count(db_manager, target_db, [
        ["schm_other_1", "some_tbl", expected_row_count],
        ["schm_other_2", "some_tbl", expected_row_count],
    ])


async def test_09_repeat_restore_in_existing_db(target_db, db_params, pg_anon_runner):
    """Restore into already populated target_db without --drop-db. Expects FAIL."""
    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_02_dump')}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.FAIL


async def test_10_repeat_restore_with_drop_db(target_db, db_params, pg_anon_runner):
    """Restore into already populated target_db with --drop-db. Expects DONE."""
    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_02_dump')}",
        "--drop-custom-check-constr",
        "--drop-db",
    ])
    assert res.result_code == ResultCode.DONE


async def test_11_dump_with_sql_conditions(source_db, target_db, db_params, pg_anon_runner, db_manager):
    """Dump with SQL conditions, restore with --drop-db. Check filtered row counts."""
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('test_sens_with_sql_conditions.py')}",
        f"--output-dir={output_path('test_11_dump_with_sql_conditions')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_11_dump_with_sql_conditions')}",
        "--drop-custom-check-constr",
        "--drop-db",
    ])
    assert res.result_code == ResultCode.DONE

    assert await check_list_tables(db_manager, target_db, FULL_TABLES_LIST)

    # fmt: off
    sql_conditions_row_counts = [
        ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'", 0],
        ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'2", 0],
        ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3", 0],
        ["public", "contracts", 0],
        ["public", "inn_info", 0],
        ["public", "key_value", 0],
        ["public", "tbl_100", 0],
        ["public", "tbl_constants", 0],
        ["schm_customer", "customer_company", 0],
        ["schm_customer", "customer_contract", 0],
        ["schm_customer", "customer_manager", 0],
        ["schm_mask_exclude_1", "other_tbl", 0],
        ["schm_mask_exclude_1", "some_tbl", 0],
        ["schm_mask_ext_exclude_2", "card_numbers", 0],
        ["schm_mask_ext_exclude_2", "other_ext_tbl_2", 0],
        ["schm_mask_ext_exclude_2", "some_ext_tbl", 0],
        ["schm_mask_ext_include_2", "other_ext_tbl", 0],
        ["schm_mask_ext_include_2", "some_ext_tbl", 0],
        ["schm_mask_include_1", "other_tbl", 0],
        ["schm_mask_include_1", "some_tbl", 0],
        ["schm_mask_include_1", "tbl_123", 0],
        ["schm_mask_include_1", "tbl_123_456", 0],
        ["schm_other_1", "some_tbl", 0],
        ["schm_other_2", "exclude_tbl", 0],
        ["schm_other_2", "some_tbl", 0],
        ["schm_other_2", "tbl_test_anon_functions", 0],
        ["schm_other_3", "data_types_test", 0],
        ["schm_other_4", "partitioned_table", 0],
        ["schm_other_4", "partitioned_table_2025_01", 0],
        ["schm_other_4", "partitioned_table_2025_02", 0],
        ["schm_other_4", "partitioned_table_2025_03", 0],
        ["schm_other_4", "partitioned_table_default", 0],
        ["schm_other_4", "goods", 5],
    ]
    # fmt: on
    assert await check_rows_count(db_manager, target_db, sql_conditions_row_counts)
