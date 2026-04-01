from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode
from tests.infrastructure.assertions import check_rows_count
from tests.infrastructure.data import DEFAULT_ROWS

from .conftest import SOURCE_DB, TARGET_DB, input_dict, output_path


async def _init_db(db_manager, pg_anon_runner, test_data, db_name, rows=DEFAULT_ROWS):
    """Drop existing schema, run pg_anon init, create tables + data."""
    await db_manager.execute(db_name, "DROP SCHEMA IF EXISTS test_simple CASCADE")
    res = await pg_anon_runner.run("init", db_name)
    assert res.result_code == ResultCode.DONE
    await test_data.simple_companies(db_name, count=rows)
    await test_data.simple_contracts(db_name, count=rows)


async def _add_data(test_data, db_name):
    """Add extra tables (clients, orders) via builder."""
    await test_data.simple_clients(db_name)
    await test_data.simple_orders(db_name)


async def _make_dump(db_params, db_name, out_dir):
    options = build_run_options([
        "dump",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={db_name}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={input_dict('test_empty_dictionary.py')}",
        f"--output-dir={out_dir}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--debug",
    ])
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE


async def _make_restore(db_params, target_name, in_dir):
    options = build_run_options([
        "restore",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={target_name}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
        "--drop-custom-check-constr",
        "--clean-db",
        "--debug",
    ])
    res = await PgAnonApp(options).run()
    return res


async def test_02_dump_and_restore_with_clean_db(
    source_db, target_db, db_manager, pg_anon_runner, db_params, test_data,
):
    """Dump simple env, restore with --clean-db, verify row counts.
    Then reinit target with 5x data, restore again (clean replaces), verify counts reset.
    """
    out = output_path("test_02")

    await _init_db(db_manager, pg_anon_runner, test_data, source_db)
    await _make_dump(db_params, source_db, out)
    res = await _make_restore(db_params, target_db, out)
    assert res.result_code == ResultCode.DONE

    assert await check_rows_count(db_manager, target_db, [
        ["test_simple", "customer_company", DEFAULT_ROWS],
        ["test_simple", "contracts", DEFAULT_ROWS],
    ])

    rows_5x = DEFAULT_ROWS * 5
    await _init_db(db_manager, pg_anon_runner, test_data, target_db, rows=rows_5x)
    assert await check_rows_count(db_manager, target_db, [
        ["test_simple", "customer_company", rows_5x],
        ["test_simple", "contracts", rows_5x],
    ])

    res = await _make_restore(db_params, target_db, out)
    assert res.result_code == ResultCode.DONE
    assert await check_rows_count(db_manager, target_db, [
        ["test_simple", "customer_company", DEFAULT_ROWS],
        ["test_simple", "contracts", DEFAULT_ROWS],
    ])


async def test_03_dump_and_wrong_restore_with_clean_db(
    source_db, target_db, db_manager, pg_anon_runner, db_params, test_data,
):
    """Second restore must fail because target DB has extra tables not in dump."""
    out = output_path("test_03")

    await _init_db(db_manager, pg_anon_runner, test_data, source_db)
    await _make_dump(db_params, source_db, out)
    res = await _make_restore(db_params, target_db, out)
    assert res.result_code == ResultCode.DONE

    assert await check_rows_count(db_manager, target_db, [
        ["test_simple", "customer_company", DEFAULT_ROWS],
        ["test_simple", "contracts", DEFAULT_ROWS],
    ])

    rows_5x = DEFAULT_ROWS * 5
    await _init_db(db_manager, pg_anon_runner, test_data, target_db, rows=rows_5x)
    await _add_data(test_data, target_db)

    assert await check_rows_count(db_manager, target_db, [
        ["test_simple", "customer_company", rows_5x],
        ["test_simple", "contracts", rows_5x],
        ["test_simple", "clients", DEFAULT_ROWS],
        ["test_simple", "orders", DEFAULT_ROWS],
    ])

    res = await _make_restore(db_params, target_db, out)
    assert res.result_code == ResultCode.FAIL
