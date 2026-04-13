import json

from .conftest import expected_result
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode
from pg_anon.modes.view_data import ViewDataMode


def _build_view_data_options(db_params, source_db, extra_args: list[str]) -> list[str]:
    """Build base view-data CLI args, appending any extra flags."""
    base = [
        "view-data",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        "--debug",
    ]
    base.extend(extra_args)
    return base


async def test_02_view_data_print(source_db, db_params):
    """View-data in print mode with prepared sens dict, schema=public, table=contracts."""
    options = build_run_options(
        _build_view_data_options(db_params, source_db, [
            f"--prepared-sens-dict-file={expected_result('test_prepared_sens_dict_result_expected.py')}",
            "--schema-name=public",
            "--table-name=contracts",
            "--limit=10",
            "--offset=0",
        ])
    )
    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE


async def test_03_view_data_json(source_db, db_params):
    """View-data with --json flag. All fields in json output must have equal number of rows."""
    options = build_run_options(
        _build_view_data_options(db_params, source_db, [
            f"--prepared-sens-dict-file={expected_result('test_prepared_sens_dict_result_expected.py')}",
            "--json",
            "--schema-name=public",
            "--table-name=contracts",
            "--limit=10",
            "--offset=0",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewDataMode(context)
    await executor.run()

    row_len = {len(row) for row in list(json.loads(executor.json).values())}
    assert len(row_len) == 1


async def test_04_view_data_null(source_db, db_params):
    """View-data with high offset (null rows). Both print and json modes should return DONE."""
    base_args = _build_view_data_options(db_params, source_db, [
        f"--prepared-sens-dict-file={expected_result('test_prepared_sens_dict_result_expected.py')}",
        "--schema-name=schm_mask_ext_exclude_2",
        "--table-name=card_numbers",
        "--limit=10",
        "--offset=30235",
    ])

    options_print = build_run_options(base_args)
    res_print = await PgAnonApp(options_print).run()
    assert res_print.result_code == ResultCode.DONE

    options_json = build_run_options(base_args + ["--json"])
    res_json = await PgAnonApp(options_json).run()
    assert res_json.result_code == ResultCode.DONE


async def test_05_view_data_without_matched_rule(source_db, db_params):
    """View-data with a dict that has no matching schema. Table should still have rows."""
    options = build_run_options(
        _build_view_data_options(db_params, source_db, [
            f"--prepared-sens-dict-file={expected_result('test_prepared_sens_dict_result_with_no_existing_schema.py')}",
            "--schema-name=schm_mask_ext_exclude_2",
            "--table-name=card_numbers",
            "--limit=10",
            "--offset=0",
        ])
    )
    context = PgAnonApp(options).context

    executor = ViewDataMode(context)
    await executor.run()

    assert len(executor.table.rows) > 0
