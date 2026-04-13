import json
from decimal import Decimal
from pathlib import Path

from tests.infrastructure.assertions import check_rows, get_list_tables_with_diff_data

from .conftest import expected_result, input_dict, output_path
from pg_anon.common.enums import ResultCode
from pg_anon.common.utils import to_json


async def test_02_mask_dump(source_db, db_params, pg_anon_runner):
    """Dump with mask_test.py dictionary."""
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('mask_test.py')}",
        f"--output-dir={output_path('test_02_mask_dump')}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE


async def test_03_mask_restore(source_db, target_db, db_manager, db_params, pg_anon_runner):
    """Restore masked dump, verify masked values and diff data."""
    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={output_path('test_02_mask_dump')}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE

    rows = [[1, round(Decimal(101010), 2)], [2, round(Decimal(101010), 2)]]
    assert await check_rows(db_manager, target_db, "public", "contracts", ["id", "amount"], rows)

    rows = [[1, round(Decimal(202020), 2)], [2, round(Decimal(202020), 2)]]
    assert await check_rows(db_manager, target_db, "public", "tbl_100", ["id", "amount"], rows)

    source_diff, target_diff = await get_list_tables_with_diff_data(
        db_manager, source_db, target_db,
    )

    expected_source_content = Path(
        expected_result("PGAnonMaskUnitTest_source_tables.result")
    ).read_text(encoding="utf-8")
    expected_target_content = Path(
        expected_result("PGAnonMaskUnitTest_target_tables.result")
    ).read_text(encoding="utf-8")

    actual_source_json = to_json(source_diff, formatted=True)
    actual_target_json = to_json(target_diff, formatted=True)

    assert json.loads(actual_source_json) == json.loads(expected_source_content)
    assert json.loads(actual_target_json) == json.loads(expected_target_content)
