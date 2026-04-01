import filecmp
from decimal import Decimal
from pathlib import Path

import pytest

from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.constants import SAVED_DICTS_INFO_FILE_NAME
from pg_anon.common.enums import ResultCode
from tests.infrastructure.assertions import (
    assert_no_sens_dicts,
    assert_sens_dicts,
    check_list_tables_and_fields,
    check_rows,
    check_rows_count,
)
from tests.infrastructure.data import DEFAULT_ROWS

from .conftest import (
    SOURCE_DB,
    TARGET_DB,
    expected_result,
    input_dict,
    output_dict,
    output_path,
)

pytestmark = pytest.mark.usefixtures("source_db")

SENS_DICT = output_dict("test_prepared_sens_dict_result.py")
NO_SENS_DICT = output_dict("test_prepared_no_sens_dict_result.py")

SENS_DICT_EXPECTED = expected_result("test_prepared_sens_dict_result_expected.py")
NO_SENS_DICT_EXPECTED = expected_result("test_prepared_no_sens_dict_result_expected.py")


def _create_dict_options(
    db_params,
    db_name: str,
    *,
    meta_dicts: list[str],
    output_sens_dict: str,
    output_no_sens_dict: str | None = None,
    prepared_sens_dict: str | None = None,
    prepared_no_sens_dict: str | None = None,
    save_dicts: bool = False,
):
    args = [
        "create-dict",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={db_name}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        "--scan-mode=full",
        f"--meta-dict-file={','.join(meta_dicts)}",
        f"--output-sens-dict-file={output_sens_dict}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--scan-partial-rows=10000",
        "--debug",
    ]
    if output_no_sens_dict is not None:
        args.append(f"--output-no-sens-dict-file={output_no_sens_dict}")
    if prepared_sens_dict is not None:
        args.append(f"--prepared-sens-dict-file={prepared_sens_dict}")
    if prepared_no_sens_dict is not None:
        args.append(f"--prepared-no-sens-dict-file={prepared_no_sens_dict}")
    if save_dicts:
        args.append("--save-dicts")
    return build_run_options(args)


async def test_02_create_dict(source_db, db_params):
    meta_dict = input_dict("test_meta_dict.py")
    options = _create_dict_options(
        db_params,
        source_db,
        meta_dicts=[meta_dict],
        output_sens_dict=SENS_DICT,
        output_no_sens_dict=NO_SENS_DICT,
    )

    res = await PgAnonApp(options).run()

    assert res.result_code == ResultCode.DONE
    assert Path(SENS_DICT).exists()
    assert Path(NO_SENS_DICT).exists()
    assert_sens_dicts(SENS_DICT, SENS_DICT_EXPECTED)
    assert_no_sens_dicts(NO_SENS_DICT, NO_SENS_DICT_EXPECTED)


async def test_03_dump(source_db, db_params):
    dump_output = output_path("test_03_dump")

    options = build_run_options([
        "dump",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={SENS_DICT}",
        f"--output-dir={dump_output}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--debug",
    ])

    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE


async def test_04_restore(source_db, target_db, db_manager, db_params):
    dump_input = output_path("test_03_dump")

    dump_options = build_run_options([
        "dump",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={SENS_DICT}",
        f"--output-dir={dump_input}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--debug",
    ])

    restore_options = build_run_options([
        "restore",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={target_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={dump_input}",
        "--drop-custom-check-constr",
        "--debug",
    ])

    res = await PgAnonApp(restore_options).run()
    assert res.result_code == ResultCode.DONE

    rows = [
        [
            1,
            "ccd778e5850ddf15d7e9a7ad11a8bbd8",
            "invalid_val_1",
            "*",
            round(Decimal("0.1"), 2),
            "8cbd2171ab4a14fc243421cde93a71c2",
            "f0b314b0620d1ad1a8af2f56cbdd22ac",
        ],
        [
            2,
            "555da16355e56e162c12c95403419eea",
            "invalid_val_2",
            "*",
            round(Decimal("0.2"), 2),
            "8cbd2171ab4a14fc243421cde93a71c2",
            "5385212a24152afd7599bdb3577c7f47",
        ],
    ]
    assert await check_rows(
        db_manager, target_db, "schm_mask_ext_exclude_2", "card_numbers", None, rows
    )

    expected_count = DEFAULT_ROWS * 3
    objs = [["schm_mask_ext_exclude_2", "card_numbers", expected_count]]
    assert await check_rows_count(db_manager, target_db, objs)

    not_found_in_target = await check_list_tables_and_fields(db_manager, source_db, target_db)
    assert len(not_found_in_target) == 3
    assert not_found_in_target == [
        ["columnar_internal", "tbl_200", "id"],
        ["columnar_internal", "tbl_200", "val"],
        ["columnar_internal", "tbl_200", "val_skip"],
    ]


async def test_05_repeat_create_dict_with_no_sens_dict(source_db, db_params):
    no_sens_dict_repeat = output_dict("test_prepared_no_sens_dict_result_repeat.py")
    options = _create_dict_options(
        db_params,
        source_db,
        meta_dicts=[input_dict("test_meta_dict.py")],
        output_sens_dict=SENS_DICT,
        output_no_sens_dict=no_sens_dict_repeat,
        prepared_no_sens_dict=NO_SENS_DICT,
    )

    res = await PgAnonApp(options).run()

    assert Path(SENS_DICT).exists()
    assert Path(no_sens_dict_repeat).exists()
    assert_sens_dicts(SENS_DICT, SENS_DICT_EXPECTED)
    assert_no_sens_dicts(no_sens_dict_repeat, NO_SENS_DICT_EXPECTED)
    assert res.result_code == ResultCode.DONE


async def test_06_repeat_create_dict_with_no_sens_dict_and_sens_dict(source_db, db_params):
    no_sens_dict_repeat = output_dict("test_prepared_no_sens_dict_result_repeat.py")
    options = _create_dict_options(
        db_params,
        source_db,
        meta_dicts=[input_dict("test_meta_dict.py")],
        output_sens_dict=SENS_DICT,
        output_no_sens_dict=no_sens_dict_repeat,
        prepared_sens_dict=SENS_DICT,
        prepared_no_sens_dict=NO_SENS_DICT,
    )

    res = await PgAnonApp(options).run()

    assert Path(SENS_DICT).exists()
    assert Path(no_sens_dict_repeat).exists()
    assert_sens_dicts(SENS_DICT, SENS_DICT_EXPECTED)
    assert_no_sens_dicts(no_sens_dict_repeat, NO_SENS_DICT_EXPECTED)
    assert res.result_code == ResultCode.DONE


@pytest.mark.parametrize(
    "test_id, meta_dict_files, sens_dict_name, sens_dict_expected_name",
    [
        pytest.param(
            "07",
            ["test_meta_dict.py", "meta_include_rules.py"],
            "test_prepared_sens_dict_result_by_include_rule.py",
            "test_prepared_sens_dict_result_by_include_rule_expected.py",
            id="include_rules",
        ),
        pytest.param(
            "08",
            ["test_meta_dict.py", "meta_include_and_skip_rules.py"],
            "test_prepared_sens_dict_result_by_include_and_skip_rules.py",
            "test_prepared_sens_dict_result_by_include_and_skip_rules_expected.py",
            id="include_and_skip_rules_with_masks",
        ),
        pytest.param(
            "09",
            ["test_meta_dict.py", "meta_partial_constants.py"],
            "test_prepared_sens_dict_result_by_partial_constants.py",
            "test_prepared_sens_dict_result_by_partial_constants_expected.py",
            id="partial_constants",
        ),
        pytest.param(
            "10",
            ["test_meta_dict.py", "meta_data_sql_condition.py"],
            "test_prepared_sens_dict_result_by_data_sql_condition.py",
            "test_prepared_sens_dict_result_by_data_sql_condition_expected.py",
            id="data_sql_condition",
        ),
        pytest.param(
            "11",
            ["test_meta_dict.py", "meta_data_func.py"],
            "test_prepared_sens_dict_result_by_data_func.py",
            "test_prepared_sens_dict_result_by_data_func_expected.py",
            id="data_func",
        ),
        pytest.param(
            "12",
            ["test_meta_dict_type_aliases.py"],
            "test_prepared_sens_dict_result_type_aliases.py",
            "test_prepared_sens_dict_result_type_aliases_expected.py",
            id="type_aliases",
        ),
        pytest.param(
            "13",
            ["test_meta_dict_type_aliases_complex.py"],
            "test_prepared_sens_dict_result_type_aliases_complex.py",
            "test_prepared_sens_dict_result_type_aliases_complex_expected.py",
            id="type_aliases_complex",
        ),
        pytest.param(
            "14",
            ["test_meta_dict_default_func.py"],
            "test_prepared_sens_dict_result_default_func.py",
            "test_prepared_sens_dict_result_default_func_expected.py",
            id="default_anonymization_function",
        ),
        pytest.param(
            "15",
            ["test_meta_dict.py", "meta_words_and_phrases_constants.py"],
            "test_prepared_sens_dict_result_by_words_and_phrases_constants.py",
            "test_prepared_sens_dict_result_by_words_and_phrases_constants_expected.py",
            id="words_and_phrases_constants",
        ),
        pytest.param(
            "20",
            ["test_meta_dict.py", "meta_data_func_per_field.py"],
            "test_prepared_sens_dict_result_by_data_func_per_field.py",
            "test_prepared_sens_dict_result_by_data_func_per_field_expected.py",
            id="data_func_per_field",
        ),
    ],
)
async def test_create_dict_variants(
    source_db,
    db_params,
    test_id,
    meta_dict_files,
    sens_dict_name,
    sens_dict_expected_name,
):
    meta_dicts = [input_dict(f) for f in meta_dict_files]
    sens_dict = output_dict(sens_dict_name)
    sens_dict_exp = expected_result(sens_dict_expected_name)

    options = _create_dict_options(
        db_params,
        source_db,
        meta_dicts=meta_dicts,
        output_sens_dict=sens_dict,
    )

    res = await PgAnonApp(options).run()

    assert Path(sens_dict).exists()
    assert_sens_dicts(sens_dict, sens_dict_exp)
    assert res.result_code == ResultCode.DONE


async def test_16_create_dict_save_dicts(source_db, db_params):
    meta_dict = input_dict("test_meta_dict.py")

    options = _create_dict_options(
        db_params,
        source_db,
        meta_dicts=[meta_dict],
        output_sens_dict=SENS_DICT,
        output_no_sens_dict=NO_SENS_DICT,
        prepared_sens_dict=SENS_DICT_EXPECTED,
        prepared_no_sens_dict=NO_SENS_DICT_EXPECTED,
        save_dicts=True,
    )

    res = await PgAnonApp(options).run()

    assert Path(SENS_DICT).exists()
    assert Path(NO_SENS_DICT).exists()
    assert_sens_dicts(SENS_DICT, SENS_DICT_EXPECTED)
    assert_no_sens_dicts(NO_SENS_DICT, NO_SENS_DICT_EXPECTED)

    run_dir = Path(options.run_dir)
    saved_dicts_info = run_dir / SAVED_DICTS_INFO_FILE_NAME
    saved_meta_dict = run_dir / "input" / Path(meta_dict).name
    saved_prepared_sens_dict = run_dir / "input" / Path(SENS_DICT_EXPECTED).name
    saved_prepared_no_sens_dict = run_dir / "input" / Path(NO_SENS_DICT_EXPECTED).name
    saved_target_sens_dict = run_dir / "output" / Path(SENS_DICT).name
    saved_target_no_sens_dict = run_dir / "output" / Path(NO_SENS_DICT).name

    assert saved_dicts_info.exists()
    assert saved_meta_dict.exists()
    assert saved_prepared_sens_dict.exists()
    assert saved_prepared_no_sens_dict.exists()
    assert saved_target_sens_dict.exists()
    assert saved_target_no_sens_dict.exists()

    assert filecmp.cmp(saved_meta_dict, meta_dict, shallow=False)
    assert filecmp.cmp(saved_prepared_sens_dict, SENS_DICT_EXPECTED, shallow=False)
    assert filecmp.cmp(saved_prepared_no_sens_dict, NO_SENS_DICT_EXPECTED, shallow=False)
    assert filecmp.cmp(saved_target_sens_dict, SENS_DICT_EXPECTED, shallow=False)
    assert filecmp.cmp(saved_target_no_sens_dict, NO_SENS_DICT_EXPECTED, shallow=False)

    assert res.result_code == ResultCode.DONE


async def test_17_dump_save_dicts(source_db, db_params):
    dump_output = output_path("test_17_dump_save_dicts")
    partial_tables_dict_file = input_dict("test_partial_tables_dict.py")
    partial_tables_exclude_dict_file = input_dict("test_partial_exclude_tables_dict.py")

    options = build_run_options([
        "dump",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={SENS_DICT}",
        f"--partial-tables-dict-file={partial_tables_dict_file}",
        f"--partial-tables-exclude-dict-file={partial_tables_exclude_dict_file}",
        f"--output-dir={dump_output}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--save-dicts",
        "--debug",
    ])

    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE

    run_dir = Path(options.run_dir)
    saved_dicts_info = run_dir / SAVED_DICTS_INFO_FILE_NAME
    saved_sens_dict = run_dir / "input" / Path(SENS_DICT).name
    saved_partial_tables = run_dir / "input" / Path(partial_tables_dict_file).name
    saved_partial_exclude = run_dir / "input" / Path(partial_tables_exclude_dict_file).name

    assert saved_dicts_info.exists()
    assert saved_sens_dict.exists()
    assert saved_partial_tables.exists()
    assert saved_partial_exclude.exists()

    assert filecmp.cmp(saved_sens_dict, SENS_DICT, shallow=False)
    assert filecmp.cmp(saved_partial_tables, partial_tables_dict_file, shallow=False)
    assert filecmp.cmp(saved_partial_exclude, partial_tables_exclude_dict_file, shallow=False)


async def test_18_restore_save_dicts(source_db, target_db, db_params):
    dump_input = output_path("test_17_dump_save_dicts")
    partial_tables_dict_file = input_dict("test_partial_tables_dict.py")
    partial_tables_exclude_dict_file = input_dict("test_partial_exclude_tables_dict.py")

    options = build_run_options([
        "restore",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={target_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={dump_input}",
        f"--partial-tables-dict-file={partial_tables_dict_file}",
        f"--partial-tables-exclude-dict-file={partial_tables_exclude_dict_file}",
        "--drop-custom-check-constr",
        "--save-dicts",
        "--drop-db",
        "--debug",
    ])

    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE

    run_dir = Path(options.run_dir)
    saved_dicts_info = run_dir / SAVED_DICTS_INFO_FILE_NAME
    saved_partial_tables = run_dir / "input" / Path(partial_tables_dict_file).name
    saved_partial_exclude = run_dir / "input" / Path(partial_tables_exclude_dict_file).name

    assert saved_dicts_info.exists()
    assert saved_partial_tables.exists()
    assert saved_partial_exclude.exists()

    assert filecmp.cmp(saved_partial_tables, partial_tables_dict_file, shallow=False)
    assert filecmp.cmp(saved_partial_exclude, partial_tables_exclude_dict_file, shallow=False)


async def test_19_create_dict_using_not_existing_functions(source_db, db_params):
    meta_dicts = [input_dict("test_meta_not_existing_functions_in_datafunc.py")]
    sens_dict = output_dict("test_prepared_sens_dict_result_by_not_existing_functions_in_datafunc.py")

    options = _create_dict_options(
        db_params,
        source_db,
        meta_dicts=meta_dicts,
        output_sens_dict=sens_dict,
    )

    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.FAIL
