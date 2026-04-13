from .conftest import input_dict, output_dict
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode

_elapsed_partial = None
_elapsed_full = None


async def test_02_create_dict_partial(source_db, db_params):
    """create-dict with scan-mode=partial, should be fast."""
    global _elapsed_partial

    options = build_run_options([
        "create-dict",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        "--scan-mode=partial",
        f"--meta-dict-file={input_dict('test_meta_dict.py')}",
        f"--output-sens-dict-file={output_dict('stress_partial.py')}",
        "--db-connections-per-process=4",
        "--processes=2",
        "--scan-partial-rows=100",
        "--debug",
    ])

    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE
    _elapsed_partial = res.elapsed


async def test_03_create_dict_full(source_db, db_params):
    """create-dict with scan-mode=full, should be slower."""
    global _elapsed_full

    options = build_run_options([
        "create-dict",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        "--scan-mode=full",
        f"--meta-dict-file={input_dict('test_meta_dict.py')}",
        f"--output-sens-dict-file={output_dict('stress_full.py')}",
        "--db-connections-per-process=4",
        "--processes=2",
        "--debug",
    ])

    res = await PgAnonApp(options).run()
    assert res.result_code == ResultCode.DONE
    _elapsed_full = res.elapsed


async def test_04_compare_performance(source_db):
    """Partial scan should be at least 5x faster than full scan."""
    assert _elapsed_partial is not None and _elapsed_full is not None
    print(f"Comparing values: {_elapsed_partial} < ({_elapsed_full} / 5)")
    assert float(_elapsed_partial) < float(_elapsed_full) / 5
