from __future__ import annotations

from .conftest import input_dict
from pg_anon.cli import build_run_options


def _dbless_options(orm_dict_file: str | None = None):
    args = [
        "view-fields",
        "--db-host=127.0.0.1",
        "--db-port=5432",
        "--db-name=stub",
        "--db-user=stub",
        f"--prepared-sens-dict-file={input_dict('view_fields.py')}",
    ]
    if orm_dict_file:
        args.append(f"--orm-dict-file={orm_dict_file}")
    return build_run_options(args)


def test_cli_accepts_orm_dict_file_option():
    options = _dbless_options("structure.json")
    assert options.orm_dict_file == "structure.json"


def test_cli_orm_dict_file_defaults_to_none():
    options = _dbless_options()
    assert options.orm_dict_file is None
