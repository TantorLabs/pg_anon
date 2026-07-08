from __future__ import annotations

from .conftest import input_dict
from pg_anon.cli import build_run_options
from pg_anon.modes.view_fields import _build_orm_index, _normalize_orm_name

ORM_DATA = {
    "Reference77815": {
        "ИмяТаблицы": "Справочник.plm_НастройкаОбмена",
        "Назначение": "Основная",
        "Поля": {
            "ID": "Ссылка",
            "Code": "Код",
            "Fld77818": "COMИмяБазы",
            "Fld77819": "",
        },
    },
    "DataHistoryVersionsExt": {
        "ИмяТаблицы": "DataHistoryVersionsExt",
        "Назначение": "ВерсииИсторииДанных",
        "Поля": {"VersionNumber": ""},
    },
}


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


def test_normalize_orm_name():
    assert _normalize_orm_name("_reference77815") == "reference77815"
    assert _normalize_orm_name("Reference77815") == "reference77815"
    assert _normalize_orm_name("_Fld77818") == "fld77818"
    assert _normalize_orm_name("plain") == "plain"


def test_build_orm_index():
    index = _build_orm_index(ORM_DATA)

    table_name, fields = index["reference77815"]
    assert table_name == "Справочник.plm_НастройкаОбмена"
    assert fields["id"] == "Ссылка"
    assert fields["fld77818"] == "COMИмяБазы"
    assert fields["fld77819"] == ""

    table_name, fields = index["datahistoryversionsext"]
    assert table_name == "DataHistoryVersionsExt"
    assert fields["versionnumber"] == ""
