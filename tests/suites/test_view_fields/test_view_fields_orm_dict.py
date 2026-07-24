from __future__ import annotations

import json

import pytest

from .conftest import input_dict
from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.dto import FieldInfo
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.modes.view_fields import (
    _apply_orm_names,
    _build_orm_index,
    _load_orm_index,
    _normalize_orm_name,
    ViewFieldsMode,
)

ORM_DATA = [
    {
        "schema": "public",
        "table_name": "_Reference77815X1",
        "table_alias": "Справочник.plm_НастройкаОбмена",
        "fields": {
            "_IDRRef": "Ссылка",
            "_Code": "Код",
            "_Fld77818": "COMИмяБазы",
            "_Fld77819": "",
        },
        "comment": "Основная",
    },
    {
        "schema": "public",
        "table_name": "_DataHistoryVersionsExtX1",
        "table_alias": "",
        "fields": {"_VersionNumber": ""},
        "comment": "ВерсииИсторииДанных",
    },
]


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
    assert _normalize_orm_name("_Reference77815X1") == "_reference77815x1"
    assert _normalize_orm_name("_IDRRef") == "_idrref"
    assert _normalize_orm_name("plain") == "plain"


def test_build_orm_index():
    index = _build_orm_index(ORM_DATA)

    table_alias, fields = index["public._reference77815x1"]
    assert table_alias == "Справочник.plm_НастройкаОбмена"
    assert fields["_idrref"] == "Ссылка"
    assert fields["_fld77818"] == "COMИмяБазы"
    assert fields["_fld77819"] == ""

    table_alias, fields = index["public._datahistoryversionsextx1"]
    assert table_alias == ""
    assert fields["_versionnumber"] == ""


def test_build_orm_index_ignores_comment():
    index = _build_orm_index(ORM_DATA)
    # comment is an operator-only field and must not leak into the index tuple
    comments = {entry["comment"] for entry in ORM_DATA if entry.get("comment")}
    assert comments  # guard: the sample data actually has comments to ignore
    for table_alias, orm_fields in index.values():
        assert table_alias not in comments
        assert not comments & set(orm_fields.values())


def _field(relname: str, column_name: str, nspname: str = "public") -> FieldInfo:
    return FieldInfo(
        nspname=nspname,
        relname=relname,
        column_name=column_name,
        type="text",
        oid=1,
        attnum=1,
        obj_id="1",
        tbl_id="1",
    )


def test_apply_orm_names_translates_table_and_field():
    fields = [_field("_reference77815x1", "_fld77818")]
    _apply_orm_names(fields, _build_orm_index(ORM_DATA))
    assert fields[0].relname == "Справочник.plm_НастройкаОбмена"
    assert fields[0].column_name == "COMИмяБазы"


def test_apply_orm_names_matches_case_insensitively():
    fields = [_field("_REFERENCE77815X1", "_IdrRef", nspname="PUBLIC")]
    _apply_orm_names(fields, _build_orm_index(ORM_DATA))
    assert fields[0].relname == "Справочник.plm_НастройкаОбмена"
    assert fields[0].column_name == "Ссылка"


def test_apply_orm_names_matches_within_schema():
    # a same-named table in another schema must not be translated
    fields = [_field("_reference77815x1", "_fld77818", nspname="other")]
    _apply_orm_names(fields, _build_orm_index(ORM_DATA))
    assert fields[0].relname == "_reference77815x1"
    assert fields[0].column_name == "_fld77818"


def test_apply_orm_names_keeps_sql_names_when_not_found_or_empty():
    fields = [
        _field("_reference77815x1", "_fld77819"),  # field alias is an empty string
        _field("_reference77815x1", "_fld99999"),  # field is absent from the ORM dict
        _field("unknown_table", "some_field"),  # table is absent from the ORM dict
        _field("reference77815x1", "_fld77818"),  # leading underscore is significant now
    ]
    _apply_orm_names(fields, _build_orm_index(ORM_DATA))
    assert fields[0].relname == "Справочник.plm_НастройкаОбмена"
    assert fields[0].column_name == "_fld77819"
    assert fields[1].column_name == "_fld99999"
    assert fields[2].relname == "unknown_table"
    assert fields[2].column_name == "some_field"
    assert fields[3].relname == "reference77815x1"
    assert fields[3].column_name == "_fld77818"


def test_load_orm_index_reads_file_with_utf8_bom(tmp_path):
    orm_file = tmp_path / "structure.json"
    orm_file.write_text(json.dumps(ORM_DATA, ensure_ascii=False), encoding="utf-8-sig")
    index = _load_orm_index(str(orm_file))
    assert "public._reference77815x1" in index


def test_load_orm_index_missing_file_raises_invalid_path(tmp_path):
    with pytest.raises(PgAnonError) as err:
        _load_orm_index(str(tmp_path / "missing.json"))
    assert err.value.code == ErrorCode.INVALID_PATH


def test_load_orm_index_invalid_json_raises_invalid_dict_file(tmp_path):
    bad_file = tmp_path / "bad.json"
    bad_file.write_text("{not json", encoding="utf-8")
    with pytest.raises(PgAnonError) as err:
        _load_orm_index(str(bad_file))
    assert err.value.code == ErrorCode.INVALID_DICT_FILE


def test_build_orm_index_rejects_non_list():
    with pytest.raises(PgAnonError) as err:
        _build_orm_index({"schema": "public", "table_name": "t"})
    assert err.value.code == ErrorCode.INVALID_DICT_FILE


def test_build_orm_index_rejects_non_object_entry():
    with pytest.raises(PgAnonError) as err:
        _build_orm_index(["not-an-object"])
    assert err.value.code == ErrorCode.INVALID_DICT_FILE


def test_build_orm_index_rejects_entry_without_required_keys():
    with pytest.raises(PgAnonError) as err:
        _build_orm_index([{"schema": "public", "fields": {}}])
    assert err.value.code == ErrorCode.INVALID_DICT_FILE


def test_build_orm_index_rejects_non_object_fields():
    with pytest.raises(PgAnonError) as err:
        _build_orm_index([{"schema": "public", "table_name": "t", "fields": []}])
    assert err.value.code == ErrorCode.INVALID_DICT_FILE


def test_build_orm_index_rejects_non_string_schema_or_table():
    with pytest.raises(PgAnonError) as err:
        _build_orm_index([{"schema": 1, "table_name": "t"}])
    assert err.value.code == ErrorCode.INVALID_DICT_FILE

    with pytest.raises(PgAnonError) as err:
        _build_orm_index([{"schema": "public", "table_name": ["t"]}])
    assert err.value.code == ErrorCode.INVALID_DICT_FILE


async def test_view_fields_orm_dict_translates_names(source_db, db_params):
    args = [
        "view-fields",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={input_dict('view_fields.py')}",
        f"--orm-dict-file={input_dict('orm_structure.json')}",
        "--schema-name=hr",
        "--debug",
    ]
    executor = ViewFieldsMode(PgAnonApp(build_run_options(args)).context)
    await executor.run()

    assert executor.fields
    employee_columns = {f.column_name for f in executor.fields if f.relname == "Справочник.Сотрудники"}
    assert employee_columns, "hr.employee rows must be shown under the 1C table name"
    assert "Имя" in employee_columns, "first_name must be translated via the ORM dict"
    assert "first_name" not in employee_columns
    assert "last_name" in employee_columns, "empty ORM display name must keep the SQL name"

    other_tables = {f.relname for f in executor.fields if f.relname != "Справочник.Сотрудники"}
    assert "employee" not in other_tables
    assert other_tables, "tables absent from the ORM dict must keep their SQL names"
