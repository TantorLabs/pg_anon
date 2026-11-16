from __future__ import annotations

import pytest

from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.modes.view_fields import ViewFieldsMode

E = 'md5("email")'
N = 'md5("note")'
I = 'md5("info")'  # noqa: E741
NO_RULE = "---"
NOT_SHOWN = "not shown"

ORDERS = {"schema": "sp", "table": "orders"}
RU = {"schema": "sp", "table": "orders_2024_ru"}
DEF = {"schema": "sp", "table": "orders_2024_def"}
CONTACT = {"schema": "sp_crm", "table": "contact"}

PARTITIONS = ("sp.orders_2024_ru", "sp.orders_2024_def", "sp_archive.orders_2023")


def rule(table: dict, **fields: str) -> dict:
    return {**table, "fields": fields}


def _options(db_params, source_db, dict_files: str, *selection: str) -> list[str]:
    """Options of view-fields with the given sens dict files and the given choice of tables."""
    return [
        "view-fields",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={dict_files}",
        *selection,
    ]


async def _view_fields(db_params, source_db, tmp_path, sens_dict) -> dict[str, str]:
    dict_file = tmp_path / "sens_dict.py"
    dict_file.write_text(repr(sens_dict))
    options = build_run_options(_options(db_params, source_db, str(dict_file), "--schema-mask=^sp"))
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()
    return {f"{field.nspname}.{field.relname}.{field.column_name}": field.rule for field in executor.fields or []}


def _rules(fields: dict[str, str], tables: tuple[str, ...], columns: tuple[str, ...]) -> dict[str, str]:
    return {f"{t}.{c}": fields.get(f"{t}.{c}", NOT_SHOWN) for t in tables for c in columns}


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("sens_dict", "expected"),
    [
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)]},
            {**{f"{t}.email": E for t in PARTITIONS}, **{f"{t}.note": NO_RULE for t in PARTITIONS}},
            id="root-rule",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E, note=N), rule(RU, note="'RU'")]},
            {
                **{f"{t}.email": E for t in PARTITIONS},
                "sp.orders_2024_ru.note": "'RU'",
                "sp.orders_2024_def.note": N,
                "sp_archive.orders_2023.note": N,
            },
            id="own-rule-merged-with-root",
        ),
        pytest.param(
            {
                "dictionary": [
                    rule(ORDERS, email=E),
                    {"schema": "sp", "table_mask": "^orders_2024", "fields": {"note": "'M'"}},
                ]
            },
            {
                **{f"{t}.email": E for t in PARTITIONS},
                "sp.orders_2024_ru.note": "'M'",
                "sp.orders_2024_def.note": "'M'",
                "sp_archive.orders_2023.note": NO_RULE,
            },
            id="mask-merged-with-root",
        ),
        pytest.param(
            {"dictionary": [], "dictionary_exclude": [ORDERS]},
            {f"{t}.{c}": NOT_SHOWN for t in PARTITIONS for c in ("email", "note")},
            id="exclude-root",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)], "dictionary_exclude": [DEF]},
            {
                "sp.orders_2024_ru.email": E,
                "sp.orders_2024_ru.note": NO_RULE,
                "sp.orders_2024_def.email": NOT_SHOWN,
                "sp.orders_2024_def.note": NOT_SHOWN,
                "sp_archive.orders_2023.email": E,
                "sp_archive.orders_2023.note": NO_RULE,
            },
            id="own-exclude-beats-root-rule",
        ),
    ],
)
async def test_view_fields_shows_rule_that_dump_applies_to_partition(
    source_db, db_params, tmp_path, sens_dict, expected
):
    fields = await _view_fields(db_params, source_db, tmp_path, sens_dict)
    assert _rules(fields, PARTITIONS, ("email", "note")) == expected


async def test_view_fields_shows_inherits_parent_rule_only_on_parent(source_db, db_params, tmp_path):
    fields = await _view_fields(db_params, source_db, tmp_path, {"dictionary": [rule(CONTACT, info=I)]})
    tables = ("sp_crm.contact", "sp_crm.person", "sp_crm.company", "sp_crm.partner")
    assert _rules(fields, tables, ("info",)) == {
        "sp_crm.contact.info": I,
        "sp_crm.person.info": NO_RULE,
        "sp_crm.company.info": NO_RULE,
        "sp_crm.partner.info": NO_RULE,
    }


@pytest.mark.min_pg(100000)
async def test_view_fields_names_the_file_of_each_partition_field(source_db, db_params, tmp_path):
    root_file, own_file = tmp_path / "root.py", tmp_path / "own.py"
    root_file.write_text(repr({"dictionary": [rule(ORDERS, email=E, note=N)]}))
    own_file.write_text(repr({"dictionary": [rule(RU, note="'RU'")]}))
    options = build_run_options(
        _options(db_params, source_db, f"{root_file},{own_file}", "--schema-name=sp", "--table-name=orders_2024_ru")
    )
    executor = ViewFieldsMode(PgAnonApp(options).context)
    await executor.run()

    files = {field.column_name: field.dict_file_name for field in executor.fields}
    assert files["email"] == str(root_file)
    assert files["note"] == str(own_file)
