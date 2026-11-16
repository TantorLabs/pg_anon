from __future__ import annotations

import pytest

from tests.infrastructure.assertions import masking, NO_ROWS

from .conftest import output_path
from pg_anon.common.enums import ResultCode

E = 'md5("email")'
N = 'md5("note")'
I = 'md5("info")'  # noqa: E741

ORDERS = {"schema": "sp", "table": "orders"}
RU = {"schema": "sp", "table": "orders_2024_ru"}
DEF = {"schema": "sp", "table": "orders_2024_def"}
CONTACT = {"schema": "sp_crm", "table": "contact"}
VENDOR = {"schema": "sp_crm", "table": "vendor"}
PERSON = {"schema": "sp_crm", "table": "person"}

PARTITIONS = ("sp.orders_2024_ru", "sp.orders_2024_def", "sp_archive.orders_2023")
EXCLUDED = NO_ROWS


def rule(table: dict, **fields: str) -> dict:
    return {**table, "fields": fields}


def mask_rule(schema: str, table_mask: str, **fields: str) -> dict:
    return {"schema": schema, "table_mask": table_mask, "fields": fields}


def mask(schema: str, table_mask: str) -> dict:
    return {"schema": schema, "table_mask": table_mask}


async def _dump_and_restore(pg_anon_runner, db_params, source_db, target_db, tmp_path, sens_dict, name):
    dict_file = tmp_path / "sens_dict.py"
    dict_file.write_text(repr(sens_dict))
    out = output_path(name)
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={dict_file}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE
    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("sens_dict", "expected"),
    [
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)]},
            dict.fromkeys(PARTITIONS, {"email": "md5", "note": "as is"}),
            id="root-rule",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E, note=N), rule(RU, note="'RU'")]},
            {
                "sp.orders_2024_ru": {"email": "md5", "note": "RU"},
                "sp.orders_2024_def": {"email": "md5", "note": "md5"},
                "sp_archive.orders_2023": {"email": "md5", "note": "md5"},
            },
            id="own-rule-merged-with-root",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E), mask_rule("sp", "^orders_2024", note="'M'")]},
            {
                "sp.orders_2024_ru": {"email": "md5", "note": "M"},
                "sp.orders_2024_def": {"email": "md5", "note": "M"},
                "sp_archive.orders_2023": {"email": "md5", "note": "as is"},
            },
            id="mask-merged-with-root",
        ),
        pytest.param(
            {"dictionary": [], "dictionary_exclude": [ORDERS]},
            dict.fromkeys(PARTITIONS, EXCLUDED),
            id="exclude-root",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)], "dictionary_exclude": [DEF]},
            {
                "sp.orders_2024_ru": {"email": "md5", "note": "as is"},
                "sp.orders_2024_def": EXCLUDED,
                "sp_archive.orders_2023": {"email": "md5", "note": "as is"},
            },
            id="own-exclude-beats-root-rule",
        ),
        pytest.param(
            {"dictionary": [rule(RU, email=E)], "dictionary_exclude": [ORDERS]},
            {
                "sp.orders_2024_ru": {"email": "md5", "note": "as is"},
                "sp.orders_2024_def": EXCLUDED,
                "sp_archive.orders_2023": EXCLUDED,
            },
            id="own-rule-beats-root-exclude",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)], "dictionary_exclude": [ORDERS]},
            dict.fromkeys(PARTITIONS, {"email": "md5", "note": "as is"}),
            id="rule-beats-exclude-on-one-level",
        ),
        pytest.param(
            {"dictionary": [mask_rule("sp", "_ru$", email=E)], "dictionary_exclude": [ORDERS]},
            dict.fromkeys(PARTITIONS, EXCLUDED),
            id="exact-root-exclude-beats-own-mask-rule",
        ),
        pytest.param(
            {"dictionary": [mask_rule("sp", "_ru$", email=E)], "dictionary_exclude": [RU]},
            {
                "sp.orders_2024_ru": EXCLUDED,
                "sp.orders_2024_def": {"email": "as is", "note": "as is"},
                "sp_archive.orders_2023": {"email": "as is", "note": "as is"},
            },
            id="own-exact-exclude-beats-own-mask-rule",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)], "dictionary_exclude": [mask("sp", "^orders$")]},
            dict.fromkeys(PARTITIONS, {"email": "md5", "note": "as is"}),
            id="exact-root-rule-beats-root-mask-exclude",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)], "dictionary_exclude": [mask("sp", "_ru$")]},
            dict.fromkeys(PARTITIONS, {"email": "md5", "note": "as is"}),
            id="exact-root-rule-beats-own-mask-exclude",
        ),
        pytest.param(
            {"dictionary": [rule(RU, email=E)]},
            {
                "sp.orders_2024_ru": {"email": "md5", "note": "as is"},
                "sp.orders_2024_def": {"email": "as is", "note": "as is"},
                "sp_archive.orders_2023": {"email": "as is", "note": "as is"},
            },
            id="only-partition-rule",
        ),
    ],
)
async def test_dump_masks_partitions_by_their_rule_and_rules_of_partitioned_tables_above(
    source_db, target_db, db_manager, db_params, pg_anon_runner, tmp_path, sens_dict, expected
):
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db, tmp_path, sens_dict, "partition_rules")

    actual = {table: await masking(db_manager, source_db, target_db, table, ("email", "note")) for table in expected}
    assert actual == expected


# keys: the key column of tables without id, to match restored rows with source rows
@pytest.mark.parametrize(
    ("sens_dict", "keys", "expected"),
    [
        pytest.param(
            {"dictionary": [rule(CONTACT, info=I)]},
            {},
            {
                "sp_crm.contact": {"info": "md5"},
                "sp_crm.person": {"info": "as is"},
                "sp_crm.company": {"info": "as is"},
                "sp_crm.partner": {"info": "as is"},
            },
            id="parent-rule",
        ),
        pytest.param(
            {"dictionary": [], "dictionary_exclude": [CONTACT]},
            {},
            {
                "sp_crm.contact": EXCLUDED,
                "sp_crm.person": {"info": "as is"},
                "sp_crm.company": {"info": "as is"},
                "sp_crm.partner": {"info": "as is"},
            },
            id="parent-exclude",
        ),
        pytest.param(
            {"dictionary": [rule(CONTACT, info=I)], "dictionary_exclude": [VENDOR]},
            {"sp_crm.vendor": "code"},
            {"sp_crm.partner": {"info": "as is"}, "sp_crm.vendor": EXCLUDED},
            id="rule-and-exclude-on-two-parents",
        ),
        pytest.param(
            {"dictionary": [rule(PERSON, info=I)]},
            {},
            {
                "sp_crm.contact": {"info": "as is"},
                "sp_crm.person": {"info": "md5"},
                "sp_crm.company": {"info": "as is"},
                "sp_crm.partner": {"info": "as is"},
            },
            id="only-child-rule",
        ),
    ],
)
async def test_dump_masks_inherits_tables_only_by_their_own_rule(
    source_db, target_db, db_manager, db_params, pg_anon_runner, tmp_path, sens_dict, keys, expected
):
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db, tmp_path, sens_dict, "inherits_rules")

    actual = {}
    for table, expected_masking in expected.items():
        columns = () if expected_masking == EXCLUDED else tuple(expected_masking)
        actual[table] = await masking(db_manager, source_db, target_db, table, columns, keys.get(table, "id"))
    assert actual == expected
