from __future__ import annotations

import json
import re
from pathlib import Path

from .conftest import META_DICT, output_dict, output_path
from pg_anon.common.enums import ResultCode

# What `create-dict` must find in the demo database when driven by the
# meta-dictionary from the guide. Written out in full: the guide claims these
# columns are detected, so a silent change in either file has to break here.
EXPECTED_SENSITIVE_FIELDS = {
    ("hr", "employee", "full_name"),
    ("hr", "employee", "email"),
    ("hr", "employee", "phone"),
    ("hr", "employee", "ssn"),
    ("hr", "employee", "salary"),
    ("shop", "customer", "full_name"),
    ("shop", "customer", "email"),
    ("shop", "customer", "phone"),
    ("shop", "customer", "birth_date"),
    ("shop", "payment_card", "cardholder_name"),
    ("shop", "payment_card", "card_number"),
    # caught by data_regex only — the column name gives nothing away
    ("shop", "customer_order", "note"),
}


def _flatten(dictionary: dict) -> set[tuple[str, str, str]]:
    return {(rule["schema"], rule["table"], field) for rule in dictionary["dictionary"] for field in rule["fields"]}


async def test_scan_finds_documented_fields(source_db, pg_anon_runner):
    sens_out = output_dict("sens_dict.json")
    no_sens_out = output_dict("no_sens_dict.json")

    res = await pg_anon_runner.run(
        "create-dict",
        source_db,
        [
            f"--meta-dict-file={META_DICT}",
            f"--output-sens-dict-file={sens_out}",
            f"--output-no-sens-dict-file={no_sens_out}",
        ],
    )
    assert res.result_code == ResultCode.DONE

    sens = json.loads(Path(sens_out).read_text(encoding="utf-8"))
    assert _flatten(sens) == EXPECTED_SENSITIVE_FIELDS

    # skip_rules keeps the product catalogue out of the scan entirely
    scanned_tables = {(rule["schema"], rule["table"]) for rule in sens["dictionary"]}
    assert ("shop", "product") not in scanned_tables

    # every sensitive type has a rule of its own, so the "default" fallback of
    # the meta-dictionary stays unused — the guide says so
    rules = [rule for table in sens["dictionary"] for rule in table["fields"].values()]
    assert not any("digest(" in rule for rule in rules)

    no_sens = json.loads(Path(no_sens_out).read_text(encoding="utf-8"))
    assert no_sens["no_sens_dictionary"], "the guide promises a non-sensitive dictionary as well"


async def test_full_chain(source_db, target_db, pg_anon_runner, db_manager):
    """init -> create-dict -> view-fields -> view-data -> dump -> restore."""
    sens_out = output_dict("chain_sens_dict.json")

    res = await pg_anon_runner.run(
        "create-dict",
        source_db,
        [
            f"--meta-dict-file={META_DICT}",
            f"--output-sens-dict-file={sens_out}",
        ],
    )
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run(
        "view-fields",
        source_db,
        [f"--prepared-sens-dict-file={sens_out}", "--schema-name=hr", "--table-name=employee"],
    )
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run(
        "view-data",
        source_db,
        [
            f"--prepared-sens-dict-file={sens_out}",
            "--schema-name=hr",
            "--table-name=employee",
            "--limit=3",
            "--offset=0",
        ],
    )
    assert res.result_code == ResultCode.DONE

    dump_dir = output_path("chain_dump")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={sens_out}",
            f"--output-dir={dump_dir}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [f"--input-dir={dump_dir}"])
    assert res.result_code == ResultCode.DONE

    # The clone must keep every row ...
    for schema, table in (
        ("hr", "employee"),
        ("shop", "customer"),
        ("shop", "payment_card"),
        ("shop", "customer_order"),
        ("shop", "product"),
    ):
        src = await db_manager.fetch(source_db, f"SELECT count(*) AS c FROM {schema}.{table}")
        tgt = await db_manager.fetch(target_db, f"SELECT count(*) AS c FROM {schema}.{table}")
        assert src[0]["c"] == tgt[0]["c"] > 0, f"{schema}.{table} lost rows"

    # ... drop the personal data ...
    src_emp = await db_manager.fetch(source_db, "SELECT full_name, email, phone, ssn FROM hr.employee ORDER BY id")
    tgt_emp = await db_manager.fetch(target_db, "SELECT full_name, email, phone, ssn FROM hr.employee ORDER BY id")
    assert all(src["full_name"] != tgt["full_name"] for src, tgt in zip(src_emp, tgt_emp, strict=True))
    assert all(src["email"] != tgt["email"] for src, tgt in zip(src_emp, tgt_emp, strict=True))

    tgt_card = await db_manager.fetch(target_db, "SELECT card_number FROM shop.payment_card")
    assert all(row["card_number"].startswith("****-****-****-") for row in tgt_card)

    # ... while keeping the shape of every value, which is what the guide claims ...
    assert all(re.fullmatch(r"XXX-XX-\d{4}", row["ssn"]) for row in tgt_emp)
    assert all(re.fullmatch(r"[a-z0-9]{8}@example\.com", row["email"]) for row in tgt_emp)
    assert all(re.fullmatch(r"\+1\d{9}", row["phone"]) for row in tgt_emp)

    tgt_salary = await db_manager.fetch(target_db, "SELECT salary FROM hr.employee")
    assert all(row["salary"].as_tuple().exponent == -2 for row in tgt_salary)

    # ... the free-text column stays readable with only the e-mail replaced ...
    notes = await db_manager.fetch(target_db, "SELECT note FROM shop.customer_order WHERE note LIKE '%@%'")
    assert notes, "the note column lost its e-mails entirely"
    assert all(row["note"] == "delivery confirmed by customer@example.com" for row in notes)

    # ... and everything non-sensitive is untouched.
    src_products = await db_manager.fetch(source_db, "SELECT id, sku, title, price FROM shop.product ORDER BY id")
    tgt_products = await db_manager.fetch(target_db, "SELECT id, sku, title, price FROM shop.product ORDER BY id")
    assert [dict(r) for r in src_products] == [dict(r) for r in tgt_products]
