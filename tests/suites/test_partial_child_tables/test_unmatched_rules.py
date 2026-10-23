"""A list rule that matches no table gets a warning, on dump and on restore; matching rules get none."""

from __future__ import annotations

import json
import logging

import pytest

from .conftest import dump, output_path, restore, SUITE
from pg_anon.common.enums import ResultCode

LIST_TITLES = {"exclude": "Exclude list", "include": "Include list"}
LIST_OPTIONS = {"exclude": "--partial-tables-exclude-dict-file", "include": "--partial-tables-dict-file"}

# the four forms of a rule; keys keep the order of the dictionary
UNMATCHED_RULES = {
    "name_name": {"schema": "ilist", "table": "missing_table"},
    "name_mask": {"schema": "ilist", "table_mask": "^missing_.*$"},
    "mask_name": {"table": "accounts", "schema_mask": "^missing_schema$"},
    "mask_mask": {"schema_mask": "_old$", "table_mask": ".*"},
}
MATCHED_RULES = [
    {"schema": "ilist", "table": "accounts"},
    {"schema": "ilist", "table_mask": "^sms_"},
    {"schema_mask": "^ilist$", "table": "reads"},
    {"schema_mask": "^ilist$", "table_mask": "^mark"},
]


def write_list(name: str, kind: str, rules: list[dict]) -> str:
    path = output_path("unmatched") / f"{name}_{kind}.py"
    path.write_text(repr({"tables": rules}), encoding="utf-8")
    return f"{LIST_OPTIONS[kind]}={path}"


def unmatched_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [message for message in caplog.messages if "match" in message and "no table" in message]


def expected_line(kind: str, rule: dict, place: str) -> str:
    return f"{LIST_TITLES[kind]}: the rule {json.dumps(rule)} matches no table in {place}."


@pytest.mark.parametrize("kind", LIST_OPTIONS)
@pytest.mark.parametrize("form", UNMATCHED_RULES)
async def test_dump_warns_about_a_rule_without_tables(form, kind, inherits_db, db_params, pg_anon_runner, caplog):
    rule = UNMATCHED_RULES[form]
    options = [write_list(f"dump_{form}", kind, [rule, MATCHED_RULES[0]])]

    with caplog.at_level(logging.WARNING):
        _, res = await dump(pg_anon_runner, db_params, inherits_db, f"unmatched_dump_{form}_{kind}", options)

    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    assert unmatched_lines(caplog) == [expected_line(kind, rule, "the database")]


@pytest.mark.parametrize("kind", LIST_OPTIONS)
@pytest.mark.parametrize("form", UNMATCHED_RULES)
async def test_restore_warns_about_a_rule_without_tables(
    form, kind, inherits_full_dump, target_dbs, db_params, pg_anon_runner, caplog
):
    rule = UNMATCHED_RULES[form]
    options = [write_list(f"restore_{form}", kind, [rule, MATCHED_RULES[0]])]

    with caplog.at_level(logging.WARNING):
        res = await restore(pg_anon_runner, db_params, target_dbs[0], inherits_full_dump, options)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert unmatched_lines(caplog) == [expected_line(kind, rule, "the dump")]


@pytest.mark.parametrize("kind", LIST_OPTIONS)
async def test_dump_and_restore_do_not_warn_about_matching_rules(
    kind, inherits_db, inherits_full_dump, target_dbs, db_params, pg_anon_runner, caplog
):
    options = [write_list("matching", kind, MATCHED_RULES)]

    with caplog.at_level(logging.WARNING):
        _, res = await dump(pg_anon_runner, db_params, inherits_db, f"unmatched_ok_dump_{kind}", options)
        assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
        res = await restore(pg_anon_runner, db_params, target_dbs[0], inherits_full_dump, options)
        assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"

    assert unmatched_lines(caplog) == []


@pytest.mark.parametrize("kind", LIST_OPTIONS)
async def test_dump_summarizes_many_rules_without_tables(kind, inherits_db, db_params, pg_anon_runner, caplog):
    rules = [{"schema": "ilist", "table": f"missing_{number:02d}"} for number in range(8)]
    options = [write_list("many_dump", kind, [*rules, MATCHED_RULES[0]])]

    with caplog.at_level(logging.WARNING):
        _, res = await dump(pg_anon_runner, db_params, inherits_db, f"unmatched_many_dump_{kind}", options)

    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    assert unmatched_lines(caplog) == [
        f"{LIST_TITLES[kind]}: 8 rules match no table in the database: "
        f"{json.dumps(rules[0])}, {json.dumps(rules[1])}, and 6 more."
    ]


@pytest.mark.parametrize("kind", LIST_OPTIONS)
async def test_restore_summarizes_many_rules_without_tables(
    kind, inherits_full_dump, target_dbs, db_params, pg_anon_runner, caplog
):
    rules = [{"schema": "ilist", "table": f"missing_{number:02d}"} for number in range(6)]
    options = [write_list("many_restore", kind, [*rules, MATCHED_RULES[0]])]

    with caplog.at_level(logging.WARNING):
        res = await restore(pg_anon_runner, db_params, target_dbs[0], inherits_full_dump, options)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert unmatched_lines(caplog) == [
        f"{LIST_TITLES[kind]}: 6 rules match no table in the dump: "
        f"{json.dumps(rules[0])}, {json.dumps(rules[1])}, and 4 more."
    ]


@pytest.mark.parametrize("mode", ["sync-struct-dump", "sync-data-dump"])
async def test_sync_dump_modes_warn_about_a_rule_without_tables(mode, inherits_db, db_params, pg_anon_runner, caplog):
    rule = UNMATCHED_RULES["name_name"]
    args = [
        f"--prepared-sens-dict-file={SUITE / 'input_dict' / 'empty.py'}",
        f"--output-dir={output_path(f'unmatched_{mode}')}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        write_list(mode, "exclude", [rule, MATCHED_RULES[0]]),
    ]

    with caplog.at_level(logging.WARNING):
        res = await pg_anon_runner.run(mode, inherits_db, args)

    assert res.result_code == ResultCode.DONE, f"{mode} failed: {res.error_message}"
    assert unmatched_lines(caplog) == [expected_line("exclude", rule, "the database")]


async def test_sync_struct_restore_warns_about_a_rule_without_tables(
    inherits_full_dump, target_dbs, db_params, pg_anon_runner, caplog
):
    rule = UNMATCHED_RULES["name_name"]
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={inherits_full_dump}",
        write_list("sync_struct_restore", "exclude", [rule, MATCHED_RULES[0]]),
    ]

    with caplog.at_level(logging.WARNING):
        res = await pg_anon_runner.run("sync-struct-restore", target_dbs[0], args)

    assert res.result_code == ResultCode.DONE, f"sync-struct-restore failed: {res.error_message}"
    assert unmatched_lines(caplog) == [expected_line("exclude", rule, "the dump")]
