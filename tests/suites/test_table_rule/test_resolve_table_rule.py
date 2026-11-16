from __future__ import annotations

import pytest

from pg_anon.common.utils import resolve_table_rule

E = 'md5("email")'
N = 'md5("note")'

ROOT = ("sp", "orders")
MIDDLE = ("sp", "orders_2024")
RU = ("sp", "orders_2024_ru")
DEF = ("sp", "orders_2024_def")
ARCHIVE = ("sp_archive", "orders_2023")

ANCESTORS = {
    MIDDLE: [ROOT],
    RU: [MIDDLE, ROOT],
    DEF: [MIDDLE, ROOT],
    ARCHIVE: [ROOT],
}


def exact(table: tuple[str, str], **rule: object) -> dict:
    return {"schema": table[0], "table": table[1], **rule}


def mask(schema: str, table_mask: str, **rule: object) -> dict:
    return {"schema": schema, "table_mask": table_mask, **rule}


def resolve(table, rules, excludes, ancestors=ANCESTORS):
    return resolve_table_rule(table, rules, excludes, ancestors)


def fields_of(table, rules, excludes, ancestors=ANCESTORS):
    """Return the fields of the resolved rule, or "excluded" when the table is dumped without data."""
    rule, excluded = resolve(table, rules, excludes, ancestors)
    if excluded:
        assert rule is None
        return "excluded"
    return None if rule is None else rule["fields"]


@pytest.mark.parametrize(
    ("rules", "excludes", "expected"),
    [
        pytest.param(
            [exact(ROOT, fields={"email": E, "note": N}), exact(MIDDLE, fields={"note": "'Y'"})],
            [],
            {RU: {"email": E, "note": "'Y'"}, ARCHIVE: {"email": E, "note": N}},
            id="closest-ancestor-wins-per-field",
        ),
        pytest.param(
            [mask("sp", "^orders$", fields={"email": E})],
            [],
            {RU: {"email": E}, ARCHIVE: {"email": E}},
            id="mask-on-root-covers-partitions-in-other-schema",
        ),
        pytest.param(
            [
                mask("sp_archive", "*", fields={"note": "'A'"}),
                mask("sp_archive", "2023", fields={"note": "'B'"}),
            ],
            [],
            {ARCHIVE: {"note": "'B'"}},
            id="last-matching-mask-of-one-level",
        ),
        pytest.param([], [], {RU: None, ROOT: None}, id="no-rules"),
    ],
)
def test_partition_takes_rule_by_precision_then_level(rules, excludes, expected):
    assert {table: fields_of(table, rules, excludes) for table in expected} == expected


def test_root_takes_only_its_own_rule():
    rules = [exact(ROOT, fields={"email": E}), exact(RU, fields={"note": "'RU'"})]
    assert fields_of(ROOT, rules, []) == {"email": E}


def test_sql_condition_comes_from_the_first_rule_that_has_it():
    root = exact(ROOT, fields={"email": E}, sql_condition="region = 'ru'")
    rule, _ = resolve(RU, [root, exact(RU, fields={"note": "'RU'"})], [])
    assert rule["sql_condition"] == "region = 'ru'"

    rule, _ = resolve(RU, [root, exact(RU, fields={"note": "'RU'"}, sql_condition="true")], [])
    assert rule["sql_condition"] == "true"


def test_empty_sql_condition_of_partition_keeps_condition_of_root():
    root = exact(ROOT, fields={"email": E}, sql_condition="region = 'ru'")
    rule, _ = resolve(RU, [root, exact(RU, fields={"note": "'RU'"}, sql_condition=None)], [])
    assert rule["sql_condition"] == "region = 'ru'"


def test_resolved_rule_does_not_change_the_dictionary():
    root = exact(ROOT, fields={"email": E})
    own = exact(RU, fields={"note": "'RU'"})
    resolve(RU, [root, own], [])
    assert root == exact(ROOT, fields={"email": E})
    assert own == exact(RU, fields={"note": "'RU'"})
