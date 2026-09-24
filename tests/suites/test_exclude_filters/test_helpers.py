from __future__ import annotations

import pytest

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME
from pg_anon.common.utils import format_names_summary, parse_pattern_list, resolve_schemas

SCHEMAS = [ANON_UTILS_DB_SCHEMA_NAME, "audit", "audit_old", "public", "sales"]


@pytest.mark.parametrize(
    ("names", "expected"),
    [
        (["a"], "1 table (a)"),
        (["b", "a"], "2 tables (a, b)"),
        (["c", "a", "b"], "3 tables (a, b, c)"),
        (["d", "c", "a", "b"], "4 tables (a, b, and 2 more)"),
    ],
)
def test_one_hidden_name_is_shown_instead_of_and_1_more(names, expected):
    assert format_names_summary(names, "table") == expected


def test_summary_without_names():
    assert format_names_summary(["a", "b", "c"], "table", limit=0) == "3 tables"


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({}, SCHEMAS),
        ({"names": ["sales"]}, [ANON_UTILS_DB_SCHEMA_NAME, "sales"]),
        ({"names": ["sales", "public"]}, [ANON_UTILS_DB_SCHEMA_NAME, "public", "sales"]),
        ({"exclude_names": ["audit"]}, [ANON_UTILS_DB_SCHEMA_NAME, "audit_old", "public", "sales"]),
        ({"masks": ["^a"]}, [ANON_UTILS_DB_SCHEMA_NAME, "audit", "audit_old"]),
        ({"exclude_masks": ["^audit"]}, [ANON_UTILS_DB_SCHEMA_NAME, "public", "sales"]),
        ({"names": ["sales"], "masks": ["^audit$"]}, [ANON_UTILS_DB_SCHEMA_NAME, "audit", "sales"]),
        ({"masks": ["^a"], "exclude_names": ["audit_old"]}, [ANON_UTILS_DB_SCHEMA_NAME, "audit"]),
        ({"names": ["audit"], "exclude_masks": ["^audit"]}, [ANON_UTILS_DB_SCHEMA_NAME]),
        ({"exclude_masks": ["*"]}, [ANON_UTILS_DB_SCHEMA_NAME]),
        ({"names": ["no_such"]}, [ANON_UTILS_DB_SCHEMA_NAME]),
    ],
)
def test_resolve_schemas(filters, expected):
    assert resolve_schemas(SCHEMAS, **filters) == expected


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({"exclude_names": ["audit"]}, ["audit"]),
        ({"exclude_names": ["AUDIT"]}, []),
        ({"exclude_masks": ["audit"]}, ["audit", "audit_old"]),
        ({"exclude_masks": ["^audit$"]}, ["audit"]),
    ],
)
def test_name_is_exact_while_mask_searches(filters, expected):
    """A name matches itself only, a mask is a regular expression, as everywhere in pg_anon."""
    kept = resolve_schemas(SCHEMAS, **filters)
    assert [schema for schema in SCHEMAS if schema not in kept] == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("a,b", ["a", "b"]),
        ("tmp_{1,2},audit", ["tmp_{1,2}", "audit"]),
        ("[a,b]x,(c,d)", ["[a,b]x", "(c,d)"]),
        ("a,,b,", ["a", "b"]),
    ],
)
def test_pattern_list_keeps_commas_inside_brackets(value, expected):
    assert parse_pattern_list(value) == expected
