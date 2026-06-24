from __future__ import annotations

import pytest

from pg_anon.common.utils import normalize_data_type


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("bank.passport", "bank.passport"),
        ("bank.account_no", "bank.account_no"),
        ("schema_x.my_type", "schema_x.my_type"),
        ("BANK.Passport", "bank.passport"),
        ("  bank.passport  ", "bank.passport"),
    ],
)
def test_schema_qualified_type_is_not_mangled(raw: str, expected: str) -> None:
    assert normalize_data_type(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("text", "text"),
        ("varchar", "character varying"),
        ("varchar(20)", "character varying(20)"),
        ("numeric(10,2)", "numeric(10,2)"),
        ("int4", "integer"),
        ("timestamptz", "timestamp with time zone"),
        ("timestamp", "timestamp without time zone"),
    ],
)
def test_builtin_type_normalization_unchanged(raw: str, expected: str) -> None:
    assert normalize_data_type(raw) == expected
