"""Scan of field values read in portions: priority of rules, n_count over the whole field, per-field functions."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.enums import ResultCode
from pg_anon.common.errors import ErrorCode

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = "scan_portions"
FA = "'FA'"
FB = "'FB'"
FC = "'FC'"

# full mode with a portion of one value, of two values and of all values; partial mode reads the whole table
FULL_1 = ("--scan-mode=full", "--scan-partial-rows=1")
FULL_2 = ("--scan-mode=full", "--scan-partial-rows=2")
FULL_5000 = ("--scan-mode=full", "--scan-partial-rows=5000")
PARTIAL = ("--scan-mode=partial", "--scan-partial-rows=5000")


def regex(*rules: tuple[str, str]) -> dict:
    return {"data_regex": {"rules": [{"rule": rule, "anon_func": function} for rule, function in rules]}}


def scan_func(name: str, function: str, n_count: int | None = None) -> dict:
    rule = {"scan_func": f"{SCHEMA}.{name}", "anon_func": function}
    if n_count is not None:
        rule["n_count"] = n_count
    return rule


def per_field_func(name: str, function: str) -> dict:
    return {"scan_func_per_field": f"{SCHEMA}.{name}", "anon_func": function}


def data_func(text: list[dict] | None = None, anyelement: list[dict] | None = None) -> dict:
    rules = {}
    if text:
        rules["text"] = text
    if anyelement:
        rules["anyelement"] = anyelement
    return {"data_func": rules}


def condition(table: str, sql_condition: str) -> dict:
    return {"data_sql_condition": [{"schema": SCHEMA, "table": table, "sql_condition": sql_condition}]}


def include(*fields: str) -> dict:
    """include_rules for fields given as "table.column"."""
    by_table: dict[str, list[str]] = {}
    for item in fields:
        table, column = item.split(".")
        by_table.setdefault(table, []).append(column)
    return {
        "include_rules": [{"schema": SCHEMA, "table": table, "fields": columns} for table, columns in by_table.items()]
    }


def found(table: str, column: str, function: str) -> dict:
    return {f"{SCHEMA}.{table}": {column: function}}


class Scan:
    def __init__(self, result, sens_file: Path, no_sens_file: Path) -> None:
        self.result = result
        self.sens_file = sens_file
        self.no_sens_file = no_sens_file
        self.sens = json.loads(sens_file.read_text()) if sens_file.exists() else None
        self.no_sens = json.loads(no_sens_file.read_text()) if no_sens_file.exists() else None

    def rules(self) -> dict[str, dict]:
        """Fields of the sens dict in the schema of this suite, as {"schema.table": fields}."""
        return {
            f"{item['schema']}.{item['table']}": item["fields"]
            for item in self.sens["dictionary"]
            if item.get("schema") == SCHEMA
        }

    def no_sens_fields(self) -> dict[str, list[str]]:
        return {
            f"{item['schema']}.{item['table']}": item["fields"]
            for item in self.no_sens["no_sens_dictionary"]
            if item["schema"] == SCHEMA
        }


@pytest.fixture
def scan(source_db, db_params, pg_anon_runner, tmp_path):
    counter = iter(range(1000))

    async def _scan(meta: dict, mode: tuple[str, ...], connections: int | None = None) -> Scan:
        number = next(counter)
        meta_file = tmp_path / f"meta_{number}.py"
        meta_file.write_text(repr({"sens_pg_types": ["text"], **meta}))
        sens_file = tmp_path / f"sens_{number}.json"
        no_sens_file = tmp_path / f"no_sens_{number}.json"
        args = [
            *mode,
            f"--meta-dict-file={meta_file}",
            f"--output-sens-dict-file={sens_file}",
            f"--output-no-sens-dict-file={no_sens_file}",
            f"--db-connections-per-process={connections or db_params.db_connections_per_process}",
        ]
        result = await pg_anon_runner.run("create-dict", source_db, args)
        return Scan(result, sens_file, no_sens_file)

    return _scan


# --------------------------------------------------------------------------
# The function of the field comes from the highest rule that matched
# --------------------------------------------------------------------------

REGEX_A_B = regex(("^A:", FA), ("^B:", FB))
FUNC_A_REGEX_B = {**data_func(text=[scan_func("has_a", FA)]), **regex(("^B:", FB))}


@pytest.mark.parametrize(
    ("field", "meta", "mode", "expected"),
    [
        pytest.param("fields.two_regex", REGEX_A_B, FULL_1, FA, id="regex-above"),
        pytest.param("fields.two_sections", FUNC_A_REGEX_B, FULL_1, FA, id="data-func-above-regex"),
        pytest.param("fields.two_sections", FUNC_A_REGEX_B, PARTIAL, FA, id="data-func-above-regex-partial"),
        pytest.param(
            "fields.const_word",
            {"data_const": {"constants": [{"rule": "alpha", "anon_func": FA}]}, **regex(("^B:", FB))},
            FULL_1,
            FA,
            id="constant-above-regex",
        ),
        pytest.param(
            "fields.const_part",
            {"data_const": {"partial_constants": [{"rule": "alph", "anon_func": FA}]}, **regex(("^B:", FB))},
            FULL_1,
            FA,
            id="partial-constant-above-regex",
        ),
        pytest.param(
            "fields.type_first",
            data_func(text=[scan_func("has_a", FA)], anyelement=[scan_func("has_b", FB)]),
            FULL_1,
            FA,
            id="type-rule-above-anyelement",
        ),
        pytest.param(
            "cond.val",
            {**condition("cond", "WHERE kind = 'keep'"), **regex(("^C:", FC), ("^A:", FA), ("^B:", FB))},
            FULL_1,
            FA,
            id="rows-out-of-condition-are-not-read",
        ),
    ],
)
async def test_scan_takes_function_of_highest_rule_matched_in_any_portion(scan, field, meta, mode, expected):
    result = await scan({**include(field), **meta}, mode)

    assert result.result.result_code == ResultCode.DONE
    table, column = field.split(".")
    assert result.rules() == found(table, column, expected)


# --------------------------------------------------------------------------
# n_count is counted over all unique values of the field
# --------------------------------------------------------------------------

HAS_A_3 = data_func(text=[scan_func("has_a", FA, n_count=3)])
HAS_A_2_THEN_B = data_func(text=[scan_func("has_a", FA, n_count=2), scan_func("has_b", FB)])
RETURNS_NULL = data_func(text=[scan_func("returns_null", FA)])


@pytest.mark.parametrize(
    ("fields", "meta", "mode", "expected"),
    [
        pytest.param(["fields.three_hits"], HAS_A_3, FULL_1, found("fields", "three_hits", FA), id="three-portions"),
        pytest.param(["fields.two_hits"], HAS_A_3, FULL_1, {}, id="below-threshold"),
        pytest.param(["fields.two_hits"], HAS_A_3, PARTIAL, {}, id="below-threshold-partial"),
        pytest.param(["fields.one_value_three_rows"], HAS_A_3, FULL_1, {}, id="one-value-in-three-rows"),
        pytest.param(["fields.one_value_three_rows"], HAS_A_3, PARTIAL, {}, id="one-value-in-three-rows-partial"),
        pytest.param(
            ["fields.upper_counts"],
            HAS_A_2_THEN_B,
            FULL_1,
            found("fields", "upper_counts", FA),
            id="upper-rule-counts-over-field",
        ),
        pytest.param(["fields.noise_only", "fields.all_null"], RETURNS_NULL, FULL_1, {}, id="null-from-function"),
        pytest.param(
            ["fields.noise_only", "fields.all_null"], RETURNS_NULL, PARTIAL, {}, id="null-from-function-partial"
        ),
    ],
)
async def test_scan_counts_n_count_over_all_unique_values_of_field(scan, fields, meta, mode, expected):
    result = await scan({**include(*fields), **meta}, mode)

    assert result.result.result_code == ResultCode.DONE
    assert result.rules() == expected
    no_sens = {field for table, columns in result.no_sens_fields().items() for field in columns}
    found_fields = {field for columns in expected.values() for field in columns}
    assert no_sens == {field.split(".")[1] for field in fields} - found_fields


# --------------------------------------------------------------------------
# The result of the full scan does not depend on the portions
# --------------------------------------------------------------------------

ALL_RULES = {
    **include(
        "fields.two_regex",
        "fields.two_sections",
        "fields.const_word",
        "fields.const_part",
        "fields.type_first",
        "fields.three_hits",
        "fields.two_hits",
        "fields.one_value_three_rows",
        "fields.upper_counts",
        "fields.noise_only",
        "fields.all_null",
        "cond.val",
    ),
    **data_func(text=[scan_func("has_a", FA, n_count=2)]),
    "data_const": {
        "constants": [{"rule": "alpha", "anon_func": FA}],
        "partial_constants": [{"rule": "alph", "anon_func": FA}],
    },
    **regex(("^C:", FC), ("^A:", FA), ("^B:", FB)),
    **condition("cond", "WHERE kind = 'keep'"),
}


async def test_full_scan_gives_same_dicts_for_any_portion_size_connections_and_rerun(scan):
    runs = [
        await scan(ALL_RULES, FULL_1),
        await scan(ALL_RULES, FULL_2),
        await scan(ALL_RULES, FULL_5000),
        await scan(ALL_RULES, FULL_1, connections=1),
        await scan(ALL_RULES, FULL_1),
    ]

    assert [run.result.result_code for run in runs] == [ResultCode.DONE] * len(runs)
    assert len({run.sens_file.read_bytes() for run in runs}) == 1
    assert len({run.no_sens_file.read_bytes() for run in runs}) == 1


# --------------------------------------------------------------------------
# Partitioned table: the field of the root is one field
# --------------------------------------------------------------------------

HAS_A_2 = data_func(text=[scan_func("has_a", FA, n_count=2)])


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("field", "meta", "mode", "expected", "expected_no_sens"),
    [
        pytest.param(
            "parts.prio",
            regex(("^A:", FA), ("^C:", FC), ("^B:", FB)),
            FULL_1,
            found("parts", "prio", FA),
            {},
            id="highest-rule-of-all-partitions",
        ),
        pytest.param("parts.sum_two", HAS_A_2, FULL_1, found("parts", "sum_two", FA), {}, id="n-count-over-partitions"),
        pytest.param(
            "parts.sum_two", HAS_A_2, PARTIAL, found("parts", "sum_two", FA), {}, id="n-count-over-partitions-partial"
        ),
        pytest.param(
            "parts.same_value",
            HAS_A_2,
            FULL_1,
            {},
            {f"{SCHEMA}.parts": ["same_value"]},
            id="same-value-in-two-partitions-counts-once",
        ),
        pytest.param(
            "parts.sum_beats",
            data_func(text=[scan_func("has_a", FA, n_count=2), scan_func("has_b", FB)]),
            FULL_1,
            found("parts", "sum_beats", FA),
            {},
            id="upper-rule-by-sum-beats-lower-rule-of-one-partition",
        ),
        pytest.param(
            "parts.sum_two",
            {**HAS_A_2, "skip_rules": [{"schema": SCHEMA, "table": "parts_2"}]},
            FULL_1,
            {},
            {f"{SCHEMA}.parts": ["sum_two"]},
            id="skipped-partition-is-not-counted",
        ),
        pytest.param(
            "half_empty.val", regex(("^A:", FA)), FULL_1, found("half_empty", "val", FA), {}, id="empty-partition"
        ),
    ],
)
async def test_scan_takes_partitions_of_root_as_one_field(scan, field, meta, mode, expected, expected_no_sens):
    result = await scan({**include(field), **meta}, mode)

    assert result.result.result_code == ResultCode.DONE
    assert result.rules() == expected
    assert result.no_sens_fields() == expected_no_sens


# --------------------------------------------------------------------------
# scan_func_per_field: one call per field, before the data is read
# --------------------------------------------------------------------------

ONCE_PER_SESSION = data_func(text=[per_field_func("once_per_session", FA)])


@pytest.mark.parametrize(
    ("meta", "mode", "expected"),
    [
        pytest.param({**include("fields.wide"), **ONCE_PER_SESSION}, FULL_1, {}, id="one-call-on-many-portions"),
        pytest.param({**include("fields.wide"), **ONCE_PER_SESSION}, PARTIAL, {}, id="one-call-partial"),
        pytest.param(
            {**include("per_field.marked"), **ONCE_PER_SESSION, **condition("per_field", "WHERE 1/0 = 1")},
            FULL_1,
            found("per_field", "marked", FA),
            id="matched-field-is-not-read",
        ),
        pytest.param(
            {**include("per_field.marked"), **ONCE_PER_SESSION, **condition("per_field", "WHERE 1/0 = 1")},
            PARTIAL,
            found("per_field", "marked", FA),
            id="matched-field-is-not-read-partial",
        ),
        pytest.param(
            {**include("fields.empty_marked"), **ONCE_PER_SESSION},
            FULL_1,
            found("fields", "empty_marked", FA),
            id="field-without-values",
        ),
        pytest.param(
            {**include("fields.empty_marked"), **ONCE_PER_SESSION},
            PARTIAL,
            found("fields", "empty_marked", FA),
            id="field-without-values-partial",
        ),
        pytest.param(
            {**include("fields.int_empty"), **data_func(anyelement=[per_field_func("fail_per_field", FA)])},
            FULL_1,
            {},
            id="field-of-other-type-is-not-checked",
        ),
        pytest.param(
            {**include("fields.int_empty"), **data_func(anyelement=[per_field_func("fail_per_field", FA)])},
            PARTIAL,
            {},
            id="field-of-other-type-is-not-checked-partial",
        ),
    ],
)
async def test_scan_calls_per_field_function_once_before_reading_data(scan, meta, mode, expected):
    result = await scan(meta, mode)

    assert result.result.result_code == ResultCode.DONE
    assert result.rules() == expected


# --------------------------------------------------------------------------
# Functions of rules below the matched rule are not called
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "meta", "mode", "expected"),
    [
        pytest.param(
            "fields.upper_first",
            data_func(text=[scan_func("has_a", FA), scan_func("fail_on_call", FB)]),
            FULL_5000,
            FA,
            id="lower-function-is-not-called",
        ),
        pytest.param(
            "fields.upper_first",
            data_func(text=[scan_func("has_a", FA), scan_func("fail_on_call", FB)]),
            PARTIAL,
            FA,
            id="lower-function-is-not-called-partial",
        ),
        pytest.param(
            "fields.after_b",
            data_func(text=[scan_func("has_a", FA), scan_func("has_b_marked", FB), scan_func("fail_after_b", FC)]),
            FULL_1,
            FB,
            id="lower-function-is-not-called-on-next-portions",
        ),
        pytest.param(
            "fields.same_func",
            data_func(text=[scan_func("fail_after_b", FB), scan_func("has_b_marked", FB)]),
            FULL_1,
            FB,
            id="scan-stops-when-no-rule-above-has-other-function",
        ),
    ],
)
async def test_scan_does_not_call_functions_below_matched_rule(scan, field, meta, mode, expected):
    result = await scan({**include(field), **meta}, mode)

    assert result.result.result_code == ResultCode.DONE
    table, column = field.split(".")
    assert result.rules() == found(table, column, expected)


# --------------------------------------------------------------------------
# An error of a check function stops the scan
# --------------------------------------------------------------------------


@pytest.mark.parametrize("mode", [pytest.param(FULL_1, id="full"), pytest.param(PARTIAL, id="partial")])
async def test_scan_fails_when_check_function_fails(scan, mode):
    result = await scan({**include("fields.noise_only"), **data_func(text=[scan_func("fail_on_call", FA)])}, mode)

    assert result.result.result_code == ResultCode.FAIL
    assert result.result.exception.code == ErrorCode.SCAN_FIELD_ERROR
