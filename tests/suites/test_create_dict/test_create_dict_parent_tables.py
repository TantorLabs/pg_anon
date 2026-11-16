"""Scan of partitioned tables and INHERITS tables: what is read, where the findings go, how prepared dicts carry over."""

from __future__ import annotations

import ast
import json
import logging
from pathlib import Path

import pytest

from tests.infrastructure.assertions import masking, NO_ROWS

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode
from pg_anon.modes.create_dict import own_masking_line

E = 'md5("email")'
N = 'md5("note")'
I = 'md5("info")'  # noqa: E741
F_CONST = "'F_CONST'"
F_REGEX = "'F_REGEX'"

SP = ("sp", "sp_archive")
CRM = ("sp_crm",)
ORDERS = {"schema": "sp", "table": "orders"}
RU = {"schema": "sp", "table": "orders_2024_ru"}
ARCHIVE = {"schema": "sp_archive", "table": "orders_2023"}
PARTITIONS = ("sp.orders_2024_ru", "sp.orders_2024_def", "sp_archive.orders_2023")


def rule(table: dict, **fields: str) -> dict:
    return {**table, "fields": fields}


class Scan:
    """One run of create-dict with the base meta dict and the given changes to it."""

    def __init__(self, result, messages: list[logging.LogRecord], sens: dict | None, no_sens: dict | None) -> None:
        self.result = result
        self.sens = sens
        self.no_sens = no_sens
        self.records = messages

    def rules(self, schemas: tuple[str, ...]) -> dict[str, dict]:
        """Exact rules of tables in the schemas, as {"schema.table": fields}."""
        return {
            f"{item['schema']}.{item['table']}": item["fields"]
            for item in self.sens["dictionary"]
            if item.get("schema") in schemas and "table" in item
        }

    def no_sens_fields(self, schemas: tuple[str, ...]) -> dict[str, list[str]]:
        return {
            f"{item['schema']}.{item['table']}": item["fields"]
            for item in self.no_sens["no_sens_dictionary"]
            if item["schema"] in schemas
        }

    def lines(self, text: str, level: int = logging.INFO) -> list[str]:
        return [r.getMessage() for r in self.records if r.levelno == level and text in r.getMessage()]


@pytest.fixture
def scan(source_db, db_params, pg_anon_runner, tmp_path, caplog):
    counter = iter(range(1000))

    async def _scan(
        meta: dict | None = None,
        *,
        prepared: dict | str | None = None,
        prepared_no_sens: dict | str | None = None,
        no_sens: bool = False,
        extra: list[str] | None = None,
    ) -> Scan:
        number = next(counter)
        base = ast.literal_eval(Path(input_dict("meta_parent_tables.py")).read_text())
        meta_file = tmp_path / f"meta_{number}.py"
        meta_file.write_text(repr({**base, **(meta or {})}))
        sens_out = tmp_path / f"sens_{number}.json"
        no_sens_out = tmp_path / f"no_sens_{number}.json"
        args = [
            "--scan-mode=full",
            f"--meta-dict-file={meta_file}",
            f"--output-sens-dict-file={sens_out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
        ]
        if no_sens:
            args.append(f"--output-no-sens-dict-file={no_sens_out}")
        args.extend(extra or [])
        for option, content in (("prepared-sens", prepared), ("prepared-no-sens", prepared_no_sens)):
            if content is None:
                continue
            path = content if isinstance(content, str) else tmp_path / f"{option}_{number}.py"
            if not isinstance(content, str):
                path.write_text(repr(content))
            args.append(f"--{option}-dict-file={path}")

        caplog.clear()
        with caplog.at_level(logging.INFO):
            res = await pg_anon_runner.run("create-dict", source_db, args)
        sens = json.loads(sens_out.read_text()) if sens_out.exists() else None
        no_sens_dict = json.loads(no_sens_out.read_text()) if no_sens_out.exists() else None
        return Scan(res, list(caplog.records), sens, no_sens_dict)

    return _scan


@pytest.fixture(scope="module")
def base_scan_cache() -> dict:
    return {}


@pytest.fixture
async def base_scan(scan, base_scan_cache) -> Scan:
    """Scan with the base meta dict and the no-sens output; made once for the module."""
    if "scan" not in base_scan_cache:
        result = await scan(no_sens=True)
        assert result.result.result_code == ResultCode.DONE
        base_scan_cache["scan"] = result
    return base_scan_cache["scan"]


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_cd_tgt_{request.node.name.replace('[', '_').replace(']', '')}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


def task_rules(sens: dict) -> dict:
    """Keep the rules of the schemas of this suite: the base meta dict breaks checks of other tables."""
    return {**sens, "dictionary": [item for item in sens["dictionary"] if item.get("schema") in (*SP, *CRM)]}


async def dump_and_restore(
    pg_anon_runner, db_params, source_db, target_db, sens_dict: dict | str, name: str, *, drop_db: bool = False
):
    if isinstance(sens_dict, dict):
        dict_file = Path(output_path(name)).parent / f"{name}_sens_dict.py"
        dict_file.write_text(repr(task_rules(sens_dict)))
        sens_dict = str(dict_file)
    out = output_path(name)
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={sens_dict}",
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
            *(["--drop-db"] if drop_db else []),
        ],
    )
    assert res.result_code == ResultCode.DONE


# --------------------------------------------------------------------------
# What the scan reads and where it writes the findings
# --------------------------------------------------------------------------


@pytest.mark.min_pg(100000)
async def test_scan_writes_findings_of_partitions_to_the_top_partitioned_table(base_scan):
    assert base_scan.rules(SP) == {
        "sp.orders": {"email": E, "note": N, "tag": F_CONST},
        "sp.empty_root": {"email": E},
    }


async def test_scan_reads_inherits_parent_only_with_its_own_rows(base_scan):
    assert base_scan.rules(CRM) == {"sp_crm.person": {"info": I}, "sp_crm.partner": {"info": I}}


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("meta", "expected"),
    [
        pytest.param(
            {"include_rules": [ARCHIVE]},
            {"sp.orders": {"email": E, "note": N, "tag": F_REGEX}},
            id="root-out-of-scope-and-in-other-schema",
        ),
        pytest.param(
            {"skip_rules": [{"schema": "sp_archive"}]},
            {"sp.orders": {"email": E, "tag": F_CONST}, "sp.empty_root": {"email": E}},
            id="skipped-partition-is-not-read-through-root",
        ),
        pytest.param(
            {"include_rules": [{"schema": "sp", "table_mask": "_ru$"}]},
            {"sp.orders": {"email": E, "tag": F_CONST}},
            id="include-mask-on-partition",
        ),
        pytest.param(
            {"include_rules": [ORDERS]},
            {"sp.orders": {"email": E, "note": N, "tag": F_CONST}},
            id="include-root-covers-partitions-in-any-schema",
        ),
        pytest.param(
            {"include_rules": [ORDERS], "skip_rules": [{"schema": "sp", "table_mask": "^orders_2024"}]},
            {"sp.orders": {"email": E, "note": N, "tag": F_REGEX}},
            id="skip-middle-partition-by-mask",
        ),
        pytest.param(
            {"include_rules": [{"schema": "sp", "table_mask": "^orders$"}], "skip_rules": [RU]},
            {"sp.orders": {"email": E, "note": N, "tag": F_REGEX}},
            id="skip-one-partition-under-included-root-mask",
        ),
        pytest.param(
            {"include_rules": [{"schema": "sp"}]},
            {"sp.orders": {"email": E, "note": N, "tag": F_CONST}, "sp.empty_root": {"email": E}},
            id="include-schema-of-root-covers-partition-in-other-schema",
        ),
        pytest.param(
            {"include_rules": [{**ORDERS, "fields": ["email"]}]},
            {"sp.orders": {"email": E}},
            id="include-fields-of-root",
        ),
        pytest.param(
            {"skip_rules": [{**ORDERS, "fields": ["email"]}]},
            {"sp.orders": {"note": N, "tag": F_CONST}, "sp.empty_root": {"email": E}},
            id="skip-fields-of-root",
        ),
        pytest.param(
            {
                "data_sql_condition": [
                    {**ORDERS, "sql_condition": "created >= '2024-01-01'"},
                    {**ARCHIVE, "sql_condition": "true"},
                ]
            },
            {"sp.orders": {"email": E, "note": N, "tag": F_CONST}, "sp.empty_root": {"email": E}},
            id="own-condition-beats-condition-of-root",
        ),
        pytest.param(
            {
                "data_sql_condition": [
                    {"schema": "sp", "table_mask": "^orders$", "sql_condition": "created >= '2024-01-01'"}
                ]
            },
            {"sp.orders": {"email": E, "tag": F_CONST}, "sp.empty_root": {"email": E}},
            id="condition-of-root-by-mask",
        ),
        pytest.param(
            {
                "data_sql_condition": [
                    {"schema": "sp_archive", "table_mask": "*", "sql_condition": "false"},
                    {"schema": "sp_archive", "table_mask": "2023", "sql_condition": "true"},
                ]
            },
            {"sp.orders": {"email": E, "note": N, "tag": F_CONST}, "sp.empty_root": {"email": E}},
            id="last-of-two-masks",
        ),
    ],
)
async def test_scan_reads_partitions_by_rules_of_their_partitioned_tables(scan, meta, expected):
    result = await scan(meta)
    assert result.result.result_code == ResultCode.DONE
    assert result.rules(SP) == expected


@pytest.mark.parametrize(
    ("meta", "expected", "expected_no_sens"),
    [
        pytest.param(
            {"include_rules": [{"schema": "sp_crm", "table": "contact"}]},
            {},
            {"sp_crm.contact": ["name", "info"]},
            id="include-parent",
        ),
        pytest.param(
            {"skip_rules": [{"schema": "sp_crm", "table": "contact"}]},
            {"sp_crm.person": {"info": I}, "sp_crm.partner": {"info": I}},
            None,
            id="skip-parent",
        ),
        pytest.param(
            {"include_rules": [{"schema": "sp_crm", "table": "contact", "fields": ["info"]}]},
            {},
            {"sp_crm.contact": ["info"]},
            id="include-fields-of-parent",
        ),
        pytest.param(
            {"skip_rules": [{"schema": "sp_crm", "table": "vendor"}]},
            {"sp_crm.person": {"info": I}, "sp_crm.partner": {"info": I}},
            None,
            id="skip-one-of-two-parents",
        ),
        pytest.param(
            {"data_sql_condition": [{"schema": "sp_crm", "table": "contact", "sql_condition": "false"}]},
            {"sp_crm.person": {"info": I}, "sp_crm.partner": {"info": I}},
            None,
            id="condition-of-parent",
        ),
    ],
)
async def test_scan_applies_rules_of_inherits_parent_only_to_the_parent(scan, meta, expected, expected_no_sens):
    result = await scan(meta, no_sens=True)
    assert result.result.result_code == ResultCode.DONE
    assert result.rules(CRM) == expected
    # the no-sens dict shows which fields were read: with "fields" only these, and nothing of the children
    if expected_no_sens is not None:
        assert result.no_sens_fields(CRM) == expected_no_sens


@pytest.mark.min_pg(100000)
async def test_dump_masks_partition_skipped_by_its_schema_with_rule_of_root(
    scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    result = await scan({"skip_rules": [{"schema": "sp_archive"}]})
    assert result.rules(SP)["sp.orders"] == {"email": E, "tag": F_CONST}

    await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, result.sens, "skipped_partition")
    assert await masking(db_manager, source_db, target_db, "sp_archive.orders_2023", ("email", "note", "tag")) == {
        "email": "md5",
        "note": "as is",
        "tag": "F_CONST",
    }


SKIP_LINE = "matches a partitioned table"


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("meta", "expected_line", "expected_rules"),
    [
        pytest.param(
            {"skip_rules": [{"schema": "sp"}]},
            'The rule {"schema": "sp"} matches a partitioned table: 1 partition is skipped too '
            "(sp_archive.orders_2023)",
            {},
            id="schema-of-root",
        ),
        pytest.param(
            {"skip_rules": [ORDERS]},
            'The rule {"schema": "sp", "table": "orders"} matches a partitioned table: 3 partitions are skipped too '
            "(sp_archive.orders_2023, sp.orders_2024_def, sp.orders_2024_ru)",
            {"sp.empty_root": {"email": E}},
            id="root",
        ),
        pytest.param(
            {"skip_rules": [{"schema": "sp", "table_mask": ".*"}]},
            'The rule {"schema": "sp", "table_mask": ".*"} matches a partitioned table: 1 partition is skipped too '
            "(sp_archive.orders_2023)",
            {},
            id="mask-of-all-tables-of-schema",
        ),
    ],
)
async def test_scan_skips_partitions_with_their_root_and_writes_one_line(scan, meta, expected_line, expected_rules):
    result = await scan(meta)
    assert result.result.result_code == ResultCode.DONE
    assert result.lines(SKIP_LINE) == [expected_line]
    assert result.rules(SP) == expected_rules
    assert result.rules(CRM) == {"sp_crm.person": {"info": I}, "sp_crm.partner": {"info": I}}


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("meta", "expected_line"),
    [
        pytest.param(
            {"include_rules": [{"schema": "sp", "table_mask": "_ru$"}], "skip_rules": [ORDERS]},
            'The rule {"schema": "sp", "table": "orders"} matches a partitioned table: 1 partition is skipped too '
            "(sp.orders_2024_ru)",
            id="skip-root-of-included-partition",
        ),
        pytest.param(
            {"include_rules": [{"schema": "sp_archive"}], "skip_rules": [{"schema": "sp"}]},
            'The rule {"schema": "sp"} matches a partitioned table: 1 partition is skipped too '
            "(sp_archive.orders_2023)",
            id="skip-wins-over-include",
        ),
    ],
)
async def test_scan_has_nothing_to_read_when_root_of_included_partitions_is_skipped(scan, meta, expected_line):
    result = await scan(meta)
    assert result.result.result_code == ResultCode.DONE
    assert result.lines("No objects for scan!", logging.WARNING)
    assert result.sens is None
    assert result.lines(SKIP_LINE) == [expected_line]


@pytest.mark.min_pg(100000)
async def test_scan_reads_partitions_by_condition_of_root_and_writes_one_line(scan):
    result = await scan({"data_sql_condition": [{**ORDERS, "sql_condition": "created >= '2024-01-01'"}]})
    assert result.result.result_code == ResultCode.DONE
    assert result.rules(SP) == {"sp.orders": {"email": E, "tag": F_CONST}, "sp.empty_root": {"email": E}}
    assert result.lines("data_sql_condition of") == [
        "data_sql_condition of sp.orders is used for its 3 partitions: "
        "sp_archive.orders_2023 (other schema), sp.orders_2024_def, sp.orders_2024_ru"
    ]


# --------------------------------------------------------------------------
# Choice of one function for a field of the partitioned table
# --------------------------------------------------------------------------


@pytest.mark.min_pg(100000)
async def test_scan_picks_function_of_earlier_section_and_warns_about_the_other(base_scan):
    assert base_scan.rules(SP)["sp.orders"]["tag"] == F_CONST
    warnings = base_scan.lines("sp.orders", logging.WARNING)
    assert len(warnings) == 1
    assert all(word in warnings[0] for word in ("tag", F_CONST, F_REGEX))


@pytest.mark.min_pg(100000)
async def test_scan_picks_function_of_rule_higher_in_the_section(scan):
    result = await scan(
        {
            "data_const": {"constants": []},
            "data_regex": {
                "rules": [
                    {"rule": r"^S-\d+$", "anon_func": "'R1'"},
                    {"rule": "secret", "anon_func": "'R2'"},
                ]
            },
        }
    )
    assert result.rules(SP)["sp.orders"]["tag"] == "'R1'"


@pytest.mark.min_pg(100000)
async def test_scan_does_not_warn_when_partitions_give_the_same_function(scan):
    result = await scan(
        {
            "data_const": {"constants": [{"rule": "secret", "anon_func": "'F'"}]},
            "data_regex": {"rules": [{"rule": r"^S-\d+$", "anon_func": "'F'"}]},
        }
    )
    assert result.rules(SP)["sp.orders"]["tag"] == "'F'"
    assert result.lines("sp.orders", logging.WARNING) == []


# --------------------------------------------------------------------------
# Key columns
# --------------------------------------------------------------------------

NUMBERS_META = {
    "sens_pg_types": ["text", "integer"],
    "data_regex": {
        "rules": [
            r"""^\+7 \d{3} \d{3}-\d{2}-\d{2}$""",
            {"rule": r"""^S-\d+$""", "anon_func": "'F_REGEX'"},
            {"rule": r"""^\d+$""", "anon_func": "'NUM'"},
        ]
    },
}


@pytest.mark.min_pg(100000)
async def test_scan_does_not_read_key_column_of_partitions(scan):
    result = await scan({**NUMBERS_META, "include_rules": [ORDERS]})
    assert result.rules(SP) == {"sp.orders": {"email": E, "note": N, "tag": F_CONST}}


async def test_scan_reads_columns_that_are_keys_only_in_inherits_parent(scan):
    # contact.id is filled by the sequence of contact, vendor.code is the primary key of vendor;
    # INHERITS children take neither the key nor the sequence link, so their columns hold plain data
    result = await scan({**NUMBERS_META, "include_rules": [{"schema": "sp_crm"}]})
    assert result.rules(CRM) == {
        "sp_crm.person": {"id": "'NUM'", "info": I},
        "sp_crm.company": {"id": "'NUM'", "inn": "'NUM'"},
        "sp_crm.partner": {"id": "'NUM'", "info": I, "code": "'NUM'"},
    }


# --------------------------------------------------------------------------
# Prepared sens dict
# --------------------------------------------------------------------------


@pytest.mark.min_pg(100000)
async def test_scan_keeps_prepared_function_of_root_for_all_partitions(
    scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    root_function = "'ROOT_' || md5(\"email\")"
    result = await scan(prepared={"dictionary": [rule(ORDERS, email=root_function)]})
    assert result.rules(SP)["sp.orders"]["email"] == root_function
    assert set(result.rules(SP)) == {"sp.orders", "sp.empty_root"}

    await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, result.sens, "prepared_root_function")
    for table in PARTITIONS:
        rows = await db_manager.fetch(target_db, f"SELECT email FROM {table}")
        assert rows and all(row["email"].startswith("ROOT_") for row in rows), table


async def test_scan_keeps_prepared_rule_of_skipped_table(scan):
    person = {"schema": "sp_crm", "table": "person"}
    result = await scan({"skip_rules": [person]}, prepared={"dictionary": [rule(person, name="'P'")]})
    assert result.rules(CRM)["sp_crm.person"] == {"name": "'P'"}


@pytest.mark.min_pg(100000)
async def test_scan_with_its_own_result_as_prepared_dict_gives_the_same_result(scan, base_scan, tmp_path):
    first = tmp_path / "first.json"
    first.write_text(json.dumps(base_scan.sens))
    result = await scan(prepared=str(first))
    assert result.sens == base_scan.sens


@pytest.mark.min_pg(100000)
async def test_scan_keeps_prepared_rule_of_partition_with_other_function(scan):
    result = await scan(prepared={"dictionary": [rule(RU, note="'RU'")]})
    rules = result.rules(SP)
    assert rules["sp.orders_2024_ru"] == {"note": "'RU'"}
    assert rules["sp.orders"] == {"email": E, "note": N, "tag": F_CONST}


# --------------------------------------------------------------------------
# Dictionary of version 1.12 with own rules on partitions
# --------------------------------------------------------------------------

DICT_1_12 = "sens_partitions_1_12.json"


@pytest.mark.min_pg(100000)
async def test_dict_of_version_1_12_gives_partitions_the_fields_of_root(
    source_db, target_db, db_manager, db_params, pg_anon_runner
):
    await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, input_dict(DICT_1_12), "dict_1_12")
    columns = ("email", "note", "tag")
    assert await masking(db_manager, source_db, target_db, "sp.orders_2024_def", columns) == {
        "email": "md5",
        "note": "md5",
        "tag": "F_CONST",
    }
    assert (await masking(db_manager, source_db, target_db, "sp_archive.orders_2023", columns))["tag"] == "F_REGEX"


@pytest.mark.min_pg(100000)
async def test_scan_cleans_fields_of_partitions_equal_to_root_and_dump_stays_the_same(
    scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    result = await scan(prepared=input_dict(DICT_1_12))
    assert result.rules(SP) == {
        "sp.orders": {"email": E, "note": N, "tag": F_CONST},
        "sp_archive.orders_2023": {"tag": F_REGEX},
        "sp.empty_root": {"email": E},
    }
    lines = result.lines("own masking")
    assert len(lines) == 1
    assert all(word in lines[0] for word in ("sp.orders", "1 partition", "tag", "sp_archive.orders_2023"))

    columns = ("email", "note", "tag")
    await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, result.sens, "dict_1_12_cleaned")
    cleaned = {table: await masking(db_manager, source_db, target_db, table, columns) for table in PARTITIONS}
    await dump_and_restore(
        pg_anon_runner, db_params, source_db, target_db, input_dict(DICT_1_12), "dict_1_12", drop_db=True
    )
    original = {table: await masking(db_manager, source_db, target_db, table, columns) for table in PARTITIONS}
    assert cleaned == original


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("meta", "prepared", "expected"),
    [
        pytest.param(
            {},
            [rule(ORDERS, email=E), {**rule(RU, email=E), "sql_condition": "region = 'ru'"}],
            {**RU, "fields": {"email": E}, "sql_condition": "region = 'ru'"},
            id="rule-without-other-fields-is-kept-whole",
        ),
        pytest.param(
            {},
            [rule(ORDERS, email=E), {**rule(RU, email=E, note="'RU'"), "sql_condition": "region = 'ru'"}],
            {**RU, "fields": {"note": "'RU'"}, "sql_condition": "region = 'ru'"},
            id="field-equal-to-root-is-removed",
        ),
        pytest.param({"skip_rules": [RU]}, [rule(ORDERS, email=E), rule(RU, email=E)], None, id="skipped-partition"),
        pytest.param({}, [rule(ORDERS, email=E), rule(RU, note=N)], None, id="equal-to-root-after-scan"),
    ],
)
async def test_scan_cleans_exact_rule_of_partition(scan, meta, prepared, expected):
    result = await scan(meta, prepared={"dictionary": prepared})
    rules = [item for item in result.sens["dictionary"] if item.get("table") == "orders_2024_ru"]
    assert rules == ([expected] if expected else [])


@pytest.mark.min_pg(100000)
async def test_scan_keeps_mask_rule_as_is(scan):
    mask = {"schema": "sp", "table_mask": "_ru$", "fields": {"email": E}}
    result = await scan(prepared={"dictionary": [rule(ORDERS, email=E), mask]})
    assert mask in result.sens["dictionary"]


# --------------------------------------------------------------------------
# Scan, dump and restore
# --------------------------------------------------------------------------


@pytest.mark.min_pg(100000)
async def test_dump_masks_partition_created_after_scan(
    base_scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    await db_manager.execute(
        source_db,
        """
        CREATE TABLE sp.orders_2025 PARTITION OF sp.orders FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
        INSERT INTO sp.orders (created, region, email, note, tag)
            VALUES ('2025-02-01', 'ru', 'new@example.com', 'ok', '-');
        """,
    )
    try:
        await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, base_scan.sens, "partition_after_scan")
        columns = ("email", "note", "tag")
        for table in ("sp.orders_2024_def", "sp_archive.orders_2023", "sp.orders_2025"):
            assert await masking(db_manager, source_db, target_db, table, columns) == {
                "email": "md5",
                "note": "md5",
                "tag": "F_CONST",
            }, table
        for table in ("sp_crm.person", "sp_crm.partner"):
            assert (await masking(db_manager, source_db, target_db, table, ("info",)))["info"] == "md5"
        for table in ("sp_crm.contact", "sp_crm.company"):
            assert (await masking(db_manager, source_db, target_db, table, ("info",)))["info"] == "as is"
        counts = await db_manager.fetch(
            target_db,
            """
            SELECT (SELECT count(*) FROM sp.orders) AS orders,
                   (SELECT count(*) FROM ONLY sp_crm.contact) AS contact,
                   (SELECT count(*) FROM sp_crm.person) AS person,
                   (SELECT count(*) FROM sp_crm.company) AS company,
                   (SELECT count(*) FROM sp_crm.partner) AS partner,
                   (SELECT count(*) FROM ONLY sp_crm.vendor) AS vendor,
                   (SELECT string_agg(code, ',' ORDER BY code) FROM ONLY sp_crm.vendor) AS codes
            """,
        )
        assert dict(counts[0]) == {
            "orders": 21,
            "contact": 5,
            "person": 5,
            "company": 5,
            "partner": 5,
            "vendor": 3,
            "codes": "1001,1002,1003",
        }
        unchanged = "SELECT id, created, region FROM sp.orders WHERE id <= 20 ORDER BY id"
        assert [tuple(r) for r in await db_manager.fetch(target_db, unchanged)] == [
            tuple(r) for r in await db_manager.fetch(source_db, unchanged)
        ]
    finally:
        await db_manager.execute(source_db, "DROP TABLE sp.orders_2025")


@pytest.mark.min_pg(100000)
async def test_dump_does_not_mask_detached_partition_by_root_rule(
    base_scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    await db_manager.execute(source_db, "ALTER TABLE sp.orders DETACH PARTITION sp_archive.orders_2023")
    try:
        await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, base_scan.sens, "detached_partition")
        assert await masking(db_manager, source_db, target_db, "sp_archive.orders_2023", ("email", "note")) == {
            "email": "as is",
            "note": "as is",
        }
    finally:
        await db_manager.execute(
            source_db,
            "ALTER TABLE sp.orders ATTACH PARTITION sp_archive.orders_2023 "
            "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')",
        )


@pytest.mark.min_pg(100000)
async def test_dump_merges_rule_of_attached_partition_with_root_rule(
    source_db, target_db, db_manager, db_params, pg_anon_runner
):
    await db_manager.execute(
        source_db,
        """
        CREATE TABLE sp.orders_2025 (LIKE sp.orders INCLUDING CONSTRAINTS);
        INSERT INTO sp.orders_2025 (id, created, region, email, note, tag)
            VALUES (100, '2025-02-01', 'ru', 'new@example.com', 'ok', '-');
        ALTER TABLE sp.orders ATTACH PARTITION sp.orders_2025 FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
        """,
    )
    try:
        sens_dict = {"dictionary": [rule(ORDERS, email=E), rule({"schema": "sp", "table": "orders_2025"}, note="'X'")]}
        await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, sens_dict, "attached_partition")
        assert await masking(db_manager, source_db, target_db, "sp.orders_2025", ("email", "note")) == {
            "email": "md5",
            "note": "X",
        }
    finally:
        await db_manager.execute(source_db, "DROP TABLE sp.orders_2025")


async def test_dump_does_not_mask_inherits_child_created_after_scan(
    base_scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    await db_manager.execute(
        source_db,
        """
        CREATE TABLE sp_crm.lead () INHERITS (sp_crm.contact);
        INSERT INTO sp_crm.lead (name, info) VALUES ('Lead', '+7 900 555-66-77');
        """,
    )
    try:
        await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, base_scan.sens, "child_after_scan")
        assert await masking(db_manager, source_db, target_db, "sp_crm.lead", ("info",)) == {"info": "as is"}
    finally:
        await db_manager.execute(source_db, "DROP TABLE sp_crm.lead")


# --------------------------------------------------------------------------
# dictionary_exclude of the prepared sens dict
# --------------------------------------------------------------------------

EXCLUDE_LINE = "dictionary_exclude"


async def test_scan_reads_tables_under_exclude_and_keeps_exclude(
    scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    person = {"schema": "sp_crm", "table": "person"}
    result = await scan(prepared={"dictionary": [], "dictionary_exclude": [person]}, no_sens=True)

    assert result.rules(CRM)["sp_crm.person"] == {"info": I}
    assert result.sens["dictionary_exclude"] == [person]
    assert result.no_sens_fields(CRM)["sp_crm.person"] == ["id", "name"]
    lines = result.lines(EXCLUDE_LINE)
    assert len(lines) == 1
    assert "skip_rules" in lines[0]

    await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, result.sens, "exclude_person")
    assert await masking(db_manager, source_db, target_db, "sp_crm.person", ("info",)) == {"info": "md5"}


async def test_scan_keeps_exclude_of_table_without_findings(
    scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    company = {"schema": "sp_crm", "table": "company"}
    result = await scan(prepared={"dictionary": [], "dictionary_exclude": [company]})

    assert "sp_crm.company" not in result.rules(CRM)
    assert result.sens["dictionary_exclude"] == [company]
    await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, result.sens, "exclude_company")
    assert await masking(db_manager, source_db, target_db, "sp_crm.company", ("info",)) == NO_ROWS


@pytest.mark.min_pg(100000)
async def test_scan_reads_partitions_under_exclude_of_root(
    scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    result = await scan(prepared={"dictionary": [rule(RU, email=E)], "dictionary_exclude": [ORDERS]})

    assert result.rules(SP) == {"sp.orders": {"email": E, "note": N, "tag": F_CONST}, "sp.empty_root": {"email": E}}
    assert result.sens["dictionary_exclude"] == [ORDERS]
    await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, result.sens, "exclude_root")
    for table in PARTITIONS:
        assert (await masking(db_manager, source_db, target_db, table, ("email",)))["email"] == "md5", table


# --------------------------------------------------------------------------
# no_sens dictionaries
# --------------------------------------------------------------------------


@pytest.mark.min_pg(100000)
async def test_no_sens_of_partition_does_not_stop_rule_of_root(
    scan, source_db, target_db, db_manager, db_params, pg_anon_runner
):
    result = await scan(
        prepared_no_sens={"no_sens_dictionary": [{"schema": "sp", "table": "orders_2024_ru", "fields": ["note"]}]}
    )
    assert result.rules(SP)["sp.orders"] == {"email": E, "note": N, "tag": F_CONST}
    await dump_and_restore(pg_anon_runner, db_params, source_db, target_db, result.sens, "no_sens_partition")
    assert (await masking(db_manager, source_db, target_db, "sp.orders_2024_ru", ("note",)))["note"] == "md5"


@pytest.mark.min_pg(100000)
async def test_no_sens_of_root_stops_reading_of_field_in_all_partitions(scan):
    result = await scan(prepared_no_sens={"no_sens_dictionary": [{**ORDERS, "fields": ["note"]}]})
    assert result.rules(SP)["sp.orders"] == {"email": E, "tag": F_CONST}


async def test_no_sens_of_inherits_parent_does_not_cover_children(scan):
    result = await scan(
        prepared_no_sens={"no_sens_dictionary": [{"schema": "sp_crm", "table": "contact", "fields": ["info"]}]}
    )
    assert result.rules(CRM)["sp_crm.person"] == {"info": I}


@pytest.mark.min_pg(100000)
async def test_no_sens_output_has_one_record_for_partitioned_table(base_scan):
    assert base_scan.no_sens_fields(SP) == {"sp.orders": ["created", "region"], "sp.empty_root": ["id"]}
    crm = base_scan.no_sens_fields(CRM)
    assert crm["sp_crm.person"] == ["id", "name"]
    assert crm["sp_crm.company"] == ["id", "name", "info", "inn"]


@pytest.mark.min_pg(100000)
async def test_scan_with_both_output_dicts_as_input_gives_the_same_dicts(scan, base_scan, tmp_path):
    sens, no_sens = tmp_path / "sens.json", tmp_path / "no_sens.json"
    sens.write_text(json.dumps(base_scan.sens))
    no_sens.write_text(json.dumps(base_scan.no_sens))
    result = await scan(prepared=str(sens), prepared_no_sens=str(no_sens), no_sens=True)
    assert result.sens == base_scan.sens
    assert result.no_sens == base_scan.no_sens


@pytest.mark.min_pg(100000)
async def test_scan_with_both_output_dicts_reads_new_column(scan, base_scan, source_db, db_manager, tmp_path):
    sens, no_sens = tmp_path / "sens.json", tmp_path / "no_sens.json"
    sens.write_text(json.dumps(base_scan.sens))
    no_sens.write_text(json.dumps(base_scan.no_sens))
    await db_manager.execute(
        source_db,
        """
        ALTER TABLE sp.orders ADD COLUMN phone text;
        ALTER TABLE sp.orders_2024_ru REPLICA IDENTITY FULL;
        UPDATE sp.orders_2024_ru SET phone = '+7 900 777-88-' || lpad(id::text, 2, '0');
        """,
    )
    try:
        result = await scan(prepared=str(sens), prepared_no_sens=str(no_sens), no_sens=True)
    finally:
        await db_manager.execute(
            source_db,
            "ALTER TABLE sp.orders DROP COLUMN phone; ALTER TABLE sp.orders_2024_ru REPLICA IDENTITY DEFAULT",
        )

    expected = json.loads(json.dumps(base_scan.sens))
    root = next(item for item in expected["dictionary"] if item.get("table") == "orders" and item["schema"] == "sp")
    root["fields"]["phone"] = 'md5("phone")'
    assert result.sens == expected
    assert result.no_sens == base_scan.no_sens


@pytest.mark.min_pg(100000)
async def test_no_sens_output_does_not_carry_records_of_partitions_from_input(scan):
    result = await scan(
        prepared_no_sens={
            "no_sens_dictionary": [{"schema": "sp", "table": "orders_2024_ru", "fields": ["region", "note"]}]
        },
        no_sens=True,
    )
    assert result.no_sens_fields(SP) == {"sp.orders": ["created", "region"], "sp.empty_root": ["id"]}
    assert result.rules(SP)["sp.orders"]["note"] == N


async def test_no_sens_output_has_no_fields_with_rules_in_the_sens_dict(scan):
    result = await scan(
        prepared={"dictionary": [rule({"schema": "sp_crm", "table": "company"}, name="'N'")]},
        prepared_no_sens={
            "no_sens_dictionary": [
                {"schema": "sp_crm", "table": "company", "fields": ["name", "inn"]},
                {"schema": "sp_crm", "table": "lead", "fields": ["info"]},
            ]
        },
        no_sens=True,
    )
    no_sens = result.no_sens_fields(CRM)
    assert "sp_crm.lead" not in no_sens
    assert no_sens["sp_crm.company"] == ["id", "info", "inn"]


@pytest.mark.min_pg(100000)
async def test_no_sens_output_of_partitioned_table_has_no_fields_with_rules_in_the_sens_dict(scan):
    result = await scan(
        prepared={"dictionary": [rule(RU, note="'RU'")]},
        prepared_no_sens={
            "no_sens_dictionary": [{"schema": "sp", "table": "orders_2024_ru", "fields": ["note", "region"]}]
        },
        no_sens=True,
    )
    assert result.no_sens_fields(SP) == {"sp.orders": ["created", "region"], "sp.empty_root": ["id"]}


def test_line_about_own_fields_of_partitions_names_three_partitions_and_counts_the_rest():
    partitions = {("sp", f"orders_{year}"): ["note"] for year in (2025, 2021, 2023, 2024, 2022)}
    partitions[("sp", "orders_2022")] = ["email", "note"]

    assert own_masking_line(("sp", "orders"), partitions) == (
        "sp.orders: 5 partitions have their own masking for fields email, note "
        "(sp.orders_2021, sp.orders_2022, sp.orders_2023, and 2 more)"
    )


# --------------------------------------------------------------------------
# Prepared rules for tables and fields that the database does not have
# --------------------------------------------------------------------------

KEEP_UNMATCHED = ["--keep-unmatched-prepared-rules"]
PERSON = {"schema": "sp_crm", "table": "person"}
COMPANY = {"schema": "sp_crm", "table": "company"}
VENDOR = {"schema": "sp_crm", "table": "vendor"}
LEAD = {"schema": "sp_crm", "table": "lead"}
OLD_CONTACT = {"schema": "crm_old", "table": "contact"}
LEAD_MASK = {"schema": "sp_crm", "table_mask": "^lead"}
COMPANY_MASK = {"schema": "sp_crm", "table_mask": "^comp"}


def _key(item: dict) -> tuple:
    return tuple(item.get(name) for name in ("schema", "schema_mask", "table", "table_mask"))


def output_rules_of(result: Scan, prepared: list[dict]) -> list[dict]:
    """The rules of the output sens dict for the same tables or masks as the prepared rules, in their order."""
    by_key = {_key(item): item for item in result.sens["dictionary"]}
    return [by_key[_key(item)] for item in prepared if _key(item) in by_key]


@pytest.mark.parametrize(
    ("prepared", "extra", "expected", "not_in_log"),
    [
        pytest.param(
            [rule(LEAD, info=I), rule(OLD_CONTACT, info=I), rule(PERSON, name="'P'")],
            [],
            [rule(PERSON, name="'P'", info=I)],
            ("sp_crm.lead", "crm_old"),
            id="tables-that-the-database-has-not",
        ),
        pytest.param(
            [rule(LEAD, info=I), rule(OLD_CONTACT, info=I), rule(PERSON, name="'P'")],
            KEEP_UNMATCHED,
            [rule(LEAD, info=I), rule(OLD_CONTACT, info=I), rule(PERSON, name="'P'", info=I)],
            (),
            id="tables-that-the-database-has-not-with-option",
        ),
        pytest.param(
            [rule(LEAD_MASK, info=I), rule(COMPANY_MASK, inn="'INN'")],
            [],
            [rule(COMPANY_MASK, inn="'INN'")],
            (),
            id="masks",
        ),
        pytest.param(
            [rule(LEAD_MASK, info=I), rule(COMPANY_MASK, inn="'INN'")],
            KEEP_UNMATCHED,
            [rule(LEAD_MASK, info=I), rule(COMPANY_MASK, inn="'INN'")],
            (),
            id="masks-with-option",
        ),
        pytest.param(
            [rule(COMPANY_MASK, inn="'INN'"), rule(COMPANY, name="'N'")],
            [],
            [rule(COMPANY_MASK, inn="'INN'"), rule(COMPANY, name="'N'")],
            (),
            id="mask-of-table-with-exact-rule",
        ),
        pytest.param(
            [
                {"schema_mask": "^sp_cr", "table_mask": "^ven", "fields": {"code": "'V'"}},
                {"schema_mask": "^crm_old", "table_mask": ".*", "fields": {"info": I}},
            ],
            [],
            [{"schema_mask": "^sp_cr", "table_mask": "^ven", "fields": {"code": "'V'"}}],
            (),
            id="schema-masks",
        ),
        pytest.param(
            [rule(VENDOR, code="'V'")], [], [rule(VENDOR, code="'V'")], (), id="table-without-scanned-columns"
        ),
        pytest.param(
            [rule(PERSON, info=I, phone="'X'")], [], [rule(PERSON, info=I)], (), id="field-that-the-table-has-not"
        ),
        pytest.param(
            [rule(PERSON, info=I, phone="'X'")],
            KEEP_UNMATCHED,
            [rule(PERSON, info=I, phone="'X'")],
            (),
            id="field-that-the-table-has-not-with-option",
        ),
        pytest.param(
            [{**rule(PERSON, phone="'X'"), "sql_condition": "true"}, rule(COMPANY, phone="'X'")],
            [],
            [{**rule(PERSON, phone="'X'", info=I), "sql_condition": "true"}],
            (),
            id="rule-without-fields-of-the-table",
        ),
    ],
)
async def test_scan_keeps_prepared_rules_only_for_tables_and_fields_of_the_database(
    scan, prepared, extra, expected, not_in_log
):
    result = await scan(prepared={"dictionary": prepared}, extra=extra)
    assert result.result.result_code == ResultCode.DONE
    assert output_rules_of(result, prepared) == expected
    # the scan says nothing about the prepared rules it leaves out
    messages = [record.getMessage() for record in result.records]
    for name in not_in_log:
        assert not [message for message in messages if name in message], name


@pytest.mark.min_pg(100000)
async def test_scan_does_not_keep_prepared_rule_of_partition_that_does_not_exist_yet(scan):
    future = {"schema": "sp", "table": "orders_2025"}
    result = await scan(prepared={"dictionary": [rule(future, note="'X'")]})
    assert output_rules_of(result, [future]) == []
    assert result.rules(SP)["sp.orders"]["note"] == N


@pytest.mark.parametrize("prepared", [{}, {"dictionary_exclude": [COMPANY]}])
async def test_scan_with_prepared_dict_without_rules_gives_only_findings(scan, base_scan, prepared):
    result = await scan(prepared=prepared, no_sens=True)
    assert result.sens["dictionary"] == base_scan.sens["dictionary"]


@pytest.mark.parametrize("extra", [[], KEEP_UNMATCHED], ids=["without-option", "with-option"])
async def test_scan_keeps_dictionary_exclude_as_is(scan, extra):
    excludes = [LEAD, COMPANY, LEAD_MASK, {"schema": "sp_crm"}]
    result = await scan(prepared={"dictionary": [], "dictionary_exclude": excludes}, extra=extra)
    assert result.sens["dictionary_exclude"] == excludes
