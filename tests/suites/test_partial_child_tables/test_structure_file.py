"""structure.json describes the parent tables of a dump, so the restore applies its lists without the source."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from .conftest import dump, list_options
from pg_anon.common.enums import ResultCode


def read_structure(dump_dir: str) -> dict:
    return json.loads((Path(dump_dir) / "structure.json").read_text(encoding="utf-8"))


def test_inherits_lists_every_child_with_all_its_parents_closest_first(inherits_full_dump):
    structure = read_structure(inherits_full_dump)

    assert structure["partitions"] == []
    assert structure["inherits"] == [
        {"schema": "ilist", "table": "email_notifications", "ancestors": [["ilist", "notifications"]]},
        {
            "schema": "ilist",
            "table": "marked_email",
            "ancestors": [["ilist", "email_notifications"], ["ilist", "marks"], ["ilist", "notifications"]],
        },
        {"schema": "ilist", "table": "sms_notifications", "ancestors": [["ilist", "notifications"]]},
        {
            "schema": "ilist",
            "table": "sms_urgent",
            "ancestors": [["ilist", "sms_notifications"], ["ilist", "notifications"]],
        },
    ]


async def test_inherits_has_no_names_of_excluded_tables(inherits_db, db_params, pg_anon_runner):
    options = list_options("structure_exclude", "ilist", [], ["sms_notifications"])

    out, res = await dump(pg_anon_runner, db_params, inherits_db, "structure_exclude", options)

    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    assert [info["table"] for info in read_structure(out)["inherits"]] == ["email_notifications", "marked_email"]


@pytest.mark.min_pg(100000)
def test_foreign_partition_is_described_with_its_partitioned_table(foreign_partition_full_dump):
    structure = read_structure(foreign_partition_full_dump)

    assert structure["inherits"] == []
    assert structure["partitions"] == [
        {"schema": "fpart", "table": "events", "is_partitioned": True, "ancestors": []},
        {"schema": "fpart", "table": "events_local", "is_partitioned": False, "ancestors": [["fpart", "events"]]},
        {"schema": "fpart", "table": "events_remote", "is_partitioned": False, "ancestors": [["fpart", "events"]]},
    ]
