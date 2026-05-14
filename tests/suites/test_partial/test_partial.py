"""Tests for --partial-tables-dict-file and --partial-tables-exclude-dict-file.

The flags are accepted on BOTH dump and restore. All 4 placement combinations
must produce the same final table set in the target — this guards against the
"the filter only worked on dump" class of bug we've seen before.
"""
from __future__ import annotations

from tests.infrastructure.assertions import check_list_tables

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode

INCLUDE_EXPECTED = [
    ("billing",   "customer"),
    ("billing",   "invoice"),
    ("ecommerce", "product"),
    ("hr",        "department"),
    ("hr",        "employee"),
    ("quirks",    "MixedCaseTable"),
    ("quirks",    "reserved_words"),
    ("quirks",    "with_nulls"),
    ("quirks",    "таблица_на_русском"),
]

EXCLUDE_EXPECTED = [
    ("_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'"),
    ("_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'2"),
    ("analytics",  "event"),
    ("analytics",  "event_by_region"),
    ("analytics",  "event_by_tenant"),
    ("billing",    "customer"),
    ("billing",    "invoice"),
    ("circular_fk", "a"),
    ("circular_fk", "b"),
    ("constraints_quirks", "booking"),
    ("constraints_quirks", "evt_attachment"),
    ("constraints_quirks", "legacy_child"),
    ("constraints_quirks", "legacy_parent"),
    ("constraints_quirks", "subpart_2025_q1"),
    ("constraints_quirks", "subpart_2025_q1_other"),
    ("constraints_quirks", "subpart_2025_q1_t123"),
    ("constraints_quirks", "subpart_2025_q2"),
    ("constraints_quirks", "subpart_root"),
    ("ecommerce",  "product"),
    ("hr",         "department"),
    ("hr",         "employee"),
    ("index_quirks", "events"),
    ("privs",      "owned_by_writer"),
    ("privs",      "public_facing"),
    ("pubs",       "feed"),
    ("quirks",     "MixedCaseTable"),
    ("quirks",     "reserved_words"),
    ("quirks",     "with_nulls"),
    ("quirks",     "таблица_на_русском"),
    ("variants",   "animal"),
    ("variants",   "bird"),
    ("variants",   "mammal"),
    ("variants",   "session_cache"),
    ("variants",   "with_identity"),
]


async def _dump(pg_anon_runner, db_params, source_db, out, extra=None):
    args = [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ]
    if extra:
        args.extend(extra)
    res = await pg_anon_runner.run("dump", source_db, args)
    assert res.result_code == ResultCode.DONE


async def _restore(pg_anon_runner, db_params, target_db, out, extra=None):
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ]
    if extra:
        args.extend(extra)
    res = await pg_anon_runner.run("restore", target_db, args)
    assert res.result_code == ResultCode.DONE


async def test_partial_include_at_dump(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Include filter at dump time only — restore takes everything from dump."""
    out = output_path("include_at_dump")
    await _dump(pg_anon_runner, db_params, source_db, out, extra=[
        f"--partial-tables-dict-file={input_dict('include.py')}",
    ])
    await _restore(pg_anon_runner, db_params, target_db, out)
    assert await check_list_tables(db_manager, target_db, INCLUDE_EXPECTED)


async def test_partial_exclude_at_dump(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Exclude filter at dump time only."""
    out = output_path("exclude_at_dump")
    await _dump(pg_anon_runner, db_params, source_db, out, extra=[
        f"--partial-tables-exclude-dict-file={input_dict('exclude.py')}",
    ])
    await _restore(pg_anon_runner, db_params, target_db, out)
    assert await check_list_tables(db_manager, target_db, EXCLUDE_EXPECTED)


async def test_partial_include_exclude_at_dump(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Both filters at dump. Include ∩ (complement of Exclude) = INCLUDE_EXPECTED."""
    out = output_path("include_exclude_at_dump")
    await _dump(pg_anon_runner, db_params, source_db, out, extra=[
        f"--partial-tables-dict-file={input_dict('include.py')}",
        f"--partial-tables-exclude-dict-file={input_dict('exclude.py')}",
    ])
    await _restore(pg_anon_runner, db_params, target_db, out)
    assert await check_list_tables(db_manager, target_db, INCLUDE_EXPECTED)


async def test_partial_include_at_dump_exclude_at_restore(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Include at dump + exclude at restore. Same net effect as both-at-dump."""
    out = output_path("include_dump_exclude_restore")
    await _dump(pg_anon_runner, db_params, source_db, out, extra=[
        f"--partial-tables-dict-file={input_dict('include.py')}",
    ])
    await _restore(pg_anon_runner, db_params, target_db, out, extra=[
        f"--partial-tables-exclude-dict-file={input_dict('exclude.py')}",
    ])
    assert await check_list_tables(db_manager, target_db, INCLUDE_EXPECTED)


async def test_partial_exclude_at_dump_include_at_restore(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Exclude at dump + include at restore. Same net effect."""
    out = output_path("exclude_dump_include_restore")
    await _dump(pg_anon_runner, db_params, source_db, out, extra=[
        f"--partial-tables-exclude-dict-file={input_dict('exclude.py')}",
    ])
    await _restore(pg_anon_runner, db_params, target_db, out, extra=[
        f"--partial-tables-dict-file={input_dict('include.py')}",
    ])
    assert await check_list_tables(db_manager, target_db, INCLUDE_EXPECTED)


async def test_partial_full_dump_both_filters_at_restore(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Full dump, both filters applied at restore only."""
    out = output_path("both_at_restore")
    await _dump(pg_anon_runner, db_params, source_db, out)
    await _restore(pg_anon_runner, db_params, target_db, out, extra=[
        f"--partial-tables-dict-file={input_dict('include.py')}",
        f"--partial-tables-exclude-dict-file={input_dict('exclude.py')}",
    ])
    assert await check_list_tables(db_manager, target_db, INCLUDE_EXPECTED)
