"""A table stays in a publication when it is restored, and leaves it when it is excluded, whatever its schema."""

from __future__ import annotations

import pytest

from .conftest import dump, input_dict, memberships, restore
from pg_anon.common.enums import ResultCode

# publications exist since PG 10
pytestmark = pytest.mark.min_pg(100000)

ALL = ["public.t", "s.t2", "s.t3", "x.t4"]

# restore options: (publication `p`)
TABLE_CASES = {
    "no_filters": ([], ALL),
    "exclude_schema_x": (["--exclude-schema-name=x"], ["public.t", "s.t2", "s.t3"]),
    "exclude_schema_s": (["--exclude-schema-name=s"], ["public.t", "x.t4"]),
    "exclude_schema_public": (["--exclude-schema-name=public"], ["s.t2", "s.t3", "x.t4"]),
    "black_list_table": (
        [f"--partial-tables-exclude-dict-file={input_dict('exclude_s_t3.py')}"],
        ["public.t", "s.t2", "x.t4"],
    ),
    "white_list": ([f"--partial-tables-dict-file={input_dict('include_public_t_s_t2.py')}"], ["public.t", "s.t2"]),
}


@pytest.mark.parametrize("case", TABLE_CASES)
async def test_restore_keeps_publication_tables_that_are_restored(
    case, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner
):
    options, expected = TABLE_CASES[case]

    res = await restore(pg_anon_runner, db_params, target_db, full_dump, options)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert (await memberships(db_manager, target_db)).get("p", []) == expected


async def test_restore_without_filters_of_a_dump_made_with_the_lists_keeps_the_table_of_public(
    source_db, target_db, db_manager, db_params, pg_anon_runner
):
    options = ["--exclude-schema-name=x", f"--partial-tables-exclude-dict-file={input_dict('exclude_s_t3.py')}"]
    out, res = await dump(pg_anon_runner, db_params, source_db, "lists_on_dump", options)
    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"

    res = await restore(pg_anon_runner, db_params, target_db, out)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert (await memberships(db_manager, target_db)).get("p", []) == ["public.t", "s.t2"]


# restore options: (publication `p_schema`, FOR TABLES IN SCHEMA s)
SCHEMA_CASES = {
    "no_filters": ([], ["s.t2", "s.t3"]),
    "exclude_schema_x": (["--exclude-schema-name=x"], ["s.t2", "s.t3"]),
    "exclude_schema_s": (["--exclude-schema-name=s"], []),
    "black_list_table": ([f"--partial-tables-exclude-dict-file={input_dict('exclude_s_t3.py')}"], ["s.t2"]),
    "white_list": ([f"--partial-tables-dict-file={input_dict('include_public_t_s_t2.py')}"], ["s.t2"]),
}


@pytest.mark.min_pg(150000)
@pytest.mark.parametrize("case", SCHEMA_CASES)
async def test_restore_keeps_tables_of_a_publication_for_tables_in_schema(
    case, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner
):
    options, expected = SCHEMA_CASES[case]

    res = await restore(pg_anon_runner, db_params, target_db, full_dump, options)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert (await memberships(db_manager, target_db)).get("p_schema", []) == expected
