"""A dump of pg_anon 1.12.0 keeps the publication tables of `public` when it is restored.

The dump `legacy_1_12/full` is made by pg_anon 1.12.0 from the database of `Fixtures.build_publication_membership`
on PG 15: `init`, then `dump --prepared-sens-dict-file=input_dict/empty.py`.
"""

from __future__ import annotations

import pytest

from .conftest import copy_of, memberships, restore, SUITE
from pg_anon.common.enums import ResultCode

# The dump was made on PG 15
pytestmark = [pytest.mark.min_pg(150000), pytest.mark.max_pg(160000)]

ALL = ["public.t", "s.t2", "s.t3", "x.t4"]

CASES = {
    "no_filters": ([], ALL, ["s.t2", "s.t3"]),
    "exclude_schema_x": (["--exclude-schema-name=x"], ["public.t", "s.t2", "s.t3"], ["s.t2", "s.t3"]),
}


@pytest.mark.parametrize("case", CASES)
async def test_restore_of_a_dump_of_version_1_12_keeps_the_publication_table_of_public(
    case, target_db, db_manager, db_params, pg_anon_runner
):
    options, expected, expected_of_schema = CASES[case]
    in_dir = copy_of(SUITE / "legacy_1_12" / "full", f"legacy_{case}")

    res = await restore(pg_anon_runner, db_params, target_db, in_dir, options)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    published = await memberships(db_manager, target_db)
    assert published.get("p", []) == expected
    assert published.get("p_schema", []) == expected_of_schema
