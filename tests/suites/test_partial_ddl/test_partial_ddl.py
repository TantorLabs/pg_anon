from __future__ import annotations

import re

import pytest

from .conftest import input_dict, output_path
from pg_anon.common.db_utils import (
    get_custom_aggregates_ddl,
    get_custom_casts_ddl,
    get_custom_domains_ddl,
    get_custom_functions_ddl,
    get_custom_operators_ddl,
    get_custom_types_ddl,
)
from pg_anon.common.enums import ResultCode


def _statement_for(ddl_list: list[str], needle: str) -> str:
    matching = [ddl for ddl in ddl_list if needle in ddl]
    assert matching, f"no DDL statement mentions {needle!r}; got: {ddl_list}"
    assert len(matching) == 1, f"expected exactly one statement for {needle!r}, got {len(matching)}"
    return matching[0]


async def test_type_guard_is_schema_aware(source_connection):
    ddl_list = await get_custom_types_ddl(source_connection, [])

    for schema in ("app", "lib"):
        statement = _statement_for(ddl_list, f"CREATE TYPE {schema}.currency")
        guard = statement.split("THEN")[0]
        assert "pg_namespace" in guard and schema in guard, (
            f"existence guard for {schema}.currency does not check the schema, "
            f"so only one of the same-named types survives a restore: {guard!r}"
        )


async def test_operator_ddl_is_valid_sql(source_connection, db_manager, target_db):
    ddl_list = await get_custom_operators_ddl(source_connection, [])
    statement = _statement_for(ddl_list, "===")

    assert not re.search(r"=\s*0\b", statement), f"zero OID rendered into operator DDL: {statement!r}"
    assert not re.search(r"=\s*-[,)]", statement), f"missing support function rendered as '-': {statement!r}"

    await db_manager.execute(
        target_db,
        """
        CREATE SCHEMA IF NOT EXISTS lib;
        CREATE DOMAIN lib.positive_amount AS numeric CHECK (VALUE > 0);
        CREATE FUNCTION public.amount_eq(lib.positive_amount, lib.positive_amount) RETURNS boolean
            AS $$ SELECT $1::numeric = $2::numeric $$ LANGUAGE sql IMMUTABLE;
        """,
    )
    await db_manager.execute(target_db, statement)


async def test_aggregate_ddl_is_valid_sql(source_connection, db_manager, target_db):
    ddl_list = await get_custom_aggregates_ddl(source_connection, [])
    statement = _statement_for(ddl_list, "max_amount")

    assert not re.search(r"=\s*-[,)]", statement), f"absent FINALFUNC rendered as '-': {statement!r}"

    await db_manager.execute(
        target_db,
        """
        CREATE SCHEMA IF NOT EXISTS lib;
        CREATE DOMAIN lib.positive_amount AS numeric CHECK (VALUE > 0);
        CREATE FUNCTION public.amount_max(lib.positive_amount, lib.positive_amount)
            RETURNS lib.positive_amount
            AS $$ SELECT greatest($1::numeric, $2::numeric)::lib.positive_amount $$ LANGUAGE sql IMMUTABLE;
        """,
    )
    await db_manager.execute(target_db, statement)


async def test_operator_commutator_is_preserved(source_connection):
    ddl_list = await get_custom_operators_ddl(source_connection, [])
    statement = _statement_for(ddl_list, "CREATE OPERATOR public.<<< (")

    assert "COMMUTATOR" in statement, f"commutator reference lost: {statement!r}"
    assert "public.>>>" in statement, f"commutator target is not schema-qualified: {statement!r}"


async def test_cast_function_is_schema_qualified(source_connection):
    ddl_list = await get_custom_casts_ddl(source_connection, [])
    statement = _statement_for(ddl_list, "doc_kind_to_text")

    assert "public.doc_kind_to_text" in statement, (
        f"cast function is not schema-qualified and will resolve through the target's "
        f"search_path instead of the source schema: {statement!r}"
    )


async def test_function_argument_types_are_schema_qualified(source_connection):
    ddl_list = await get_custom_functions_ddl(source_connection, [])
    statement = _statement_for(ddl_list, "lib.describe")

    assert "public.doc_kind" in statement, (
        f"argument type is not schema-qualified; on restore it may silently bind to a "
        f"different type with the same name: {statement!r}"
    )


async def test_domain_ddl_has_existence_guard(source_connection):
    ddl_list = await get_custom_domains_ddl(source_connection, [])
    statement = _statement_for(ddl_list, "lib.positive_amount")

    assert "IF NOT EXISTS" in statement, (
        f"domain DDL has no existence guard, unlike custom types and ranges: {statement!r}"
    )


@pytest.fixture
async def partial_dump(source_db, db_params, pg_anon_runner):
    out = output_path("partial_ddl")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--partial-tables-dict-file={input_dict('include_app_only.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, "partial dump must succeed"
    return out


async def test_partial_restore_succeeds_with_custom_objects(
    partial_dump,
    target_db,
    db_params,
    pg_anon_runner,
):
    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={partial_dump}",
            f"--partial-tables-dict-file={input_dict('include_app_only.py')}",
        ],
    )
    assert res.result_code == ResultCode.DONE, f"restore failed on user-defined objects: {res.error_message}"


async def test_same_named_types_are_both_created_on_target(
    source_connection,
    db_manager,
    target_db,
):
    ddl_list = await get_custom_types_ddl(source_connection, [])

    await db_manager.execute(target_db, "CREATE SCHEMA IF NOT EXISTS app; CREATE SCHEMA IF NOT EXISTS lib;")
    for statement in ddl_list:
        await db_manager.execute(target_db, statement)

    rows = await db_manager.fetch(
        target_db,
        """
        SELECT n.nspname AS schema_name
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'currency' AND n.nspname IN ('app', 'lib')
        ORDER BY n.nspname
        """,
    )
    restored = {row["schema_name"] for row in rows}
    assert restored == {"app", "lib"}, (
        f"a same-named type in another schema was silently skipped, restored only: {sorted(restored)}"
    )
