from __future__ import annotations

from pathlib import Path

import pytest

from .conftest import (
    count_rows,
    dump,
    fk_state,
    fk_warnings,
    key_commands,
    key_label,
    NOT_CREATED_WARNING,
    not_created_warnings,
    not_valid_summaries,
    NOT_VALID_SUMMARY,
    NOT_VALID_WARNING,
    not_valid_warnings,
    restore,
    Restored,
)

# the server can keep a NOT VALID key on a partitioned table from this version; older servers do not get the key
NOT_VALID_ON_PARTITIONED = 180000

EXCLUDE_PATHS = ["exclude_on_dump", "exclude_on_restore"]


async def restore_without_partition(
    path: str, partition: str, source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
) -> Restored:
    """Leave the partition out on dump, or on restore of the full dump."""
    exclude_file = f"exclude_{partition}.py"
    if path == "exclude_on_dump":
        out = await dump(
            pg_anon_runner, db_params, source_db, f"no_{partition}", dict_file="empty.py", exclude_file=exclude_file
        )
        return await restore(pg_anon_runner, db_params, target_db, caplog, in_dir=out)
    return await restore(pg_anon_runner, db_params, target_db, caplog, in_dir=full_dump, exclude_file=exclude_file)


async def restore_with_dict(dict_file: str, source_db, target_db, db_params, pg_anon_runner, caplog) -> Restored:
    out = await dump(pg_anon_runner, db_params, source_db, dict_file.removesuffix(".py"), dict_file=dict_file)
    return await restore(pg_anon_runner, db_params, target_db, caplog, in_dir=out)


async def assert_key_is_valid(db_manager, restored: Restored, key: str) -> None:
    assert await fk_state(db_manager, restored.db_name, key) == "valid"
    assert not not_valid_warnings(restored, key)
    assert not not_created_warnings(restored, key)


async def assert_key_is_not_valid(db_manager, restored: Restored, key: str, tables: tuple[str, str]) -> None:
    assert await fk_state(db_manager, restored.db_name, key) == "not valid"
    assert "VALIDATE" not in key_commands(restored, key), "a key that is left as NOT VALID is not validated"
    assert not_valid_warnings(restored, key) == [NOT_VALID_WARNING.format(key=key_label(key, tables))], (
        f"one warning must name the key and both tables, got: {restored.warnings}"
    )
    assert not_valid_summaries(restored.warnings) == [NOT_VALID_SUMMARY.format(count="1 foreign key")], (
        f"one final warning must give the number of keys, got: {restored.warnings}"
    )
    assert not not_created_warnings(restored, key)


async def assert_key_is_not_created(db_manager, restored: Restored, key: str, tables: tuple[str, str]) -> None:
    assert await fk_state(db_manager, restored.db_name, key) == "missing"
    assert key_commands(restored, key)[-1] == "ADD NOT VALID", "the key is given up only after the NOT VALID try"
    assert not_created_warnings(restored, key) == [NOT_CREATED_WARNING.format(key=key_label(key, tables))], (
        f"one warning must name the key and both tables without the answer of the server, got: {restored.warnings}"
    )
    assert not not_valid_warnings(restored, key)


@pytest.mark.min_pg(120000)
@pytest.mark.parametrize("path", EXCLUDE_PATHS)
async def test_plain_to_partitioned_key_is_valid_when_no_row_refers_to_excluded_partition(
    path, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_without_partition(
        path, "a_ref_spare", source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_valid(db_manager, restored, "a_src_fk")
    assert await count_rows(db_manager, target_db, "a_ref") == 100
    assert await count_rows(db_manager, target_db, "a_src") == 100


@pytest.mark.min_pg(120000)
@pytest.mark.parametrize("path", EXCLUDE_PATHS)
async def test_plain_to_partitioned_key_is_not_valid_when_rows_refer_to_excluded_partition(
    path, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_without_partition(
        path, "a_ref_old", source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_not_valid(db_manager, restored, "a_src_fk", ("a_src", "a_ref"))
    assert await count_rows(db_manager, target_db, "a_ref") == 70
    assert await count_rows(db_manager, target_db, "a_src") == 100


@pytest.mark.min_pg(120000)
@pytest.mark.parametrize("path", EXCLUDE_PATHS)
async def test_partitioned_to_partitioned_key_is_valid_when_no_row_refers_to_excluded_partition(
    path, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_without_partition(
        path, "b_ref_spare", source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_valid(db_manager, restored, "b_src_fk")
    assert await count_rows(db_manager, target_db, "b_ref") == 100
    assert await count_rows(db_manager, target_db, "b_src") == 100


@pytest.mark.min_pg(NOT_VALID_ON_PARTITIONED)
@pytest.mark.parametrize("path", EXCLUDE_PATHS)
async def test_partitioned_to_partitioned_key_is_not_valid_when_rows_refer_to_excluded_partition(
    path, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_without_partition(
        path, "b_ref_old", source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_not_valid(db_manager, restored, "b_src_fk", ("b_src", "b_ref"))
    assert await count_rows(db_manager, target_db, "b_ref") == 70
    assert await count_rows(db_manager, target_db, "b_src") == 100


@pytest.mark.min_pg(120000)
@pytest.mark.max_pg(NOT_VALID_ON_PARTITIONED)
@pytest.mark.parametrize("path", EXCLUDE_PATHS)
async def test_partitioned_to_partitioned_key_is_not_created_when_rows_refer_to_excluded_partition(
    path, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_without_partition(
        path, "b_ref_old", source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_not_created(db_manager, restored, "b_src_fk", ("b_src", "b_ref"))
    assert await count_rows(db_manager, target_db, "b_ref") == 70
    assert await count_rows(db_manager, target_db, "b_src") == 100


@pytest.mark.min_pg(120000)
@pytest.mark.parametrize("path", EXCLUDE_PATHS)
async def test_partitioned_to_partitioned_key_is_valid_when_partitions_of_both_tables_are_excluded(
    path, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_without_partition(
        path, "b_ref_spare_and_b_src_low", source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_valid(db_manager, restored, "b_src_fk")
    assert await count_rows(db_manager, target_db, "b_ref") == 100
    assert await count_rows(db_manager, target_db, "b_src") == 50


@pytest.mark.min_pg(110000)
async def test_partitioned_to_trimmed_plain_key_is_valid_when_rows_match(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict(
        "c_ref_trimmed_matching.py", source_db, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_valid(db_manager, restored, "c_src_fk")
    assert await count_rows(db_manager, target_db, "c_ref") == 50
    assert await count_rows(db_manager, target_db, "c_src") == 50


@pytest.mark.min_pg(NOT_VALID_ON_PARTITIONED)
async def test_partitioned_to_trimmed_plain_key_is_not_valid_when_rows_do_not_match(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict(
        "c_ref_trimmed_missing.py", source_db, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_not_valid(db_manager, restored, "c_src_fk", ("c_src", "c_ref"))
    assert await count_rows(db_manager, target_db, "c_ref") == 75
    assert await count_rows(db_manager, target_db, "c_src") == 50


@pytest.mark.min_pg(110000)
@pytest.mark.max_pg(NOT_VALID_ON_PARTITIONED)
async def test_partitioned_to_trimmed_plain_key_is_not_created_when_rows_do_not_match(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict(
        "c_ref_trimmed_missing.py", source_db, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_not_created(db_manager, restored, "c_src_fk", ("c_src", "c_ref"))
    assert await count_rows(db_manager, target_db, "c_ref") == 75
    assert await count_rows(db_manager, target_db, "c_src") == 50


async def test_plain_key_is_not_valid_when_only_referenced_table_is_trimmed(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict("d_ref_trimmed.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_not_valid(db_manager, restored, "d_src_fk", ("d_src", "d_ref"))
    assert await count_rows(db_manager, target_db, "d_ref") == 50
    assert await count_rows(db_manager, target_db, "d_src") == 100


async def test_plain_key_is_valid_when_both_tables_are_trimmed_the_same_way(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict("d_both_trimmed.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_valid(db_manager, restored, "d_src_fk")
    assert await count_rows(db_manager, target_db, "d_ref") == 50
    assert await count_rows(db_manager, target_db, "d_src") == 50


async def test_plain_key_is_not_valid_when_referenced_table_has_no_data_in_dump(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict("d_ref_no_data.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_not_valid(db_manager, restored, "d_src_fk", ("d_src", "d_ref"))
    assert await count_rows(db_manager, target_db, "d_ref") == 0
    assert await count_rows(db_manager, target_db, "d_src") == 100


async def test_plain_key_is_valid_when_referenced_table_has_no_data_and_every_reference_is_null(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict("d_ref_no_data.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_valid(db_manager, restored, "d_null_src_fk")
    assert key_commands(restored, "d_null_src_fk") == ["ADD"]
    assert await count_rows(db_manager, target_db, "d_null_src") == 10


@pytest.mark.min_pg(110000)
async def test_partitioned_to_plain_key_is_valid_when_referenced_table_has_no_data_and_every_reference_is_null(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict("c_ref_no_data.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_valid(db_manager, restored, "c_null_src_fk")
    assert key_commands(restored, "c_null_src_fk") == ["ADD"]
    assert await count_rows(db_manager, target_db, "c_null_src") == 10


@pytest.mark.parametrize(
    ("dict_file", "commands"),
    [
        pytest.param("d_both_trimmed.py", ["ADD"], id="rows_match"),
        pytest.param("d_ref_trimmed.py", ["ADD", "ADD NOT VALID"], id="rows_do_not_match"),
        pytest.param("d_ref_no_data.py", ["ADD", "ADD NOT VALID"], id="referenced_table_has_no_data"),
    ],
)
async def test_plain_key_is_created_in_the_usual_way_first_and_never_validated(
    dict_file, commands, source_db, target_db, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict(dict_file, source_db, target_db, db_params, pg_anon_runner, caplog)

    assert key_commands(restored, "d_src_fk") == commands


@pytest.mark.min_pg(120000)
@pytest.mark.parametrize(("partition", "key"), [("a_ref_spare", "a_src_fk"), ("b_ref_spare", "b_src_fk")])
async def test_key_to_partitioned_table_is_valid_on_every_partition(
    partition, key, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """The server keeps a copy of the key for each partition; the usual way marks all of them as valid."""
    restored = await restore_without_partition(
        "exclude_on_dump", partition, source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    assert key_commands(restored, key) == ["ADD"]
    rows = await db_manager.fetch(
        target_db,
        f"""
        SELECT con.conname, con.convalidated FROM pg_constraint con
        WHERE con.contype = 'f'
          AND con.confrelid::regclass::text LIKE 'pfk.{key[0]}_ref%'
        """,
    )
    assert len(rows) > 1, "the key must have copies for the partitions"
    not_valid = [row["conname"] for row in rows if not row["convalidated"]]
    assert not not_valid, f"copies of the key are not valid: {not_valid}"


@pytest.mark.min_pg(NOT_VALID_ON_PARTITIONED)
async def test_partitioned_to_plain_key_is_not_valid_when_referenced_table_has_no_data_in_dump(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict("c_ref_no_data.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_not_valid(db_manager, restored, "c_src_fk", ("c_src", "c_ref"))
    assert key_commands(restored, "c_src_fk") == ["ADD", "ADD NOT VALID"]
    assert await count_rows(db_manager, target_db, "c_ref") == 0
    assert await count_rows(db_manager, target_db, "c_src") == 50


@pytest.mark.min_pg(110000)
@pytest.mark.max_pg(NOT_VALID_ON_PARTITIONED)
async def test_partitioned_to_plain_key_is_not_created_when_referenced_table_has_no_data_in_dump(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict("c_ref_no_data.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_not_created(db_manager, restored, "c_src_fk", ("c_src", "c_ref"))
    assert key_commands(restored, "c_src_fk") == ["ADD", "ADD NOT VALID"]
    assert await count_rows(db_manager, target_db, "c_ref") == 0
    assert await count_rows(db_manager, target_db, "c_src") == 50


async def test_plain_key_is_valid_when_raw_sql_keeps_every_referenced_row(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    """A table dumped with raw_sql is handled as a table without all its rows, so pg_anon creates the key itself."""
    restored = await restore_with_dict("d_ref_raw_sql_all.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_valid(db_manager, restored, "d_src_fk")
    assert key_commands(restored, "d_src_fk") == ["ADD"]
    assert await count_rows(db_manager, target_db, "d_ref") == 100
    assert await count_rows(db_manager, target_db, "d_src") == 100


async def test_plain_key_is_not_valid_when_raw_sql_drops_referenced_rows(
    source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_with_dict("d_ref_raw_sql_half.py", source_db, target_db, db_params, pg_anon_runner, caplog)

    await assert_key_is_not_valid(db_manager, restored, "d_src_fk", ("d_src", "d_ref"))
    assert key_commands(restored, "d_src_fk") == ["ADD", "ADD NOT VALID"]
    assert await count_rows(db_manager, target_db, "d_ref") == 50
    assert await count_rows(db_manager, target_db, "d_src") == 100


async def assert_full_dump_keeps_keys_valid(full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog, keys):
    restored = await restore(pg_anon_runner, db_params, target_db, caplog, in_dir=full_dump)

    for key in keys:
        assert await fk_state(db_manager, target_db, key) == "valid", key
    assert not fk_warnings(restored)


async def test_full_dump_keeps_every_key_valid_without_warnings(
    full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    await assert_full_dump_keeps_keys_valid(
        full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog, ["d_src_fk"]
    )
    assert await count_rows(db_manager, target_db, "d_src") == 100


@pytest.mark.min_pg(110000)
async def test_full_dump_keeps_key_to_a_partitioned_table_valid(
    full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    await assert_full_dump_keeps_keys_valid(
        full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog, ["c_src_fk"]
    )


@pytest.mark.min_pg(120000)
async def test_full_dump_keeps_keys_between_partitioned_tables_valid(
    full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    await assert_full_dump_keeps_keys_valid(
        full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog, ["a_src_fk", "b_src_fk"]
    )


@pytest.mark.min_pg(120000)
async def test_dump_metadata_does_not_name_excluded_partitions(source_db, db_params, pg_anon_runner):
    out = await dump(
        pg_anon_runner,
        db_params,
        source_db,
        "no_names_of_excluded_partitions",
        dict_file="empty.py",
        exclude_file="exclude_b_ref_spare_and_b_src_low.py",
    )

    for file_name in ("structure.json", "metadata.json"):
        metadata = (Path(out) / file_name).read_text(encoding="utf-8")
        assert "b_ref_spare" not in metadata, f"a referenced partition left out of the dump is named in {file_name}"
        assert "b_src_low" not in metadata, f"a referrer partition left out of the dump is named in {file_name}"


@pytest.mark.min_pg(120000)
@pytest.mark.parametrize("path", EXCLUDE_PATHS)
async def test_keys_to_two_level_table_are_not_valid_when_sub_partition_is_excluded(
    path, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_without_partition(
        path, "e_ref_y1_a", source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    assert await fk_state(db_manager, target_db, "e_src_fk") == "not valid"
    assert await fk_state(db_manager, target_db, "e_mid_src_fk") == "not valid"
    key_warnings = [
        NOT_VALID_WARNING.format(key=key_label("e_mid_src_fk", ("e_mid_src", "e_ref_y1"))),
        NOT_VALID_WARNING.format(key=key_label("e_src_fk", ("e_src", "e_ref"))),
    ]
    summary = NOT_VALID_SUMMARY.format(count="2 foreign keys")
    fk_lines = [message for message in restored.warnings if "created as NOT VALID" in message]
    assert sorted(fk_lines[:-1]) == key_warnings, f"one warning for each key, got: {restored.warnings}"
    assert fk_lines[-1] == summary, f"one final warning with the number of keys, got: {restored.warnings}"
    assert await count_rows(db_manager, target_db, "e_ref") == 70
    assert await count_rows(db_manager, target_db, "e_mid_src") == 100


@pytest.mark.min_pg(120000)
@pytest.mark.parametrize("path", EXCLUDE_PATHS)
async def test_key_to_excluded_middle_level_is_not_created_and_key_to_root_is_not_valid(
    path, source_db, full_dump, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    restored = await restore_without_partition(
        path, "e_ref_y1", source_db, full_dump, target_db, db_params, pg_anon_runner, caplog
    )

    await assert_key_is_not_valid(db_manager, restored, "e_src_fk", ("e_src", "e_ref"))
    assert await fk_state(db_manager, target_db, "e_mid_src_fk") == "missing", "no key to a table that is not created"
    assert not not_valid_warnings(restored, "e_mid_src_fk")
    assert await count_rows(db_manager, target_db, "e_ref") == 20
    assert await count_rows(db_manager, target_db, "e_mid_src") == 100
