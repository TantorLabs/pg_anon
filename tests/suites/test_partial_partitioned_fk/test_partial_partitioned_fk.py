from __future__ import annotations

import json
from pathlib import Path

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


async def _fk_present(db_manager, db: str) -> bool:
    rows = await db_manager.fetch(db, """
        SELECT 1 FROM pg_constraint
        WHERE conname = 'attachment_evt_fk'
          AND connamespace = 'fkpart'::regnamespace
    """)
    return bool(rows)


async def _per_leaf_clones(db_manager, db: str) -> int:
    rows = await db_manager.fetch(db, """
        SELECT count(*) AS c FROM pg_constraint
        WHERE conrelid = 'fkpart.attachment'::regclass
          AND contype = 'f' AND conparentid <> 0
    """)
    return rows[0]["c"]


async def _table_present(db_manager, db: str, schema: str, table: str) -> bool:
    rows = await db_manager.fetch(db, f"""
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}' AND c.relname = '{table}'
    """)
    return bool(rows)


async def test_dump_metadata_links_partitioned_index_to_leaves(
    source_db, db_params, pg_anon_runner,
):
    out = output_path("metadata_parent_index")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    metadata = json.loads((Path(out) / "metadata.json").read_text(encoding="utf-8"))
    indexes = metadata.get("indexes") or {}

    parent_entry = indexes.get("events_occurred_at_idx")
    assert parent_entry, (
        f"events_occurred_at_idx must be in metadata.indexes; got keys: "
        f"{sorted(indexes.keys())}"
    )
    assert parent_entry.get("parent_index_name") is None, (
        f"top-level partitioned index must have parent_index_name=None, got "
        f"{parent_entry.get('parent_index_name')!r}"
    )

    occurred_at_leaves = [
        k for k in indexes
        if indexes[k].get("table", "").startswith("events_2025")
           and "occurred_at" in k
    ]
    assert len(occurred_at_leaves) == 2, (
        f"expected one occurred_at leaf-index per partition (2 leaves); got: "
        f"{sorted(occurred_at_leaves)}"
    )
    for k in occurred_at_leaves:
        entry = indexes[k]
        assert entry.get("parent_index_schema") == "fkpart", (
            f"leaf index {k!r} must have parent_index_schema='fkpart', got "
            f"{entry.get('parent_index_schema')!r}"
        )
        assert entry.get("parent_index_name") == "events_occurred_at_idx", (
            f"leaf index {k!r} must have parent_index_name='events_occurred_at_idx', "
            f"got {entry.get('parent_index_name')!r}"
        )

    pk_leaves = [
        k for k in indexes
        if indexes[k].get("table", "").startswith("events_2025")
           and k.endswith("_pkey")
    ]
    for k in pk_leaves:
        entry = indexes[k]
        assert entry.get("parent_index_name") == "events_pkey", (
            f"leaf PK {k!r} must point at parent_index_name='events_pkey', "
            f"got {entry.get('parent_index_name')!r}"
        )


async def test_restore_drops_index_attach_when_parent_index_excluded(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("index_attach_excluded_parent")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    metadata_path = Path(out) / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    indexes = metadata.get("indexes") or {}
    assert "events_occurred_at_idx" in indexes, (
        "fixture must produce events_occurred_at_idx in metadata.indexes"
    )
    indexes["events_occurred_at_idx"]["is_excluded"] = True
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE, (
        "restore must complete even when the parent partitioned index is "
        "marked is_excluded=True"
    )

    rows = await db_manager.fetch(target_db, """
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'fkpart' AND c.relname = 'events_occurred_at_idx'
    """)
    assert not rows, (
        "events_occurred_at_idx was marked excluded — it must be absent on target"
    )

    inh = await db_manager.fetch(target_db, """
        SELECT child.relname AS child FROM pg_inherits i
        JOIN pg_class child  ON child.oid  = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relnamespace = 'fkpart'::regnamespace
          AND parent.relname = 'events'
    """)
    assert {r["child"] for r in inh} == {"events_2025_q1", "events_2025_q2"}, (
        "leaf partitions of fkpart.events must still be attached on target"
    )


async def test_full_clone_preserves_fk_on_partitioned(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("full_clone")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE

    assert await _fk_present(db_manager, target_db), \
        "user-defined FK on partitioned root must survive full clone"
    clones = await _per_leaf_clones(db_manager, target_db)
    assert clones >= 2, (
        f"expected at least 2 per-leaf FK clones, got {clones}"
    )


async def test_partial_whitelist_leaves_and_referrer_keeps_partitioning_and_fk(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("whitelist_leaves_and_referrer")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        f"--partial-tables-dict-file={input_dict('whitelist_leaves_and_referrer.py')}",
    ])
    assert res.result_code == ResultCode.DONE, "dump must succeed"

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE, "restore must succeed"

    parent_rows = await db_manager.fetch(target_db, """
        SELECT c.relkind::text AS relkind FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'fkpart' AND c.relname = 'events'
    """)
    assert parent_rows, "partitioned root fkpart.events must exist on target"
    assert parent_rows[0]["relkind"] == "p", (
        f"fkpart.events must be relkind='p', got '{parent_rows[0]['relkind']}'"
    )

    inh = await db_manager.fetch(target_db, """
        SELECT child.relname AS child FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relnamespace = 'fkpart'::regnamespace
          AND parent.relname = 'events'
    """)
    assert {r["child"] for r in inh} == {"events_2025_q1", "events_2025_q2"}, (
        f"both leaves must be attached as partitions of fkpart.events; "
        f"got {sorted(r['child'] for r in inh)}"
    )

    assert await _fk_present(db_manager, target_db), (
        "FK must survive when all leaves and the referrer are whitelisted"
    )


async def test_partial_whitelist_subpartition_leaves_includes_all_ancestors(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("subpartition_leaves")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        f"--partial-tables-dict-file={input_dict('whitelist_subpartition_leaves.py')}",
    ])
    assert res.result_code == ResultCode.DONE, "dump must succeed"

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE, "restore must succeed"

    ancestors = await db_manager.fetch(target_db, """
        SELECT c.relname, c.relkind::text AS relkind
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'fkpart' AND c.relname IN ('metrics', 'metrics_q1')
        ORDER BY c.relname
    """)
    actual = [(r["relname"], r["relkind"]) for r in ancestors]
    assert actual == [("metrics", "p"), ("metrics_q1", "p")], (
        f"both partitioned ancestors must be re-created as relkind='p'; got {actual}"
    )

    inh = await db_manager.fetch(target_db, """
        SELECT parent.relname AS parent, child.relname AS child
        FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relnamespace = 'fkpart'::regnamespace
          AND parent.relname IN ('metrics', 'metrics_q1')
        ORDER BY parent.relname, child.relname
    """)
    pairs = {(r["parent"], r["child"]) for r in inh}
    assert pairs == {
        ("metrics", "metrics_q1"),
        ("metrics", "metrics_q2"),
        ("metrics_q1", "metrics_q1_t1"),
        ("metrics_q1", "metrics_q1_other"),
    }, (
        f"sub-partition attach chain must be fully re-created; got {sorted(pairs)}"
    )


async def test_partial_whitelist_legacy_inherits_child_does_not_pull_parent(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("legacy_inherits_child")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        f"--partial-tables-dict-file={input_dict('whitelist_legacy_inherits_child.py')}",
    ])
    assert res.result_code == ResultCode.DONE, "dump on legacy INHERITS child must succeed"

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    if res.result_code == ResultCode.DONE:
        animal = await db_manager.fetch(target_db, """
            SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'fkpart' AND c.relname = 'animal'
        """)
        assert not animal, (
            "legacy INHERITS parent must not be auto-pulled into the dump"
        )


async def test_partial_blacklist_one_leaf_drops_fk(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    out = output_path("blacklist_one_leaf")

    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        f"--partial-tables-exclude-dict-file={input_dict('exclude_one_leaf.py')}",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE, (
        "restore must complete even when an FK is dropped due to leaf blacklist"
    )

    assert not await _table_present(db_manager, target_db, "fkpart", "events_2025_q1"), (
        "blacklisted leaf partition must not appear on target"
    )

    assert not await _fk_present(db_manager, target_db), (
        "FK must be dropped when the blacklist excludes one of the partition leaves"
    )

    assert await _table_present(db_manager, target_db, "fkpart", "attachment"), (
        "referrer table must be restored even when the FK is dropped"
    )
