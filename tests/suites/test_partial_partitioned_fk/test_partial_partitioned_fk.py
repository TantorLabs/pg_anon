from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode
from pg_anon.common.toc import read_toc_lines

# The suite is about a foreign key that refers to a partitioned table, possible since PG 12.
pytestmark = pytest.mark.min_pg(120000)


def _pg_restore_bin(db_params) -> str:
    config = yaml.safe_load(Path(db_params.test_config).read_text())
    return config["pg-utils-versions"]["default"]["pg_restore"]


async def _fk_present(db_manager, db: str) -> bool:
    rows = await db_manager.fetch(
        db,
        """
        SELECT 1 FROM pg_constraint
        WHERE conname = 'attachment_evt_fk'
          AND connamespace = 'fkpart'::regnamespace
    """,
    )
    return bool(rows)


async def _table_present(db_manager, db: str, schema: str, table: str) -> bool:
    rows = await db_manager.fetch(
        db,
        f"""
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}' AND c.relname = '{table}'
    """,
    )
    return bool(rows)


async def test_dump_metadata_links_partitioned_index_to_leaves(
    source_db,
    db_params,
    pg_anon_runner,
):
    out = output_path("metadata_parent_index")

    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE

    metadata = json.loads((Path(out) / "structure.json").read_text(encoding="utf-8"))
    indexes = metadata.get("indexes") or {}

    parent_entry = indexes.get("events_occurred_at_idx")
    assert parent_entry, f"events_occurred_at_idx must be in metadata.indexes; got keys: {sorted(indexes.keys())}"
    assert parent_entry.get("parent_index_name") is None, (
        f"top-level partitioned index must have parent_index_name=None, got {parent_entry.get('parent_index_name')!r}"
    )

    occurred_at_leaves = [
        k for k in indexes if indexes[k].get("table", "").startswith("events_2025") and "occurred_at" in k
    ]
    assert len(occurred_at_leaves) == 2, (
        f"expected one occurred_at leaf-index per partition (2 leaves); got: {sorted(occurred_at_leaves)}"
    )
    for k in occurred_at_leaves:
        entry = indexes[k]
        assert entry.get("parent_index_schema") == "fkpart", (
            f"leaf index {k!r} must have parent_index_schema='fkpart', got {entry.get('parent_index_schema')!r}"
        )
        assert entry.get("parent_index_name") == "events_occurred_at_idx", (
            f"leaf index {k!r} must have parent_index_name='events_occurred_at_idx', "
            f"got {entry.get('parent_index_name')!r}"
        )

    pk_leaves = [k for k in indexes if indexes[k].get("table", "").startswith("events_2025") and k.endswith("_pkey")]
    for k in pk_leaves:
        entry = indexes[k]
        assert entry.get("parent_index_name") == "events_pkey", (
            f"leaf PK {k!r} must point at parent_index_name='events_pkey', got {entry.get('parent_index_name')!r}"
        )


async def test_restore_drops_index_attach_when_parent_index_excluded(
    source_db,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    out = output_path("index_attach_excluded_parent")

    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE

    # a dump marks an object to skip by its OIDs; the INDEX ATTACH entries that need it follow it
    toc = read_toc_lines(_pg_restore_bin(db_params), Path(out) / "post_data.backup")
    skipped = [
        f"{match.group(1)}:{match.group(2)}"
        for line in toc
        if (match := re.match(r"\d+;\s+(\d+)\s+(\d+)\s+INDEX fkpart events_occurred_at_idx ", line))
    ]
    assert len(skipped) == 1, f"fixture must produce the entry of the partitioned index; got {skipped}"
    attach_entries = [line for line in toc if re.search(r" INDEX ATTACH fkpart events_2025_q\d_occurred_at_idx ", line)]
    assert len(attach_entries) == 2, f"fixture must produce two INDEX ATTACH entries; got {attach_entries}"
    metadata_path = Path(out) / "structure.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["dump_dependents_to_skip"].extend(skipped)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must complete even when the parent partitioned index is skipped"

    rows = await db_manager.fetch(
        target_db,
        """
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'fkpart' AND c.relname = 'events_occurred_at_idx'
    """,
    )
    assert not rows, "events_occurred_at_idx was skipped — it must be absent on target"

    inh = await db_manager.fetch(
        target_db,
        """
        SELECT child.relname AS child FROM pg_inherits i
        JOIN pg_class child  ON child.oid  = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relnamespace = 'fkpart'::regnamespace
          AND parent.relname = 'events'
    """,
    )
    assert {r["child"] for r in inh} == {"events_2025_q1", "events_2025_q2"}, (
        "leaf partitions of fkpart.events must still be attached on target"
    )


async def test_partial_whitelist_leaves_and_referrer_keeps_partitioning_and_fk(
    source_db,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    out = output_path("whitelist_leaves_and_referrer")

    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
            f"--partial-tables-dict-file={input_dict('whitelist_leaves_and_referrer.py')}",
        ],
    )
    assert res.result_code == ResultCode.DONE, "dump must succeed"

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must succeed"

    parent_rows = await db_manager.fetch(
        target_db,
        """
        SELECT c.relkind::text AS relkind FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'fkpart' AND c.relname = 'events'
    """,
    )
    assert parent_rows, "partitioned root fkpart.events must exist on target"
    assert parent_rows[0]["relkind"] == "p", f"fkpart.events must be relkind='p', got '{parent_rows[0]['relkind']}'"

    inh = await db_manager.fetch(
        target_db,
        """
        SELECT child.relname AS child FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relnamespace = 'fkpart'::regnamespace
          AND parent.relname = 'events'
    """,
    )
    assert {r["child"] for r in inh} == {"events_2025_q1", "events_2025_q2"}, (
        f"both leaves must be attached as partitions of fkpart.events; got {sorted(r['child'] for r in inh)}"
    )

    assert await _fk_present(db_manager, target_db), "FK must survive when all leaves and the referrer are whitelisted"


async def test_partial_whitelist_subpartition_leaves_includes_all_ancestors(
    source_db,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    out = output_path("subpartition_leaves")

    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
            f"--partial-tables-dict-file={input_dict('whitelist_subpartition_leaves.py')}",
        ],
    )
    assert res.result_code == ResultCode.DONE, "dump must succeed"

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must succeed"

    ancestors = await db_manager.fetch(
        target_db,
        """
        SELECT c.relname, c.relkind::text AS relkind
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'fkpart' AND c.relname IN ('metrics', 'metrics_q1')
        ORDER BY c.relname
    """,
    )
    actual = [(r["relname"], r["relkind"]) for r in ancestors]
    assert actual == [("metrics", "p"), ("metrics_q1", "p")], (
        f"both partitioned ancestors must be re-created as relkind='p'; got {actual}"
    )

    inh = await db_manager.fetch(
        target_db,
        """
        SELECT parent.relname AS parent, child.relname AS child
        FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relnamespace = 'fkpart'::regnamespace
          AND parent.relname IN ('metrics', 'metrics_q1')
        ORDER BY parent.relname, child.relname
    """,
    )
    pairs = {(r["parent"], r["child"]) for r in inh}
    assert pairs == {
        ("metrics", "metrics_q1"),
        ("metrics", "metrics_q2"),
        ("metrics_q1", "metrics_q1_t1"),
        ("metrics_q1", "metrics_q1_other"),
    }, f"sub-partition attach chain must be fully re-created; got {sorted(pairs)}"


async def test_partial_whitelist_legacy_inherits_child_pulls_parent_without_its_rows(
    source_db,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    out = output_path("legacy_inherits_child")

    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
            f"--partial-tables-dict-file={input_dict('whitelist_legacy_inherits_child.py')}",
        ],
    )
    assert res.result_code == ResultCode.DONE, "dump on legacy INHERITS child must succeed"

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must succeed: the child cannot be created without its parent"

    assert await _table_present(db_manager, target_db, "fkpart", "animal"), (
        "legacy INHERITS parent must be pulled into the dump with its child"
    )
    rows = await db_manager.fetch(target_db, "SELECT count(*) AS n FROM ONLY fkpart.animal")
    assert rows[0]["n"] == 0, "rows of the parent itself are not in the whitelist and must not be dumped"


async def _events_descr(db_manager, db: str, evt_id: int) -> str | None:
    rows = await db_manager.fetch(db, f"SELECT descr FROM fkpart.events WHERE id = {evt_id}")
    return rows[0]["descr"] if rows else None


async def test_anon_rule_on_partitioned_parent_applies_to_leaves(
    source_db,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    out = output_path("anon_parent_rule")

    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('anon_parent_rule.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, "dump must succeed"

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must succeed"

    q1 = await _events_descr(db_manager, target_db, 1)
    q2 = await _events_descr(db_manager, target_db, 2)
    assert q1 != "secret-q1", (
        f"the rule of the parent fkpart.events must mask descr in partition events_2025_q1, got {q1!r}"
    )
    assert q2 != "secret-q2", (
        f"the rule of the parent fkpart.events must mask descr in partition events_2025_q2, got {q2!r}"
    )


async def test_anon_rule_on_partition_leaves_directly(
    source_db,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    out = output_path("anon_leaf_rules")

    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('anon_leaf_rules.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, "dump must succeed"

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must succeed"

    q1 = await _events_descr(db_manager, target_db, 1)
    q2 = await _events_descr(db_manager, target_db, 2)
    assert q1 != "secret-q1", f"the rule of events_2025_q1 must mask descr, got {q1!r}"
    assert q2 != "secret-q2", f"the rule of events_2025_q2 must mask descr, got {q2!r}"


async def test_anon_partition_rule_overrides_parent_rule(
    source_db,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    out = output_path("anon_parent_and_leaf_rules")

    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('anon_parent_and_leaf_rules.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, "dump must succeed"

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
            "--drop-custom-check-constr",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must succeed"

    q1 = await _events_descr(db_manager, target_db, 1)
    q2 = await _events_descr(db_manager, target_db, 2)
    assert q1 == "REDACTED_Q1", (
        f"the own rule of partition events_2025_q1 must win over the rule of the parent, got {q1!r}"
    )
    assert q2 not in ("secret-q2", "REDACTED_Q1"), (
        f"partition events_2025_q2 has no rule of its own, so the rule of the parent must apply, got {q2!r}"
    )
