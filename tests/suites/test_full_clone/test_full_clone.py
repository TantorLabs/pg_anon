from __future__ import annotations

import math
from decimal import Decimal

from tests.infrastructure.assertions import checksum_tables, diff_catalog, diff_schema, list_tables

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode


async def _dump_and_restore(pg_anon_runner, db_params, source_db, target_db, out):
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ])
    assert res.result_code == ResultCode.DONE, "dump with empty dict must succeed"

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE, "restore must succeed"


async def test_full_clone_preserves_every_table(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Every source table/partition must appear in target with the same schema."""
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("preserve_tables"))
    missing = await diff_schema(db_manager, source_db, target_db)
    assert not missing, f"columns missing in target: {missing}"


async def test_full_clone_preserves_catalog_objects(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Schemas, views, materialized views, types, functions, triggers, policies,
    sequences, indexes, and constraints must all round-trip.
    """
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("preserve_catalog"))
    diffs = await diff_catalog(db_manager, source_db, target_db)
    assert not diffs, f"catalog differences (source→target): {diffs}"


async def test_full_clone_preserves_row_content(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Row-for-row checksums on key tables must match across source and target."""
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("preserve_rows"))

    tables = await list_tables(db_manager, source_db)
    src = await checksum_tables(db_manager, source_db, tables)
    tgt = await checksum_tables(db_manager, target_db, tables)

    diffs = {k: (src[k], tgt.get(k)) for k in src if src[k] != tgt.get(k)}
    assert not diffs, f"content differs on tables: {diffs}"


async def test_full_clone_preserves_data_types_round_trip(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Every PG type in data_types.sample must survive dump/restore unchanged."""
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("preserve_types"))

    src = await db_manager.fetch(source_db, "SELECT * FROM data_types.sample ORDER BY id")
    tgt = await db_manager.fetch(target_db, "SELECT * FROM data_types.sample ORDER BY id")
    assert len(src) == len(tgt)
    for s_row, t_row in zip(src, tgt, strict=True):
        assert dict(s_row) == dict(t_row), f"row differs: {dict(s_row)} vs {dict(t_row)}"


async def test_full_clone_preserves_data_type_edge_cases(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Boundary values (NaN/Infinity, BC/infinity dates, 1MB+ text/bytea TOAST,
    NUL-bytes, escape sequences) must round-trip identically.
    """
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("preserve_edge_cases"))

    src = await db_manager.fetch(source_db,
        "SELECT * FROM data_types.edge_cases ORDER BY id", text_dates=True)
    tgt = await db_manager.fetch(target_db,
        "SELECT * FROM data_types.edge_cases ORDER BY id", text_dates=True)
    assert len(src) == len(tgt)

    def _is_nan(x):
        if isinstance(x, float):
            return math.isnan(x)
        if isinstance(x, Decimal):
            return x.is_nan()
        return False

    def _equal(a, b):
        # IEEE 754 / Decimal: NaN != NaN, but we still want to treat them as round-tripped.
        if _is_nan(a) and _is_nan(b):
            return True
        return a == b

    for s_row, t_row in zip(src, tgt, strict=True):
        s_dict, t_dict = dict(s_row), dict(t_row)
        assert set(s_dict) == set(t_dict)
        diffs = {k: (s_dict[k], t_dict[k]) for k in s_dict if not _equal(s_dict[k], t_dict[k])}
        assert not diffs, (
            f"edge-case row {s_row['id']} ({s_row['label']}) differs: {diffs}"
        )


async def test_full_clone_restores_circular_fk_tables(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Two tables with mutual DEFERRABLE INITIALLY DEFERRED FKs must restore
    cleanly via parallel COPY. Exercises session_replication_role='replica'.
    """
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("circular_fk"))

    src_a = await db_manager.fetch(source_db, "SELECT id, peer_id FROM circular_fk.a ORDER BY id")
    tgt_a = await db_manager.fetch(target_db, "SELECT id, peer_id FROM circular_fk.a ORDER BY id")
    src_b = await db_manager.fetch(source_db, "SELECT id, peer_id FROM circular_fk.b ORDER BY id")
    tgt_b = await db_manager.fetch(target_db, "SELECT id, peer_id FROM circular_fk.b ORDER BY id")
    assert [dict(r) for r in src_a] == [dict(r) for r in tgt_a]
    assert [dict(r) for r in src_b] == [dict(r) for r in tgt_b]


async def test_full_clone_preserves_table_variants(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Exotic table features must round-trip: GENERATED AS IDENTITY, UNLOGGED,
    INHERITS, custom SEQUENCE (OWNED BY, non-default increment), RULE ON INSERT
    DO INSTEAD on a view, and event trigger functions.
    """
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("preserve_variants"))

    src_ident = await db_manager.fetch(source_db,
        "SELECT id, label, seq_val FROM variants.with_identity ORDER BY id")
    tgt_ident = await db_manager.fetch(target_db,
        "SELECT id, label, seq_val FROM variants.with_identity ORDER BY id")
    assert [dict(r) for r in src_ident] == [dict(r) for r in tgt_ident]

    src_seq = await db_manager.fetch(source_db,
        "SELECT increment_by, min_value, max_value, cache_size "
        "FROM pg_sequences WHERE schemaname='variants' AND sequencename='custom_seq'")
    tgt_seq = await db_manager.fetch(target_db,
        "SELECT increment_by, min_value, max_value, cache_size "
        "FROM pg_sequences WHERE schemaname='variants' AND sequencename='custom_seq'")
    assert [dict(r) for r in src_seq] == [dict(r) for r in tgt_seq], \
        "custom sequence params must round-trip"

    src_unl = await db_manager.fetch(source_db,
        "SELECT relpersistence FROM pg_class "
        "WHERE relname='session_cache' AND relnamespace='variants'::regnamespace")
    tgt_unl = await db_manager.fetch(target_db,
        "SELECT relpersistence FROM pg_class "
        "WHERE relname='session_cache' AND relnamespace='variants'::regnamespace")
    assert src_unl[0]["relpersistence"] == tgt_unl[0]["relpersistence"], \
        "UNLOGGED persistence flag must be preserved"

    src_mammal = await db_manager.fetch(source_db,
        "SELECT name, legs FROM variants.mammal ORDER BY name")
    tgt_mammal = await db_manager.fetch(target_db,
        "SELECT name, legs FROM variants.mammal ORDER BY name")
    assert [dict(r) for r in src_mammal] == [dict(r) for r in tgt_mammal]

    src_rule = await db_manager.fetch(source_db,
        "SELECT rulename FROM pg_rules "
        "WHERE schemaname='variants' AND tablename='active_animals'")
    tgt_rule = await db_manager.fetch(target_db,
        "SELECT rulename FROM pg_rules "
        "WHERE schemaname='variants' AND tablename='active_animals'")
    assert {r["rulename"] for r in src_rule} == {r["rulename"] for r in tgt_rule}


async def test_full_clone_preserves_privileges(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """GRANT/REVOKE on tables and columns and DEFAULT PRIVILEGES must round-trip.

    OWNER is intentionally NOT preserved: pg_anon always passes --no-owner to
    pg_dump/pg_restore because anonymized copies target dev/test DBs where the
    source's roles typically don't exist.
    """
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("preserve_privileges"))

    # Table-level ACLs on privs.public_facing.
    src_acl = await db_manager.fetch(source_db, """
        SELECT relacl::text AS acl FROM pg_class
        WHERE relname = 'public_facing' AND relnamespace = 'privs'::regnamespace
    """)
    tgt_acl = await db_manager.fetch(target_db, """
        SELECT relacl::text AS acl FROM pg_class
        WHERE relname = 'public_facing' AND relnamespace = 'privs'::regnamespace
    """)
    if src_acl and src_acl[0]["acl"]:
        assert src_acl[0]["acl"] == tgt_acl[0]["acl"], \
            f"table-level ACL differs: src={src_acl[0]['acl']} tgt={tgt_acl[0]['acl']}"

    # Column-level ACL on secret_payload (REVOKE SELECT for anon_tester_reader).
    src_col_acl = await db_manager.fetch(source_db, """
        SELECT a.attacl::text AS acl FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        WHERE c.relname = 'public_facing'
          AND c.relnamespace = 'privs'::regnamespace
          AND a.attname = 'secret_payload'
    """)
    tgt_col_acl = await db_manager.fetch(target_db, """
        SELECT a.attacl::text AS acl FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        WHERE c.relname = 'public_facing'
          AND c.relnamespace = 'privs'::regnamespace
          AND a.attname = 'secret_payload'
    """)
    if src_col_acl and src_col_acl[0]["acl"]:
        assert src_col_acl[0]["acl"] == tgt_col_acl[0]["acl"], \
            f"column-level ACL differs: src={src_col_acl[0]['acl']} tgt={tgt_col_acl[0]['acl']}"

    # DEFAULT PRIVILEGES entry for schema privs.
    src_def = await db_manager.fetch(source_db, """
        SELECT defaclacl::text AS acl FROM pg_default_acl d
        JOIN pg_namespace n ON n.oid = d.defaclnamespace
        WHERE n.nspname = 'privs' AND d.defaclobjtype = 'r'
    """)
    tgt_def = await db_manager.fetch(target_db, """
        SELECT defaclacl::text AS acl FROM pg_default_acl d
        JOIN pg_namespace n ON n.oid = d.defaclnamespace
        WHERE n.nspname = 'privs' AND d.defaclobjtype = 'r'
    """)
    if src_def:
        assert [dict(r) for r in src_def] == [dict(r) for r in tgt_def], \
            "DEFAULT PRIVILEGES not preserved"


async def test_full_clone_preserves_publications(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """Logical replication PUBLICATIONs must round-trip: name, FOR-clause,
    publish actions. SUBSCRIPTIONs are out of scope (single-node test cluster
    can't run real logical replication, and a degraded subscription blocks
    DROP DATABASE on teardown).
    """
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("preserve_publications"))

    src_pubs = await db_manager.fetch(source_db, """
        SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
        FROM pg_publication ORDER BY pubname
    """)
    tgt_pubs = await db_manager.fetch(target_db, """
        SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
        FROM pg_publication ORDER BY pubname
    """)
    if src_pubs:
        assert [dict(r) for r in src_pubs] == [dict(r) for r in tgt_pubs], \
            "PUBLICATION metadata not preserved"

    # FOR TABLE pubs.feed mapping must round-trip on pub_for_feed.
    src_rel = await db_manager.fetch(source_db, """
        SELECT p.pubname, c.relname, n.nspname
        FROM pg_publication_rel pr
        JOIN pg_publication p ON p.oid = pr.prpubid
        JOIN pg_class c ON c.oid = pr.prrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE p.pubname = 'pub_for_feed'
    """)
    tgt_rel = await db_manager.fetch(target_db, """
        SELECT p.pubname, c.relname, n.nspname
        FROM pg_publication_rel pr
        JOIN pg_publication p ON p.oid = pr.prpubid
        JOIN pg_class c ON c.oid = pr.prrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE p.pubname = 'pub_for_feed'
    """)
    if src_rel:
        assert [dict(r) for r in src_rel] == [dict(r) for r in tgt_rel], \
            "PUBLICATION → table mapping not preserved"


async def test_full_clone_with_no_publications_strips_them(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    """`--pg-dump-options=--no-publications` must keep PUBLICATIONs out of
    the resulting target DB even if they exist in source.
    """
    out = output_path("no_publications")
    res = await pg_anon_runner.run("dump", source_db, [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--processes={db_params.test_processes}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        "--pg-dump-options=--no-publications",
    ])
    assert res.result_code == ResultCode.DONE

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
    ])
    assert res.result_code == ResultCode.DONE

    src_pubs = await db_manager.fetch(source_db, "SELECT pubname FROM pg_publication ORDER BY pubname")
    tgt_pubs = await db_manager.fetch(target_db, "SELECT pubname FROM pg_publication ORDER BY pubname")
    if src_pubs:
        assert tgt_pubs == [], (
            f"--no-publications must strip publications; target still has: "
            f"{[r['pubname'] for r in tgt_pubs]}"
        )


async def test_full_clone_with_drop_db_is_idempotent(
    source_db, target_db, db_params, pg_anon_runner,
):
    """Running clone twice into the same target must succeed with --drop-db."""
    out = output_path("idempotent")
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db, out)

    res = await pg_anon_runner.run("restore", target_db, [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={out}",
        "--drop-custom-check-constr",
        "--drop-db",
    ])
    assert res.result_code == ResultCode.DONE


async def test_full_clone_resets_sequence_for_special_char_schema(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    schema = "_SCHM.$complex#имя;@&* a'"
    table = "_TBL.$complex#имя;@&* a'"

    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("seq_reset_special"))

    src_max = await db_manager.fetch(source_db,
        f'SELECT max(id) AS m FROM "{schema}"."{table}"')
    src_max_id = src_max[0]["m"]
    assert src_max_id is not None, "source table must have data to anchor the assertion"

    inserted = await db_manager.fetch(target_db, f"""
        INSERT INTO "{schema}"."{table}" (fld_key, "_FLD.$complex#имя;@&* a'")
        VALUES ('post_restore_probe', 'value')
        RETURNING id
    """)
    new_id = inserted[0]["id"]
    assert new_id > src_max_id, (
        f"sequence not advanced past source max: max(src.id)={src_max_id}, "
        f"new id from target after restore={new_id}"
    )


async def test_full_clone_preserves_index_quirks(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("index_quirks"))

    expected_index_names = {
        "events_user_hash",
        "events_payload_spgist",
        "events_location_gist",
        "events_region_covering",
        "events_name_collate_c",
        "_idx.$weird#имя",
        "events_pkey",
    }

    src_idx = await db_manager.fetch(source_db, """
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'index_quirks'
        ORDER BY indexname
    """)
    tgt_idx = await db_manager.fetch(target_db, """
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'index_quirks'
        ORDER BY indexname
    """)
    src_map = {r["indexname"]: r["indexdef"] for r in src_idx}
    tgt_map = {r["indexname"]: r["indexdef"] for r in tgt_idx}

    missing_on_source = expected_index_names - set(src_map)
    assert not missing_on_source, (
        f"builder didn't produce expected indexes: {missing_on_source}"
    )

    missing_on_target = expected_index_names - set(tgt_map)
    assert not missing_on_target, (
        f"indexes lost on restore: {missing_on_target}"
    )

    diffs = {
        name: (src_map[name], tgt_map[name])
        for name in expected_index_names
        if src_map[name] != tgt_map[name]
    }
    assert not diffs, f"indexdef differs (source -> target): {diffs}"

    src_methods = await db_manager.fetch(source_db, """
        SELECT i.relname AS indexname, am.amname
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am   ON am.oid = i.relam
        WHERE i.relnamespace = 'index_quirks'::regnamespace
        ORDER BY i.relname
    """)
    tgt_methods = await db_manager.fetch(target_db, """
        SELECT i.relname AS indexname, am.amname
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am   ON am.oid = i.relam
        WHERE i.relnamespace = 'index_quirks'::regnamespace
        ORDER BY i.relname
    """)
    assert [dict(r) for r in src_methods] == [dict(r) for r in tgt_methods], \
        "access method (amname) per index must round-trip"

    cover = await db_manager.fetch(target_db, """
        SELECT ix.indnkeyatts, ix.indnatts
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        WHERE i.relname = 'events_region_covering'
          AND i.relnamespace = 'index_quirks'::regnamespace
    """)
    assert cover, "covering index missing on target"
    assert cover[0]["indnkeyatts"] == 1 and cover[0]["indnatts"] == 3, (
        f"covering index lost INCLUDE structure: indnkeyatts={cover[0]['indnkeyatts']}, "
        f"indnatts={cover[0]['indnatts']} (expected 1 and 3)"
    )


async def test_full_clone_preserves_constraint_quirks(
    source_db, target_db, db_manager, db_params, pg_anon_runner,
):
    await _dump_and_restore(pg_anon_runner, db_params, source_db, target_db,
                            output_path("constraint_quirks"))

    src_inh = await db_manager.fetch(source_db, """
        SELECT child.relname AS child, parent.relname AS parent
        FROM pg_inherits i
        JOIN pg_class child  ON child.oid  = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE child.relnamespace = 'constraints_quirks'::regnamespace
        ORDER BY parent.relname, child.relname
    """)
    tgt_inh = await db_manager.fetch(target_db, """
        SELECT child.relname AS child, parent.relname AS parent
        FROM pg_inherits i
        JOIN pg_class child  ON child.oid  = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE child.relnamespace = 'constraints_quirks'::regnamespace
        ORDER BY parent.relname, child.relname
    """)
    assert [dict(r) for r in src_inh] == [dict(r) for r in tgt_inh], \
        "sub-partition tree must round-trip"

    tgt_partitioned = await db_manager.fetch(target_db, """
        SELECT relname FROM pg_class
        WHERE relnamespace = 'constraints_quirks'::regnamespace
          AND relkind = 'p'
        ORDER BY relname
    """)
    assert [r["relname"] for r in tgt_partitioned] == ["subpart_2025_q1", "subpart_root"], \
        "partitioned parents must round-trip as partitioned, not plain tables"

    expected_constraints = {
        "booking_no_overlap":      ("x", True),
        "legacy_child_parent_fk":  ("f", False),
        "evt_attachment_evt_fk":   ("f", True),
    }
    tgt_cs = await db_manager.fetch(target_db, """
        SELECT conname, contype::text AS contype, convalidated
        FROM pg_constraint
        WHERE connamespace = 'constraints_quirks'::regnamespace
          AND conname IN ('booking_no_overlap', 'legacy_child_parent_fk',
                          'evt_attachment_evt_fk')
    """)
    actual = {r["conname"]: (r["contype"], r["convalidated"]) for r in tgt_cs}
    assert actual == expected_constraints, (
        f"named constraints differ on target: expected={expected_constraints}, actual={actual}"
    )

    src_clones = await db_manager.fetch(source_db, """
        SELECT count(*) AS c FROM pg_constraint
        WHERE conrelid = 'constraints_quirks.evt_attachment'::regclass
          AND contype = 'f' AND conparentid <> 0
    """)
    tgt_clones = await db_manager.fetch(target_db, """
        SELECT count(*) AS c FROM pg_constraint
        WHERE conrelid = 'constraints_quirks.evt_attachment'::regclass
          AND contype = 'f' AND conparentid <> 0
    """)
    assert src_clones[0]["c"] == tgt_clones[0]["c"], (
        f"per-leaf FK clones count mismatch: src={src_clones[0]['c']} "
        f"tgt={tgt_clones[0]['c']}"
    )
    assert tgt_clones[0]["c"] >= 3, (
        f"expected at least 3 per-leaf FK clones on target, got {tgt_clones[0]['c']}"
    )

    src_rows = await db_manager.fetch(source_db,
        "SELECT count(*) AS c FROM constraints_quirks.subpart_root")
    tgt_rows = await db_manager.fetch(target_db,
        "SELECT count(*) AS c FROM constraints_quirks.subpart_root")
    assert src_rows[0]["c"] == tgt_rows[0]["c"]

    try:
        await db_manager.execute(target_db, """
            INSERT INTO constraints_quirks.booking (room_id, period)
            VALUES (1, tstzrange('2026-01-02', '2026-01-04'))
        """)
        raised = False
    except Exception:
        raised = True
    assert raised, "EXCLUDE USING gist must reject overlapping range on target"

    try:
        await db_manager.execute(target_db, """
            INSERT INTO constraints_quirks.evt_attachment (evt_id, tenant_id, occurred_at, note)
            VALUES (9999, 1, '2025-01-15 10:00+00', 'orphan probe')
        """)
        fk_raised = False
    except Exception:
        fk_raised = True
    assert fk_raised, (
        "FK on partitioned parent must reject orphan rows on target"
    )
