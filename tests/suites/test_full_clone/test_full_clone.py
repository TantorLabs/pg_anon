from __future__ import annotations

import math
from decimal import Decimal

import pytest

from tests.infrastructure.assertions import checksum_tables, diff_catalog, list_tables, sequence_params

from .conftest import input_dict, output_path, restore
from pg_anon.common.db_utils import create_connection, get_invalid_partitioned_indexes
from pg_anon.common.enums import ResultCode


async def test_full_clone_preserves_catalog_objects(
    source_db,
    clone_db,
    db_manager,
):
    diffs = await diff_catalog(db_manager, source_db, clone_db)
    assert not diffs, f"catalog differences (source→target): {diffs}"


async def test_full_clone_preserves_row_content(
    source_db,
    clone_db,
    db_manager,
):
    tables = await list_tables(db_manager, source_db)
    src = await checksum_tables(db_manager, source_db, tables)
    tgt = await checksum_tables(db_manager, clone_db, tables)

    diffs = {k: (src[k], tgt.get(k)) for k in src if src[k] != tgt.get(k)}
    assert not diffs, f"content differs on tables: {diffs}"


async def test_full_clone_preserves_data_type_edge_cases(
    source_db,
    clone_db,
    db_manager,
):
    src = await db_manager.fetch(source_db, "SELECT * FROM data_types.edge_cases ORDER BY id", text_dates=True)
    tgt = await db_manager.fetch(clone_db, "SELECT * FROM data_types.edge_cases ORDER BY id", text_dates=True)
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
        assert not diffs, f"edge-case row {s_row['id']} ({s_row['label']}) differs: {diffs}"


async def test_full_clone_restores_circular_fk_tables(
    source_db,
    clone_db,
    db_manager,
):
    src_a = await db_manager.fetch(source_db, "SELECT id, peer_id FROM circular_fk.a ORDER BY id")
    tgt_a = await db_manager.fetch(clone_db, "SELECT id, peer_id FROM circular_fk.a ORDER BY id")
    src_b = await db_manager.fetch(source_db, "SELECT id, peer_id FROM circular_fk.b ORDER BY id")
    tgt_b = await db_manager.fetch(clone_db, "SELECT id, peer_id FROM circular_fk.b ORDER BY id")
    assert [dict(r) for r in src_a] == [dict(r) for r in tgt_a]
    assert [dict(r) for r in src_b] == [dict(r) for r in tgt_b]


async def test_full_clone_preserves_table_variants(
    source_db,
    clone_db,
    db_manager,
):
    src_ident = await db_manager.fetch(source_db, "SELECT id, label, seq_val FROM variants.with_identity ORDER BY id")
    tgt_ident = await db_manager.fetch(clone_db, "SELECT id, label, seq_val FROM variants.with_identity ORDER BY id")
    assert [dict(r) for r in src_ident] == [dict(r) for r in tgt_ident]

    src_seq = await sequence_params(db_manager, source_db, "variants", "custom_seq")
    tgt_seq = await sequence_params(db_manager, clone_db, "variants", "custom_seq")
    assert src_seq == tgt_seq, "custom sequence params must round-trip"

    src_unl = await db_manager.fetch(
        source_db,
        "SELECT relpersistence FROM pg_class WHERE relname='session_cache' AND relnamespace='variants'::regnamespace",
    )
    tgt_unl = await db_manager.fetch(
        clone_db,
        "SELECT relpersistence FROM pg_class WHERE relname='session_cache' AND relnamespace='variants'::regnamespace",
    )
    assert src_unl[0]["relpersistence"] == tgt_unl[0]["relpersistence"], "UNLOGGED persistence flag must be preserved"

    src_mammal = await db_manager.fetch(source_db, "SELECT name, legs FROM variants.mammal ORDER BY name")
    tgt_mammal = await db_manager.fetch(clone_db, "SELECT name, legs FROM variants.mammal ORDER BY name")
    assert [dict(r) for r in src_mammal] == [dict(r) for r in tgt_mammal]

    src_rule = await db_manager.fetch(
        source_db, "SELECT rulename FROM pg_rules WHERE schemaname='variants' AND tablename='active_animals'"
    )
    tgt_rule = await db_manager.fetch(
        clone_db, "SELECT rulename FROM pg_rules WHERE schemaname='variants' AND tablename='active_animals'"
    )
    assert {r["rulename"] for r in src_rule} == {r["rulename"] for r in tgt_rule}


async def test_full_clone_preserves_privileges(
    source_db,
    clone_db,
    db_manager,
):
    # Table-level ACLs on privs.public_facing.
    src_acl = await db_manager.fetch(
        source_db,
        """
        SELECT relacl::text AS acl FROM pg_class
        WHERE relname = 'public_facing' AND relnamespace = 'privs'::regnamespace
    """,
    )
    tgt_acl = await db_manager.fetch(
        clone_db,
        """
        SELECT relacl::text AS acl FROM pg_class
        WHERE relname = 'public_facing' AND relnamespace = 'privs'::regnamespace
    """,
    )
    assert src_acl and src_acl[0]["acl"], "fixture sanity: privs.public_facing must have ACL entries on source"
    assert src_acl[0]["acl"] == tgt_acl[0]["acl"], (
        f"table-level ACL differs: src={src_acl[0]['acl']} tgt={tgt_acl[0]['acl']}"
    )

    # Column-level ACL on secret_payload (GRANT UPDATE to anon_tester_reader).
    src_col_acl = await db_manager.fetch(
        source_db,
        """
        SELECT a.attacl::text AS acl FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        WHERE c.relname = 'public_facing'
          AND c.relnamespace = 'privs'::regnamespace
          AND a.attname = 'secret_payload'
    """,
    )
    tgt_col_acl = await db_manager.fetch(
        clone_db,
        """
        SELECT a.attacl::text AS acl FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        WHERE c.relname = 'public_facing'
          AND c.relnamespace = 'privs'::regnamespace
          AND a.attname = 'secret_payload'
    """,
    )
    assert src_col_acl and src_col_acl[0]["acl"], "fixture sanity: secret_payload must have a column ACL on source"
    assert src_col_acl[0]["acl"] == tgt_col_acl[0]["acl"], (
        f"column-level ACL differs: src={src_col_acl[0]['acl']} tgt={tgt_col_acl[0]['acl']}"
    )

    # DEFAULT PRIVILEGES entry for schema privs.
    src_def = await db_manager.fetch(
        source_db,
        """
        SELECT defaclacl::text AS acl FROM pg_default_acl d
        JOIN pg_namespace n ON n.oid = d.defaclnamespace
        WHERE n.nspname = 'privs' AND d.defaclobjtype = 'r'
    """,
    )
    tgt_def = await db_manager.fetch(
        clone_db,
        """
        SELECT defaclacl::text AS acl FROM pg_default_acl d
        JOIN pg_namespace n ON n.oid = d.defaclnamespace
        WHERE n.nspname = 'privs' AND d.defaclobjtype = 'r'
    """,
    )
    assert src_def, "fixture sanity: schema privs must have DEFAULT PRIVILEGES on source"
    assert [dict(r) for r in src_def] == [dict(r) for r in tgt_def], "DEFAULT PRIVILEGES not preserved"


@pytest.mark.min_pg(100000)
async def test_full_clone_preserves_publications(
    source_db,
    clone_db,
    db_manager,
):
    pub_columns = "pubname, puballtables, pubinsert, pubupdate, pubdelete"
    src_pubs = await db_manager.fetch(
        source_db,
        f"SELECT {pub_columns} FROM pg_publication ORDER BY pubname",
    )
    tgt_pubs = await db_manager.fetch(
        clone_db,
        f"SELECT {pub_columns} FROM pg_publication ORDER BY pubname",
    )
    assert src_pubs, "fixture sanity: source must have publications"
    assert [dict(r) for r in src_pubs] == [dict(r) for r in tgt_pubs], "PUBLICATION metadata not preserved"

    # FOR TABLE pubs.feed mapping must round-trip on pub_for_feed.
    src_rel = await db_manager.fetch(
        source_db,
        """
        SELECT p.pubname, c.relname, n.nspname
        FROM pg_publication_rel pr
        JOIN pg_publication p ON p.oid = pr.prpubid
        JOIN pg_class c ON c.oid = pr.prrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE p.pubname = 'pub_for_feed'
    """,
    )
    tgt_rel = await db_manager.fetch(
        clone_db,
        """
        SELECT p.pubname, c.relname, n.nspname
        FROM pg_publication_rel pr
        JOIN pg_publication p ON p.oid = pr.prpubid
        JOIN pg_class c ON c.oid = pr.prrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE p.pubname = 'pub_for_feed'
    """,
    )
    assert src_rel, "fixture sanity: pub_for_feed must publish a table on source"
    assert [dict(r) for r in src_rel] == [dict(r) for r in tgt_rel], "PUBLICATION → table mapping not preserved"


@pytest.mark.min_pg(110000)
async def test_full_clone_preserves_publication_truncate_flag(
    source_db,
    clone_db,
    db_manager,
):
    query = "SELECT pubname, pubtruncate FROM pg_publication ORDER BY pubname"
    src_pubs = await db_manager.fetch(source_db, query)
    tgt_pubs = await db_manager.fetch(clone_db, query)
    assert src_pubs, "fixture sanity: source must have publications"
    assert [dict(r) for r in src_pubs] == [dict(r) for r in tgt_pubs], "PUBLICATION truncate flag not preserved"


@pytest.mark.min_pg(100000)
async def test_full_clone_with_no_publications_strips_them(
    source_db,
    target_db,
    db_manager,
    db_params,
    pg_anon_runner,
):
    out = output_path("no_publications")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
            "--pg-dump-options=--no-publications",
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
        ],
    )
    assert res.result_code == ResultCode.DONE

    src_pubs = await db_manager.fetch(source_db, "SELECT pubname FROM pg_publication ORDER BY pubname")
    tgt_pubs = await db_manager.fetch(target_db, "SELECT pubname FROM pg_publication ORDER BY pubname")
    assert src_pubs, "fixture sanity: source must have publications"
    assert tgt_pubs == [], (
        f"--no-publications must strip publications; target still has: {[r['pubname'] for r in tgt_pubs]}"
    )


async def test_full_clone_with_drop_db_is_idempotent(
    full_dump,
    target_db,
    db_params,
    pg_anon_runner,
):
    for extra in ([], ["--drop-db"]):
        res = await restore(pg_anon_runner, db_params, target_db, full_dump, extra)
        assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"


async def test_full_clone_resets_sequence_for_special_char_schema(
    source_db,
    clone_db,
    db_manager,
):
    schema = "_SCHM.$complex#имя;@&* a'"
    table = "_TBL.$complex#имя;@&* a'"

    src_max = await db_manager.fetch(source_db, f'SELECT max(id) AS m FROM "{schema}"."{table}"')
    src_max_id = src_max[0]["m"]
    assert src_max_id is not None, "source table must have data to anchor the assertion"

    # the clone is shared by the module, so the probe row is rolled back
    connection = await create_connection(db_manager._connection_params(clone_db))  # noqa: SLF001
    try:
        transaction = connection.transaction()
        await transaction.start()
        new_id = await connection.fetchval(
            f"""
            INSERT INTO "{schema}"."{table}" (fld_key, "_FLD.$complex#имя;@&* a'")
            VALUES ('post_restore_probe', 'value')
            RETURNING id
        """
        )
        await transaction.rollback()
    finally:
        await connection.close()
    assert new_id > src_max_id, (
        f"sequence not advanced past source max: max(src.id)={src_max_id}, new id from target after restore={new_id}"
    )


async def test_full_clone_preserves_index_quirks(
    source_db,
    clone_db,
    db_manager,
):
    expected_index_names = {
        "events_user_hash",
        "events_payload_spgist",
        "events_location_gist",
        "events_region_covering",
        "events_name_collate_c",
        "_idx.$weird#имя",
        "events_pkey",
    }

    src_idx = await db_manager.fetch(
        source_db,
        """
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'index_quirks'
        ORDER BY indexname
    """,
    )
    tgt_idx = await db_manager.fetch(
        clone_db,
        """
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'index_quirks'
        ORDER BY indexname
    """,
    )
    src_map = {r["indexname"]: r["indexdef"] for r in src_idx}
    tgt_map = {r["indexname"]: r["indexdef"] for r in tgt_idx}

    missing_on_source = expected_index_names - set(src_map)
    assert not missing_on_source, f"builder didn't produce expected indexes: {missing_on_source}"

    missing_on_target = expected_index_names - set(tgt_map)
    assert not missing_on_target, f"indexes lost on restore: {missing_on_target}"

    diffs = {name: (src_map[name], tgt_map[name]) for name in expected_index_names if src_map[name] != tgt_map[name]}
    assert not diffs, f"indexdef differs (source -> target): {diffs}"

    src_methods = await db_manager.fetch(
        source_db,
        """
        SELECT i.relname AS indexname, am.amname
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am   ON am.oid = i.relam
        WHERE i.relnamespace = 'index_quirks'::regnamespace
        ORDER BY i.relname
    """,
    )
    tgt_methods = await db_manager.fetch(
        clone_db,
        """
        SELECT i.relname AS indexname, am.amname
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am   ON am.oid = i.relam
        WHERE i.relnamespace = 'index_quirks'::regnamespace
        ORDER BY i.relname
    """,
    )
    assert [dict(r) for r in src_methods] == [dict(r) for r in tgt_methods], (
        "access method (amname) per index must round-trip"
    )


@pytest.mark.min_pg(110000)
async def test_full_clone_preserves_covering_index_include_columns(
    source_db,
    clone_db,
    db_manager,
):
    cover = await db_manager.fetch(
        clone_db,
        """
        SELECT ix.indnkeyatts, ix.indnatts
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        WHERE i.relname = 'events_region_covering'
          AND i.relnamespace = 'index_quirks'::regnamespace
    """,
    )
    assert cover, "covering index missing on target"
    assert cover[0]["indnkeyatts"] == 1 and cover[0]["indnatts"] == 3, (
        f"covering index lost INCLUDE structure: indnkeyatts={cover[0]['indnkeyatts']}, "
        f"indnatts={cover[0]['indnatts']} (expected 1 and 3)"
    )


@pytest.mark.min_pg(120000)
async def test_full_clone_preserves_constraint_quirks(
    source_db,
    clone_db,
    db_manager,
):
    src_inh = await db_manager.fetch(
        source_db,
        """
        SELECT child.relname AS child, parent.relname AS parent
        FROM pg_inherits i
        JOIN pg_class child  ON child.oid  = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE child.relnamespace = 'constraints_quirks'::regnamespace
        ORDER BY parent.relname, child.relname
    """,
    )
    tgt_inh = await db_manager.fetch(
        clone_db,
        """
        SELECT child.relname AS child, parent.relname AS parent
        FROM pg_inherits i
        JOIN pg_class child  ON child.oid  = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE child.relnamespace = 'constraints_quirks'::regnamespace
        ORDER BY parent.relname, child.relname
    """,
    )
    assert [dict(r) for r in src_inh] == [dict(r) for r in tgt_inh], "sub-partition tree must round-trip"

    tgt_partitioned = await db_manager.fetch(
        clone_db,
        """
        SELECT relname FROM pg_class
        WHERE relnamespace = 'constraints_quirks'::regnamespace
          AND relkind = 'p'
        ORDER BY relname
    """,
    )
    assert [r["relname"] for r in tgt_partitioned] == ["subpart_2025_q1", "subpart_root"], (
        "partitioned parents must round-trip as partitioned, not plain tables"
    )

    expected_constraints = {
        "booking_no_overlap": ("x", True),
        "legacy_child_parent_fk": ("f", False),
        "evt_attachment_evt_fk": ("f", True),
    }
    tgt_cs = await db_manager.fetch(
        clone_db,
        """
        SELECT conname, contype::text AS contype, convalidated
        FROM pg_constraint
        WHERE connamespace = 'constraints_quirks'::regnamespace
          AND conname IN ('booking_no_overlap', 'legacy_child_parent_fk',
                          'evt_attachment_evt_fk')
    """,
    )
    actual = {r["conname"]: (r["contype"], r["convalidated"]) for r in tgt_cs}
    assert actual == expected_constraints, (
        f"named constraints differ on target: expected={expected_constraints}, actual={actual}"
    )

    src_clones = await db_manager.fetch(
        source_db,
        """
        SELECT count(*) AS c FROM pg_constraint
        WHERE conrelid = 'constraints_quirks.evt_attachment'::regclass
          AND contype = 'f' AND conparentid <> 0
    """,
    )
    tgt_clones = await db_manager.fetch(
        clone_db,
        """
        SELECT count(*) AS c FROM pg_constraint
        WHERE conrelid = 'constraints_quirks.evt_attachment'::regclass
          AND contype = 'f' AND conparentid <> 0
    """,
    )
    assert src_clones[0]["c"] == tgt_clones[0]["c"], (
        f"per-leaf FK clones count mismatch: src={src_clones[0]['c']} tgt={tgt_clones[0]['c']}"
    )
    assert tgt_clones[0]["c"] >= 3, f"expected at least 3 per-leaf FK clones on target, got {tgt_clones[0]['c']}"

    src_rows = await db_manager.fetch(source_db, "SELECT count(*) AS c FROM constraints_quirks.subpart_root")
    tgt_rows = await db_manager.fetch(clone_db, "SELECT count(*) AS c FROM constraints_quirks.subpart_root")
    assert src_rows[0]["c"] == tgt_rows[0]["c"]

    try:
        await db_manager.execute(
            clone_db,
            """
            INSERT INTO constraints_quirks.booking (room_id, period)
            VALUES (1, tstzrange('2026-01-02', '2026-01-04'))
        """,
        )
        raised = False
    except Exception:
        raised = True
    assert raised, "EXCLUDE USING gist must reject overlapping range on target"

    try:
        await db_manager.execute(
            clone_db,
            """
            INSERT INTO constraints_quirks.evt_attachment (evt_id, tenant_id, occurred_at, note)
            VALUES (9999, 1, '2025-01-15 10:00+00', 'orphan probe')
        """,
        )
        fk_raised = False
    except Exception:
        fk_raised = True
    assert fk_raised, "FK on partitioned parent must reject orphan rows on target"


@pytest.mark.min_pg(110000)
async def test_invalid_partitioned_index_is_reported(db_manager, target_db):
    """The guard that runs after post-data must notice a partitioned index left invalid."""
    await db_manager.execute(
        target_db,
        """
        CREATE SCHEMA part_check;
        CREATE TABLE part_check.root (id bigint, k int) PARTITION BY LIST (k);
        CREATE TABLE part_check.p1 PARTITION OF part_check.root FOR VALUES IN (1);
        CREATE TABLE part_check.p2 PARTITION OF part_check.root FOR VALUES IN (2);
        -- ON ONLY leaves the parent index invalid until every partition has its index attached,
        -- which is the state a concurrent post-data restore can leave behind
        CREATE INDEX root_id_idx ON ONLY part_check.root (id);
        """,
    )
    connection = await create_connection(db_manager._connection_params(target_db))  # noqa: SLF001
    try:
        invalid = await get_invalid_partitioned_indexes(connection)
        assert "part_check.root_id_idx" in invalid, (
            f"an invalid partitioned index went unnoticed, so a restore would report success "
            f"while its primary key does not work; reported: {invalid}"
        )

        await db_manager.execute(
            target_db,
            """
            CREATE INDEX p1_id_idx ON part_check.p1 (id);
            CREATE INDEX p2_id_idx ON part_check.p2 (id);
            ALTER INDEX part_check.root_id_idx ATTACH PARTITION part_check.p1_id_idx;
            ALTER INDEX part_check.root_id_idx ATTACH PARTITION part_check.p2_id_idx;
            """,
        )
        assert await get_invalid_partitioned_indexes(connection) == [], (
            "a fully attached partitioned index must not be reported, otherwise every restore fails"
        )
    finally:
        await connection.close()
