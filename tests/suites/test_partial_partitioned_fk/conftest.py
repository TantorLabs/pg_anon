from __future__ import annotations

from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_partfk_source"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner):
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE

    await db_manager.execute(SOURCE_DB, """
        CREATE SCHEMA IF NOT EXISTS fkpart;

        CREATE TABLE IF NOT EXISTS fkpart.events (
            id bigint NOT NULL,
            occurred_at timestamptz NOT NULL,
            payload jsonb,
            PRIMARY KEY (id, occurred_at)
        ) PARTITION BY RANGE (occurred_at);

        CREATE TABLE IF NOT EXISTS fkpart.events_2025_q1
            PARTITION OF fkpart.events
            FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');

        CREATE TABLE IF NOT EXISTS fkpart.events_2025_q2
            PARTITION OF fkpart.events
            FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');

        CREATE INDEX IF NOT EXISTS events_occurred_at_idx
            ON fkpart.events (occurred_at);

        INSERT INTO fkpart.events (id, occurred_at, payload) VALUES
            (1, '2025-02-10 10:00+00', '{"k":1}'::jsonb),
            (2, '2025-05-10 10:00+00', '{"k":2}'::jsonb)
        ON CONFLICT DO NOTHING;

        CREATE TABLE IF NOT EXISTS fkpart.attachment (
            id serial PRIMARY KEY,
            evt_id bigint NOT NULL,
            occurred_at timestamptz NOT NULL,
            note text,
            CONSTRAINT attachment_evt_fk FOREIGN KEY (evt_id, occurred_at)
                REFERENCES fkpart.events (id, occurred_at)
                ON DELETE CASCADE
        );

        INSERT INTO fkpart.attachment (evt_id, occurred_at, note) VALUES
            (1, '2025-02-10 10:00+00', 'q1 attachment'),
            (2, '2025-05-10 10:00+00', 'q2 attachment')
        ON CONFLICT DO NOTHING;

        CREATE TABLE IF NOT EXISTS fkpart.metrics (
            id bigint NOT NULL,
            tenant_id integer NOT NULL,
            occurred_at timestamptz NOT NULL,
            value numeric,
            PRIMARY KEY (id, tenant_id, occurred_at)
        ) PARTITION BY RANGE (occurred_at);

        CREATE TABLE IF NOT EXISTS fkpart.metrics_q1
            PARTITION OF fkpart.metrics
            FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')
            PARTITION BY LIST (tenant_id);

        CREATE TABLE IF NOT EXISTS fkpart.metrics_q1_t1
            PARTITION OF fkpart.metrics_q1
            FOR VALUES IN (1, 2);

        CREATE TABLE IF NOT EXISTS fkpart.metrics_q1_other
            PARTITION OF fkpart.metrics_q1 DEFAULT;

        CREATE TABLE IF NOT EXISTS fkpart.metrics_q2
            PARTITION OF fkpart.metrics
            FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');

        INSERT INTO fkpart.metrics (id, tenant_id, occurred_at, value) VALUES
            (1, 1, '2025-01-15 10:00+00', 1.5),
            (2, 7, '2025-02-20 10:00+00', 2.5),
            (3, 1, '2025-05-10 10:00+00', 3.5)
        ON CONFLICT DO NOTHING;

        CREATE TABLE IF NOT EXISTS fkpart.animal (
            id serial PRIMARY KEY,
            name text NOT NULL,
            kingdom text NOT NULL DEFAULT 'Animalia'
        );

        CREATE TABLE IF NOT EXISTS fkpart.mammal (
            legs smallint NOT NULL DEFAULT 4
        ) INHERITS (fkpart.animal);

        INSERT INTO fkpart.animal (name) VALUES ('amoeba')
        ON CONFLICT DO NOTHING;
        INSERT INTO fkpart.mammal (name, legs) VALUES ('dog', 4), ('whale', 0);
    """)

    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_partfk_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
