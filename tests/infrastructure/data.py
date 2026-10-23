"""Domain-oriented test data builders for pg_anon.

Schemas are chosen to mirror common real-world subsystems:

- hr         — employees, departments, salary history, performance reviews
- billing    — customers, invoices, payment cards, transactions
- ecommerce  — products, categories, orders, reviews (self-ref, circular FK via deferred)
- audit      — log entries, login attempts (inet, jsonb, uuid)
- content    — articles with tsvector + jsonb + text[] + GIN indexes
- analytics  — partitioned tables: RANGE by date, LIST by region, HASH by tenant
- quirks     — edge cases: quoted identifiers, reserved words, Cyrillic, NULLs, generated columns
- anon_ext   — plpgsql functions used by anonymization rules (kept separate so scanner can invoke them)

Each builder is idempotent (uses IF NOT EXISTS / ON CONFLICT) and takes an
explicit row-count arg so callers can pick TINY/SMALL/MEDIUM/LARGE.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from tests.infrastructure.sizes import SMALL, TINY

if TYPE_CHECKING:
    from tests.infrastructure.db import DBManager


class _Partitioning:
    """DDL pieces for partitioned tables that depend on the server: PG 9.6 has none, PG 10 has no keys on them."""

    def __init__(self, version_num: int) -> None:
        self.partitioned = version_num >= 100000
        self.keys_allowed = version_num >= 110000 or not self.partitioned

    def key(self, columns: str) -> str:
        """Primary key clause that follows the last column, or nothing when it is not allowed."""
        return f",\n                PRIMARY KEY ({columns})" if self.keys_allowed else ""

    def partition_by(self, method: str, column: str) -> str:
        return f" PARTITION BY {method} ({column})" if self.partitioned else ""

    def partition(self, name: str, parent: str, bound: str, tail: str = "") -> str:
        """A partition of `parent`; on PG 9.6 a plain table with the same columns."""
        if not self.partitioned:
            return f"CREATE TABLE IF NOT EXISTS {name} (LIKE {parent});"
        return f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF {parent} {bound}{tail};"


class Fixtures:
    def __init__(self, db: DBManager) -> None:
        self.db = db

    # ==================================================================
    # Extensions / shared prerequisites
    # ==================================================================

    async def ensure_extensions(self, db_name: str) -> None:
        """Enable extensions that fixtures rely on. Safe to call repeatedly."""
        await self.db.execute(
            db_name,
            """
            CREATE EXTENSION IF NOT EXISTS pgcrypto;
            CREATE EXTENSION IF NOT EXISTS citext;
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            CREATE EXTENSION IF NOT EXISTS hstore;
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        """,
        )

    async def ensure_anon_ext(self, db_name: str) -> None:
        """Helper plpgsql functions used by anonymization rules and scanner checks."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS anon_ext;

            CREATE OR REPLACE FUNCTION anon_ext.is_company_email(
                value TEXT, schema_name TEXT, table_name TEXT, field_name TEXT
            ) RETURNS boolean AS $$
            DECLARE
                result boolean;
                domain TEXT;
            BEGIN
                domain := SPLIT_PART(value, '@', 2);
                IF domain = '' THEN
                    RETURN false;
                END IF;
                EXECUTE 'SELECT EXISTS (SELECT 1 FROM billing.customer WHERE email LIKE ''%' || domain || ''')'
                INTO result;
                RETURN result;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION anon_ext.has_organization_title(
                schema_name TEXT, table_name TEXT, field_name TEXT, field_type TEXT
            ) RETURNS BOOLEAN AS $fn$
            DECLARE
                organization_titles TEXT;
                sql TEXT;
                res BOOLEAN;
            BEGIN
                IF field_type NOT IN ('text', 'character varying', 'varchar', 'character', 'char') THEN
                    RETURN FALSE;
                END IF;
                BEGIN
                    organization_titles := current_setting('anon_ext.org_titles');
                EXCEPTION WHEN OTHERS THEN
                    SELECT string_agg(regexp_replace(name, '\\s+', ' & ', 'g'), ' | ')
                    INTO organization_titles
                    FROM billing.customer;
                    PERFORM set_config('anon_ext.org_titles', organization_titles, false);
                END;
                IF organization_titles IS NULL THEN
                    RETURN FALSE;
                END IF;
                sql := format(
                    'SELECT EXISTS (SELECT 1 FROM %I.%I WHERE to_tsvector(''simple'', %I) @@ to_tsquery(''simple'', %L))',
                    schema_name, table_name, field_name, organization_titles
                );
                EXECUTE sql INTO res;
                RETURN res;
            END;
            $fn$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION anon_ext.is_email_value(
                value TEXT, schema_name TEXT, table_name TEXT, field_name TEXT
            ) RETURNS boolean AS $fn$
            BEGIN
                RETURN value IS NOT NULL
                   AND value ~ '^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$';
            END;
            $fn$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION anon_ext.match_pii_field_name(
                schema_name TEXT, table_name TEXT, field_name TEXT, field_type TEXT
            ) RETURNS boolean AS $fn$
            BEGIN
                RETURN field_name IN (
                    'email', 'phone', 'ssn', 'tax_id',
                    'cardholder_name', 'pan_last4', 'client_ip', 'username'
                );
            END;
            $fn$ LANGUAGE plpgsql;
        """,
        )

    # ==================================================================
    # hr — employees, departments, salaries
    # ==================================================================

    async def build_hr(self, db_name: str, employees: int = SMALL) -> None:
        version_num = await self.db.server_version_num(db_name)
        # Generated columns exist since PG 12.
        full_name_column = (
            "full_name varchar(130) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,"
            if version_num >= 120000
            else ""
        )
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS hr;

            CREATE TABLE IF NOT EXISTS hr.department (
                id serial PRIMARY KEY,
                name varchar(64) NOT NULL UNIQUE,
                budget numeric(14, 2) NOT NULL DEFAULT 0,
                created_at timestamptz NOT NULL DEFAULT now()
            );
            COMMENT ON TABLE hr.department IS 'Подразделения компании';
            COMMENT ON COLUMN hr.department.budget IS 'Годовой бюджет в рублях';

            CREATE TABLE IF NOT EXISTS hr.employee (
                id serial PRIMARY KEY,
                first_name varchar(64) NOT NULL,
                last_name varchar(64) NOT NULL,
                __FULL_NAME_COLUMN__
                email citext NOT NULL UNIQUE,
                phone varchar(32),
                ssn char(11),
                birth_date date NOT NULL,
                hire_date date NOT NULL DEFAULT CURRENT_DATE,
                department_id integer NOT NULL REFERENCES hr.department(id),
                manager_id integer REFERENCES hr.employee(id),
                salary numeric(12, 2) NOT NULL CHECK (salary >= 0),
                CONSTRAINT employee_phone_format CHECK (phone IS NULL OR phone ~ '^\\+?[0-9]{10,15}$')
            );

            CREATE INDEX IF NOT EXISTS employee_department_idx ON hr.employee (department_id);
            CREATE INDEX IF NOT EXISTS employee_last_name_lower_idx ON hr.employee (lower(last_name));
            CREATE INDEX IF NOT EXISTS employee_active_salary_idx ON hr.employee (salary) WHERE salary > 0;

            CREATE TABLE IF NOT EXISTS hr.salary_history (
                id bigserial PRIMARY KEY,
                employee_id integer NOT NULL REFERENCES hr.employee(id) ON DELETE CASCADE,
                effective_date date NOT NULL,
                amount numeric(12, 2) NOT NULL,
                reason text,
                UNIQUE (employee_id, effective_date)
            );

            CREATE TABLE IF NOT EXISTS hr.performance_review (
                id bigserial PRIMARY KEY,
                employee_id integer NOT NULL REFERENCES hr.employee(id) ON DELETE CASCADE,
                reviewer_id integer REFERENCES hr.employee(id) ON DELETE SET NULL,
                score smallint NOT NULL CHECK (score BETWEEN 1 AND 5),
                notes text,
                reviewed_at timestamptz NOT NULL DEFAULT now()
            );

            INSERT INTO hr.department (name, budget) VALUES
                ('Engineering', 50000000),
                ('Sales', 15000000),
                ('HR', 5000000),
                ('Finance', 8000000),
                ('Отдел аналитики', 12000000)
            ON CONFLICT (name) DO NOTHING;
        """.replace("__FULL_NAME_COLUMN__", full_name_column),
        )

        await self.db.execute(
            db_name,
            f"""
            WITH seed AS (
                SELECT generate_series(1, {employees}) AS v
            ), dept AS (
                SELECT id, row_number() OVER () AS rn FROM hr.department
            )
            INSERT INTO hr.employee
                (first_name, last_name, email, phone, ssn, birth_date, hire_date,
                 department_id, manager_id, salary)
            SELECT
                (array['Ivan','Olga','Sergey','Maria','Dmitry','Anna','Pavel','Ekaterina'])
                    [(v % 8) + 1] AS first_name,
                (array['Petrov','Ivanova','Smirnov','Kuznetsova','Popov','Sokolova'])
                    [(v % 6) + 1] || '_' || v AS last_name,
                ('user' || v || '@' ||
                    (array['acme.com','example.org','corp.io','test.ru'])[(v % 4) + 1])::citext AS email,
                CASE WHEN v % 10 = 0 THEN NULL
                     ELSE '+7' || lpad((9000000000 + v)::text, 10, '0')
                END AS phone,
                lpad(((v * 31) % 1000000000)::text, 9, '0')
                    || substring(lpad(v::text, 2, '0') from 1 for 2) AS ssn,
                DATE '1970-01-01' + ((v * 37) % 18000) AS birth_date,
                DATE '2015-01-01' + ((v * 11) % 3500) AS hire_date,
                (SELECT id FROM dept WHERE rn = ((v % (SELECT count(*) FROM dept)) + 1)),
                NULL,
                50000 + (v % 7) * 10000 + (v * 17 % 5000)
            FROM seed
            ON CONFLICT (email) DO NOTHING;

            UPDATE hr.employee e
            SET manager_id = m.id
            FROM hr.employee m
            WHERE m.id = ((e.id - 1) / 5) + 1
              AND m.id <> e.id
              AND e.manager_id IS NULL;

            INSERT INTO hr.salary_history (employee_id, effective_date, amount, reason)
            SELECT
                e.id,
                e.hire_date + (step.n * interval '1 year'),
                e.salary * (1.0 + step.n * 0.05),
                (array['hire','yearly raise','promotion','cost of living'])[(step.n % 4) + 1]
            FROM hr.employee e
            CROSS JOIN LATERAL (SELECT generate_series(0, (e.id % 3)) AS n) step
            ON CONFLICT (employee_id, effective_date) DO NOTHING;

            INSERT INTO hr.performance_review (employee_id, reviewer_id, score, notes)
            SELECT
                e.id,
                e.manager_id,
                ((e.id * 7) % 5) + 1,
                'Квартальная оценка ' || e.id
            FROM hr.employee e
            WHERE e.manager_id IS NOT NULL;
        """,
        )

    # ==================================================================
    # billing — customers, invoices, payment cards, transactions
    # ==================================================================

    async def build_billing(self, db_name: str, customers: int = SMALL) -> None:
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS billing;

            DO $$ BEGIN
                CREATE DOMAIN billing.email_t AS citext
                    CHECK (VALUE ~ '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$');
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;

            DO $$ BEGIN
                CREATE TYPE billing.card_brand AS ENUM ('visa', 'mastercard', 'mir', 'amex', 'unionpay');
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;

            DO $$ BEGIN
                CREATE TYPE billing.invoice_status AS ENUM
                    ('draft', 'issued', 'paid', 'overdue', 'void');
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;

            DO $$ BEGIN
                CREATE TYPE billing.postal_address AS (
                    country char(2),
                    city text,
                    street text,
                    zip text
                );
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;

            CREATE TABLE IF NOT EXISTS billing.customer (
                id serial PRIMARY KEY,
                name varchar(128) NOT NULL,
                email billing.email_t NOT NULL UNIQUE,
                tax_id varchar(20) UNIQUE,
                billing_address billing.postal_address,
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                created_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS customer_metadata_gin
                ON billing.customer USING GIN (metadata jsonb_path_ops);

            CREATE TABLE IF NOT EXISTS billing.payment_card (
                id bigserial PRIMARY KEY,
                customer_id integer NOT NULL REFERENCES billing.customer(id) ON DELETE CASCADE,
                pan_hash bytea NOT NULL,
                pan_last4 char(4) NOT NULL,
                cardholder_name varchar(128) NOT NULL,
                exp_month smallint NOT NULL CHECK (exp_month BETWEEN 1 AND 12),
                exp_year smallint NOT NULL CHECK (exp_year BETWEEN 2020 AND 2099),
                brand billing.card_brand NOT NULL,
                is_primary boolean NOT NULL DEFAULT false
            );

            CREATE UNIQUE INDEX IF NOT EXISTS payment_card_one_primary_per_customer
                ON billing.payment_card (customer_id) WHERE is_primary;

            CREATE TABLE IF NOT EXISTS billing.invoice (
                id bigserial PRIMARY KEY,
                customer_id integer NOT NULL,
                amount numeric(14, 2) NOT NULL CHECK (amount >= 0),
                currency char(3) NOT NULL DEFAULT 'RUB',
                issued_at timestamptz NOT NULL DEFAULT now(),
                due_at timestamptz,
                status billing.invoice_status NOT NULL DEFAULT 'draft',
                notes text,
                CONSTRAINT invoice_customer_fk FOREIGN KEY (customer_id)
                    REFERENCES billing.customer(id)
                    DEFERRABLE INITIALLY IMMEDIATE
            );

            CREATE INDEX IF NOT EXISTS invoice_customer_issued_idx
                ON billing.invoice (customer_id, issued_at DESC);

            CREATE TABLE IF NOT EXISTS billing.transaction (
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                invoice_id bigint NOT NULL REFERENCES billing.invoice(id) ON DELETE CASCADE,
                method varchar(32) NOT NULL,
                amount numeric(14, 2) NOT NULL,
                processed_at timestamptz NOT NULL DEFAULT now(),
                external_ref uuid NOT NULL DEFAULT uuid_generate_v4(),
                raw_response jsonb
            );
        """,
        )

        await self.db.execute(
            db_name,
            f"""
            INSERT INTO billing.customer (name, email, tax_id, billing_address, metadata)
            SELECT
                'ООО «Клиент ' || v || '»',
                ('billing+' || v || '@customer' || v || '.example')::citext,
                lpad((7700000000 + v * 13)::text, 10, '0'),
                ROW(
                    (array['RU','RU','BY','KZ'])[(v % 4) + 1],
                    (array['Москва','Санкт-Петербург','Минск','Алматы'])[(v % 4) + 1],
                    'ул. Тестовая, д. ' || v,
                    lpad(((v * 13) % 999999)::text, 6, '0')
                )::billing.postal_address,
                jsonb_build_object(
                    'source', (array['web','api','import'])[(v % 3) + 1],
                    'tags', jsonb_build_array('vip', 'segment_' || (v % 5)),
                    'score', (v % 100),
                    'notes', 'some text for customer ' || v
                )
            FROM generate_series(1, {customers}) v
            ON CONFLICT (email) DO NOTHING;

            INSERT INTO billing.payment_card
                (customer_id, pan_hash, pan_last4, cardholder_name, exp_month, exp_year, brand, is_primary)
            SELECT
                c.id,
                digest('4111111111' || lpad(c.id::text, 6, '0'), 'sha256'),
                lpad((c.id % 10000)::text, 4, '0'),
                upper('TEST USER ' || c.id),
                ((c.id % 12) + 1)::smallint,
                (2025 + (c.id % 5))::smallint,
                (array['visa','mastercard','mir','amex','unionpay']::billing.card_brand[])[(c.id % 5) + 1],
                true
            FROM billing.customer c;

            INSERT INTO billing.invoice (customer_id, amount, currency, due_at, status, notes)
            SELECT
                c.id,
                (100 + (c.id % 50) * 25)::numeric(14, 2),
                (array['RUB','USD','EUR'])[(c.id % 3) + 1],
                now() + ((c.id % 30 + 1) || ' days')::interval,
                (array['draft','issued','paid','overdue','void']::billing.invoice_status[])[(c.id % 5) + 1],
                'Счёт #' || c.id || ' для клиента ' || c.id
            FROM billing.customer c;

            INSERT INTO billing.transaction (invoice_id, method, amount, raw_response)
            SELECT
                i.id,
                (array['card','wire','applepay','sbp'])[(i.id % 4) + 1],
                i.amount,
                jsonb_build_object(
                    'provider_code', 200,
                    'request_id', uuid_generate_v4(),
                    'auth_code', lpad((i.id % 1000000)::text, 6, '0')
                )
            FROM billing.invoice i
            WHERE i.status IN ('paid', 'issued');
        """,
        )

    # ==================================================================
    # ecommerce — products, categories (self-ref), orders, reviews
    # ==================================================================

    async def build_ecommerce(self, db_name: str, products: int = SMALL) -> None:
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS ecommerce;

            DO $$ BEGIN
                CREATE TYPE ecommerce.order_status AS ENUM
                    ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'refunded');
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;

            CREATE TABLE IF NOT EXISTS ecommerce.category (
                id serial PRIMARY KEY,
                parent_id integer REFERENCES ecommerce.category(id),
                name varchar(128) NOT NULL,
                slug varchar(128) NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS ecommerce.product (
                id bigserial PRIMARY KEY,
                sku varchar(24) NOT NULL UNIQUE,
                name varchar(256) NOT NULL,
                description text,
                price numeric(12, 2) NOT NULL CHECK (price > 0),
                category_id integer REFERENCES ecommerce.category(id),
                tags text[] NOT NULL DEFAULT ARRAY[]::text[],
                attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
                created_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS product_tags_gin
                ON ecommerce.product USING GIN (tags);
            CREATE INDEX IF NOT EXISTS product_attr_gin
                ON ecommerce.product USING GIN (attributes);
            CREATE INDEX IF NOT EXISTS product_name_trgm
                ON ecommerce.product USING GIN (name gin_trgm_ops);

            CREATE TABLE IF NOT EXISTS ecommerce."order" (
                id bigserial PRIMARY KEY,
                customer_id integer NOT NULL REFERENCES billing.customer(id),
                placed_at timestamptz NOT NULL DEFAULT now(),
                status ecommerce.order_status NOT NULL DEFAULT 'pending',
                total numeric(14, 2) NOT NULL,
                shipping_address billing.postal_address
            );

            CREATE TABLE IF NOT EXISTS ecommerce.order_item (
                id bigserial PRIMARY KEY,
                order_id bigint NOT NULL REFERENCES ecommerce."order"(id) ON DELETE CASCADE,
                product_id bigint NOT NULL REFERENCES ecommerce.product(id),
                quantity integer NOT NULL CHECK (quantity > 0),
                unit_price numeric(12, 2) NOT NULL,
                UNIQUE (order_id, product_id)
            );

            CREATE TABLE IF NOT EXISTS ecommerce.review (
                id bigserial PRIMARY KEY,
                product_id bigint NOT NULL REFERENCES ecommerce.product(id) ON DELETE CASCADE,
                customer_id integer REFERENCES billing.customer(id) ON DELETE SET NULL,
                rating smallint NOT NULL CHECK (rating BETWEEN 1 AND 5),
                body text,
                posted_at timestamptz NOT NULL DEFAULT now()
            );

            INSERT INTO ecommerce.category (parent_id, name, slug) VALUES
                (NULL, 'Electronics',  'electronics'),
                (NULL, 'Books',        'books'),
                (NULL, 'Clothing',     'clothing')
            ON CONFLICT (slug) DO NOTHING;

            INSERT INTO ecommerce.category (parent_id, name, slug)
            SELECT c.id, c.name || ' / Sub', c.slug || '-sub'
            FROM ecommerce.category c WHERE c.parent_id IS NULL
            ON CONFLICT (slug) DO NOTHING;
        """,
        )

        await self.db.execute(
            db_name,
            f"""
            INSERT INTO ecommerce.product (sku, name, description, price, category_id, tags, attributes)
            SELECT
                -- SKU intentionally LOOKS like a card number: decoy for scanner false-positives
                lpad((v % 10000)::text, 4, '0') || '-' ||
                    lpad(((v * 7) % 10000)::text, 4, '0') || '-' ||
                    lpad(((v * 11) % 10000)::text, 4, '0') || '-' ||
                    lpad(((v * 13) % 10000)::text, 4, '0'),
                'Product ' || v,
                'Detailed description for product number ' || v,
                (10 + (v % 500) * 0.5)::numeric(12, 2),
                (SELECT id FROM ecommerce.category ORDER BY id LIMIT 1 OFFSET (v % 3)),
                ARRAY['tag_' || (v % 10), 'region_' || (v % 4), 'bestseller']::text[],
                jsonb_build_object(
                    'weight_g', (v % 5000) + 10,
                    'dimensions', jsonb_build_object('w', v % 100, 'h', v % 200, 'd', v % 50),
                    'color', (array['red','blue','green','black','white'])[(v % 5) + 1]
                )
            FROM generate_series(1, {products}) v
            ON CONFLICT (sku) DO NOTHING;

            INSERT INTO ecommerce."order" (customer_id, status, total, shipping_address)
            SELECT
                c.id,
                (array['pending','confirmed','shipped','delivered','cancelled','refunded']::ecommerce.order_status[])
                    [(c.id % 6) + 1],
                (50 + (c.id % 100) * 3)::numeric(14, 2),
                c.billing_address
            FROM billing.customer c;

            INSERT INTO ecommerce.order_item (order_id, product_id, quantity, unit_price)
            SELECT o.id, p.id, (o.id % 5) + 1, p.price
            FROM ecommerce."order" o
            CROSS JOIN LATERAL (
                SELECT id, price FROM ecommerce.product
                ORDER BY id LIMIT 2 OFFSET (o.id % 10)
            ) p
            ON CONFLICT (order_id, product_id) DO NOTHING;

            INSERT INTO ecommerce.review (product_id, customer_id, rating, body)
            SELECT
                p.id,
                (SELECT id FROM billing.customer ORDER BY id LIMIT 1 OFFSET (p.id % (SELECT count(*) FROM billing.customer))),
                ((p.id * 3) % 5) + 1,
                'Отзыв на товар ' || p.id || '. Всё хорошо, спасибо!'
            FROM ecommerce.product p
            WHERE p.id % 3 = 0;
        """,
        )

    # ==================================================================
    # audit — log entries, login attempts (inet, uuid, jsonb)
    # ==================================================================

    async def build_audit(self, db_name: str, entries: int = SMALL) -> None:
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS audit;

            CREATE TABLE IF NOT EXISTS audit.log_entry (
                id bigserial PRIMARY KEY,
                occurred_at timestamptz NOT NULL DEFAULT now(),
                actor uuid,
                action varchar(64) NOT NULL,
                target text,
                client_ip inet,
                client_mac macaddr,
                payload jsonb NOT NULL DEFAULT '{}'::jsonb,
                duration_ms integer
            );

            CREATE INDEX IF NOT EXISTS log_entry_occurred_brin
                ON audit.log_entry USING BRIN (occurred_at);
            CREATE INDEX IF NOT EXISTS log_entry_action_idx
                ON audit.log_entry (action);

            CREATE TABLE IF NOT EXISTS audit.login_attempt (
                id bigserial PRIMARY KEY,
                username varchar(128) NOT NULL,
                client_ip inet NOT NULL,
                user_agent text,
                success boolean NOT NULL,
                attempted_at timestamptz NOT NULL DEFAULT now()
            );
        """,
        )

        await self.db.execute(
            db_name,
            f"""
            INSERT INTO audit.log_entry (actor, action, target, client_ip, client_mac, payload, duration_ms)
            SELECT
                uuid_generate_v4(),
                (array['login','logout','create','update','delete','export'])[(v % 6) + 1],
                'resource:' || v,
                ((v % 255) || '.' || ((v * 7) % 255) || '.' ||
                 ((v * 11) % 255) || '.' || ((v * 13) % 255))::inet,
                ('52:54:00:' ||
                  lpad(to_hex(v % 256), 2, '0') || ':' ||
                  lpad(to_hex((v * 3) % 256), 2, '0') || ':' ||
                  lpad(to_hex((v * 5) % 256), 2, '0'))::macaddr,
                jsonb_build_object(
                    'ua', 'pytest/' || v,
                    'latency_ms', v % 1000,
                    'request_id', uuid_generate_v4()
                ),
                v % 2000
            FROM generate_series(1, {entries}) v;

            INSERT INTO audit.login_attempt (username, client_ip, user_agent, success)
            SELECT
                'user' || (v % 50),
                ((v % 255) || '.0.0.' || (v % 255))::inet,
                'Mozilla/5.0 session ' || v,
                (v % 4) <> 0
            FROM generate_series(1, {entries}) v;
        """,
        )

    # ==================================================================
    # content — articles (tsvector, jsonb, text[], materialized view)
    # ==================================================================

    async def build_content(self, db_name: str, articles: int = SMALL) -> None:
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS content;

            CREATE TABLE IF NOT EXISTS content.article (
                id bigserial PRIMARY KEY,
                slug varchar(128) NOT NULL UNIQUE,
                title text NOT NULL,
                body text NOT NULL,
                tags text[] NOT NULL DEFAULT ARRAY[]::text[],
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                search_vector tsvector,
                published_at timestamptz
            );

            CREATE INDEX IF NOT EXISTS article_search_gin
                ON content.article USING GIN (search_vector);
            CREATE INDEX IF NOT EXISTS article_tags_gin
                ON content.article USING GIN (tags);

            CREATE OR REPLACE FUNCTION content.article_update_search_vector()
            RETURNS trigger AS $$
            BEGIN
                NEW.search_vector :=
                    setweight(to_tsvector('simple', coalesce(NEW.title, '')), 'A') ||
                    setweight(to_tsvector('simple', coalesce(NEW.body, '')),  'B');
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS article_search_vector_tg ON content.article;
            CREATE TRIGGER article_search_vector_tg
                BEFORE INSERT OR UPDATE ON content.article
                FOR EACH ROW EXECUTE PROCEDURE content.article_update_search_vector();

            CREATE TABLE IF NOT EXISTS content.comment (
                id bigserial PRIMARY KEY,
                article_id bigint NOT NULL REFERENCES content.article(id) ON DELETE CASCADE,
                author_email citext NOT NULL,
                body text NOT NULL,
                posted_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE OR REPLACE VIEW content.published_article AS
            SELECT id, slug, title, published_at
            FROM content.article
            WHERE published_at IS NOT NULL;
        """,
        )

        await self.db.execute(
            db_name,
            f"""
            INSERT INTO content.article (slug, title, body, tags, metadata, published_at)
            SELECT
                'article-' || v,
                'Article Title ' || v || ' about PostgreSQL',
                'Long body text for article ' || v ||
                    '. Lorem ipsum dolor sit amet, consectetur adipiscing elit. ' ||
                    repeat('Filler content paragraph. ', v % 5 + 1),
                ARRAY['postgres','article','topic_' || (v % 10)]::text[],
                jsonb_build_object(
                    'author', 'author' || (v % 20),
                    'reads', (v * 13) % 1000,
                    'related', jsonb_build_array(v - 1, v + 1)
                ),
                CASE WHEN v % 3 = 0 THEN NULL ELSE now() - ((v || ' days')::interval) END
            FROM generate_series(1, {articles}) v
            ON CONFLICT (slug) DO NOTHING;

            INSERT INTO content.comment (article_id, author_email, body)
            SELECT
                a.id,
                ('reader' || (a.id % 50) || '@example.com')::citext,
                'Комментарий к статье ' || a.id
            FROM content.article a
            WHERE a.id % 2 = 0;
        """,
        )

        await self.db.execute(
            db_name,
            """
            DROP MATERIALIZED VIEW IF EXISTS content.article_stats;
            CREATE MATERIALIZED VIEW content.article_stats AS
            SELECT
                a.id,
                a.slug,
                a.published_at IS NOT NULL AS is_published,
                count(c.id) AS comment_count
            FROM content.article a
            LEFT JOIN content.comment c ON c.article_id = a.id
            GROUP BY a.id, a.slug, a.published_at;

            CREATE UNIQUE INDEX IF NOT EXISTS article_stats_pk
                ON content.article_stats (id);
        """,
        )

    # ==================================================================
    # analytics — partitioned tables: RANGE, LIST, HASH
    # ==================================================================

    async def build_analytics(self, db_name: str) -> None:
        """Partitioned tables; PG 10 has no HASH, DEFAULT and keys on them, PG 9.6 has no partitioning at all."""
        version_num = await self.db.server_version_num(db_name)
        parts = _Partitioning(version_num)
        key, suffix, partition = parts.key, parts.partition_by, parts.partition

        if version_num >= 110000:
            event_default = partition("analytics.event_default", "analytics.event", "DEFAULT")
            region_other = partition("analytics.event_by_region_other", "analytics.event_by_region", "DEFAULT")
            tenant_method = "HASH"
            tenant_bounds = [f"FOR VALUES WITH (modulus 4, remainder {r})" for r in range(4)]
        else:
            # Without a DEFAULT partition the last partitions cover the remaining values.
            event_default = partition(
                "analytics.event_default", "analytics.event", "FOR VALUES FROM ('2025-03-01') TO ('2100-01-01')"
            )
            region_other = partition(
                "analytics.event_by_region_other", "analytics.event_by_region", "FOR VALUES IN ('US','BR','JP')"
            )
            tenant_method = "RANGE"
            tenant_bounds = [f"FOR VALUES FROM ({4 * r}) TO ({4 * r + 4})" for r in range(4)]
        tenant_partitions = "\n            ".join(
            partition(f"analytics.event_by_tenant_h{r}", "analytics.event_by_tenant", bound)
            for r, bound in enumerate(tenant_bounds)
        )
        tenant_index = (
            "CREATE INDEX IF NOT EXISTS event_tenant_idx ON analytics.event (tenant_id);" if parts.keys_allowed else ""
        )

        await self.db.execute(
            db_name,
            f"""
            CREATE SCHEMA IF NOT EXISTS analytics;
            CREATE SCHEMA IF NOT EXISTS analytics_archive;

            -- RANGE partitioning by date
            CREATE TABLE IF NOT EXISTS analytics.event (
                id bigserial,
                event_date date NOT NULL,
                tenant_id integer NOT NULL,
                region varchar(16) NOT NULL,
                payload jsonb NOT NULL DEFAULT '{{}}'::jsonb{key("id, event_date")}
            ){suffix("RANGE", "event_date")};

            {partition("analytics.event_2025_01", "analytics.event", "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")}
            {partition("analytics.event_2025_02", "analytics.event", "FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')")}
            {partition("analytics_archive.event_2024", "analytics.event", "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')")}
            {event_default}

            {tenant_index}

            -- LIST partitioning by region
            CREATE TABLE IF NOT EXISTS analytics.event_by_region (
                id bigserial,
                region varchar(16) NOT NULL,
                occurred_at timestamptz NOT NULL DEFAULT now(),
                metric_name varchar(64) NOT NULL,
                value numeric NOT NULL{key("id, region")}
            ){suffix("LIST", "region")};

            {partition("analytics.event_by_region_ru", "analytics.event_by_region", "FOR VALUES IN ('RU')")}
            {partition("analytics.event_by_region_eu", "analytics.event_by_region", "FOR VALUES IN ('EU','DE','FR')")}
            {region_other}

            -- HASH partitioning by tenant (RANGE on PG 10)
            CREATE TABLE IF NOT EXISTS analytics.event_by_tenant (
                id bigserial,
                tenant_id integer NOT NULL,
                occurred_at timestamptz NOT NULL DEFAULT now(),
                data jsonb NOT NULL DEFAULT '{{}}'::jsonb{key("id, tenant_id")}
            ){suffix(tenant_method, "tenant_id")};

            {tenant_partitions}
        """,
        )

        await self.db.execute(
            db_name,
            """
            INSERT INTO analytics.event (event_date, tenant_id, region, payload)
            SELECT
                DATE '2024-06-01' + ((v * 7) % 400),
                (v % 8) + 1,
                (array['RU','EU','US','DE','FR'])[(v % 5) + 1],
                jsonb_build_object('n', v, 'ok', (v % 2 = 0))
            FROM generate_series(1, 300) v;

            INSERT INTO analytics.event_by_region (region, metric_name, value)
            SELECT
                (array['RU','EU','DE','FR','US','BR','JP'])[(v % 7) + 1],
                (array['cpu','memory','disk_read','net_in'])[(v % 4) + 1],
                (v * 1.5)::numeric
            FROM generate_series(1, 200) v;

            INSERT INTO analytics.event_by_tenant (tenant_id, data)
            SELECT v % 16, jsonb_build_object('payload', 'data-' || v)
            FROM generate_series(1, 200) v;
        """,
        )

    # ==================================================================
    # quirks — quoted identifiers, reserved words, Cyrillic, NULLs
    # ==================================================================

    async def build_quirks(self, db_name: str) -> None:
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS quirks;

            CREATE TABLE IF NOT EXISTS quirks."MixedCaseTable" (
                id serial PRIMARY KEY,
                "UserId" integer NOT NULL,
                "Email Address" text
            );

            CREATE TABLE IF NOT EXISTS quirks.reserved_words (
                id serial PRIMARY KEY,
                "user" text,
                "order" integer,
                "grant" boolean,
                "select" text
            );

            CREATE TABLE IF NOT EXISTS quirks."таблица_на_русском" (
                "идентификатор" serial PRIMARY KEY,
                "имя_поля" text,
                "другое_поле" text
            );

            CREATE TABLE IF NOT EXISTS quirks.with_nulls (
                id serial PRIMARY KEY,
                val text,
                amount numeric(30, 4)
            );
        """,
        )

        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS "_SCHM.$complex#имя;@&* a'";
            CREATE TABLE IF NOT EXISTS "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'" (
                id serial PRIMARY KEY,
                fld_key text,
                "_FLD.$complex#имя;@&* a'" text
            );

            INSERT INTO "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'"
                (fld_key, "_FLD.$complex#имя;@&* a'")
            VALUES
                ('email', 'user@example.com'),
                ('password', 'p@ssw0rd'),
                ('address', 'Москва, Красная пл., 1'),
                ('login', 'admin'),
                ('first_name', 'Иван'),
                ('amount', '100500');

            DROP TABLE IF EXISTS "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'2";
            CREATE TABLE "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'2"
                AS SELECT * FROM "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'" WITH DATA;
        """,
        )

        await self.db.execute(
            db_name,
            """
            INSERT INTO quirks."MixedCaseTable" ("UserId", "Email Address") VALUES
                (1, 'alice@example.com'),
                (2, NULL),
                (3, 'bob+tag@example.com');

            INSERT INTO quirks.reserved_words ("user", "order", "grant", "select") VALUES
                ('admin', 1, true,  'SELECT *'),
                ('guest', 2, false, NULL);

            INSERT INTO quirks."таблица_на_русском" ("имя_поля", "другое_поле") VALUES
                ('значение 1', 'другое значение 1'),
                ('значение 2', NULL),
                ('emoji: 🎉', '中文测试');

            INSERT INTO quirks.with_nulls (val, amount) VALUES
                ('normal value', 1.5),
                (NULL, NULL),
                ('', 0),
                ('value with ''quote''', -99999.9999);
        """,
        )

    # ==================================================================
    # data_types — one row per PG type for round-trip checks
    # ==================================================================

    async def build_data_types(self, db_name: str) -> None:
        version_num = await self.db.server_version_num(db_name)
        sample_ddl = """
            CREATE SCHEMA IF NOT EXISTS data_types;

            DO $$ BEGIN
                CREATE TYPE data_types.price_range AS RANGE (subtype = numeric);
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;

            CREATE TABLE IF NOT EXISTS data_types.sample (
                id serial PRIMARY KEY,
                c_bool           bool,
                c_int2           smallint,
                c_int4           integer,
                c_int8           bigint,
                c_numeric        numeric(20, 5),
                c_float4         real,
                c_float8         double precision,
                c_char           char(5),
                c_varchar        varchar(32),
                c_text           text,
                c_bit            bit(5),
                c_varbit         varbit(16),
                c_uuid           uuid,
                c_bytea          bytea,
                c_date           date,
                c_time           time,
                c_timetz         timetz,
                c_timestamp      timestamp,
                c_timestamptz    timestamptz,
                c_interval       interval,
                c_json           json,
                c_jsonb          jsonb,
                c_xml            xml,
                c_inet           inet,
                c_cidr           cidr,
                c_macaddr        macaddr,
                c_macaddr8       macaddr8,
                c_text_array     text[],
                c_int_array      int[],
                c_text_array_2d  text[][],
                c_int4range      int4range,
                c_tstzrange      tstzrange,
                c_daterange      daterange,
                c_numrange       numrange,
                c_price_range    data_types.price_range,
                c_point          point,
                c_line           line,
                c_lseg           lseg,
                c_box            box,
                c_path           path,
                c_polygon        polygon,
                c_circle         circle,
                c_money          money,
                c_tsvector       tsvector,
                c_tsquery        tsquery
            );

            INSERT INTO data_types.sample (
                c_bool, c_int2, c_int4, c_int8, c_numeric, c_float4, c_float8,
                c_char, c_varchar, c_text,
                c_bit, c_varbit, c_uuid, c_bytea,
                c_date, c_time, c_timetz, c_timestamp, c_timestamptz, c_interval,
                c_json, c_jsonb, c_xml, c_inet, c_cidr, c_macaddr, c_macaddr8,
                c_text_array, c_int_array, c_text_array_2d,
                c_int4range, c_tstzrange, c_daterange, c_numrange, c_price_range,
                c_point, c_line, c_lseg, c_box, c_path, c_polygon, c_circle,
                c_money, c_tsvector, c_tsquery
            ) VALUES (
                true, 32767, 2147483647, 9223372036854775807,
                12345.67890, 3.14::real, 2.718281828459045,
                'abcde', 'varchar value', 'text with Русский & emoji 🎉',
                B'10101', B'1100110011',
                '11111111-2222-3333-4444-555555555555'::uuid,
                decode('deadbeef', 'hex'),
                DATE '2024-06-15', TIME '12:34:56.789',
                TIMETZ '12:34:56+03', TIMESTAMP '2024-06-15 12:34:56.789',
                TIMESTAMPTZ '2024-06-15 12:34:56.789+03', INTERVAL '1 year 2 months 3 days 04:05:06',
                '{"a":1}'::json, '{"a":1,"b":[1,2,3]}'::jsonb,
                '<root><node>value</node></root>'::xml,
                '192.168.1.1'::inet, '10.0.0.0/24'::cidr,
                '08:00:2b:01:02:03'::macaddr, '08:00:2b:01:02:03:04:05'::macaddr8,
                ARRAY['a','b','с-кириллицей'], ARRAY[1,2,3,4,5],
                ARRAY[['a','b'],['c','d']]::text[][],
                int4range(1, 100, '[)'),
                tstzrange('2024-01-01'::timestamptz, '2025-01-01'::timestamptz, '[)'),
                daterange(DATE '2024-01-01', DATE '2025-01-01', '[)'),
                numrange(1.5, 99.9, '[)'),
                data_types.price_range(10::numeric, 100::numeric),
                POINT(1.5, 2.5),
                '{1,-1,0}'::line,
                '[(0,0),(1,1)]'::lseg,
                '((0,0),(1,1))'::box,
                '[(0,0),(1,1),(2,0)]'::path,
                '((0,0),(1,0),(1,1),(0,1))'::polygon,
                '<(0,0),5>'::circle,
                1234.56::money,
                to_tsvector('simple', 'sample text for search'),
                to_tsquery('simple', 'sample & search')
            );

            -- Edge cases: one row per boundary value so the row-level assert in
            -- test_full_clone_preserves_data_type_edge_cases can pinpoint the
            -- exact failing case.
            CREATE TABLE IF NOT EXISTS data_types.edge_cases (
                id            integer PRIMARY KEY,
                label         text NOT NULL,
                f_float4      real,
                f_float8      double precision,
                f_numeric     numeric,
                f_timestamp   timestamp,
                f_timestamptz timestamptz,
                f_date        date,
                f_text        text,
                f_bytea       bytea
            );

            INSERT INTO data_types.edge_cases VALUES
                ( 1, 'float4 NaN',           'NaN'::real,       NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                ( 2, 'float4 Infinity',      'Infinity'::real,  NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                ( 3, 'float4 -Infinity',     '-Infinity'::real, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                ( 4, 'float8 NaN',           NULL, 'NaN'::double precision,       NULL, NULL, NULL, NULL, NULL, NULL),
                ( 5, 'float8 Infinity',      NULL, 'Infinity'::double precision,  NULL, NULL, NULL, NULL, NULL, NULL),
                ( 6, 'float8 -Infinity',     NULL, '-Infinity'::double precision, NULL, NULL, NULL, NULL, NULL, NULL),
                ( 7, 'numeric NaN',          NULL, NULL, 'NaN'::numeric, NULL, NULL, NULL, NULL, NULL),
                ( 8, 'timestamp infinity',   NULL, NULL, NULL, 'infinity'::timestamp,  NULL, NULL, NULL, NULL),
                ( 9, 'timestamp -infinity',  NULL, NULL, NULL, '-infinity'::timestamp, NULL, NULL, NULL, NULL),
                (10, 'timestamptz infinity', NULL, NULL, NULL, NULL, 'infinity'::timestamptz, NULL, NULL, NULL),
                (11, 'date 4713 BC',         NULL, NULL, NULL, NULL, NULL, '4713-01-01 BC'::date, NULL, NULL),
                (12, 'date infinity',        NULL, NULL, NULL, NULL, NULL, 'infinity'::date,      NULL, NULL),
                (13, 'text 1.1MB',           NULL, NULL, NULL, NULL, NULL, NULL, repeat('x', 1100000), NULL),
                (14, 'bytea 1.1MB TOAST',    NULL, NULL, NULL, NULL, NULL, NULL, NULL, repeat('a', 1100000)::bytea),
                (15, 'text CR LF tab',       NULL, NULL, NULL, NULL, NULL, NULL, E'line1\\r\\nline2\\ttab', NULL),
                (16, 'text quote backslash', NULL, NULL, NULL, NULL, NULL, NULL, E'O''Brien \\\\o/ 100%', NULL),
                (17, 'bytea with NUL',       NULL, NULL, NULL, NULL, NULL, NULL, NULL, decode('0001020304ff00fe', 'hex'));
        """
        if version_num < 100000:
            # macaddr8 exists since PG 10
            sample_ddl = (
                sample_ddl.replace("                c_macaddr8       macaddr8,\n", "")
                .replace("c_cidr, c_macaddr, c_macaddr8,", "c_cidr, c_macaddr,")
                .replace(
                    "'08:00:2b:01:02:03'::macaddr, '08:00:2b:01:02:03:04:05'::macaddr8,",
                    "'08:00:2b:01:02:03'::macaddr,",
                )
            )
        await self.db.execute(db_name, sample_ddl)

    # ==================================================================
    # table_variants — rare declarations that pg_dump must preserve
    # ==================================================================

    async def build_table_variants(self, db_name: str) -> None:
        version_num = await self.db.server_version_num(db_name)
        # Identity columns exist since PG 10; older servers get a serial column.
        identity_column = (
            "id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,"
            if version_num >= 100000
            else "id serial PRIMARY KEY,"
        )
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS variants;

            -- Custom SEQUENCE with non-default parameters + OWNED BY
            CREATE SEQUENCE IF NOT EXISTS variants.custom_seq
                INCREMENT BY 10
                MINVALUE 100
                MAXVALUE 100000
                START WITH 500
                CACHE 5;

            -- GENERATED BY DEFAULT AS IDENTITY (pg10+, serial before)
            CREATE TABLE IF NOT EXISTS variants.with_identity (
                __IDENTITY_COLUMN__
                label text NOT NULL,
                seq_val bigint NOT NULL DEFAULT nextval('variants.custom_seq')
            );

            ALTER SEQUENCE variants.custom_seq OWNED BY variants.with_identity.seq_val;

            INSERT INTO variants.with_identity (label) VALUES
                ('alpha'), ('beta'), ('gamma')
            ON CONFLICT DO NOTHING;

            -- UNLOGGED TABLE (skips WAL, not replicated, but still dumped by pg_dump)
            CREATE UNLOGGED TABLE IF NOT EXISTS variants.session_cache (
                id serial PRIMARY KEY,
                session_key text UNIQUE,
                payload jsonb
            );

            INSERT INTO variants.session_cache (session_key, payload) VALUES
                ('s1', '{"ttl": 60}'::jsonb),
                ('s2', '{"ttl": 120}'::jsonb)
            ON CONFLICT (session_key) DO NOTHING;

            -- Table INHERITANCE (legacy, not partitioning)
            CREATE TABLE IF NOT EXISTS variants.animal (
                id serial PRIMARY KEY,
                name text NOT NULL,
                kingdom text NOT NULL DEFAULT 'Animalia'
            );

            CREATE TABLE IF NOT EXISTS variants.mammal (
                legs smallint NOT NULL DEFAULT 4
            ) INHERITS (variants.animal);

            CREATE TABLE IF NOT EXISTS variants.bird (
                can_fly boolean NOT NULL DEFAULT true
            ) INHERITS (variants.animal);

            INSERT INTO variants.animal (name, kingdom) VALUES ('amoeba', 'Animalia')
            ON CONFLICT DO NOTHING;
            INSERT INTO variants.mammal (name, legs) VALUES ('dog', 4), ('whale', 0);
            INSERT INTO variants.bird (name, can_fly) VALUES ('sparrow', true), ('penguin', false);

            -- VIEW + RULE ON INSERT DO INSTEAD
            CREATE OR REPLACE VIEW variants.active_animals AS
                SELECT id, name, kingdom FROM variants.animal;

            CREATE OR REPLACE RULE active_animals_ins AS
                ON INSERT TO variants.active_animals
                DO INSTEAD
                INSERT INTO variants.animal (name, kingdom)
                VALUES (NEW.name, coalesce(NEW.kingdom, 'Animalia'));
        """.replace("__IDENTITY_COLUMN__", identity_column),
        )

        # Event trigger must be created at DB level, ignore if role lacks perms.
        await self.db.execute(
            db_name,
            """
            DO $$ BEGIN
                CREATE OR REPLACE FUNCTION variants.log_ddl() RETURNS event_trigger
                LANGUAGE plpgsql AS $fn$
                BEGIN
                    -- noop; the trigger body is what pg_dump must preserve
                    PERFORM 1;
                END;
                $fn$;
            EXCEPTION WHEN insufficient_privilege THEN NULL; END $$;

            DO $$ BEGIN
                CREATE EVENT TRIGGER variants_ddl_log
                    ON ddl_command_end
                    EXECUTE PROCEDURE variants.log_ddl();
            EXCEPTION
                WHEN insufficient_privilege THEN NULL;
                WHEN duplicate_object THEN NULL;
            END $$;
        """,
        )

    # ==================================================================
    # fdw — foreign data wrapper loopback (optional; skipped if perms missing)
    # ==================================================================

    async def build_fdw(self, db_name: str) -> None:
        """Foreign server + foreign table via postgres_fdw. If the extension is.

        not installed or the role can't CREATE SERVER, the builder silently
        degrades so the rest of the zoo is unaffected.
        """
        await self.db.execute(
            db_name,
            """
            DO $$ BEGIN
                CREATE EXTENSION IF NOT EXISTS postgres_fdw;
            EXCEPTION WHEN insufficient_privilege THEN NULL; END $$;

            -- CREATE SERVER has no IF NOT EXISTS before PG 11
            DO $$ BEGIN
                CREATE SERVER loopback_server
                    FOREIGN DATA WRAPPER postgres_fdw
                    OPTIONS (host 'localhost', dbname 'postgres', port '5432');
            EXCEPTION
                WHEN insufficient_privilege THEN NULL;
                WHEN undefined_object THEN NULL;
                WHEN feature_not_supported THEN NULL;
                WHEN duplicate_object THEN NULL;
            END $$;

            DO $$ BEGIN
                CREATE SCHEMA IF NOT EXISTS fdw_ext;

                CREATE FOREIGN TABLE IF NOT EXISTS fdw_ext.remote_ping (
                    id integer,
                    note text
                ) SERVER loopback_server
                  OPTIONS (schema_name 'public', table_name 'non_existent_remote');
            EXCEPTION
                WHEN insufficient_privilege THEN NULL;
                WHEN undefined_object THEN NULL;  -- postgres_fdw not available
                WHEN feature_not_supported THEN NULL;
            END $$;
        """,
        )

    # ==================================================================
    # circular_fk — mutually referencing tables with DEFERRABLE FKs
    # ==================================================================

    async def build_circular_fk(self, db_name: str) -> None:
        """Two tables with circular FKs using DEFERRABLE INITIALLY DEFERRED.

        Exercises pg_anon's parallel COPY + session_replication_role='replica'
        path: neither table can be loaded first if FKs are eagerly checked.
        """
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS circular_fk;

            CREATE TABLE IF NOT EXISTS circular_fk.a (
                id integer PRIMARY KEY,
                peer_id integer
            );

            CREATE TABLE IF NOT EXISTS circular_fk.b (
                id integer PRIMARY KEY,
                peer_id integer
            );

            DO $$ BEGIN
                ALTER TABLE circular_fk.a
                    ADD CONSTRAINT a_peer_fk FOREIGN KEY (peer_id)
                    REFERENCES circular_fk.b(id)
                    DEFERRABLE INITIALLY DEFERRED;
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;

            DO $$ BEGIN
                ALTER TABLE circular_fk.b
                    ADD CONSTRAINT b_peer_fk FOREIGN KEY (peer_id)
                    REFERENCES circular_fk.a(id)
                    DEFERRABLE INITIALLY DEFERRED;
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;

            -- Populate with matched pairs. DEFERRED FKs allow us to insert both
            -- sides within a single transaction without constraint violation.
            BEGIN;
            INSERT INTO circular_fk.a (id, peer_id) VALUES (1, 101), (2, 102), (3, 103)
                ON CONFLICT (id) DO NOTHING;
            INSERT INTO circular_fk.b (id, peer_id) VALUES (101, 1), (102, 2), (103, 3)
                ON CONFLICT (id) DO NOTHING;
            COMMIT;
        """,
        )

    # ==================================================================
    # Security — RLS
    # ==================================================================

    async def build_security(self, db_name: str) -> None:
        """Row-level security policies on a table. pg_dump must preserve policies."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS security;

            CREATE TABLE IF NOT EXISTS security.tenant_data (
                id bigserial PRIMARY KEY,
                tenant_id integer NOT NULL,
                content text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now()
            );

            ALTER TABLE security.tenant_data ENABLE ROW LEVEL SECURITY;

            DROP POLICY IF EXISTS tenant_isolation ON security.tenant_data;
            CREATE POLICY tenant_isolation ON security.tenant_data
                USING (tenant_id = coalesce(current_setting('app.tenant_id', true)::int, -1));

            INSERT INTO security.tenant_data (tenant_id, content)
            SELECT v % 5, 'tenant payload ' || v FROM generate_series(1, 50) v;
        """,
        )

    # ==================================================================
    # privileges — GRANT / REVOKE / OWNER / DEFAULT PRIVILEGES / multi-role
    # ==================================================================

    async def build_privileges(self, db_name: str) -> None:
        """Custom roles + table-level and column-level GRANTs, an ALTER OWNER,
        and a DEFAULT PRIVILEGES rule. Wrapped in DO blocks — role creation
        requires CREATEROLE and fails gracefully on restricted test setups.
        """
        await self.db.execute(
            db_name,
            """
            DO $$ BEGIN
                CREATE ROLE anon_tester_reader NOINHERIT;
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN insufficient_privilege THEN NULL;
            END $$;

            DO $$ BEGIN
                CREATE ROLE anon_tester_writer NOINHERIT;
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN insufficient_privilege THEN NULL;
            END $$;

            CREATE SCHEMA IF NOT EXISTS privs;

            CREATE TABLE IF NOT EXISTS privs.public_facing (
                id serial PRIMARY KEY,
                title text NOT NULL,
                secret_payload text
            );

            INSERT INTO privs.public_facing (title, secret_payload) VALUES
                ('alpha', 'hidden-1'),
                ('beta',  'hidden-2')
            ON CONFLICT DO NOTHING;

            -- Table-level GRANT
            DO $$ BEGIN
                GRANT SELECT ON privs.public_facing TO anon_tester_reader;
                GRANT INSERT, UPDATE ON privs.public_facing TO anon_tester_writer;
            EXCEPTION
                WHEN undefined_object THEN NULL;
                WHEN insufficient_privilege THEN NULL;
            END $$;

            -- Column-level GRANT; a column REVOKE under a table GRANT would leave no column ACL
            DO $$ BEGIN
                GRANT UPDATE (secret_payload) ON privs.public_facing TO anon_tester_reader;
            EXCEPTION
                WHEN undefined_object THEN NULL;
                WHEN insufficient_privilege THEN NULL;
            END $$;

            -- Separate table with an explicit OWNER change
            CREATE TABLE IF NOT EXISTS privs.owned_by_writer (
                id serial PRIMARY KEY,
                note text
            );

            DO $$ BEGIN
                ALTER TABLE privs.owned_by_writer OWNER TO anon_tester_writer;
            EXCEPTION
                WHEN undefined_object THEN NULL;
                WHEN insufficient_privilege THEN NULL;
            END $$;

            -- DEFAULT PRIVILEGES: any future table created in privs by the
            -- current role will be readable by anon_tester_reader.
            DO $$ BEGIN
                ALTER DEFAULT PRIVILEGES IN SCHEMA privs
                    GRANT SELECT ON TABLES TO anon_tester_reader;
            EXCEPTION
                WHEN undefined_object THEN NULL;
                WHEN insufficient_privilege THEN NULL;
            END $$;
        """,
        )

    async def build_index_quirks(self, db_name: str) -> None:
        """Index types: hash, spgist, gist, covering INCLUDE (PG 11+), COLLATE, special names."""
        version_num = await self.db.server_version_num(db_name)
        region_include = "INCLUDE (name, occurred_at)" if version_num >= 110000 else ""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS index_quirks;

            CREATE TABLE IF NOT EXISTS index_quirks.events (
                id serial PRIMARY KEY,
                user_id integer NOT NULL,
                payload text NOT NULL,
                location point NOT NULL,
                name text NOT NULL,
                region text NOT NULL,
                occurred_at timestamptz NOT NULL DEFAULT now()
            );

            INSERT INTO index_quirks.events (user_id, payload, location, name, region) VALUES
                (1, 'first event payload',  POINT(1.0,  2.0), 'Alpha event', 'eu'),
                (2, 'second event payload', POINT(3.5,  4.5), 'Beta event',  'us'),
                (3, 'third event payload',  POINT(5.0, -1.0), 'gamma event', 'eu'),
                (4, 'fourth event payload', POINT(0.0,  0.0), 'Delta event', 'apac')
            ON CONFLICT DO NOTHING;

            CREATE INDEX IF NOT EXISTS events_user_hash
                ON index_quirks.events USING hash (user_id);

            CREATE INDEX IF NOT EXISTS events_payload_spgist
                ON index_quirks.events USING spgist (payload);

            CREATE INDEX IF NOT EXISTS events_location_gist
                ON index_quirks.events USING gist (location);

            CREATE INDEX IF NOT EXISTS events_region_covering
                ON index_quirks.events (region) __REGION_INCLUDE__;

            CREATE INDEX IF NOT EXISTS events_name_collate_c
                ON index_quirks.events (name COLLATE "C");

            CREATE INDEX IF NOT EXISTS "_idx.$weird#имя"
                ON index_quirks.events (region, user_id);
        """.replace("__REGION_INCLUDE__", region_include),
        )

    async def build_constraints_quirks(self, db_name: str) -> None:
        """Sub-partitioning, FK on partitioned parent, EXCLUDE USING gist, NOT VALID FK.

        PG 10 has no DEFAULT partition and no keys on partitioned tables, a foreign key to one needs PG 12.
        """
        version_num = await self.db.server_version_num(db_name)
        parts = _Partitioning(version_num)
        other_bound = "DEFAULT" if version_num >= 110000 else "FOR VALUES IN (4, 5, 6, 7, 8, 9)"
        # ON CONFLICT on a partitioned table needs PG 11
        subpart_conflict = "ON CONFLICT DO NOTHING" if parts.keys_allowed else ""
        evt_fk = ""
        if version_num >= 120000 or not parts.partitioned:
            evt_fk = """,
                CONSTRAINT evt_attachment_evt_fk FOREIGN KEY
                    (evt_id, tenant_id, occurred_at)
                    REFERENCES constraints_quirks.subpart_root (id, tenant_id, occurred_at)
                    ON DELETE CASCADE"""
        root, q1 = "constraints_quirks.subpart_root", "constraints_quirks.subpart_2025_q1"
        q1_ddl = parts.partition(
            q1, root, "FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')", " PARTITION BY LIST (tenant_id)"
        )
        q1_t123 = parts.partition(f"{q1}_t123", q1, "FOR VALUES IN (1, 2, 3)")
        q1_other = parts.partition(f"{q1}_other", q1, other_bound)
        q2 = parts.partition(
            "constraints_quirks.subpart_2025_q2", root, "FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')"
        )
        subpart_ddl = f"""
            CREATE TABLE IF NOT EXISTS constraints_quirks.subpart_root (
                id bigserial NOT NULL,
                tenant_id integer NOT NULL,
                occurred_at timestamptz NOT NULL,
                payload jsonb{parts.key("id, tenant_id, occurred_at")}
            ){parts.partition_by("RANGE", "occurred_at")};

            {q1_ddl}
            {q1_t123}
            {q1_other}
            {q2}
        """
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS constraints_quirks;
            """
            + subpart_ddl
            + """
            INSERT INTO constraints_quirks.subpart_root (id, tenant_id, occurred_at, payload) VALUES
                (1, 1, '2025-01-15 10:00+00', '{"k":1}'::jsonb),
                (2, 2, '2025-02-10 12:00+00', '{"k":2}'::jsonb),
                (3, 9, '2025-03-05 18:30+00', '{"k":3}'::jsonb),
                (4, 1, '2025-05-20 09:15+00', '{"k":4}'::jsonb)
            __SUBPART_CONFLICT__;

            CREATE TABLE IF NOT EXISTS constraints_quirks.evt_attachment (
                id serial PRIMARY KEY,
                evt_id bigint NOT NULL,
                tenant_id integer NOT NULL,
                occurred_at timestamptz NOT NULL,
                note text__EVT_FK__
            );

            INSERT INTO constraints_quirks.evt_attachment (evt_id, tenant_id, occurred_at, note) VALUES
                (1, 1, '2025-01-15 10:00+00', 'attached to evt#1'),
                (2, 2, '2025-02-10 12:00+00', 'attached to evt#2'),
                (4, 1, '2025-05-20 09:15+00', 'attached to evt#4')
            ON CONFLICT DO NOTHING;

            CREATE TABLE IF NOT EXISTS constraints_quirks.booking (
                id serial PRIMARY KEY,
                room_id integer NOT NULL,
                period tstzrange NOT NULL,
                CONSTRAINT booking_no_overlap EXCLUDE USING gist (period WITH &&)
                    WHERE (room_id IS NOT NULL)
            );

            INSERT INTO constraints_quirks.booking (room_id, period) VALUES
                (1, tstzrange('2026-01-01 00:00+00', '2026-01-05 00:00+00')),
                (2, tstzrange('2026-01-10 00:00+00', '2026-01-15 00:00+00'))
            ON CONFLICT DO NOTHING;

            CREATE TABLE IF NOT EXISTS constraints_quirks.legacy_parent (
                id integer PRIMARY KEY
            );
            INSERT INTO constraints_quirks.legacy_parent (id) VALUES (1), (2)
            ON CONFLICT DO NOTHING;

            CREATE TABLE IF NOT EXISTS constraints_quirks.legacy_child (
                id serial PRIMARY KEY,
                parent_id integer NOT NULL
            );
            INSERT INTO constraints_quirks.legacy_child (parent_id) VALUES (1), (999)
            ON CONFLICT DO NOTHING;

            DO $$ BEGIN
                ALTER TABLE constraints_quirks.legacy_child
                    ADD CONSTRAINT legacy_child_parent_fk
                    FOREIGN KEY (parent_id)
                    REFERENCES constraints_quirks.legacy_parent (id)
                    NOT VALID;
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """.replace("__EVT_FK__", evt_fk).replace("__SUBPART_CONFLICT__", subpart_conflict),
        )

    # ==================================================================
    # publications — logical replication PUBLICATION
    # ==================================================================

    async def build_publications(self, db_name: str) -> None:
        """Logical replication PUBLICATION on a curated table set.

        SUBSCRIPTION is intentionally NOT created here: it requires a second
        PG instance to be meaningful, and even a degraded `connect = false`
        subscription registers in pg_subscription and blocks DROP DATABASE.
        """
        version_num = await self.db.server_version_num(db_name)
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA IF NOT EXISTS pubs;

            CREATE TABLE IF NOT EXISTS pubs.feed (
                id serial PRIMARY KEY,
                topic text NOT NULL,
                payload jsonb NOT NULL DEFAULT '{}'::jsonb,
                published_at timestamptz NOT NULL DEFAULT now()
            );

            INSERT INTO pubs.feed (topic, payload) VALUES
                ('news',    '{"k":1}'::jsonb),
                ('alerts',  '{"k":2}'::jsonb)
            ON CONFLICT DO NOTHING;
        """,
        )
        if version_num < 100000:
            return  # publications exist since PG 10
        await self.db.execute(
            db_name,
            """
            DO $$ BEGIN
                CREATE PUBLICATION pub_for_feed FOR TABLE pubs.feed;
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN insufficient_privilege THEN NULL;
            END $$;

            DO $$ BEGIN
                CREATE PUBLICATION pub_all_tables FOR ALL TABLES
                    WITH (publish = 'insert, update');
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN insufficient_privilege THEN NULL;
            END $$;
        """,
        )

    # ==================================================================
    # Composition helpers
    # ==================================================================

    async def build_full_env(
        self,
        db_name: str,
        *,
        hr_employees: int = SMALL,
        billing_customers: int = SMALL,
        ecommerce_products: int = SMALL,
        audit_entries: int = SMALL,
        content_articles: int = SMALL,
    ) -> None:
        """Materialize the whole zoo. Used by end-to-end and full-clone suites."""
        await self.ensure_extensions(db_name)
        await self.ensure_anon_ext(db_name)
        await self.build_hr(db_name, employees=hr_employees)
        await self.build_billing(db_name, customers=billing_customers)
        await self.build_ecommerce(db_name, products=ecommerce_products)
        await self.build_audit(db_name, entries=audit_entries)
        await self.build_content(db_name, articles=content_articles)
        await self.build_analytics(db_name)
        await self.build_quirks(db_name)
        await self.build_circular_fk(db_name)
        await self.build_table_variants(db_name)
        await self.build_fdw(db_name)
        await self.build_data_types(db_name)
        await self.build_security(db_name)
        await self.build_privileges(db_name)
        await self.build_constraints_quirks(db_name)
        await self.build_index_quirks(db_name)
        await self.build_publications(db_name)

    async def build_minimal_env(self, db_name: str, rows: int = TINY) -> None:
        """Lightweight env for restore-clean / view-* / stress: only hr + billing."""
        await self.ensure_extensions(db_name)
        await self.build_hr(db_name, employees=rows)
        await self.build_billing(db_name, customers=rows)

    # ==================================================================
    # Partial-mode "tangle": kept objects, DDL anomalies and objects to exclude
    # ==================================================================

    async def build_partial_tangle(
        self,
        db_name: str,
        *,
        anomalies: bool = True,
        superuser_objects: bool = True,
        gone_objects: bool = True,
        gone_row_dependents: bool = True,
        gone_schema: bool = False,
    ) -> None:
        """Build the database for partial dump/restore tests; the pieces can be combined.

        `superuser_objects=False` skips what only a superuser can create (cast, `internal` function).
        `gone_row_dependents=False` skips functions and types on the row of gone_t (pg_anon 1.12.0 fails on them).
        Every object to exclude starts with `gone_`, so a leak is easy to find in text.
        """
        version_num = await self.db.server_version_num(db_name)
        await self.tangle_core(db_name)
        if anomalies:
            await self.tangle_anomalies(db_name, version_num)
        if superuser_objects:
            await self.tangle_superuser_objects(db_name)
        if gone_objects:
            await self.tangle_gone_objects(db_name, version_num)
            if gone_row_dependents:
                await self.tangle_gone_row_dependents(db_name, version_num)
        if gone_schema:
            await self.tangle_gone_schema(db_name)

    async def tangle_core(self, db_name: str) -> None:
        """Schema tg_core: linked tables with data, sequences, a view and a materialized view."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA tg_core;

            CREATE TABLE tg_core.customers (
                id serial PRIMARY KEY,
                name text NOT NULL,
                email text
            );
            CREATE TABLE tg_core.orders (
                id serial PRIMARY KEY,
                customer_id int NOT NULL REFERENCES tg_core.customers (id),
                amount numeric(12, 2) NOT NULL,
                ref_id int
            );
            CREATE TABLE tg_core.order_items (
                order_id int NOT NULL REFERENCES tg_core.orders (id),
                line int NOT NULL,
                sku text NOT NULL,
                qty int NOT NULL,
                PRIMARY KEY (order_id, line)
            );
            CREATE SEQUENCE tg_core.invoice_seq START 100;

            INSERT INTO tg_core.customers (name, email)
                SELECT 'customer ' || g, 'c' || g || '@example.com' FROM generate_series(1, 5) g;
            INSERT INTO tg_core.orders (customer_id, amount)
                SELECT 1 + g % 5, g * 10.5 FROM generate_series(1, 10) g;
            INSERT INTO tg_core.order_items (order_id, line, sku, qty)
                SELECT o, l, 'sku-' || o || '-' || l, o + l FROM generate_series(1, 10) o, generate_series(1, 2) l;
            SELECT nextval('tg_core.invoice_seq');

            CREATE VIEW tg_core.customer_orders AS
                SELECT c.id AS customer_id, c.name, o.id AS order_id, o.amount
                FROM tg_core.customers c JOIN tg_core.orders o ON o.customer_id = c.id;
            CREATE MATERIALIZED VIEW tg_core.order_totals AS
                SELECT o.id AS order_id, sum(i.qty) AS total_qty
                FROM tg_core.orders o JOIN tg_core.order_items i ON i.order_id = o.id
                GROUP BY o.id;
            """,
        )

    async def tangle_anomalies(self, db_name: str, version_num: int) -> None:
        """Objects of anomalies 1-16 from SIMPLE_DDL_AUDIT.md, all on kept objects."""
        await self.db.execute(
            db_name,
            """
            -- 1, plpgsql
            CREATE FUNCTION tg_core.fn_customer_count() RETURNS bigint LANGUAGE sql
                AS 'SELECT count(*) FROM tg_core.customers';
            CREATE FUNCTION tg_core.fn_greet(who text) RETURNS text LANGUAGE plpgsql IMMUTABLE
                AS $$ BEGIN RETURN 'hello ' || who; END $$;

            -- 2, 16: range types
            CREATE TYPE tg_core.price_range AS RANGE (subtype = numeric);
            CREATE TYPE tg_core.r_diff AS RANGE (subtype = float8, subtype_diff = float8mi);
            CREATE TYPE tg_core.r_opc AS RANGE (subtype = text, subtype_opclass = text_pattern_ops);
            CREATE TYPE tg_core.r_coll AS RANGE (subtype = text, collation = "C");

            -- 3: row types of tables in a function header
            CREATE FUNCTION tg_core.fn_items_of(int) RETURNS SETOF tg_core.order_items LANGUAGE sql
                AS 'SELECT * FROM tg_core.order_items WHERE order_id = $1';
            CREATE FUNCTION tg_core.fn_fmt_customer(c tg_core.customers) RETURNS text LANGUAGE sql IMMUTABLE
                AS 'SELECT c.name';

            -- 5, 6, 11, 12, 16: domains
            CREATE DOMAIN tg_core.d_not_valid AS int;
            ALTER DOMAIN tg_core.d_not_valid ADD CONSTRAINT d_not_valid_check CHECK (VALUE > 0) NOT VALID;
            CREATE DOMAIN tg_core.d_seq AS int DEFAULT nextval('tg_core.invoice_seq');
            CREATE DOMAIN tg_core.d_multi AS int NOT NULL CHECK (VALUE > 0) CHECK (VALUE < 100);
            CREATE DOMAIN tg_core.d_notnull AS text NOT NULL;
            CREATE DOMAIN tg_core.d_coll AS text COLLATE "C";

            -- 7, 8, 16: composite types and enums
            CREATE TYPE tg_core.t_empty AS ();
            CREATE TYPE tg_core.e_empty AS ENUM ();
            CREATE TYPE tg_core.e_status AS ENUM ('new', 'paid', 'shipped');
            CREATE TYPE tg_core.t_row AS (c tg_core.customers, n int);
            CREATE TYPE tg_core.t_coll AS (a text COLLATE "C", b int);

            -- 9, 15: aggregates
            CREATE FUNCTION tg_core.fn_ffx(int, int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1';
            CREATE AGGREGATE tg_core.agg_par(int) (
                SFUNC = int4pl, STYPE = int, SSPACE = 16, COMBINEFUNC = int4pl, PARALLEL = SAFE
            );
            CREATE AGGREGATE tg_core.agg_mv(int) (
                SFUNC = int4pl, STYPE = int, INITCOND = '0',
                MSFUNC = int4pl, MINVFUNC = int4mi, MSTYPE = int, MINITCOND = '0'
            );
            CREATE AGGREGATE tg_core.agg_max(int) (SFUNC = int4larger, STYPE = int, SORTOP = >);
            CREATE FUNCTION tg_core.fn_os_final(float8[], float8, float8) RETURNS float8 LANGUAGE sql IMMUTABLE
                AS 'SELECT $2';
            CREATE AGGREGATE tg_core.agg_os(float8 ORDER BY float8) (
                SFUNC = array_append, STYPE = float8[], FINALFUNC = tg_core.fn_os_final, FINALFUNC_EXTRA
            );

            -- 15: operator (its operator class is in tangle_superuser_objects)
            CREATE FUNCTION tg_core.fn_int_eq(int, int) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT $1 = $2';
            CREATE OPERATOR tg_core.=== (
                PROCEDURE = tg_core.fn_int_eq, LEFTARG = int, RIGHTARG = int,
                COMMUTATOR = OPERATOR(tg_core.===), HASHES, MERGES
            );
            """,
        )
        if version_num >= 110000:
            await self.db.execute(
                db_name,
                """
                CREATE AGGREGATE tg_core.agg_fx(int) (
                    SFUNC = int4pl, STYPE = int, FINALFUNC = tg_core.fn_ffx,
                    FINALFUNC_EXTRA, FINALFUNC_MODIFY = SHAREABLE
                );
                """,
            )
        else:
            await self.db.execute(
                db_name,
                """
                CREATE AGGREGATE tg_core.agg_fx(int) (
                    SFUNC = int4pl, STYPE = int, FINALFUNC = tg_core.fn_ffx, FINALFUNC_EXTRA
                );
                """,
            )
        if version_num >= 140000:
            await self.db.execute(
                db_name,
                """
                -- 4, 16
                CREATE FUNCTION tg_core.fn_atomic_total() RETURNS numeric LANGUAGE sql
                    BEGIN ATOMIC SELECT sum(amount) FROM tg_core.orders; END;
                CREATE TYPE tg_core.r_many AS RANGE (subtype = int, multirange_type_name = tg_core.r_many_set);
                """,
            )

    async def tangle_superuser_objects(self, db_name: str) -> None:
        """Objects only a superuser can create: a cast, an `internal` function, an operator class (13)."""
        await self.db.execute(
            db_name,
            """
            CREATE FUNCTION tg_core.fn_json_to_int(json) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT 1';
            CREATE CAST (json AS int4) WITH FUNCTION tg_core.fn_json_to_int(json);
            CREATE FUNCTION tg_core.fn_internal_abs(int) RETURNS int LANGUAGE internal IMMUTABLE STRICT AS 'int4abs';
            """,
        )
        if not await self.db.fetch(db_name, "SELECT 1 FROM pg_operator WHERE oprname = '===' LIMIT 1"):
            return
        await self.db.execute(
            db_name,
            """
            CREATE OPERATOR CLASS tg_core.int_hash_ops FOR TYPE int USING hash AS
                OPERATOR 1 tg_core.===, FUNCTION 1 hashint4(int);
            CREATE INDEX order_items_qty_hash ON tg_core.order_items USING hash (qty tg_core.int_hash_ops);
            """,
        )

    async def tangle_gone_objects(self, db_name: str, version_num: int) -> None:
        """Table tg_core.gone_t and what depends on it, to be excluded together with it."""
        await self.db.execute(
            db_name,
            """
            CREATE TABLE tg_core.gone_t (
                id serial PRIMARY KEY,
                customer_id int REFERENCES tg_core.customers (id),
                a int,
                b int
            );
            INSERT INTO tg_core.gone_t (customer_id, a, b) SELECT g, g, g * 2 FROM generate_series(1, 3) g;
            CREATE INDEX gone_idx ON tg_core.gone_t (a);
            COMMENT ON TABLE tg_core.gone_t IS 'gone table comment';
            COMMENT ON COLUMN tg_core.gone_t.a IS 'gone column comment';

            CREATE FUNCTION tg_core.tg_trg_fn() RETURNS trigger LANGUAGE plpgsql
                AS $$ BEGIN RETURN NEW; END $$;
            CREATE TRIGGER gone_trg BEFORE INSERT ON tg_core.gone_t
                FOR EACH ROW EXECUTE PROCEDURE tg_core.tg_trg_fn();
            ALTER TABLE tg_core.gone_t ENABLE ROW LEVEL SECURITY;
            CREATE POLICY gone_pol ON tg_core.gone_t USING (true);

            ALTER TABLE tg_core.orders
                ADD CONSTRAINT gone_fk_out FOREIGN KEY (ref_id) REFERENCES tg_core.gone_t (id);

            CREATE VIEW tg_core.gone_v AS SELECT id, a FROM tg_core.gone_t;
            CREATE MATERIALIZED VIEW tg_core.gone_mv AS SELECT id, b FROM tg_core.gone_t;
            """,
        )
        if version_num >= 100000:
            await self.db.execute(
                db_name,
                """
                CREATE STATISTICS tg_core.gone_stat ON a, b FROM tg_core.gone_t;
                CREATE PUBLICATION tg_pub FOR TABLE tg_core.customers, tg_core.gone_t;
                """,
            )

    async def tangle_gone_row_dependents(self, db_name: str, version_num: int) -> None:
        """Functions and a type that use the row of tg_core.gone_t."""
        await self.db.execute(
            db_name,
            """
            CREATE FUNCTION tg_core.gone_fn_setof() RETURNS SETOF tg_core.gone_t LANGUAGE sql
                AS 'SELECT * FROM tg_core.gone_t';
            CREATE TYPE tg_core.gone_t_row AS (r tg_core.gone_t, n int);
            """,
        )
        if version_num >= 140000:
            await self.db.execute(
                db_name,
                """
                CREATE FUNCTION tg_core.gone_fn_atomic() RETURNS bigint LANGUAGE sql
                    BEGIN ATOMIC SELECT count(*) FROM tg_core.gone_t; END;
                """,
            )

    async def tangle_gone_schema(self, db_name: str) -> None:
        """Schema tg_gone_schema (excluded by schema options) and a kept view that depends on its table."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA tg_gone_schema;
            CREATE TYPE tg_gone_schema.kind AS ENUM ('a', 'b');
            CREATE TABLE tg_gone_schema.items (id int PRIMARY KEY, kind tg_gone_schema.kind);
            INSERT INTO tg_gone_schema.items VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b');
            CREATE VIEW tg_core.gone_schema_view AS SELECT id FROM tg_gone_schema.items;
            """,
        )

    async def build_partial_dependency_case(self, db_name: str, case: str) -> None:
        """Small database where a kept table depends on a table or schema that a test excludes.

        Cases: `inheritance` (child of dep.inh_parent), `partition` (dep.part_1 of dep.part_parent, PG 10+),
        `row_column` (column with the row type of dep.rowed).
        """
        ddl = {
            "inheritance": """
                CREATE SCHEMA dep;
                CREATE TABLE dep.inh_parent (id int PRIMARY KEY);
                CREATE TABLE dep.inh_child (extra int) INHERITS (dep.inh_parent);
                CREATE TABLE dep.plain (id int);
                INSERT INTO dep.inh_parent VALUES (1), (2);
                INSERT INTO dep.inh_child VALUES (3, 30);
                INSERT INTO dep.plain VALUES (1);
            """,
            "partition": """
                CREATE SCHEMA dep;
                CREATE TABLE dep.part_parent (id int, day date) PARTITION BY RANGE (day);
                CREATE TABLE dep.part_1 PARTITION OF dep.part_parent FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
                CREATE TABLE dep.part_2 PARTITION OF dep.part_parent FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
                CREATE TABLE dep.plain (id int);
                INSERT INTO dep.part_parent VALUES (1, '2024-05-05'), (2, '2025-05-05');
                INSERT INTO dep.plain VALUES (1);
            """,
            "row_column": """
                CREATE SCHEMA dep;
                CREATE TABLE dep.rowed (id int PRIMARY KEY, name text);
                CREATE TABLE dep.holder (id int PRIMARY KEY, r dep.rowed);
                CREATE TABLE dep.plain (id int);
                INSERT INTO dep.rowed VALUES (1, 'a');
                INSERT INTO dep.holder VALUES (1, ROW(1, 'a'));
                INSERT INTO dep.plain VALUES (1);
            """,
        }
        await self.db.execute(db_name, ddl[case])

    # ==================================================================
    # Foreign keys that a partial dump or restore cannot check
    # ==================================================================

    async def build_partial_fk(self, db_name: str) -> None:
        """Schema pfk: four pairs of tables with a foreign key; the server version limits the pairs.

        `a` (PG 12+): plain a_src refers to partitioned a_ref. `b` (PG 12+): partitioned b_src refers to
        partitioned b_ref. `c` (PG 11+): partitioned c_src refers to plain c_ref. `d`: plain d_src refers to plain d_ref.
        Rows refer to the partitions `_old` and `_new` only, nothing refers to `_spare`.
        """
        version_num = await self.db.server_version_num(db_name)
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA pfk;

            CREATE TABLE pfk.d_ref (id int PRIMARY KEY, note text);
            INSERT INTO pfk.d_ref SELECT g, 'note ' || g FROM generate_series(1, 100) g;
            CREATE TABLE pfk.d_src (
                id int PRIMARY KEY,
                ref_id int,
                CONSTRAINT d_src_fk FOREIGN KEY (ref_id) REFERENCES pfk.d_ref (id)
            );
            INSERT INTO pfk.d_src SELECT g, g FROM generate_series(1, 100) g;
            """,
        )

        if version_num >= 110000:
            await self.db.execute(
                db_name,
                """
                CREATE TABLE pfk.c_ref (id int PRIMARY KEY, note text);
                INSERT INTO pfk.c_ref SELECT g, 'note ' || g FROM generate_series(1, 100) g;
                CREATE TABLE pfk.c_src (
                    id int,
                    ref_id int,
                    CONSTRAINT c_src_fk FOREIGN KEY (ref_id) REFERENCES pfk.c_ref (id)
                ) PARTITION BY RANGE (id);
                CREATE TABLE pfk.c_src_low PARTITION OF pfk.c_src FOR VALUES FROM (1) TO (26);
                CREATE TABLE pfk.c_src_high PARTITION OF pfk.c_src FOR VALUES FROM (26) TO (1000);
                INSERT INTO pfk.c_src SELECT g, g FROM generate_series(1, 50) g;
                """,
            )

        if version_num >= 120000:
            for pair in ("a", "b"):
                await self.db.execute(
                    db_name,
                    f"""
                    CREATE TABLE pfk.{pair}_ref (id int, day int, PRIMARY KEY (id, day)) PARTITION BY RANGE (day);
                    CREATE TABLE pfk.{pair}_ref_old PARTITION OF pfk.{pair}_ref FOR VALUES FROM (0) TO (100);
                    CREATE TABLE pfk.{pair}_ref_new PARTITION OF pfk.{pair}_ref FOR VALUES FROM (100) TO (200);
                    CREATE TABLE pfk.{pair}_ref_spare PARTITION OF pfk.{pair}_ref FOR VALUES FROM (200) TO (300);
                    INSERT INTO pfk.{pair}_ref SELECT g, 10 FROM generate_series(1, 50) g;
                    INSERT INTO pfk.{pair}_ref SELECT g, 150 FROM generate_series(51, 100) g;
                    INSERT INTO pfk.{pair}_ref SELECT g, 250 FROM generate_series(101, 120) g;
                    """,
                )

            await self.db.execute(
                db_name,
                """
                CREATE TABLE pfk.a_src (
                    id int PRIMARY KEY,
                    ref_id int,
                    ref_day int,
                    CONSTRAINT a_src_fk FOREIGN KEY (ref_id, ref_day) REFERENCES pfk.a_ref (id, day)
                );
                INSERT INTO pfk.a_src
                SELECT g, g, CASE WHEN g <= 50 THEN 10 ELSE 150 END FROM generate_series(1, 100) g;

                CREATE TABLE pfk.b_src (
                    id int,
                    ref_id int,
                    ref_day int,
                    CONSTRAINT b_src_fk FOREIGN KEY (ref_id, ref_day) REFERENCES pfk.b_ref (id, day)
                ) PARTITION BY RANGE (id);
                CREATE TABLE pfk.b_src_low PARTITION OF pfk.b_src FOR VALUES FROM (1) TO (51);
                CREATE TABLE pfk.b_src_high PARTITION OF pfk.b_src FOR VALUES FROM (51) TO (1000);
                INSERT INTO pfk.b_src
                SELECT g, g, CASE WHEN g <= 50 THEN 10 ELSE 150 END FROM generate_series(1, 100) g;
                """,
            )

    async def build_partial_fk_two_levels(self, db_name: str) -> None:
        """Schema pfk (PG 12+): e_ref with two levels of partitions; e_src refers to its root, e_mid_src to its middle level."""
        if await self.db.server_version_num(db_name) < 120000:
            return
        await self.db.execute(
            db_name,
            """
            CREATE TABLE pfk.e_ref (id int, day int, PRIMARY KEY (id, day)) PARTITION BY RANGE (day);
            CREATE TABLE pfk.e_ref_y1 PARTITION OF pfk.e_ref FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id);
            CREATE TABLE pfk.e_ref_y1_a PARTITION OF pfk.e_ref_y1 FOR VALUES FROM (1) TO (51);
            CREATE TABLE pfk.e_ref_y1_b PARTITION OF pfk.e_ref_y1 FOR VALUES FROM (51) TO (1000);
            CREATE TABLE pfk.e_ref_y2 PARTITION OF pfk.e_ref FOR VALUES FROM (100) TO (200);
            INSERT INTO pfk.e_ref SELECT g, 10 FROM generate_series(1, 100) g;
            INSERT INTO pfk.e_ref SELECT g, 150 FROM generate_series(101, 120) g;

            CREATE TABLE pfk.e_src (
                id int PRIMARY KEY,
                ref_id int,
                ref_day int,
                CONSTRAINT e_src_fk FOREIGN KEY (ref_id, ref_day) REFERENCES pfk.e_ref (id, day)
            );
            INSERT INTO pfk.e_src SELECT id, id, day FROM pfk.e_ref;

            CREATE TABLE pfk.e_mid_src (
                id int PRIMARY KEY,
                ref_id int,
                ref_day int,
                CONSTRAINT e_mid_src_fk FOREIGN KEY (ref_id, ref_day) REFERENCES pfk.e_ref_y1 (id, day)
            );
            INSERT INTO pfk.e_mid_src SELECT g, g, 10 FROM generate_series(1, 100) g;
            """,
        )

    async def build_partial_fk_null_refs(self, db_name: str) -> None:
        """Schema pfk: d_null_src and partitioned c_null_src (PG 11+) refer to d_ref and c_ref with NULL only."""
        await self.db.execute(
            db_name,
            """
            CREATE TABLE pfk.d_null_src (
                id int PRIMARY KEY,
                ref_id int,
                CONSTRAINT d_null_src_fk FOREIGN KEY (ref_id) REFERENCES pfk.d_ref (id)
            );
            INSERT INTO pfk.d_null_src SELECT g, NULL FROM generate_series(1, 10) g;
            """,
        )
        if await self.db.server_version_num(db_name) < 110000:
            return
        await self.db.execute(
            db_name,
            """
            CREATE TABLE pfk.c_null_src (
                id int,
                ref_id int,
                CONSTRAINT c_null_src_fk FOREIGN KEY (ref_id) REFERENCES pfk.c_ref (id)
            ) PARTITION BY RANGE (id);
            CREATE TABLE pfk.c_null_src_low PARTITION OF pfk.c_null_src FOR VALUES FROM (1) TO (6);
            CREATE TABLE pfk.c_null_src_high PARTITION OF pfk.c_null_src FOR VALUES FROM (6) TO (1000);
            INSERT INTO pfk.c_null_src SELECT g, NULL FROM generate_series(1, 10) g;
            """,
        )

    # ==================================================================
    # Partial lists that name a partitioned table
    # ==================================================================

    async def build_partition_lists(self, db_name: str) -> None:
        """Schema plist: partitioned `transactions` (102 rows) with a nested partition, and plain `accounts`."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA plist;

            CREATE TABLE plist.transactions (id int, day date) PARTITION BY RANGE (day);
            CREATE TABLE plist.transactions_2025_01 PARTITION OF plist.transactions
                FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
            CREATE TABLE plist.transactions_2025_02 PARTITION OF plist.transactions
                FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
            CREATE TABLE plist.transactions_2025_03 PARTITION OF plist.transactions
                FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
            CREATE TABLE plist.transactions_2026 PARTITION OF plist.transactions
                FOR VALUES FROM ('2026-01-01') TO ('2027-01-01') PARTITION BY RANGE (day);
            CREATE TABLE plist.transactions_2026_h1 PARTITION OF plist.transactions_2026
                FOR VALUES FROM ('2026-01-01') TO ('2026-07-01');
            CREATE TABLE plist.transactions_2026_h2 PARTITION OF plist.transactions_2026
                FOR VALUES FROM ('2026-07-01') TO ('2027-01-01');

            INSERT INTO plist.transactions SELECT g, '2025-01-01'::date + g - 1 FROM generate_series(1, 30) g;
            INSERT INTO plist.transactions SELECT g, '2025-02-01'::date + g - 1 FROM generate_series(1, 28) g;
            INSERT INTO plist.transactions SELECT g, '2025-03-01'::date + g - 1 FROM generate_series(1, 31) g;
            INSERT INTO plist.transactions VALUES (32, '2025-03-31');
            INSERT INTO plist.transactions SELECT g, '2026-01-01'::date + g - 1 FROM generate_series(1, 7) g;
            INSERT INTO plist.transactions SELECT g, '2026-07-01'::date + g - 1 FROM generate_series(1, 5) g;

            CREATE TABLE plist.accounts (id int PRIMARY KEY);
            INSERT INTO plist.accounts SELECT generate_series(1, 3);
            """,
        )

    async def build_inherits_lists(self, db_name: str) -> None:
        """Schema ilist: `notifications` with INHERITS children on two levels, `reads` with a key to it, `accounts`."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA ilist;

            CREATE TABLE ilist.notifications (id int PRIMARY KEY, message text);
            CREATE TABLE ilist.email_notifications (recipient text) INHERITS (ilist.notifications);
            CREATE TABLE ilist.sms_notifications (phone text) INHERITS (ilist.notifications);
            CREATE TABLE ilist.sms_urgent (level int) INHERITS (ilist.sms_notifications);
            CREATE TABLE ilist.marks (mark text);
            CREATE TABLE ilist.marked_email () INHERITS (ilist.email_notifications, ilist.marks);

            INSERT INTO ilist.notifications SELECT g, 'message ' || g FROM generate_series(1, 10) g;
            INSERT INTO ilist.email_notifications SELECT g, 'email ' || g, 'user' || g FROM generate_series(11, 15) g;
            INSERT INTO ilist.sms_notifications SELECT g, 'sms ' || g, '555-' || g FROM generate_series(16, 19) g;
            INSERT INTO ilist.sms_urgent SELECT g, 'urgent ' || g, '555-' || g, 1 FROM generate_series(20, 22) g;
            INSERT INTO ilist.marks VALUES ('first'), ('second');
            INSERT INTO ilist.marked_email VALUES (23, 'marked', 'user23', 'third');

            CREATE TABLE ilist.accounts (id int PRIMARY KEY);
            INSERT INTO ilist.accounts SELECT generate_series(1, 3);

            CREATE TABLE ilist.reads (
                id int PRIMARY KEY,
                notification_id int CONSTRAINT reads_notification_fk REFERENCES ilist.notifications (id)
            );
            INSERT INTO ilist.reads SELECT g, g FROM generate_series(1, 4) g;
            """,
        )

    async def build_foreign_lists(self, db_name: str) -> None:
        """Schema flist: two foreign tables and plain `accounts`; needs postgres_fdw."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA flist;
            CREATE EXTENSION postgres_fdw;
            CREATE SERVER flist_srv FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'partner.internal', dbname 'partner');

            CREATE FOREIGN TABLE flist.remote_rates (id int, rate int) SERVER flist_srv;
            CREATE FOREIGN TABLE flist.remote_spare (id int) SERVER flist_srv;

            CREATE TABLE flist.accounts (id int PRIMARY KEY);
            INSERT INTO flist.accounts SELECT generate_series(1, 3);
            """,
        )

    async def build_foreign_child(self, db_name: str) -> None:
        """Schema fchild: local `local_parent` with a foreign INHERITS child, plain `accounts`; needs postgres_fdw."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA fchild;
            CREATE EXTENSION postgres_fdw;
            CREATE SERVER fchild_srv FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'partner.internal', dbname 'partner');

            CREATE TABLE fchild.local_parent (id int, note text);
            CREATE FOREIGN TABLE fchild.remote_archive () INHERITS (fchild.local_parent) SERVER fchild_srv;
            INSERT INTO fchild.local_parent VALUES (1, 'a'), (2, 'b');

            CREATE TABLE fchild.accounts (id int PRIMARY KEY);
            INSERT INTO fchild.accounts SELECT generate_series(1, 3);
            """,
        )

    async def build_foreign_partition(self, db_name: str) -> None:
        """Schema fpart: partitioned `events` with a local and a foreign partition, plain `accounts`; PG 10+, postgres_fdw."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA fpart;
            CREATE EXTENSION postgres_fdw;
            CREATE SERVER fpart_srv FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'partner.internal', dbname 'partner');

            CREATE TABLE fpart.events (id int, day date) PARTITION BY RANGE (day);
            CREATE TABLE fpart.events_local PARTITION OF fpart.events
                FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
            CREATE FOREIGN TABLE fpart.events_remote PARTITION OF fpart.events
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01') SERVER fpart_srv;
            INSERT INTO fpart.events SELECT g, '2025-01-01'::date + g FROM generate_series(1, 4) g;

            CREATE TABLE fpart.accounts (id int PRIMARY KEY);
            INSERT INTO fpart.accounts SELECT generate_series(1, 3);
            """,
        )

    async def build_publication_membership(self, db_name: str) -> None:
        """Publication `p` over public.t, s.t2, s.t3, x.t4; PG 15+ also `p_schema` FOR TABLES IN SCHEMA s; PG 10+."""
        await self.db.execute(
            db_name,
            """
            CREATE TABLE public.t (a int PRIMARY KEY);
            CREATE SCHEMA s;
            CREATE TABLE s.t2 (a int PRIMARY KEY);
            CREATE TABLE s.t3 (a int PRIMARY KEY);
            CREATE SCHEMA x;
            CREATE TABLE x.t4 (a int PRIMARY KEY);
            INSERT INTO public.t VALUES (1), (2);
            INSERT INTO s.t2 VALUES (1);
            INSERT INTO s.t3 VALUES (1);
            INSERT INTO x.t4 VALUES (1);
            CREATE PUBLICATION p FOR TABLE public.t, s.t2, s.t3, x.t4;
            """,
        )
        if await self.db.server_version_num(db_name) >= 150000:
            await self.db.execute(db_name, "CREATE PUBLICATION p_schema FOR TABLES IN SCHEMA s;")

    # ==================================================================
    # Structure cleanup in a staging database
    # ==================================================================

    async def build_cleanup_case(self, db_name: str) -> None:
        """Schema cln: tables with rows and objects that a cleanup in a staging database removes; needs postgres_fdw."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA cln;
            CREATE EXTENSION postgres_fdw;
            CREATE SERVER cln_partner_srv FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'partner.internal', dbname 'partner');

            CREATE FUNCTION cln.secret_rate() RETURNS int LANGUAGE sql AS 'SELECT 42';
            CREATE FUNCTION cln.touch_payment() RETURNS trigger LANGUAGE plpgsql AS
                'BEGIN NEW.amount := NEW.amount + 1; RETURN NEW; END';

            CREATE TABLE cln.clients (
                id serial PRIMARY KEY,
                name text NOT NULL,
                balance int CONSTRAINT clients_balance_check CHECK (balance >= 0)
            );
            CREATE TABLE cln.payments (
                id serial PRIMARY KEY,
                client_id int NOT NULL CONSTRAINT payments_client_fk REFERENCES cln.clients (id),
                amount int
            );
            CREATE INDEX payments_amount_idx ON cln.payments (amount);
            CREATE TABLE cln.spare (id int PRIMARY KEY, label text);

            INSERT INTO cln.clients (name, balance) SELECT 'client ' || g, g FROM generate_series(1, 5) g;
            INSERT INTO cln.payments (client_id, amount) SELECT (g % 5) + 1, g FROM generate_series(1, 10) g;
            INSERT INTO cln.spare SELECT g, 'label ' || g FROM generate_series(1, 3) g;

            CREATE TRIGGER payments_touch_trg BEFORE INSERT ON cln.payments
                FOR EACH ROW EXECUTE PROCEDURE cln.touch_payment();
            """,
        )

    # ==================================================================
    # Two foreign keys with one name on different tables
    # ==================================================================

    async def build_same_name_fk(self, db_name: str) -> None:
        """Schema dup: x refers to a and y refers to b, both keys have the name fk_ref; 100 rows in each table."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA dup;
            CREATE TABLE dup.a (id int PRIMARY KEY);
            CREATE TABLE dup.b (id int PRIMARY KEY);
            CREATE TABLE dup.x (
                id int PRIMARY KEY, ref_id int, CONSTRAINT fk_ref FOREIGN KEY (ref_id) REFERENCES dup.a (id)
            );
            CREATE TABLE dup.y (
                id int PRIMARY KEY, ref_id int, CONSTRAINT fk_ref FOREIGN KEY (ref_id) REFERENCES dup.b (id)
            );
            INSERT INTO dup.a SELECT generate_series(1, 100);
            INSERT INTO dup.b SELECT generate_series(1, 100);
            INSERT INTO dup.x SELECT g, g FROM generate_series(1, 100) g;
            INSERT INTO dup.y SELECT g, g FROM generate_series(1, 100) g;
            """,
        )

    # ==================================================================
    # Filters in the sync modes
    # ==================================================================

    async def build_sync_filters(self, db_name: str) -> None:
        """Schemas s1 (a: 10 rows, b: 20 rows) and s2 (d: 5 rows, e: 7 rows); the tables have no links."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA s1;
            CREATE SCHEMA s2;
            CREATE TABLE s1.a (id int PRIMARY KEY);
            CREATE TABLE s1.b (id int PRIMARY KEY);
            CREATE TABLE s2.d (id int PRIMARY KEY);
            CREATE TABLE s2.e (id int PRIMARY KEY);
            INSERT INTO s1.a SELECT generate_series(1, 10);
            INSERT INTO s1.b SELECT generate_series(1, 20);
            INSERT INTO s2.d SELECT generate_series(1, 5);
            INSERT INTO s2.e SELECT generate_series(1, 7);
            """,
        )

    async def build_sync_views(self, db_name: str) -> None:
        """Schema slink: clients (5 rows) and orders (10 rows) with an index, a trigger and views over them."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA slink;
            CREATE FUNCTION slink.touch_client() RETURNS trigger LANGUAGE plpgsql AS
                'BEGIN NEW.name := upper(NEW.name); RETURN NEW; END';

            CREATE TABLE slink.clients (id int PRIMARY KEY, name text CONSTRAINT clients_name_check CHECK (name <> ''));
            CREATE INDEX clients_name_idx ON slink.clients (name);
            CREATE TRIGGER clients_touch_trg BEFORE INSERT ON slink.clients
                FOR EACH ROW EXECUTE PROCEDURE slink.touch_client();
            CREATE TABLE slink.orders (id int PRIMARY KEY, client_id int, note text);
            CREATE INDEX orders_note_idx ON slink.orders (note);

            INSERT INTO slink.clients SELECT g, 'client ' || g FROM generate_series(1, 5) g;
            INSERT INTO slink.orders SELECT g, (g % 5) + 1, 'note ' || g FROM generate_series(1, 10) g;

            CREATE VIEW slink.client_names AS SELECT id, name FROM slink.clients;
            CREATE VIEW slink.order_notes AS SELECT id, note FROM slink.orders;
            CREATE VIEW slink.client_orders AS
                SELECT o.id, c.name FROM slink.orders o JOIN slink.clients c ON c.id = o.client_id;
            """,
        )

    async def build_sync_fk(self, db_name: str) -> None:
        """Schema sfk: c1 and c2 refer to p, r refers to q, `lone` has no keys; 5 rows in each table."""
        await self.db.execute(
            db_name,
            """
            CREATE SCHEMA sfk;
            CREATE TABLE sfk.p (id int PRIMARY KEY);
            CREATE TABLE sfk.q (id int PRIMARY KEY);
            CREATE TABLE sfk.lone (id int PRIMARY KEY);
            CREATE TABLE sfk.c1 (id int PRIMARY KEY, p_id int CONSTRAINT c1_p_fk REFERENCES sfk.p (id));
            CREATE TABLE sfk.c2 (id int PRIMARY KEY, p_id int CONSTRAINT c2_p_fk REFERENCES sfk.p (id));
            CREATE TABLE sfk.r (id int PRIMARY KEY, q_id int CONSTRAINT r_q_fk REFERENCES sfk.q (id));
            INSERT INTO sfk.p SELECT generate_series(1, 5);
            INSERT INTO sfk.q SELECT generate_series(1, 5);
            INSERT INTO sfk.lone SELECT generate_series(1, 5);
            INSERT INTO sfk.c1 SELECT g, g FROM generate_series(1, 5) g;
            INSERT INTO sfk.c2 SELECT g, g FROM generate_series(1, 5) g;
            INSERT INTO sfk.r SELECT g, g FROM generate_series(1, 5) g;
            """,
        )

    async def build_wide_1c(self, db_name: str, tables: int, rows: int = 2, batch: int = 500, workers: int = 4) -> None:
        """Schema public in 1C style: tables like _reference1, _document2 with _fldN columns; every 10th has e-mails."""
        semaphore = asyncio.Semaphore(workers)

        async def create_batch(start: int) -> None:
            end = min(start + batch - 1, tables)
            async with semaphore:
                await self._create_wide_1c_batch(db_name, start, end, rows)

        await asyncio.gather(*(create_batch(start) for start in range(1, tables + 1, batch)))

    async def _create_wide_1c_batch(self, db_name: str, start: int, end: int, rows: int) -> None:
        # One batch is one transaction: keep it small, so it does not run out of lock slots.
        await self.db.execute(
            db_name,
            f"""
            DO $$
            DECLARE
                i int;
                t text;
            BEGIN
                FOR i IN {start}..{end} LOOP
                    t := (ARRAY['_reference', '_document', '_inforg', '_accumrg'])[i % 4 + 1] || i;
                    EXECUTE format(
                        'CREATE TABLE public.%I (
                            _idrref bytea PRIMARY KEY,
                            _version int NOT NULL,
                            _marked boolean NOT NULL,
                            _description varchar(150),
                            %I numeric(15, 2),
                            %I varchar(100),
                            %I timestamp
                        )',
                        t, '_fld' || i * 10 + 1, '_fld' || i * 10 + 2, '_fld' || i * 10 + 3
                    );
                    EXECUTE format('CREATE INDEX ON public.%I (_description)', t);
                    EXECUTE format(
                        'INSERT INTO public.%I
                         SELECT decode(md5(%L || g), ''hex''), 1, false, ''item '' || g, g * 1.5,
                                CASE WHEN %s THEN ''user'' || g || ''@example.com'' ELSE ''value '' || g END,
                                now()
                         FROM generate_series(1, %s) g',
                        t, t, (i % 10 = 0)::text, {rows}
                    );
                END LOOP;
            END
            $$;
            """,
        )
