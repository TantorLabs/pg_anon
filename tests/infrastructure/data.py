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

from typing import TYPE_CHECKING

from tests.infrastructure.sizes import SMALL, TINY

if TYPE_CHECKING:
    from tests.infrastructure.db import DBManager


class Fixtures:
    def __init__(self, db: DBManager) -> None:
        self.db = db

    # ==================================================================
    # Extensions / shared prerequisites
    # ==================================================================

    async def ensure_extensions(self, db_name: str) -> None:
        """Enable extensions that fixtures rely on. Safe to call repeatedly."""
        await self.db.execute(db_name, """
            CREATE EXTENSION IF NOT EXISTS pgcrypto;
            CREATE EXTENSION IF NOT EXISTS citext;
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            CREATE EXTENSION IF NOT EXISTS hstore;
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        """)

    async def ensure_anon_ext(self, db_name: str) -> None:
        """Helper plpgsql functions used by anonymization rules and scanner checks."""
        await self.db.execute(db_name, """
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
        """)

    # ==================================================================
    # hr — employees, departments, salaries
    # ==================================================================

    async def build_hr(self, db_name: str, employees: int = SMALL) -> None:
        await self.db.execute(db_name, """
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
                full_name varchar(130) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
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
        """)

        await self.db.execute(db_name, f"""
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
        """)

    # ==================================================================
    # billing — customers, invoices, payment cards, transactions
    # ==================================================================

    async def build_billing(self, db_name: str, customers: int = SMALL) -> None:
        await self.db.execute(db_name, """
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
        """)

        await self.db.execute(db_name, f"""
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
        """)

    # ==================================================================
    # ecommerce — products, categories (self-ref), orders, reviews
    # ==================================================================

    async def build_ecommerce(self, db_name: str, products: int = SMALL) -> None:
        await self.db.execute(db_name, """
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
        """)

        await self.db.execute(db_name, f"""
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
        """)

    # ==================================================================
    # audit — log entries, login attempts (inet, uuid, jsonb)
    # ==================================================================

    async def build_audit(self, db_name: str, entries: int = SMALL) -> None:
        await self.db.execute(db_name, """
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
        """)

        await self.db.execute(db_name, f"""
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
        """)

    # ==================================================================
    # content — articles (tsvector, jsonb, text[], materialized view)
    # ==================================================================

    async def build_content(self, db_name: str, articles: int = SMALL) -> None:
        await self.db.execute(db_name, """
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
                FOR EACH ROW EXECUTE FUNCTION content.article_update_search_vector();

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
        """)

        await self.db.execute(db_name, f"""
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
        """)

        await self.db.execute(db_name, """
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
        """)

    # ==================================================================
    # analytics — partitioned tables: RANGE, LIST, HASH
    # ==================================================================

    async def build_analytics(self, db_name: str) -> None:
        await self.db.execute(db_name, """
            CREATE SCHEMA IF NOT EXISTS analytics;
            CREATE SCHEMA IF NOT EXISTS analytics_archive;

            -- RANGE partitioning by date
            CREATE TABLE IF NOT EXISTS analytics.event (
                id bigserial,
                event_date date NOT NULL,
                tenant_id integer NOT NULL,
                region varchar(16) NOT NULL,
                payload jsonb NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (id, event_date)
            ) PARTITION BY RANGE (event_date);

            CREATE TABLE IF NOT EXISTS analytics.event_2025_01
                PARTITION OF analytics.event
                FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
            CREATE TABLE IF NOT EXISTS analytics.event_2025_02
                PARTITION OF analytics.event
                FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
            CREATE TABLE IF NOT EXISTS analytics_archive.event_2024
                PARTITION OF analytics.event
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
            CREATE TABLE IF NOT EXISTS analytics.event_default
                PARTITION OF analytics.event DEFAULT;

            CREATE INDEX IF NOT EXISTS event_tenant_idx ON analytics.event (tenant_id);

            -- LIST partitioning by region
            CREATE TABLE IF NOT EXISTS analytics.event_by_region (
                id bigserial,
                region varchar(16) NOT NULL,
                occurred_at timestamptz NOT NULL DEFAULT now(),
                metric_name varchar(64) NOT NULL,
                value numeric NOT NULL,
                PRIMARY KEY (id, region)
            ) PARTITION BY LIST (region);

            CREATE TABLE IF NOT EXISTS analytics.event_by_region_ru
                PARTITION OF analytics.event_by_region FOR VALUES IN ('RU');
            CREATE TABLE IF NOT EXISTS analytics.event_by_region_eu
                PARTITION OF analytics.event_by_region FOR VALUES IN ('EU','DE','FR');
            CREATE TABLE IF NOT EXISTS analytics.event_by_region_other
                PARTITION OF analytics.event_by_region DEFAULT;

            -- HASH partitioning by tenant
            CREATE TABLE IF NOT EXISTS analytics.event_by_tenant (
                id bigserial,
                tenant_id integer NOT NULL,
                occurred_at timestamptz NOT NULL DEFAULT now(),
                data jsonb NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (id, tenant_id)
            ) PARTITION BY HASH (tenant_id);

            CREATE TABLE IF NOT EXISTS analytics.event_by_tenant_h0
                PARTITION OF analytics.event_by_tenant FOR VALUES WITH (modulus 4, remainder 0);
            CREATE TABLE IF NOT EXISTS analytics.event_by_tenant_h1
                PARTITION OF analytics.event_by_tenant FOR VALUES WITH (modulus 4, remainder 1);
            CREATE TABLE IF NOT EXISTS analytics.event_by_tenant_h2
                PARTITION OF analytics.event_by_tenant FOR VALUES WITH (modulus 4, remainder 2);
            CREATE TABLE IF NOT EXISTS analytics.event_by_tenant_h3
                PARTITION OF analytics.event_by_tenant FOR VALUES WITH (modulus 4, remainder 3);
        """)

        await self.db.execute(db_name, """
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
        """)

    # ==================================================================
    # quirks — quoted identifiers, reserved words, Cyrillic, NULLs
    # ==================================================================

    async def build_quirks(self, db_name: str) -> None:
        await self.db.execute(db_name, """
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
        """)

        await self.db.execute(db_name, """
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
        """)

        await self.db.execute(db_name, """
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
        """)

    # ==================================================================
    # data_types — one row per PG type for round-trip checks
    # ==================================================================

    async def build_data_types(self, db_name: str) -> None:
        await self.db.execute(db_name, """
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
        """)

    # ==================================================================
    # table_variants — rare declarations that pg_dump must preserve
    # ==================================================================

    async def build_table_variants(self, db_name: str) -> None:
        await self.db.execute(db_name, """
            CREATE SCHEMA IF NOT EXISTS variants;

            -- Custom SEQUENCE with non-default parameters + OWNED BY
            CREATE SEQUENCE IF NOT EXISTS variants.custom_seq
                INCREMENT BY 10
                MINVALUE 100
                MAXVALUE 100000
                START WITH 500
                CACHE 5;

            -- GENERATED BY DEFAULT AS IDENTITY (pg10+)
            CREATE TABLE IF NOT EXISTS variants.with_identity (
                id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
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
        """)

        # Event trigger must be created at DB level, ignore if role lacks perms.
        await self.db.execute(db_name, """
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
                    EXECUTE FUNCTION variants.log_ddl();
            EXCEPTION
                WHEN insufficient_privilege THEN NULL;
                WHEN duplicate_object THEN NULL;
            END $$;
        """)

    # ==================================================================
    # fdw — foreign data wrapper loopback (optional; skipped if perms missing)
    # ==================================================================

    async def build_fdw(self, db_name: str) -> None:
        """Foreign server + foreign table via postgres_fdw. If the extension is.

        not installed or the role can't CREATE SERVER, the builder silently
        degrades so the rest of the zoo is unaffected.
        """
        await self.db.execute(db_name, """
            DO $$ BEGIN
                CREATE EXTENSION IF NOT EXISTS postgres_fdw;
            EXCEPTION WHEN insufficient_privilege THEN NULL; END $$;

            DO $$ BEGIN
                CREATE SCHEMA IF NOT EXISTS fdw_ext;

                CREATE SERVER IF NOT EXISTS loopback_server
                    FOREIGN DATA WRAPPER postgres_fdw
                    OPTIONS (host 'localhost', dbname 'postgres', port '5432');

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
        """)

    # ==================================================================
    # circular_fk — mutually referencing tables with DEFERRABLE FKs
    # ==================================================================

    async def build_circular_fk(self, db_name: str) -> None:
        """Two tables with circular FKs using DEFERRABLE INITIALLY DEFERRED.

        Exercises pg_anon's parallel COPY + session_replication_role='replica'
        path: neither table can be loaded first if FKs are eagerly checked.
        """
        await self.db.execute(db_name, """
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
        """)

    # ==================================================================
    # Security — RLS
    # ==================================================================

    async def build_security(self, db_name: str) -> None:
        """Row-level security policies on a table. pg_dump must preserve policies."""
        await self.db.execute(db_name, """
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
        """)

    # ==================================================================
    # privileges — GRANT / REVOKE / OWNER / DEFAULT PRIVILEGES / multi-role
    # ==================================================================

    async def build_privileges(self, db_name: str) -> None:
        """Custom roles + table-level and column-level GRANTs, an ALTER OWNER,
        and a DEFAULT PRIVILEGES rule. Wrapped in DO blocks — role creation
        requires CREATEROLE and fails gracefully on restricted test setups.
        """
        await self.db.execute(db_name, """
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

            -- Column-level GRANT: anon_tester_reader must NOT see secret_payload
            DO $$ BEGIN
                REVOKE SELECT (secret_payload) ON privs.public_facing FROM anon_tester_reader;
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
        """)

    async def build_index_quirks(self, db_name: str) -> None:
        """Index types: hash, spgist, gist, covering INCLUDE, COLLATE, special names."""
        await self.db.execute(db_name, """
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
                ON index_quirks.events (region) INCLUDE (name, occurred_at);

            CREATE INDEX IF NOT EXISTS events_name_collate_c
                ON index_quirks.events (name COLLATE "C");

            CREATE INDEX IF NOT EXISTS "_idx.$weird#имя"
                ON index_quirks.events (region, user_id);
        """)

    async def build_constraints_quirks(self, db_name: str) -> None:
        """Sub-partitioning, FK on partitioned parent, EXCLUDE USING gist, NOT VALID FK."""
        await self.db.execute(db_name, """
            CREATE SCHEMA IF NOT EXISTS constraints_quirks;

            CREATE TABLE IF NOT EXISTS constraints_quirks.subpart_root (
                id bigserial NOT NULL,
                tenant_id integer NOT NULL,
                occurred_at timestamptz NOT NULL,
                payload jsonb,
                PRIMARY KEY (id, tenant_id, occurred_at)
            ) PARTITION BY RANGE (occurred_at);

            CREATE TABLE IF NOT EXISTS constraints_quirks.subpart_2025_q1
                PARTITION OF constraints_quirks.subpart_root
                FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')
                PARTITION BY LIST (tenant_id);

            CREATE TABLE IF NOT EXISTS constraints_quirks.subpart_2025_q1_t123
                PARTITION OF constraints_quirks.subpart_2025_q1
                FOR VALUES IN (1, 2, 3);

            CREATE TABLE IF NOT EXISTS constraints_quirks.subpart_2025_q1_other
                PARTITION OF constraints_quirks.subpart_2025_q1 DEFAULT;

            CREATE TABLE IF NOT EXISTS constraints_quirks.subpart_2025_q2
                PARTITION OF constraints_quirks.subpart_root
                FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');

            INSERT INTO constraints_quirks.subpart_root (id, tenant_id, occurred_at, payload) VALUES
                (1, 1, '2025-01-15 10:00+00', '{"k":1}'::jsonb),
                (2, 2, '2025-02-10 12:00+00', '{"k":2}'::jsonb),
                (3, 9, '2025-03-05 18:30+00', '{"k":3}'::jsonb),
                (4, 1, '2025-05-20 09:15+00', '{"k":4}'::jsonb)
            ON CONFLICT DO NOTHING;

            CREATE TABLE IF NOT EXISTS constraints_quirks.evt_attachment (
                id serial PRIMARY KEY,
                evt_id bigint NOT NULL,
                tenant_id integer NOT NULL,
                occurred_at timestamptz NOT NULL,
                note text,
                CONSTRAINT evt_attachment_evt_fk FOREIGN KEY
                    (evt_id, tenant_id, occurred_at)
                    REFERENCES constraints_quirks.subpart_root (id, tenant_id, occurred_at)
                    ON DELETE CASCADE
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
        """)

    # ==================================================================
    # publications — logical replication PUBLICATION
    # ==================================================================

    async def build_publications(self, db_name: str) -> None:
        """Logical replication PUBLICATION on a curated table set.

        SUBSCRIPTION is intentionally NOT created here: it requires a second
        PG instance to be meaningful, and even a degraded `connect = false`
        subscription registers in pg_subscription and blocks DROP DATABASE.
        """
        await self.db.execute(db_name, """
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
        """)

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
