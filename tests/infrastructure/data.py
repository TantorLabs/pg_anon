from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.infrastructure.db import DBManager

DEFAULT_ROWS = 1512


class TestData:

    def __init__(self, db_manager: DBManager, rows: int = DEFAULT_ROWS) -> None:
        self.db = db_manager
        self.rows = rows

    def _n(self, count: int | None) -> int:
        return count if count is not None else self.rows

    # ------------------------------------------------------------------
    # Shared idempotent helpers
    # ------------------------------------------------------------------

    async def _ensure_contract_status_enum(self, db_name: str) -> None:
        await self.db.execute(db_name, """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'contract_status') THEN
                    CREATE TYPE contract_status AS ENUM('pending', 'processing', 'active', 'closed');
                END IF;
            END$$
        """)

    async def _ensure_test_anon_funcs(self, db_name: str) -> None:
        await self.db.execute(db_name, """
            CREATE SCHEMA IF NOT EXISTS test_anon_funcs;

            CREATE OR REPLACE FUNCTION test_anon_funcs.test_check_is_company_email(
                value TEXT, schema_name TEXT, table_name TEXT, field_name TEXT
            ) RETURNS boolean AS $$
            DECLARE
                result boolean;
                email_part TEXT;
            BEGIN
                email_part := SPLIT_PART(value, '@', '2');
                IF email_part = '' THEN
                    RETURN false;
                END IF;
                EXECUTE 'SELECT EXISTS (SELECT * FROM schm_customer.customer_company WHERE email LIKE ''%' || email_part || ''')' INTO result;
                RETURN result;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION test_anon_funcs.test_check_by_fts_is_include_organization_title(
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
                    organization_titles := current_setting('test_scan.org_titles');
                EXCEPTION WHEN OTHERS THEN
                    SELECT string_agg(regexp_replace(company_name, '\\s+', ' & ', 'g'), ' | ')
                    INTO organization_titles
                    FROM schm_customer.customer_company;
                    PERFORM set_config('test_scan.org_titles', organization_titles, false);
                END;
                IF organization_titles IS NULL THEN
                    RETURN FALSE;
                END IF;
                sql := format(
                    'SELECT EXISTS (
                        SELECT 1 FROM %I.%I
                        WHERE to_tsvector(''simple'', %I) @@ to_tsquery(''simple'', %L)
                    )',
                    schema_name, table_name, field_name, organization_titles
                );
                EXECUTE sql INTO res;
                RETURN res;
            END;
            $fn$ LANGUAGE plpgsql
        """)

    async def _ensure_columnar_internal(self, db_name: str) -> None:
        await self.db.execute(db_name, """
            CREATE SCHEMA IF NOT EXISTS columnar_internal;
            CREATE TABLE IF NOT EXISTS columnar_internal.tbl_200 (
                id integer,
                val text,
                val_skip text
            )
        """)

    # ------------------------------------------------------------------
    # Generic: simple (id serial, val text) tables
    # ------------------------------------------------------------------

    async def simple_text_table(
        self, db_name: str, schema: str, table: str, constraint_name: str,
        count: int | None = None,
    ) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS "{schema}";
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                id serial,
                val text,
                CONSTRAINT {constraint_name} UNIQUE (id)
            );
            INSERT INTO "{schema}"."{table}" (val)
            SELECT 'text_val_' || v AS val
            FROM generate_series(1, {n}) AS v
        """)

    # ------------------------------------------------------------------
    # Tables
    # ------------------------------------------------------------------

    async def customer_companies(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_customer;
            CREATE TABLE IF NOT EXISTS schm_customer.customer_company (
                id serial,
                company_name character varying(32),
                email character varying(64),
                phone character varying(32),
                site character varying(64),
                inn bigint,
                CONSTRAINT customer_company_pkey UNIQUE (id),
                CONSTRAINT inn_uniq UNIQUE (inn)
            );
            INSERT INTO schm_customer.customer_company
                (company_name, email, phone, site, inn)
            SELECT
                'company_name_' || v AS company_name,
                'info' || v || '@' || 'company_name_' || v || '.com' AS email,
                79101438060 + v AS phone,
                'company_name_' || v || '.com' AS site,
                10000000 + v * 10 AS inn
            FROM generate_series(1, {n}) AS v
        """)

    async def customer_contracts(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_customer;
            CREATE TABLE IF NOT EXISTS schm_customer.customer_contract (
                id serial PRIMARY KEY,
                customer_company_id integer NOT NULL
                    REFERENCES schm_customer.customer_company(id),
                contract_text text NOT NULL
            );
            INSERT INTO schm_customer.customer_contract (customer_company_id, contract_text)
            SELECT
                c.id,
                format(
                    'Настоящий договор заключён между ООО "%s" и Заказчиком.
                    ООО "%s" обязуется оказать услуги в соответствии с условиями договора.',
                    c.company_name,
                    c.company_name
                )
            FROM (
                SELECT id, company_name
                FROM schm_customer.customer_company
                ORDER BY id
                LIMIT {n}
            ) c;
            CREATE INDEX IF NOT EXISTS customer_contract_fts_idx
                ON schm_customer.customer_contract
                USING GIN (to_tsvector('simple', contract_text))
        """)

    async def customer_managers(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_customer;
            CREATE TABLE IF NOT EXISTS schm_customer.customer_manager (
                id serial,
                customer_company_id integer NOT NULL,
                first_name character varying(32),
                last_name character varying(32),
                email character varying(64),
                phone character varying(32),
                CONSTRAINT customer_manager_pkey UNIQUE (id),
                CONSTRAINT customer_company_id_fk
                    FOREIGN KEY (customer_company_id)
                    REFERENCES schm_customer.customer_company(id)
            );
            INSERT INTO schm_customer.customer_manager
                (customer_company_id, first_name, last_name, email, phone)
            SELECT
                v AS customer_company_id,
                'first_name_' || v AS first_name,
                'last_name_' || v AS last_name,
                'first_name_' || v || '@' || 'company_name_' || v || '.com' AS email,
                79101538060 + v AS phone
            FROM generate_series(1, {n}) AS v
        """)

    async def inn_info(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE TABLE IF NOT EXISTS public.inn_info (
                inn bigint,
                company_info text,
                CONSTRAINT inn_info_pkey UNIQUE (inn),
                CONSTRAINT inn_info_fk
                    FOREIGN KEY (inn)
                    REFERENCES schm_customer.customer_company(inn)
            );
            INSERT INTO public.inn_info (inn, company_info)
            SELECT
                10000000 + v * 10 AS inn,
                'company_info_' || v AS company_info
            FROM generate_series(1, {n}) AS v
        """)

    async def contracts(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self._ensure_contract_status_enum(db_name)
        await self.db.execute(db_name, f"""
            CREATE TABLE IF NOT EXISTS public.contracts (
                id serial,
                customer_company_id integer NOT NULL,
                customer_manager_id integer NOT NULL,
                amount numeric(16,4) DEFAULT 0 NOT NULL,
                details text,
                status contract_status,
                contract_expires timestamp,
                CONSTRAINT contracts_pk UNIQUE (id)
            );
            INSERT INTO public.contracts
                (customer_company_id, customer_manager_id, amount, details, status, contract_expires)
            SELECT
                v AS customer_company_id,
                v AS customer_manager_id,
                floor(v * 0.7)::integer AS amount,
                'details_' || v AS details,
                (
                    SELECT s.cs FROM (
                        SELECT unnest(enum_range(NULL::contract_status)) AS cs
                    ) s
                    ORDER BY random() LIMIT 1
                ),
                NOW() + (random() * (NOW() + '365 days' - NOW())) + '365 days' AS contract_expires
            FROM generate_series(1, {n}) AS v
        """)

    async def card_numbers(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_mask_ext_exclude_2;
            CREATE TABLE IF NOT EXISTS schm_mask_ext_exclude_2.card_numbers (
                id serial,
                val text,
                val_skip text,
                usd numeric(30, 4),
                num_val numeric(30, 4),
                "имя_поля" text,
                "другое_поле" text,
                CONSTRAINT some_tbl_9_pkey UNIQUE (id)
            );
            INSERT INTO schm_mask_ext_exclude_2.card_numbers
                (val, val_skip, usd, num_val, "имя_поля", "другое_поле")
            SELECT
                'invalid_val_' || v AS val,
                'invalid_val_' || v AS val_skip,
                v * 0.1,
                v * 0.1,
                'abc' AS "имя_поля",
                'некоторое слово ' || v AS "другое_поле"
            FROM generate_series(1, {n}) AS v
        """)
        await self.db.execute(db_name, f"""
            INSERT INTO schm_mask_ext_exclude_2.card_numbers (val, val_skip, usd, num_val)
            SELECT
                '1234-7568-5678-4587' AS val,
                '1234-7568-5678-4587' AS val_skip,
                v * 0.1,
                v * 0.1
            FROM generate_series(1, {n}) AS v
        """)
        await self.db.execute(db_name, f"""
            INSERT INTO schm_mask_ext_exclude_2.card_numbers (val, val_skip, usd, num_val)
            SELECT
                NULL AS val,
                NULL AS val_skip,
                v * 0.1,
                v * 0.1
            FROM generate_series(1, {n}) AS v
        """)

    async def key_value(self, db_name: str) -> None:
        await self.db.execute(db_name, """
            CREATE TABLE IF NOT EXISTS public.key_value (
                id serial,
                fld_key text,
                fld_value text,
                CONSTRAINT key_value_pkey UNIQUE (id)
            );
            INSERT INTO public.key_value (fld_key, fld_value)
            VALUES
                ('email', 'email@example.com'),
                ('password', '123456'),
                ('address', 'Moscow city'),
                ('login', 'login_name'),
                ('first_name', 'Name'),
                ('amount', '100')
        """)

    async def schm_other_1_some_tbl(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_other_1;
            CREATE TABLE IF NOT EXISTS schm_other_1.some_tbl (
                id serial,
                val text,
                CONSTRAINT some_tbl_pkey UNIQUE (id)
            );
            INSERT INTO schm_other_1.some_tbl (val)
            SELECT 'text_val_' || v AS val
            FROM generate_series(1, {n}) AS v
        """)
        await self.db.execute(db_name, """
            CREATE OR REPLACE FUNCTION schm_other_1.slow_func(value text)
            RETURNS smallint LANGUAGE plpgsql AS $func$
            BEGIN
                IF COALESCE(value, '') <> '' THEN RETURN 1; ELSE RETURN 0; END IF;
            END; $func$;

            DO $$ BEGIN
                ALTER TABLE schm_other_1.some_tbl
                    ADD CONSTRAINT custom_check CHECK (schm_other_1.slow_func(val) = 1);
            EXCEPTION WHEN duplicate_object THEN NULL;
            END $$;

            CREATE OR REPLACE FUNCTION schm_other_1.slow_func(value text)
            RETURNS smallint LANGUAGE plpgsql AS $func$
            BEGIN
                PERFORM pg_sleep(5);
                IF COALESCE(value, '') <> '' THEN RETURN 1; ELSE RETURN 0; END IF;
            END; $func$
        """)

    async def complex_schema_tables(self, db_name: str) -> None:
        schema = "_SCHM.$complex#имя;@&* a'"
        tbl1 = "_TBL.$complex#имя;@&* a'"
        fld = "_FLD.$complex#имя;@&* a'"

        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS "{schema}";
            CREATE TABLE IF NOT EXISTS "{schema}"."{tbl1}" (
                id serial,
                fld_key text,
                "{fld}" text,
                CONSTRAINT key_value_pkey UNIQUE (id)
            );
            INSERT INTO "{schema}"."{tbl1}" (fld_key, "{fld}")
            VALUES
                ('email', 'email@example.com'),
                ('password', '123456'),
                ('address', 'Moscow city'),
                ('login', 'login_name'),
                ('first_name', 'Name'),
                ('amount', '100')
        """)
        await self.db.execute(db_name, f"""
            CREATE TABLE "{schema}"."{tbl1}2"
            AS SELECT * FROM "{schema}"."{tbl1}" WITH DATA
        """)
        await self.db.execute(db_name, f"""
            CREATE TABLE "{schema}"."{tbl1}3"
            AS SELECT * FROM "{schema}"."{tbl1}" WITH DATA
        """)

    async def anon_functions_data(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_other_2;
            CREATE TABLE IF NOT EXISTS schm_other_2.tbl_test_anon_functions (
                id serial,
                fld_1_int bigint,
                fld_2_datetime timestamp,
                fld_3_txt text,
                fld_4_txt text,
                fld_5_email text,
                fld_6_txt text,
                fld_7_zip int,
                fld_8_datetime timestamp,
                fld_9_datetime timestamp,
                fld_10_int int,
                fld_11_int bigint,
                fld_12_phone text,
                fld_13_txt text,
                fld_14_txt text,
                fld_15_txt text,
                CONSTRAINT tbl_test_anon_functions_pkey UNIQUE (id)
            );
            INSERT INTO schm_other_2.tbl_test_anon_functions
            (
                fld_1_int, fld_2_datetime, fld_3_txt, fld_4_txt, fld_5_email,
                fld_6_txt, fld_7_zip, fld_8_datetime, fld_9_datetime, fld_10_int,
                fld_11_int, fld_12_phone, fld_13_txt, fld_14_txt, fld_15_txt
            )
            SELECT
                v,
                NOW() + (random() * (NOW() + '365 days' - NOW())) + '365 days',
                'fld_3_txt_' || v,
                'fld_4_txt_' || v,
                'info' || v || '@' || 'company_name_' || v || '.com',
                'fld_6_txt' || v,
                v,
                NOW() + (random() * (NOW() + '100 days' - NOW())) + '100 days',
                NOW() + (random() * (NOW() + '200 days' - NOW())) + '200 days',
                v,
                v,
                '+7' || (1000000 + v),
                'fld_13_txt' || v,
                'fld_14_txt' || v,
                to_hex(v)::text
            FROM generate_series(1, {n}) AS v
        """)

    async def tbl_100(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE TABLE IF NOT EXISTS public.tbl_100 (
                id serial,
                val text,
                val_skip text,
                amount numeric(30, 4),
                num_val numeric(30, 4),
                "имя_поля" text,
                "другое_поле" text,
                CONSTRAINT tbl_100_pkey UNIQUE (id)
            );
            INSERT INTO public.tbl_100
                (val, val_skip, amount, num_val, "имя_поля", "другое_поле")
            SELECT
                'invalid_val_' || v AS val,
                'invalid_val_' || v AS val_skip,
                v * 0.1,
                v * 0.1,
                'abc' AS "имя_поля",
                'некоторое слово ' || v AS "другое_поле"
            FROM generate_series(1, {n}) AS v
        """)

    async def tbl_constants(self, db_name: str, count: int = 100) -> None:
        await self.db.execute(db_name, f"""
            CREATE TABLE IF NOT EXISTS public.tbl_constants (
                id serial,
                words_no_sens_1 text,
                words_no_sens_2 text,
                words_sens text,
                phrases_no_sens_1 text,
                phrases_no_sens_2 text,
                phrases_sens_1 text,
                phrases_sens_2 text
            );
            INSERT INTO public.tbl_constants
                (words_no_sens_1, words_no_sens_2, words_sens,
                 phrases_no_sens_1, phrases_no_sens_2, phrases_sens_1, phrases_sens_2)
            SELECT
                'some words as no sens ' || v AS words_no_sens_1,
                'some words as no sens - CompanyNameWordSens' || v AS words_no_sens_2,
                'some words as sens - CompanyNameWordSens ' || v AS words_sens,
                'some phrases as no sens' || v AS phrases_no_sens_1,
                'some phrases as no sens - CompanyNamePhrase include ' || v AS phrases_no_sens_2,
                'some phrases as sens - include CompanyNamePhrase' || v AS phrases_sens_1,
                'some phrases as sens - include CompanyNamePhrase ' || v AS phrases_sens_2
            FROM generate_series(1, {count}) AS v
        """)

    async def other_ext_tbl_2(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_mask_ext_exclude_2;
            CREATE TABLE IF NOT EXISTS schm_mask_ext_exclude_2.other_ext_tbl_2 (
                id serial,
                val_1 text,
                val_2 text,
                CONSTRAINT some_tbl_8_pkey UNIQUE (id)
            );
            INSERT INTO schm_mask_ext_exclude_2.other_ext_tbl_2 (val_1, val_2)
            SELECT
                'other_ext_tbl_text_val_' || v AS val_1,
                'other_ext_tbl_text_val_' || v AS val_2
            FROM generate_series(1, {n}) AS v
        """)

    async def data_types_test(self, db_name: str, count: int = 100) -> None:
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_other_3;
            CREATE TABLE IF NOT EXISTS schm_other_3.data_types_test (
                field_type_bit             bit(5),
                field_type_varbit          varbit(5),
                field_type_bool            bool,
                field_type_char            char(5),
                field_type_varchar         varchar(20),
                field_type_int             int,
                field_type_int4            int4,
                field_type_int2            int2,
                field_type_int8            int8,
                field_type_float           float,
                field_type_float8          float8,
                field_type_float4          float4,
                field_type_decimal         decimal(10,2),
                field_type_serial2         serial2,
                field_type_serial4         serial4,
                field_type_serial8         serial8,
                field_type_time            time,
                field_type_time_p          time(3),
                field_type_timetz          timetz,
                field_type_timetz_p        timetz(3),
                field_type_timestamp       timestamp,
                field_type_timestamp_p     timestamp(3),
                field_type_timestamptz     timestamptz,
                field_type_timestamptz_p   timestamptz(3)
            );
            INSERT INTO schm_other_3.data_types_test
            (
                field_type_bit, field_type_varbit, field_type_bool,
                field_type_char, field_type_varchar,
                field_type_int, field_type_int4, field_type_int2, field_type_int8,
                field_type_float, field_type_float8, field_type_float4, field_type_decimal,
                field_type_time, field_type_time_p,
                field_type_timetz, field_type_timetz_p,
                field_type_timestamp, field_type_timestamp_p,
                field_type_timestamptz, field_type_timestamptz_p
            )
            SELECT
                (lpad((v % 32)::bit(5)::text, 5, '0'))::bit(5),
                (lpad((v % 32)::bit(5)::text, 5, '0'))::bit(5),
                (v % 2 = 0),
                rpad('c' || v::text, 5, 'x'),
                'varchar_' || v,
                v, v, v % 32767, v * 1000000,
                v * 1.1, v * 1.2345, v / 3.0,
                (v * 0.01)::numeric(10,2),
                make_time((v % 24), (v % 60), (v % 60)),
                make_time((v % 24), (v % 60), (v % 60) + 0.123),
                make_time((v % 24), (v % 60), (v % 60))::timetz,
                make_time((v % 24), (v % 60), (v % 60) + 0.456)::timetz,
                make_timestamp(2024, 1, (v % 28)+1, (v % 24), (v % 60), (v % 60)),
                make_timestamp(2024, 1, (v % 28)+1, (v % 24), (v % 60), (v % 60) + 0.789),
                make_timestamp(2024, 1, (v % 28)+1, (v % 24), (v % 60), (v % 60))
                    AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow',
                make_timestamp(2024, 1, (v % 28)+1, (v % 24), (v % 60), (v % 60) + 0.987)
                    AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow'
            FROM generate_series(1, {count}) AS v
        """)

    async def partitioned_table(self, db_name: str) -> None:
        await self.db.execute(db_name, """
            CREATE SCHEMA IF NOT EXISTS schm_other_4;
            CREATE TABLE IF NOT EXISTS schm_other_4.partitioned_table (
                id BIGSERIAL,
                sale_date DATE NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                region_code VARCHAR(10),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            ) PARTITION BY RANGE (sale_date);

            DO $$ BEGIN
                ALTER TABLE schm_other_4.partitioned_table
                    ADD CONSTRAINT partitioned_data_pkey PRIMARY KEY (id, sale_date);
            EXCEPTION WHEN duplicate_object THEN NULL;
            END $$;

            CREATE TABLE IF NOT EXISTS schm_other_4.partitioned_table_2025_01
                PARTITION OF schm_other_4.partitioned_table
                FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

            CREATE TABLE IF NOT EXISTS schm_other_4.partitioned_table_2025_02
                PARTITION OF schm_other_4.partitioned_table
                FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

            CREATE TABLE IF NOT EXISTS schm_other_4.partitioned_table_2025_03
                PARTITION OF schm_other_4.partitioned_table
                FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

            CREATE TABLE IF NOT EXISTS schm_other_4.partitioned_table_default
                PARTITION OF schm_other_4.partitioned_table
                DEFAULT;

            INSERT INTO schm_other_4.partitioned_table
                (sale_date, product_id, quantity, amount, region_code)
            VALUES
                ('2025-01-15', 1, 2, 99.98, 'US'),
                ('2025-01-20', 2, 1, 25.50, 'EU'),
                ('2025-02-10', 1, 3, 149.97, 'US'),
                ('2025-03-03', 3, 1, 15.70, 'US'),
                ('2025-05-05', 1, 4, 76.23, 'EU')
        """)

    async def goods(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS schm_other_4;
            CREATE TABLE IF NOT EXISTS schm_other_4.goods (
                id BIGSERIAL,
                title varchar(64) NOT NULL,
                description text,
                release_date DATE NOT NULL,
                valid_until DATE NOT NULL,
                type_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO schm_other_4.goods
                (title, description, release_date, valid_until, type_id, quantity)
            SELECT
                'Good ' || v AS title,
                'Description of good ' || v AS description,
                NOW() - (v || ' days')::interval AS release_date,
                NOW() + ((v + 10) || ' days')::interval AS valid_until,
                v AS type_id,
                v AS quantity
            FROM generate_series(1, {n}) AS v
        """)

    # ------------------------------------------------------------------
    # Simple env
    # ------------------------------------------------------------------

    async def simple_companies(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS test_simple;
            CREATE TABLE IF NOT EXISTS test_simple.customer_company (
                id serial,
                company_name character varying(32),
                email character varying(64),
                phone character varying(32),
                site character varying(64),
                inn bigint,
                CONSTRAINT customer_company_pkey UNIQUE (id),
                CONSTRAINT inn_uniq UNIQUE (inn)
            );
            INSERT INTO test_simple.customer_company
                (company_name, email, phone, site, inn)
            SELECT
                'company_name_' || v AS company_name,
                'info' || v || '@' || 'company_name_' || v || '.com' AS email,
                79101438060 + v AS phone,
                'company_name_' || v || '.com' AS site,
                10000000 + v * 10 AS inn
            FROM generate_series(1, {n}) AS v
        """)

    async def simple_contracts(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS test_simple;
            CREATE TABLE IF NOT EXISTS test_simple.contracts (
                id serial,
                customer_company_id integer NOT NULL,
                customer_manager_id integer NOT NULL,
                amount numeric(16,4) DEFAULT 0 NOT NULL,
                details text,
                status_id integer NOT NULL,
                contract_expires timestamp,
                CONSTRAINT contracts_pk UNIQUE (id)
            );
            INSERT INTO test_simple.contracts
                (customer_company_id, customer_manager_id, amount, details, status_id, contract_expires)
            SELECT
                v AS customer_company_id,
                v AS customer_manager_id,
                floor(v * 0.7)::integer AS amount,
                'details_' || v AS details,
                v % 2,
                NOW() + (random() * (NOW() + '365 days' - NOW())) + '365 days' AS contract_expires
            FROM generate_series(1, {n}) AS v
        """)

    async def simple_clients(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS test_simple;
            CREATE TABLE IF NOT EXISTS test_simple.clients (
                id serial,
                firstname character varying(32),
                lastname character varying(32),
                email character varying(64),
                phone character varying(32),
                CONSTRAINT clients_pk UNIQUE (id)
            );
            INSERT INTO test_simple.clients (firstname, lastname, email, phone)
            SELECT
                'first_name_' || v AS firstname,
                'last_name_' || v AS lastname,
                'first_name_' || v || '.last_name_' || v || '@' || 'some_hoster_' || v || '.com' AS email,
                79101438060 + v AS phone
            FROM generate_series(1, {n}) AS v
        """)

    async def simple_orders(self, db_name: str, count: int | None = None) -> None:
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS test_simple;
            CREATE TABLE IF NOT EXISTS test_simple.orders (
                id serial,
                item_id integer NOT NULL,
                amount numeric(16,4) DEFAULT 0 NOT NULL,
                details text,
                status_id integer NOT NULL,
                CONSTRAINT orders_pk UNIQUE (id)
            );
            INSERT INTO test_simple.orders (item_id, amount, details, status_id)
            SELECT
                v AS item_id,
                floor(v * 0.7)::integer AS amount,
                'details_' || v AS details,
                v % 2
            FROM generate_series(1, {n}) AS v
        """)

    async def core_tables(self, db_name: str) -> None:
        await self.schm_other_1_some_tbl(db_name)
        await self.simple_text_table(db_name, "schm_other_2", "some_tbl", "some_tbl_pkey")
        await self.simple_text_table(db_name, "schm_other_2", "exclude_tbl", "exclude_tbl_pkey")

    async def customer_domain(self, db_name: str) -> None:
        await self.customer_companies(db_name)
        await self.customer_contracts(db_name)
        await self.customer_managers(db_name)
        await self.contracts(db_name)
        await self.inn_info(db_name)

    async def mask_include_tables(self, db_name: str) -> None:
        for schema, table, constraint in [
            ("schm_mask_include_1", "some_tbl", "some_tbl_1_pkey"),
            ("schm_mask_include_1", "tbl_123", "tbl_123_pkey"),
            ("schm_mask_include_1", "tbl_123_456", "tbl_123_456_pkey"),
            ("schm_mask_include_1", "other_tbl", "some_tbl_2_pkey"),
            ("schm_mask_ext_include_2", "some_ext_tbl", "some_tbl_3_pkey"),
            ("schm_mask_ext_include_2", "other_ext_tbl", "some_tbl_4_pkey"),
        ]:
            await self.simple_text_table(db_name, schema, table, constraint)

    async def mask_exclude_tables(self, db_name: str) -> None:
        await self.simple_text_table(db_name, "schm_mask_exclude_1", "some_tbl", "some_tbl_5_pkey")
        await self.simple_text_table(db_name, "schm_mask_exclude_1", "other_tbl", "some_tbl_6_pkey")
        await self.simple_text_table(db_name, "schm_mask_ext_exclude_2", "some_ext_tbl", "some_tbl_7_pkey")
        await self.other_ext_tbl_2(db_name)
        await self.card_numbers(db_name)

    async def misc_public_tables(self, db_name: str) -> None:
        await self.key_value(db_name)
        await self.tbl_100(db_name)
        await self.tbl_constants(db_name)

    async def schm_other_extras(self, db_name: str) -> None:
        await self._ensure_test_anon_funcs(db_name)
        await self._ensure_columnar_internal(db_name)
        await self.anon_functions_data(db_name)
        await self.data_types_test(db_name)
        await self.partitioned_table(db_name)
        await self.goods(db_name)

    # ------------------------------------------------------------------
    # Stress env (10 tables in schema "stress")
    # ------------------------------------------------------------------

    async def stress_table(self, db_name: str, table_num: int, count: int | None = None) -> None:
        """stress.tbl_N — requires anon_funcs.random_string (loaded via pg_anon init)."""
        n = self._n(count)
        await self.db.execute(db_name, f"""
            CREATE SCHEMA IF NOT EXISTS stress;
            CREATE TABLE IF NOT EXISTS stress.tbl_{table_num} (
                id serial,
                customer_company_id integer NOT NULL,
                first_name character varying(32),
                last_name character varying(32),
                name text,
                email character varying(64),
                phone character varying(32),
                fld_datetime timestamp,
                CONSTRAINT tbl_{table_num}_pkey UNIQUE (id)
            );
            INSERT INTO stress.tbl_{table_num}
                (customer_company_id, first_name, last_name, name, email, phone, fld_datetime)
            SELECT
                v AS customer_company_id,
                'first_name_' || v AS first_name,
                'last_name_' || v AS last_name,
                (SELECT array_to_string(array_agg(t.v::text), ' ')
                 FROM (
                     SELECT anon_funcs.random_string(10) AS v
                     FROM generate_series(1, 100)
                 ) t) AS name,
                'first_name_' || v || '@' || 'company_name_' || v || '.com' AS email,
                79101538060 + v AS phone,
                NOW() + (random() * (NOW() + '100 days' - NOW())) + '100 days'
            FROM generate_series(1, {n}) AS v
        """)

    async def stress_env(self, db_name: str, count: int | None = None) -> None:
        """Insert data into all 10 stress tables.

        Call AFTER pg_anon init (for anon_funcs).
        """
        for i in range(1, 11):
            await self.stress_table(db_name, i, count)
