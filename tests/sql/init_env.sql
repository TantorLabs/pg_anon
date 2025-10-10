DROP SCHEMA IF EXISTS schm_other_1 CASCADE;
DROP SCHEMA IF EXISTS schm_other_2 CASCADE;
DROP SCHEMA IF EXISTS "_SCHM.$complex#имя;@&* a'" CASCADE;

CREATE SCHEMA IF NOT EXISTS schm_other_1;
CREATE SCHEMA IF NOT EXISTS schm_other_2;
CREATE SCHEMA IF NOT EXISTS "_SCHM.$complex#имя;@&* a'";

DROP TABLE IF EXISTS schm_other_1.some_tbl CASCADE;
DROP TABLE IF EXISTS schm_other_2.some_tbl CASCADE;
DROP TABLE IF EXISTS schm_other_2.exclude_tbl CASCADE;

CREATE TABLE schm_other_1.some_tbl
(
    id serial,
    val text,
    CONSTRAINT some_tbl_pkey UNIQUE (id)
);

INSERT INTO schm_other_1.some_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

CREATE OR REPLACE FUNCTION schm_other_1.slow_func (value text)
RETURNS smallint
language plpgsql
as
$$
BEGIN
   -- PERFORM pg_sleep(5);
   IF COALESCE (value, '') <> '' THEN
      RETURN 1;
   ELSE
      RETURN 0;
   END IF;
END;
$$;

alter table schm_other_1.some_tbl
   add constraint custom_check CHECK (schm_other_1.slow_func ( val ) = 1);

-- to avoid pg_sleep on "add constraint"
-- this function in the restore phase should interfere with the process
CREATE OR REPLACE FUNCTION schm_other_1.slow_func (value text)
RETURNS smallint
language plpgsql
as
$$
BEGIN
   PERFORM pg_sleep(5);
   IF COALESCE (value, '') <> '' THEN
      RETURN 1;
   ELSE
      RETURN 0;
   END IF;
END;
$$;

CREATE TABLE schm_other_2.some_tbl
(
    id serial,
    val text,
    CONSTRAINT some_tbl_pkey UNIQUE (id)
);

INSERT INTO schm_other_2.some_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

CREATE TABLE schm_other_2.exclude_tbl
(
    id serial,
    val text,
    CONSTRAINT exclude_tbl_pkey UNIQUE (id)
);

INSERT INTO schm_other_2.exclude_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

DROP SCHEMA IF EXISTS schm_customer CASCADE;
CREATE SCHEMA IF NOT EXISTS schm_customer;

DROP TABLE IF EXISTS schm_customer.customer_company CASCADE;
DROP TABLE IF EXISTS schm_customer.customer_manager CASCADE;
DROP TABLE IF EXISTS public.contracts CASCADE;
DROP TABLE IF EXISTS public.inn_info CASCADE;

CREATE TABLE schm_customer.customer_company
(
    id serial,
    company_name character varying(32),
    email character varying(64),
    phone character varying(32),
    site character varying(64),
    inn bigint,
    CONSTRAINT customer_company_pkey UNIQUE (id),
    CONSTRAINT inn_uniq UNIQUE (inn)
);

CREATE TABLE public.inn_info
(
    inn bigint,
    company_info text,
    CONSTRAINT inn_info_pkey UNIQUE (inn),
    CONSTRAINT inn_info_fk
        FOREIGN KEY (inn)
        REFERENCES schm_customer.customer_company(inn)
);

CREATE TABLE schm_customer.customer_manager
(
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

drop type if exists contract_status;
create type contract_status as enum('pending', 'processing', 'active', 'closed');

CREATE TABLE public.contracts
(
    id serial,
    customer_company_id integer NOT NULL,
    customer_manager_id integer NOT NULL,
    amount numeric(16,4) DEFAULT 0 NOT NULL,
    details text,
    status contract_status,
    contract_expires timestamp,
	CONSTRAINT contracts_pk UNIQUE (id)
);

-- prepare data
INSERT INTO schm_customer.customer_company
(company_name, email, phone, site, inn)
select
	'company_name_' || v as company_name,
	'info' || v || '@' || 'company_name_' || v || '.com' as email,
	79101438060 + v as phone,
	'company_name_' || v || '.com' as site,
	10000000 + v * 10 as inn
from generate_series(1,1512) as v;

INSERT INTO schm_customer.customer_manager
(customer_company_id, first_name, last_name, email, phone)
select
	v as customer_company_id,
	'first_name_' || v as first_name,
	'last_name_' || v as last_name,
	'first_name_' || v || '@' || 'company_name_' || v || '.com' as email,
	79101538060 + v as phone
from generate_series(1,1512) as v;

INSERT INTO public.contracts
(customer_company_id, customer_manager_id, amount, details, status, contract_expires)
select
	v as customer_company_id,
	v as customer_manager_id,
	floor(v * 0.7)::integer as amount,
	'details_' || v as details,
	(
		SELECT s.cs FROM (
			SELECT unnest(enum_range(NULL::contract_status)) as cs
		) s
		ORDER BY random() LIMIT 1
	),
	NOW() + (random() * (NOW() + '365 days' - NOW())) + '365 days' as contract_expires
from generate_series(1,1512) as v;

INSERT INTO public.inn_info
(inn, company_info)
select
	10000000 + v * 10 as inn,
	'company_info_' || v as company_info
from generate_series(1,1512) as v;

--------------------------------------------------------------
DROP TABLE IF EXISTS public.key_value CASCADE;

CREATE TABLE public.key_value
(
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
    ('amount', '100');
--------------------------------------------------------------
DROP TABLE IF EXISTS "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'" CASCADE;    -- table "as is"
DROP TABLE IF EXISTS "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'2" CASCADE;   -- table with SQL fld
DROP TABLE IF EXISTS "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'3" CASCADE;   -- table with raw_sql

CREATE TABLE "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'"
(
    id serial,
    fld_key text,
    "_FLD.$complex#имя;@&* a'" text,
    CONSTRAINT key_value_pkey UNIQUE (id)
);

INSERT INTO "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'" (fld_key, "_FLD.$complex#имя;@&* a'")
VALUES
    ('email', 'email@example.com'),
    ('password', '123456'),
    ('address', 'Moscow city'),
    ('login', 'login_name'),
    ('first_name', 'Name'),
    ('amount', '100');

CREATE TABLE "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'2"
AS SELECT * FROM "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'" WITH DATA;

CREATE TABLE "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'3"
AS SELECT * FROM "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'" WITH DATA;
--------------------------------------------------------------
DROP TABLE IF EXISTS schm_other_2.tbl_test_anon_functions CASCADE;

CREATE TABLE schm_other_2.tbl_test_anon_functions
(
    id serial,
    fld_1_int bigint,           -- anon_funcs.noise
    fld_2_datetime timestamp,   -- anon_funcs.dnoise
    fld_3_txt text,             -- anon_funcs.digest
    fld_4_txt text,             -- anon_funcs.partial
    fld_5_email text,           -- anon_funcs.partial_email
    fld_6_txt text,             -- anon_funcs.random_string
    fld_7_zip int,              -- anon_funcs.random_zip
    fld_8_datetime timestamp,   -- anon_funcs.random_date_between
    fld_9_datetime timestamp,   -- anon_funcs.random_date()
    fld_10_int int,             -- anon_funcs.random_int_between
    fld_11_int bigint,          -- anon_funcs.random_bigint_between
    fld_12_phone text,          -- anon_funcs.random_phone
    fld_13_txt text,            -- anon_funcs.random_hash
    fld_14_txt text,            -- anon_funcs.random_in
    fld_15_txt text,            -- anon_funcs.hex_to_int
    CONSTRAINT tbl_test_anon_functions_pkey UNIQUE (id)
);

INSERT INTO schm_other_2.tbl_test_anon_functions
(
	fld_1_int,
	fld_2_datetime,
	fld_3_txt,
	fld_4_txt,
	fld_5_email,
	fld_6_txt,
	fld_7_zip,
	fld_8_datetime,
	fld_9_datetime,
	fld_10_int,
	fld_11_int,
	fld_12_phone,
	fld_13_txt,
	fld_14_txt,
	fld_15_txt
)
select
	v, 					            -- fld_1_int,
	NOW() + (random() * (NOW() + '365 days' - NOW())) + '365 days', -- fld_2_datetime,
	'fld_3_txt_' || v, 	            -- fld_3_txt,
	'fld_4_txt_' || v, 	            -- fld_4_txt,
	'info' || v || '@' || 'company_name_' || v || '.com', -- fld_5_email,
	'fld_6_txt' || v, 	            -- fld_6_txt,
	v, 					            -- fld_7_zip,
	NOW() + (random() * (NOW() + '100 days' - NOW())) + '100 days', -- fld_8_datetime,
	NOW() + (random() * (NOW() + '200 days' - NOW())) + '200 days', -- fld_9_datetime,
	v, 								-- fld_10_int,
	v, 								-- fld_11_int,
	'+7' || (1000000 + v), 			-- fld_12_phone,
	'fld_13_txt' || v, 				-- fld_13_txt,
	'fld_14_txt' || v,				-- fld_14_txt,
	to_hex(v)::text					-- fld_15_txt
from generate_series(1,1512) as v;
--------------------------------------------------------------
--------------------------------------------------------------
-- table and schema masks in the dictionary

DROP SCHEMA IF EXISTS schm_mask_include_1 CASCADE;
DROP SCHEMA IF EXISTS schm_mask_ext_include_2 CASCADE;
DROP SCHEMA IF EXISTS schm_mask_exclude_1 CASCADE;
DROP SCHEMA IF EXISTS schm_mask_ext_exclude_2 CASCADE;
------------
CREATE SCHEMA IF NOT EXISTS schm_mask_include_1;
CREATE SCHEMA IF NOT EXISTS schm_mask_ext_include_2;
CREATE SCHEMA IF NOT EXISTS schm_mask_exclude_1;
CREATE SCHEMA IF NOT EXISTS schm_mask_ext_exclude_2;
------------
CREATE TABLE schm_mask_include_1.some_tbl
-- should be copied
-- schema_mask: ^schm_mask_incl (begins with)
-- table_mask: ^some_t (begins with)
(
    id serial,
    val text,
    CONSTRAINT some_tbl_1_pkey UNIQUE (id)
);

INSERT INTO schm_mask_include_1.some_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

CREATE TABLE schm_mask_include_1.tbl_123
-- should be copied
-- schema_mask: ^schm_mask_incl (begins with)
-- table: tbl_123
(
    id serial,
    val text,
    CONSTRAINT tbl_123_pkey UNIQUE (id)
);

INSERT INTO schm_mask_include_1.tbl_123 (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

CREATE TABLE schm_mask_include_1.tbl_123_456
-- should be copied
-- schema: schm_mask_include_1
-- table_mask: \w+\_\d+\_\d+
(
    id serial,
    val text,
    CONSTRAINT tbl_123_456_pkey UNIQUE (id)
);

INSERT INTO schm_mask_include_1.tbl_123_456 (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

CREATE TABLE schm_mask_include_1.other_tbl
-- should be copied
-- schema_mask: ^schm_mask_incl (begins with)
-- table_mask: er_tbl$ (ends with)
(
    id serial,
    val text,
    CONSTRAINT some_tbl_2_pkey UNIQUE (id)
);

INSERT INTO schm_mask_include_1.other_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;
------------
CREATE TABLE schm_mask_ext_include_2.some_ext_tbl
-- should be copied
-- schema_mask: ^schm_mask_ext_ (begins with)
-- table_mask: \w+\_\w+\_ (contains "word_word_")
(
    id serial,
    val text,
    CONSTRAINT some_tbl_3_pkey UNIQUE (id)
);

INSERT INTO schm_mask_ext_include_2.some_ext_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

CREATE TABLE schm_mask_ext_include_2.other_ext_tbl
-- should be copied
-- rule above
(
    id serial,
    val text,
    CONSTRAINT some_tbl_4_pkey UNIQUE (id)
);

INSERT INTO schm_mask_ext_include_2.other_ext_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;
------------
------------
------------
CREATE TABLE schm_mask_exclude_1.some_tbl
-- should be passed

-- dictionary
-- schema_mask: mask_exclude_1$ (ends with)
-- table_mask: \w+\_\w+\_ (contains "word_word_")

-- dictionary_exclude
-- schema_mask: mask_exclude_1$ (ends with)
-- table_mask: e_tbl$ (ends with)
(
    id serial,
    val text,
    CONSTRAINT some_tbl_5_pkey UNIQUE (id)
);

INSERT INTO schm_mask_exclude_1.some_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

CREATE TABLE schm_mask_exclude_1.other_tbl
-- should be copied
-- table is described in dictionary
(
    id serial,
    val text,
    CONSTRAINT some_tbl_6_pkey UNIQUE (id)
);

INSERT INTO schm_mask_exclude_1.other_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;
------------
CREATE TABLE schm_mask_ext_exclude_2.some_ext_tbl
-- should be passed

-- dictionary
-- schema_mask: mask_ext_exclude_2$ (ends with)
-- table_mask: \w+\_\w+\_ (contains "word_word_")

-- dictionary_exclude
-- schema_mask: ext_exclude_2$
-- table_mask: e_tbl$
(
    id serial,
    val text,
    CONSTRAINT some_tbl_7_pkey UNIQUE (id)
);

INSERT INTO schm_mask_ext_exclude_2.some_ext_tbl (val)
select 'text_val_' || v as val
from generate_series(1,1512) as v;

CREATE TABLE schm_mask_ext_exclude_2.other_ext_tbl_2
-- should be copied
(
    id serial,
    val_1 text,
    val_2 text,
    CONSTRAINT some_tbl_8_pkey UNIQUE (id)
);

INSERT INTO schm_mask_ext_exclude_2.other_ext_tbl_2 (val_1, val_2)
select 'other_ext_tbl_text_val_' || v as val_1, 'other_ext_tbl_text_val_' || v as val_2
from generate_series(1,1512) as v;
--------------------------------------------------------------
CREATE TABLE schm_mask_ext_exclude_2.card_numbers
-- should be copied
(
    id serial,
    val text,
    val_skip text,
    usd numeric(30, 4),
    num_val numeric(30, 4),
    "имя_поля" text,
    "другое_поле" text,
    CONSTRAINT some_tbl_9_pkey UNIQUE (id)
);

INSERT INTO schm_mask_ext_exclude_2.card_numbers (val, val_skip, usd, num_val, "имя_поля", "другое_поле")
select
    'invalid_val_' || v as val,
    'invalid_val_' || v as val_skip,
    v * 0.1,
    v * 0.1,
    'abc' as "имя_поля",
    'некоторое слово ' || v as "другое_поле"
from generate_series(1,1512) as v;

INSERT INTO schm_mask_ext_exclude_2.card_numbers (val, val_skip, usd, num_val)
select
    '1234-7568-5678-4587' as val,
    '1234-7568-5678-4587' as val_skip,
    v * 0.1,
    v * 0.1
from generate_series(1,1512) as v;

INSERT INTO schm_mask_ext_exclude_2.card_numbers (val, val_skip, usd, num_val)
select
    NULL as val,
    NULL as val_skip,
    v * 0.1,
    v * 0.1
from generate_series(1,1512) as v;
--------------------------------------------------------------
CREATE TABLE public.tbl_100
(
    id serial,
    val text,
    val_skip text,
    amount numeric(30, 4),
    num_val numeric(30, 4),
    "имя_поля" text,
    "другое_поле" text,
    CONSTRAINT tbl_100_pkey UNIQUE (id)
);

INSERT INTO public.tbl_100 (val, val_skip, amount, num_val, "имя_поля", "другое_поле")
select
    'invalid_val_' || v as val,
    'invalid_val_' || v as val_skip,
    v * 0.1,
    v * 0.1,
    'abc' as "имя_поля",
    'некоторое слово ' || v as "другое_поле"
from generate_series(1,1512) as v;

--------------------------------------------------------------

CREATE TABLE public.tbl_constants
(
    id serial,
    words_no_sens_1 text,
    words_no_sens_2 text,
    words_sens text,
    phrases_no_sens_1 text,
    phrases_no_sens_2 text,
    phrases_sens_1 text,
    phrases_sens_2 text
);

INSERT INTO public.tbl_constants (words_no_sens_1, words_no_sens_2, words_sens, phrases_no_sens_1, phrases_no_sens_2, phrases_sens_1, phrases_sens_2)
select
    'some words as no sens ' || v as words_no_sens_1,
    'some words as no sens - CompanyNameWordSens' || v as words_no_sens_2,
    'some words as sens - CompanyNameWordSens ' || v as words_sens,
    'some phrases as no sens' || v as phrases_no_sens_1,
    'some phrases as no sens - CompanyNamePhrase include ' || v as phrases_no_sens_2,
    'some phrases as sens - include CompanyNamePhrase' || v as phrases_sens_1,
    'some phrases as sens - include CompanyNamePhrase ' || v as phrases_sens_2
from generate_series(1,100) as v;

--------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS columnar_internal;

CREATE TABLE columnar_internal.tbl_200
(
    id integer,
    val text,
    val_skip text
);
--------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS test_anon_funcs;
CREATE OR REPLACE FUNCTION test_anon_funcs.test_check_is_company_email(
  value TEXT,
  schema_name TEXT,
  table_name TEXT,
  field_name TEXT
)
RETURNS boolean AS $$
DECLARE
    result boolean;
    email_part TEXT;
BEGIN
    email_part := SPLIT_PART(value, '@', '2');

    if email_part = ''
    then
    	return false;
    end if;

    execute 'SELECT EXISTS (SELECT * FROM schm_customer.customer_company WHERE email LIKE ''%' || email_part || ''')' into result;
    return result;
END;
$$
  LANGUAGE plpgsql;

--------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS schm_other_3;
DROP TABLE IF EXISTS schm_other_3.some_tbl;
CREATE TABLE schm_other_3.some_tbl
(
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

    -- TIME
    field_type_time            time,            -- alias = time without time zone
    field_type_time_p          time(3),         -- alias + precision
    field_type_timetz          timetz,          -- alias = time with time zone
    field_type_timetz_p        timetz(3),       -- alias + precision

    -- TIMESTAMP
    field_type_timestamp       timestamp,       -- alias = timestamp without time zone
    field_type_timestamp_p     timestamp(3),    -- alias + precision
    field_type_timestamptz     timestamptz,     -- alias = timestamp with time zone
    field_type_timestamptz_p   timestamptz(3)   -- alias + precision
);

-- вставляем тестовые данные
INSERT INTO schm_other_3.some_tbl
(
    field_type_varbit,
    field_type_bool,
    field_type_char,
    field_type_varchar,
    field_type_int,
    field_type_int4,
    field_type_int2,
    field_type_int8,
    field_type_float,
    field_type_float8,
    field_type_float4,
    field_type_decimal,

    field_type_time,
    field_type_time_p,
    field_type_timetz,
    field_type_timetz_p,
    field_type_timestamp,
    field_type_timestamp_p,
    field_type_timestamptz,
    field_type_timestamptz_p
)
SELECT
    -- varbit(5)
    (lpad((v % 32)::bit(5)::text, 5, '0'))::bit(5),

    -- bool
    (v % 2 = 0),

    -- char(5)
    rpad('c' || v::text, 5, 'x'),

    -- varchar
    'varchar_' || v,

    -- numbers
    v,               -- int
    v,               -- int4
    v % 32767,       -- int2
    v * 1000000,     -- int8
    v * 1.1,         -- float
    v * 1.2345,      -- float8
    v / 3.0,         -- float4
    (v * 0.01)::numeric(10,2), -- decimal(10,2)

    -- TIME
    make_time((v % 24), (v % 60), (v % 60)),                      -- time
    make_time((v % 24), (v % 60), (v % 60) + 0.123),              -- time(3)

    -- TIMETZ
    make_time((v % 24), (v % 60), (v % 60))::timetz,              -- timetz
    make_time((v % 24), (v % 60), (v % 60) + 0.456)::timetz,      -- timetz(3)

    -- TIMESTAMP
    make_timestamp(2024, 1, (v % 28)+1, (v % 24), (v % 60), (v % 60)),               -- timestamp
    make_timestamp(2024, 1, (v % 28)+1, (v % 24), (v % 60), (v % 60) + 0.789),       -- timestamp(3)

    -- TIMESTAMPTZ
    (make_timestamp(2024, 1, (v % 28)+1, (v % 24), (v % 60), (v % 60)) AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Moscow',
    (make_timestamp(2024, 1, (v % 28)+1, (v % 24), (v % 60), (v % 60) + 0.987) AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Moscow'
FROM generate_series(1, 100) AS v;

----------
CREATE SCHEMA IF NOT EXISTS schm_other_4;
DROP TABLE IF EXISTS schm_other_4.partitioned_table;
DROP TABLE IF EXISTS schm_other_4.orders;

CREATE TABLE schm_other_4.partitioned_table (
    id BIGSERIAL,
    sale_date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    amount NUMERIC (10,2) NOT NULL,
    region_code VARCHAR (10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (sale_date);

ALTER TABLE schm_other_4.partitioned_table
ADD CONSTRAINT partitioned_data_pkey
PRIMARY KEY (id, sale_date);

-- Create partition for January 2025
CREATE TABLE schm_other_4.partitioned_table_2025_01
PARTITION OF schm_other_4.partitioned_table
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- Create partition for February 2025
CREATE TABLE schm_other_4.partitioned_table_2025_02
PARTITION OF schm_other_4.partitioned_table
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

-- Create partition for March 2025
CREATE TABLE schm_other_4.partitioned_table_2025_03
PARTITION OF schm_other_4.partitioned_table
FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

-- Create default partition for future dates
CREATE TABLE schm_other_4.partitioned_table_default
PARTITION OF schm_other_4.partitioned_table
DEFAULT;

CREATE TABLE schm_other_4.goods (
    id BIGSERIAL,
    title varchar(64) NOT NULL,
    description text,
    release_date DATE NOT NULL,
    valid_until DATE NOT NULL,
    type_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO schm_other_4.partitioned_table(sale_date, product_id, quantity, amount, region_code) VALUES
('2025-01-15', 1, 2, 99.98, 'US'),
('2025-01-20', 2, 1, 25.50, 'EU' ),
('2025-02-10', 1, 3, 149.97, 'US'),
('2025-03-03', 3, 1, 15.70, 'US'),
('2025-05-05', 1, 4, 76.23, 'EU');


INSERT INTO schm_other_4.goods (title, description, release_date, valid_until, type_id, quantity)
SELECT
    'Good ' || v AS title,
    'Description of good ' || v AS description,
    NOW() - (v || ' days')::interval AS release_date,
    NOW() + ((v + 10) || ' days')::interval AS valid_until,
    v AS type_id,
    v AS quantity
FROM generate_series(1, 1512) AS v;
