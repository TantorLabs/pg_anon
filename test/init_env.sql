DROP SCHEMA IF EXISTS schm_other_1 CASCADE;
DROP SCHEMA IF EXISTS schm_other_2 CASCADE;

CREATE SCHEMA IF NOT EXISTS schm_other_1;
CREATE SCHEMA IF NOT EXISTS schm_other_2;

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
from generate_series(1,15001) as v;

CREATE TABLE schm_other_2.some_tbl
(
    id serial,
    val text,
    CONSTRAINT some_tbl_pkey UNIQUE (id)
);

INSERT INTO schm_other_2.some_tbl (val)
select 'text_val_' || v as val
from generate_series(1,15001) as v;

CREATE TABLE schm_other_2.exclude_tbl
(
    id serial,
    val text,
    CONSTRAINT exclude_tbl_pkey UNIQUE (id)
);

INSERT INTO schm_other_2.exclude_tbl (val)
select 'text_val_' || v as val
from generate_series(1,15001) as v;

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
from generate_series(1,15001) as v;

INSERT INTO schm_customer.customer_manager
(customer_company_id, first_name, last_name, email, phone)
select
	v as customer_company_id,
	'first_name_' || v as first_name,
	'last_name_' || v as last_name,
	'first_name_' || v || '@' || 'company_name_' || v || '.com' as email,
	79101538060 + v as phone
from generate_series(1,15001) as v;

INSERT INTO public.contracts
(customer_company_id, customer_manager_id, amount, details, status, contract_expires)
select
	v as customer_company_id,
	v as customer_manager_id,
	floor(random() * 1000)::integer as amount,
	'details_' || v as details,
	(
		SELECT s.cs FROM (
			SELECT unnest(enum_range(NULL::contract_status)) as cs
		) s
		ORDER BY random() LIMIT 1
	),
	NOW() + (random() * (NOW() + '365 days' - NOW())) + '365 days' as contract_expires
from generate_series(1,15001) as v;

INSERT INTO public.inn_info
(inn, company_info)
select
	10000000 + v * 10 as inn,
	'company_info_' || v as company_info
from generate_series(1,15001) as v;
