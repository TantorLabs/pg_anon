DROP SCHEMA IF EXISTS test_simple CASCADE;
CREATE SCHEMA IF NOT EXISTS test_simple;

DROP TABLE IF EXISTS test_simple.customer_company CASCADE;
DROP TABLE IF EXISTS test_simple.contracts CASCADE;

CREATE TABLE test_simple.customer_company
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

CREATE TABLE test_simple.contracts
(
    id serial,
    customer_company_id integer NOT NULL,
    customer_manager_id integer NOT NULL,
    amount numeric(16,4) DEFAULT 0 NOT NULL,
    details text,
    status_id integer NOT NULL,
    contract_expires timestamp,
	CONSTRAINT contracts_pk UNIQUE (id)
);

-- prepare data
INSERT INTO test_simple.customer_company
(company_name, email, phone, site, inn)
select
	'company_name_' || v as company_name,
	'info' || v || '@' || 'company_name_' || v || '.com' as email,
	79101438060 + v as phone,
	'company_name_' || v || '.com' as site,
	10000000 + v * 10 as inn
from generate_series(1,1512) as v;

INSERT INTO test_simple.contracts
(customer_company_id, customer_manager_id, amount, details, status_id, contract_expires)
select
	v as customer_company_id,
	v as customer_manager_id,
	floor(v * 0.7)::integer as amount,
	'details_' || v as details,
	v % 2,
	NOW() + (random() * (NOW() + '365 days' - NOW())) + '365 days' as contract_expires
from generate_series(1,1512) as v;
