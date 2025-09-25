DROP TABLE IF EXISTS test_simple.orders CASCADE;
DROP TABLE IF EXISTS test_simple.clients CASCADE;

CREATE TABLE test_simple.clients
(
    id serial,
    firstname character varying(32),
    lastname character varying(32),
    email character varying(64),
    phone character varying(32),
    CONSTRAINT clients_pk UNIQUE (id)
);

CREATE TABLE test_simple.orders
(
    id serial,
    item_id integer NOT NULL,
    amount numeric(16,4) DEFAULT 0 NOT NULL,
    details text,
    status_id integer NOT NULL,
	CONSTRAINT orders_pk UNIQUE (id)
);

-- prepare data
INSERT INTO test_simple.clients
(firstname, lastname, email, phone)
select
	'first_name_' || v as firstname,
	'last_name_' || v as lastname,
	'first_name_' ||v || '.last_name_' || v || '@' || 'some_hoster_' || v || '.com' as email,
	79101438060 + v as phone
from generate_series(1,1512) as v;

INSERT INTO test_simple.orders
(item_id, amount, details, status_id)
select
	v as item_id,
	floor(v * 0.7)::integer as amount,
	'details_' || v as details,
	v % 2
from generate_series(1,1512) as v;
