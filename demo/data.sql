-- Demo database for the pg_anon Quick Start: two schemas, five tables, ~255 rows.
-- Re-runnable: both schemas are dropped first.

SET client_min_messages = warning;

DROP SCHEMA IF EXISTS shop CASCADE;
DROP SCHEMA IF EXISTS hr CASCADE;

CREATE SCHEMA shop;
CREATE SCHEMA hr;

CREATE TABLE shop.customer (
    id          serial PRIMARY KEY,
    full_name   varchar(120) NOT NULL,
    email       varchar(100) NOT NULL,
    phone       varchar(20)  NOT NULL,
    birth_date  date         NOT NULL,
    city        varchar(60)  NOT NULL,
    created_at  timestamp    NOT NULL DEFAULT now()
);

INSERT INTO shop.customer (full_name, email, phone, birth_date, city, created_at)
SELECT
    (array['Anna', 'Ben', 'Clara', 'David', 'Elena', 'Felix'])[1 + (i % 6)]
        || ' ' ||
        (array['Weber', 'Novak', 'Silva', 'Kowal', 'Meyer'])[1 + (i % 5)],
    'customer' || i || '@example.com',
    '+1' || (202555000 + i)::text,
    date '1960-01-01' + (i * 137),
    (array['Berlin', 'Lisbon', 'Prague', 'Tallinn', 'Vienna'])[1 + (i % 5)],
    now() - (i || ' days')::interval
FROM generate_series(1, 60) AS i;

CREATE TABLE shop.payment_card (
    id               serial PRIMARY KEY,
    customer_id      integer      NOT NULL REFERENCES shop.customer (id),
    cardholder_name  varchar(120) NOT NULL,
    card_number      varchar(19)  NOT NULL,
    expires_on       date         NOT NULL
);

INSERT INTO shop.payment_card (customer_id, cardholder_name, card_number, expires_on)
SELECT
    i,
    (SELECT full_name FROM shop.customer WHERE id = i),
    '4111-' || lpad((1000 + i)::text, 4, '0')
             || '-' || lpad((2000 + i)::text, 4, '0')
             || '-' || lpad((3000 + i)::text, 4, '0'),
    date '2027-01-01' + (i * 11)
FROM generate_series(1, 40) AS i;

CREATE TABLE shop.product (
    id     serial PRIMARY KEY,
    sku    varchar(20)   NOT NULL UNIQUE,
    title  varchar(120)  NOT NULL,
    price  numeric(10,2) NOT NULL
);

INSERT INTO shop.product (sku, title, price)
SELECT
    'SKU-' || lpad(i::text, 5, '0'),
    'Product ' || i,
    round((10 + (i % 90))::numeric, 2)
FROM generate_series(1, 30) AS i;

CREATE TABLE shop.customer_order (
    id           serial PRIMARY KEY,
    customer_id  integer       NOT NULL REFERENCES shop.customer (id),
    product_id   integer       NOT NULL REFERENCES shop.product (id),
    ordered_at   date          NOT NULL,
    quantity     integer       NOT NULL,
    total_amount numeric(10,2) NOT NULL,
    status       varchar(20)   NOT NULL,
    -- The column name says nothing about personal data, but every third row
    -- leaks a customer e-mail into it. Only a look at the values finds it.
    note         text
);

INSERT INTO shop.customer_order (customer_id, product_id, ordered_at, quantity, total_amount, status, note)
SELECT
    1 + (i % 60),
    1 + (i % 30),
    date '2025-01-01' + (i % 300),
    1 + (i % 4),
    round(((1 + (i % 4)) * (10 + (i % 90)))::numeric, 2),
    (array['new', 'paid', 'shipped', 'cancelled'])[1 + (i % 4)],
    CASE WHEN i % 3 = 0
         THEN 'delivery confirmed by customer' || (1 + (i % 60)) || '@example.com'
         ELSE 'no comments'
    END
FROM generate_series(1, 100) AS i;

CREATE TABLE hr.employee (
    id          serial PRIMARY KEY,
    full_name   varchar(120)  NOT NULL,
    email       varchar(100)  NOT NULL,
    phone       varchar(20)   NOT NULL,
    ssn         varchar(11)   NOT NULL,
    department  varchar(60)   NOT NULL,
    salary      numeric(10,2) NOT NULL,
    hired_on    date          NOT NULL
);

INSERT INTO hr.employee (full_name, email, phone, ssn, department, salary, hired_on)
SELECT
    (array['Greta', 'Hugo', 'Irina', 'Jonas', 'Karla'])[1 + (i % 5)]
        || ' ' ||
        (array['Berger', 'Costa', 'Dvorak', 'Egger'])[1 + (i % 4)],
    'employee' || i || '@example.com',
    '+1' || (202566000 + i)::text,
    lpad((100 + i)::text, 3, '0') || '-' || lpad((10 + i)::text, 2, '0')
        || '-' || lpad((1000 + i)::text, 4, '0'),
    (array['Sales', 'Support', 'Warehouse', 'Engineering'])[1 + (i % 4)],
    round((45000 + i * 750)::numeric, 2),
    date '2018-01-01' + (i * 53)
FROM generate_series(1, 25) AS i;
