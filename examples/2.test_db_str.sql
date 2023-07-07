/*
CREATE DATABASE test_db
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'ru_RU.UTF-8'
    LC_CTYPE = 'ru_RU.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

COMMENT ON DATABASE test_db
    IS 'test database to demonstrate features of pg_anon';
	
*/
DROP VIEW IF EXISTS cln.v_client_order;

DROP TABLE IF EXISTS cln.client_order;
DROP TABLE IF EXISTS stf.staff2branch;
DROP TABLE IF EXISTS org.branch;
DROP TABLE IF EXISTS org.prod;
DROP TABLE IF EXISTS cln.client;
DROP TABLE IF EXISTS stf.staff;

DROP SCHEMA IF EXISTS cln ;
DROP SCHEMA IF EXISTS org ;
DROP SCHEMA IF EXISTS stf ;
DROP SCHEMA IF EXISTS prod ;

--DROP SCHEMA IF EXISTS anon_funcs cascade;ниже

-- SCHEMA: cln

CREATE SCHEMA IF NOT EXISTS cln
    AUTHORIZATION postgres;

COMMENT ON SCHEMA cln
    IS 'clients';

    

-- SCHEMA: cln

CREATE SCHEMA IF NOT EXISTS cln
    AUTHORIZATION postgres;

COMMENT ON SCHEMA cln
    IS 'clients';
	

-- SCHEMA: org

CREATE SCHEMA IF NOT EXISTS org
    AUTHORIZATION postgres;

COMMENT ON SCHEMA org
    IS 'companies';

-- SCHEMA: stf

CREATE SCHEMA IF NOT EXISTS stf
    AUTHORIZATION postgres;

COMMENT ON SCHEMA stf
    IS 'staff';



-- Table: cln.client

CREATE TABLE IF NOT EXISTS cln.client
(
    client_id serial NOT NULL,
    client_firstname character varying(200) COLLATE pg_catalog."default" NOT NULL,
    client_lastname character varying(200) COLLATE pg_catalog."default" NOT NULL,
    client_birthdate date,
    client_email character varying(200) COLLATE pg_catalog."default",
    client_phone character varying(200) COLLATE pg_catalog."default",
    CONSTRAINT client_pkey PRIMARY KEY (client_id)
);

ALTER TABLE IF EXISTS cln.client
    OWNER to postgres;

COMMENT ON TABLE cln.client
    IS 'client''s info';


-- Table org.branch

CREATE TABLE IF NOT EXISTS org.branch
(
    branch_id serial NOT NULL,
    branch_code character varying(300) COLLATE pg_catalog."default" NOT NULL,
    branch_address character varying(300) COLLATE pg_catalog."default" NOT NULL,
	branch_description character varying(1000),
    CONSTRAINT branch_pkey PRIMARY KEY (branch_id)
);

ALTER TABLE IF EXISTS org.branch
    OWNER to postgres;

COMMENT ON TABLE org.branch
    IS 'branch info';


-- Table org.prod

CREATE TABLE IF NOT EXISTS org.prod
(
    prod_id serial NOT NULL,
    prod_code character varying(100) COLLATE pg_catalog."default" NOT NULL,
    prod_name character varying(200) COLLATE pg_catalog."default" NOT NULL,
    prod_cost numeric NOT NULL,
    CONSTRAINT prod_pkey PRIMARY KEY (prod_id)
);

ALTER TABLE IF EXISTS org.prod
    OWNER to postgres;

COMMENT ON TABLE org.prod
    IS 'product info';
    

	
-- Table: stf.staff

-- 

CREATE TABLE IF NOT EXISTS stf.staff
(
    staff_id serial NOT NULL,
    staff_firstname character varying(200) COLLATE pg_catalog."default",
    staff_lastname character varying(200) COLLATE pg_catalog."default",
    staff_birthdate date,
	staff_email character varying(200) COLLATE pg_catalog."default",
    staff_phone character varying(200) COLLATE pg_catalog."default",
	topsecretic text,
    CONSTRAINT staff_pkey PRIMARY KEY (staff_id)
);

ALTER TABLE IF EXISTS stf.staff
    OWNER to postgres;

COMMENT ON TABLE stf.staff
    IS 'companies staff';


-- Table: stf.staff2branch

--

CREATE TABLE IF NOT EXISTS stf.staff2branch
(
    s2b_id serial NOT NULL,
    staff_id integer,
    branch_id integer,
    CONSTRAINT staff2branch_pkey PRIMARY KEY (s2b_id),
	CONSTRAINT staff2branch_fk_staff_id FOREIGN KEY (staff_id)
		REFERENCES stf.staff (staff_id),
	CONSTRAINT staff2branch_fk_branch_id FOREIGN KEY (branch_id)
		REFERENCES org.branch (branch_id)
);

ALTER TABLE IF EXISTS stf.staff2branch
    OWNER to postgres;

COMMENT ON TABLE stf.staff2branch
    IS 'branches staff';





-- Table: cln.client_order

--

CREATE TABLE IF NOT EXISTS cln.client_order
(
    c2b_id serial NOT NULL,
    client_id integer,
    prod_id integer,
    prod_quantity integer,
    branch_id integer,
	contractnum character varying,
	description text,
    CONSTRAINT client_order_pkey PRIMARY KEY (c2b_id),
	CONSTRAINT client_order_fk_client_id FOREIGN KEY (client_id)
		REFERENCES cln.client (client_id),
	CONSTRAINT client_order_fk_branch_id FOREIGN KEY (branch_id)
		REFERENCES org.branch (branch_id),
	CONSTRAINT client_order_fk_prod_id FOREIGN KEY (prod_id)
		REFERENCES org.prod (prod_id)

);

ALTER TABLE IF EXISTS cln.client_order
    OWNER to postgres;

COMMENT ON TABLE cln.client_order
    IS 'clients branch';
