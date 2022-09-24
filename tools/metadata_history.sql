drop schema if exists dbadmin cascade;
create schema dbadmin;
set search_path = 'dbadmin';

DROP EVENT TRIGGER IF EXISTS event_trigger;
DROP EVENT TRIGGER IF EXISTS event_trigger_for_drops;

CREATE OR REPLACE FUNCTION get_table_def(objid oid)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
	res text;
BEGIN
	select (
		SELECT
			'CREATE ' || obj_type || ' ' || quote_ident(nspname) || '.' ||
			quote_ident(relname) || E'\n(\n' ||
			array_to_string(
				array_agg(
					'    ' || column_name || ' ' ||  type || ' '|| not_null
				), E',\n'
			) || E'\n);\n'
		from
		(
			SELECT
				c.relname, a.attname AS column_name,
				pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
				CASE
					when a.attnotnull
					then 'NOT NULL'
					else ''
				END as not_null,
				CASE
					when relkind in ('r', 'p') then 'TABLE'
					when relkind = 'v' then 'VIEW'
					when relkind = 'm' then 'MATERIALIZED VIEW'
					else 'unsupported'
				END as obj_type,
				n.nspname
			FROM pg_class c
			JOIN pg_namespace n on c.relnamespace = n.oid
			JOIN pg_attribute a ON a.attrelid = c.oid
			JOIN pg_type t ON a.atttypid = t.oid
			WHERE
				c.oid = objid
				AND a.attnum > 0
				AND a.atttypid = t.oid
			ORDER BY a.attnum
		) as t
		GROUP BY obj_type, nspname, relname
	) into res;

	return res;
END;
$$;

----------------
-- ALTER EVENT TRIGGER dbadmin.event_trigger DISABLE;
-- ALTER EVENT TRIGGER event_trigger_for_drops DISABLE;

DROP TABLE IF EXISTS metadata_history;

CREATE TABLE metadata_history
(
    id bigserial,
	user_id text,
	schema_name text,
	object_name text,
	object_identity text,
	object_type text,
	command_tag text,
	ddl text,
	objid oid,
    ts timestamp with time zone DEFAULT clock_timestamp(),
	username name default current_user,
	datname name default current_database(),
	client_addr inet default inet_client_addr(),
	client_port int default inet_client_port(),
	xid bigint default txid_current(),
	CONSTRAINT metadata_history_pkey UNIQUE (id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE metadata_history_p1
	PARTITION OF metadata_history
	FOR VALUES FROM ('2022-08-01') TO ('2022-09-01');

CREATE TABLE metadata_history_p2
	PARTITION OF metadata_history
	FOR VALUES FROM ('2022-09-01') TO ('2022-10-01');

CREATE TABLE metadata_history_p3
	PARTITION OF metadata_history
	FOR VALUES FROM ('2022-10-01') TO ('2022-11-01');

CREATE TABLE metadata_history_p4
	PARTITION OF metadata_history
	FOR VALUES FROM ('2022-11-01') TO ('2022-12-01');

CREATE TABLE metadata_history_p5
	PARTITION OF metadata_history
	FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE TABLE metadata_history_p6
	PARTITION OF metadata_history
	FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');

CREATE TABLE metadata_history_p7
	PARTITION OF metadata_history
	FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');

CREATE TABLE metadata_history_p8
	PARTITION OF metadata_history
	FOR VALUES FROM ('2023-04-01') TO ('2023-05-01');

--------------

CREATE OR REPLACE FUNCTION app_get_user_id()
RETURNS INT as $$
DECLARE
	user_id integer;
BEGIN
	BEGIN
		user_id := current_setting('app.user_id');
	exception
		when others then user_id := NULL;
	END;
	return user_id;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION event_trigger_func()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
	obj record;
BEGIN
	FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
	WHERE schema_name not in ('pg_toast')
	LOOP
		INSERT INTO dbadmin.metadata_history
		(user_id, schema_name, object_identity, object_type, command_tag, ddl, objid)
		VALUES(
			(select app_get_user_id()),
			obj.schema_name,
			obj.object_identity,
			obj.object_type,
			obj.command_tag,
			(select get_table_def(obj.objid)),
			obj.objid
		);
	END LOOP;
END;
$$;


CREATE OR REPLACE FUNCTION event_trigger_for_drops_func()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
	obj record;
BEGIN
	FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
	WHERE schema_name not in ('pg_toast')
	LOOP
		INSERT INTO dbadmin.metadata_history
		(user_id, schema_name, object_name, object_identity, object_type, command_tag, objid)
		VALUES(
			(select app_get_user_id()),
			obj.schema_name,
			obj.object_name,
			obj.object_identity,
			obj.object_type,
			'DROP',
			obj.objid
		);
	END LOOP;
END;
$$;


CREATE EVENT TRIGGER event_trigger ON ddl_command_end
EXECUTE PROCEDURE event_trigger_func();

CREATE EVENT TRIGGER event_trigger_for_drops ON sql_drop
EXECUTE PROCEDURE event_trigger_for_drops_func();

-- ALTER EVENT TRIGGER event_trigger ENABLE;
-- ALTER EVENT TRIGGER event_trigger_for_drops ENABLE;


set search_path = 'public';

select dbadmin.app_get_user_id();

set session "app.user_id" = 555;

truncate table dbadmin.metadata_history;

DROP TABLE IF EXISTS test_tbl;
CREATE TABLE test_tbl
(
	fld_1 integer,
	fld_2 text
);
ALTER TABLE test_tbl ADD COLUMN fld_3 smallint;


select * from dbadmin.metadata_history;
>>
	13	555	public	test_tbl	public.test_tbl	table	DROP	2022-09-21 02:21:49.808 +0300	postgres	test_source_db	127.0.0.1	42678	1343168
	14	555	public	test_tbl	public.test_tbl	type	DROP	2022-09-21 02:21:49.808 +0300	postgres	test_source_db	127.0.0.1	42678	1343168
	15	555	public	_test_tbl	public.test_tbl[]	type	DROP	2022-09-21 02:21:49.808 +0300	postgres	test_source_db	127.0.0.1	42678	1343168
	16	555	public		public.test_tbl	table	CREATE TABLE	2022-09-21 02:22:01.164 +0300	postgres	test_source_db	127.0.0.1	42678	1343169
	17	555	public		public.test_tbl	table	ALTER TABLE	2022-09-21 02:22:01.165 +0300	postgres	test_source_db	127.0.0.1	42678	1343170


