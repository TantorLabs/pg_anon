do $$
declare
	count_tbls integer;
	test_res text;
    q_tbl text = 'CREATE TABLE stress.tbl_%s'
		'('
		'    id serial,'
		'    customer_company_id integer NOT NULL,'
		'    first_name character varying(32),'
		'    last_name character varying(32),'
        '    name text,'
		'    email character varying(64),'
		'    phone character varying(32),'
        '    fld_datetime timestamp,'
		'    CONSTRAINT tbl_%s_pkey UNIQUE (id)'
		');';
	q_insert text = 'INSERT INTO stress.tbl_%s'
		'(customer_company_id, first_name, last_name, name, email, phone, fld_datetime)'
		' select'
		'	v as customer_company_id,'
		'	''first_name_'' || v as first_name,'
		'	''last_name_'' || v as last_name,'
		'	(select array_to_string(array_agg(t.v::text), '' '')'
		'	from ('
		'			select anon_funcs.random_string(10) as v'
		'		from generate_series(1,100)'
		'	) t) as name,'
		'	''first_name_'' || v || ''@'' || ''company_name_'' || v || ''.com'' as email,'
		'	79101538060 + v as phone,'
	    '  NOW() + (random() * (NOW() + ''100 days'' - NOW())) + ''100 days'''
		' from generate_series(1,1512) as v';
	query text;
begin
	execute 'DROP SCHEMA IF EXISTS stress CASCADE';
    execute 'CREATE SCHEMA stress';
	FOR i IN 1..10 LOOP
		query = format(q_tbl, i, i);
		--raise notice '%', query;
		execute query;
		query = format(q_insert, i);
		--raise notice '%', query;
		execute query;
		if i % 100 = 0 then
			raise notice 'i = %', i;
		end if;
	END LOOP;
end$$;

SELECT pg_size_pretty(pg_database_size(datname)), datname, pg_database_size(datname) as v
from pg_database
order by v desc;
-->>
--	20 GB	test_source_db_stress	21824553763
