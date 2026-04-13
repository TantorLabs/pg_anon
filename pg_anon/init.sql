CREATE SCHEMA IF NOT EXISTS anon_funcs;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION anon_funcs.noise(
  noise_value ANYELEMENT,
  ratio DOUBLE PRECISION
)
 RETURNS ANYELEMENT
AS $func$
DECLARE
  res ALIAS FOR $0;
  ran float;
BEGIN
  ran = (2.0 * random() - 1.0) * ratio;
  SELECT (noise_value * (1.0 - ran))::ANYELEMENT
    INTO res;
  RETURN res;
EXCEPTION
  WHEN numeric_value_out_of_range THEN
    SELECT (noise_value * (1.0 + ran))::ANYELEMENT
      INTO res;
    RETURN res;
END;
$func$
  LANGUAGE plpgsql
  VOLATILE
  PARALLEL UNSAFE -- because of the EXCEPTION
  SECURITY INVOKER;

-- for time and timestamp values
CREATE OR REPLACE FUNCTION anon_funcs.dnoise(
  noise_value ANYELEMENT,
  noise_range INTERVAL
)
 RETURNS ANYELEMENT
AS $func$
DECLARE
  res ALIAS FOR $0;
  ran INTERVAL;
BEGIN
  ran = (2.0 * random() - 1.0) * noise_range;
  SELECT (noise_value + ran)::ANYELEMENT
    INTO res;
  RETURN res;
EXCEPTION
  WHEN datetime_field_overflow THEN
    SELECT (noise_value - ran)::ANYELEMENT
      INTO res;
    RETURN res;
END;
$func$
  LANGUAGE plpgsql
  VOLATILE
  PARALLEL UNSAFE -- because of the EXCEPTION
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.digest(
  seed TEXT,
  salt TEXT,
  algorithm TEXT
)
RETURNS TEXT AS
$$
  SELECT encode(digest(concat(seed,salt),algorithm),'hex');
$$
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  PARALLEL SAFE
  SECURITY INVOKER;

-- partial('abcdefgh',1,'xxxx',3) will return 'axxxxfgh';
CREATE OR REPLACE FUNCTION anon_funcs.partial(
  ov TEXT,
  prefix INT,
  padding TEXT,
  suffix INT
)
RETURNS TEXT AS $$
  SELECT substring(ov FROM 1 FOR prefix)
      || padding
      || substring(ov FROM (length(ov)-suffix+1) FOR suffix);
$$
  LANGUAGE SQL
  IMMUTABLE
  PARALLEL SAFE
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.partial_email(ov TEXT)
RETURNS TEXT AS $$
BEGIN
  RETURN substring(ov, 1, 2) || '******' ||
         '@' ||
         substring(ov from position('@' in ov) + 1 for position('.' in reverse(ov)) - 2) ||
         '******' ||
         '.' ||
         right(ov, position('.' in reverse(ov)) - 1);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.random_string(
  l integer
)
RETURNS text
AS $$
  SELECT array_to_string(
    array(
        select substr('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                      ((random()*(36-1)+1)::integer)
                      ,1)
        from generate_series(1,l)
    ),''
  );
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER;

-- Zip code
CREATE OR REPLACE FUNCTION anon_funcs.random_zip()
RETURNS text
AS $$
  SELECT array_to_string(
         array(
                select substr('0123456789',((random()*(10-1)+1)::integer),1)
                from generate_series(1,6)
            ),''
          );
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.random_inn()
RETURNS text
AS $$
  SELECT array_to_string(
         array(
                select substr('0123456789',((random()*(10-1)+1)::integer),1)
                from generate_series(1,8)
            ),''
          );
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.random_date_between(
  date_start timestamp WITH TIME ZONE,
  date_end timestamp WITH TIME ZONE
)
RETURNS timestamp WITH TIME ZONE AS $$
    SELECT (random()*(date_end-date_start))::interval+date_start;
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.random_date()
RETURNS timestamp with time zone AS $$
  SELECT anon_funcs.random_date_between('1900-01-01'::timestamp with time zone,now());
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.random_int_between(
  int_start INTEGER,
  int_stop INTEGER
)
RETURNS INTEGER AS $$
    SELECT CAST ( random()*(int_stop-int_start)+int_start AS INTEGER );
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.random_bigint_between(
  int_start BIGINT,
  int_stop BIGINT
)
RETURNS BIGINT AS $$
    SELECT CAST ( random()*(int_stop-int_start)+int_start AS BIGINT );
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.random_phone(
  phone_prefix TEXT DEFAULT '0'
)
RETURNS TEXT AS $$
  SELECT  phone_prefix
          || CAST(anon_funcs.random_int_between(100000000,999999999) AS TEXT)
          AS "phone";
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER;

CREATE OR REPLACE FUNCTION anon_funcs.random_hash(
  seed TEXT,
  algorithm TEXT
)
RETURNS TEXT AS
$$
  SELECT anon_funcs.digest(
    seed,
    anon_funcs.random_string(6),
    algorithm
  );
$$
  LANGUAGE SQL
  VOLATILE
  SECURITY DEFINER
  PARALLEL RESTRICTED -- because random
  RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION anon_funcs.random_in(
  a ANYARRAY
)
RETURNS ANYELEMENT AS
$$
  SELECT a[pg_catalog.floor(pg_catalog.random()*array_length(a,1)+1)]
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION anon_funcs.hex_to_int(
  hexval TEXT
)
RETURNS INT AS $$
DECLARE
    result  INT;
BEGIN
    EXECUTE 'SELECT x' || quote_literal(hexval) || '::INT' INTO result;
    RETURN result;
END;
$$
  LANGUAGE plpgsql;

---------------------------------------------------------------------------

do $$
declare
	common_res text[];
	test_res text;
begin
	select array(
		select test from (
			----------------------------------------------------
			select
				'test: encode' as test,
				encode((select encrypt('data', 'password', 'aes')), 'base64') = 'jpi7Ar3VWiESPg0luyHzRQ==' as res
			union all
			select
				'test: decode' as test,
				decode(
					(
						select encode((select encrypt('data', 'password', 'aes')), 'base64')
					),
					'base64'
				) = encrypt('data', 'password', 'aes')
			union all
			select
				'test: decrypt' as test,
				decrypt(
				(
					select decode('jpi7Ar3VWiESPg0luyHzRQ==', 'base64')
				), 'password', 'aes') = 'data'
			union all
			select
				'test: convert_from' as test,
				convert_from(decode('0YHQu9C+0LLQvg==', 'base64'), 'UTF8') = 'слово'
			----------------------------------------------------
			union all
			select 'test: noise' as test, anon_funcs.noise(100, 1.2) < 300
			union all
			select 'test: dnoise' as test,
				anon_funcs.dnoise('2020-02-02 10:10:10'::timestamp, interval '1 month') < '2020-03-03 10:10:10'::timestamp and
				anon_funcs.dnoise('2020-02-02 10:10:10'::timestamp, interval '1 month') > '2020-01-01 10:10:10'::timestamp
			union all
			select 'test: digest' as test, anon_funcs.digest('text', 'salt', 'sha256') = '3353e16497ad272fea4382119ff2801e54f0a4cf2057f4e32d00317bda5126c3'
			union all
			select 'test: partial' as test, anon_funcs.partial('123456789',1,'***',3) = '1***789'
			union all
			select 'test: partial_email' as test, anon_funcs.partial_email('example@gmail.com') = 'ex******@gm******.com'
			union all
			select 'test: random_string' as test, length(anon_funcs.random_string(7)) = 7
			union all
			select 'test: random_zip' as test, anon_funcs.random_zip()::integer < 999999
			union all
			select 'test: random_date_between' as test,
				anon_funcs.random_date_between('2020-02-02 10:10:10'::timestamp, '2022-02-05 10:10:10'::timestamp) <= '2022-02-05 10:10:10'::timestamp and
				anon_funcs.random_date_between('2020-02-02 10:10:10'::timestamp, '2022-02-05 10:10:10'::timestamp) >= '2020-02-02 10:10:10'::timestamp
			union all
			select 'test: random_date' as test, anon_funcs.random_date() < now()
			union all
			select 'test: random_int_between' as test, anon_funcs.random_int_between(100, 200) < 200
			union all
			select 'test: random_bigint_between' as test, anon_funcs.random_bigint_between(6000000000, 7000000000) < 7000000000
			union all
			select 'test: random_phone' as test, length(anon_funcs.random_phone('+7')) = 11
			union all
			select 'test: random_hash' as test, length(anon_funcs.random_hash('seed', 'sha512')) = 128
			union all
			select 'test: random_in' as test, (select anon_funcs.random_in(array['a', 'b', 'c'])) in ('a', 'b', 'c')
			union all
			select 'test: hex_to_int' as test, anon_funcs.hex_to_int('8AB') = 2219
			----------------------------------------------------
		) t
		where res = false
	)
	into common_res;

	FOREACH test_res IN ARRAY common_res
	LOOP
		RAISE NOTICE 'FAILED %', test_res;
	end loop;

	if array_length(common_res, 1) > 0 then
		raise exception '% test(s) failed! See "init.sql"', array_length(common_res, 1);
	end if;
end$$;
