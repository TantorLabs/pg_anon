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
  SECURITY INVOKER
  ;

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
  SECURITY INVOKER
;

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
  SECURITY INVOKER

;



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
  SECURITY INVOKER

;

--
-- partial_email('daamien@gmail.com') will becomme 'da******@gm******.com'
--
CREATE OR REPLACE FUNCTION anon_funcs.partial_email(
  ov TEXT
)
RETURNS TEXT AS $$
-- This is an oversimplistic way to scramble an email address
-- The main goal is to avoid any complex regexp
-- by splitting the job into simpler tasks
  SELECT substring(regexp_replace(ov, '@.*', '') FROM 1 FOR 2) -- da
      || '******'
      || '@'
      || substring(regexp_replace(ov, '.*@', '') FROM 1 FOR 2) -- gm
      || '******'
      || '.'
      || regexp_replace(ov, '.*\.', '') -- com
  ;
$$
  LANGUAGE SQL
  IMMUTABLE
  PARALLEL SAFE
  SECURITY INVOKER

;

-- Transform an integer into a range of integer
CREATE OR REPLACE FUNCTION anon_funcs.generalize_int4range(
  val INTEGER,
  step INTEGER default 10
)
RETURNS INT4RANGE
AS $$
SELECT int4range(
    val / step * step,
    ((val / step)+1) * step
  );
$$
  LANGUAGE SQL
  IMMUTABLE
  PARALLEL SAFE
  SECURITY INVOKER

;

-- Transform a bigint into a range of bigint
CREATE OR REPLACE FUNCTION anon_funcs.generalize_int8range(
  val BIGINT,
  step BIGINT DEFAULT 10
)
RETURNS INT8RANGE
AS $$
SELECT int8range(
    val / step * step,
    ((val / step)+1) * step
  );
$$
  LANGUAGE SQL
  IMMUTABLE
  PARALLEL SAFE
  SECURITY INVOKER

;

-- Transform a numeric into a range of numeric
CREATE OR REPLACE FUNCTION anon_funcs.generalize_numrange(
  val NUMERIC,
  step INTEGER DEFAULT 10
)
RETURNS NUMRANGE
AS $$
WITH i AS (
  SELECT anon_funcs.generalize_int4range(val::INTEGER,step) as r
)
SELECT numrange(
    lower(i.r)::NUMERIC,
    upper(i.r)::NUMERIC
  )
FROM i
;
$$
  LANGUAGE SQL
  IMMUTABLE
  PARALLEL SAFE
  SECURITY INVOKER

;


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
  SECURITY INVOKER

;

-- Zip code
CREATE OR REPLACE FUNCTION anon_funcs.random_zip()
RETURNS text
AS $$
  SELECT array_to_string(
         array(
                select substr('0123456789',((random()*(10-1)+1)::integer),1)
                from generate_series(1,5)
            ),''
          );
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER

;

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
  SECURITY INVOKER

;

CREATE OR REPLACE FUNCTION anon_funcs.random_date()
RETURNS timestamp with time zone AS $$
  SELECT anon_funcs.random_date_between('1900-01-01'::timestamp with time zone,now());
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  PARALLEL RESTRICTED -- because random
  SECURITY INVOKER

;


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
  SECURITY INVOKER

;

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
  SECURITY INVOKER

;

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
  SECURITY INVOKER

;

--
-- hashing a seed with a random salt
--
CREATE OR REPLACE FUNCTION anon_funcs.random_hash(
  seed TEXT
)
RETURNS TEXT AS
$$
  SELECT anon_funcs.digest(
    seed,
    anon_funcs.random_string(6),
    pg_catalog.current_setting('anon_funcs.algorithm')
  );
$$
  LANGUAGE SQL
  VOLATILE
  SECURITY DEFINER
  PARALLEL RESTRICTED -- because random
  SET search_path = ''
  RETURNS NULL ON NULL INPUT
;

CREATE OR REPLACE FUNCTION anon_funcs.random_in(
  a ANYARRAY
)
RETURNS ANYELEMENT AS
$$
  SELECT a[pg_catalog.floor(pg_catalog.random()*array_length(a,1)+1)]
$$
  LANGUAGE SQL
  VOLATILE
  RETURNS NULL ON NULL INPUT
  ;

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
  LANGUAGE plpgsql
  ;





