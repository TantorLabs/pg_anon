# README #


### Installation ###
```python
apt install -y python3-pip
pip3 install -r requirements.txt
```

```sql
psql -c "CREATE USER anon_test_user WITH PASSWORD 'mYy5RexGsZ' SUPERUSER;" -U postgres

```

```bash
set TEST_DB_USER=anon_test_user
set TEST_DB_USER_PASSWORD=mYy5RexGsZ
set TEST_DB_HOST=127.0.0.1
set TEST_DB_PORT=5432
set TEST_SOURCE_DB=test_source_db
set TEST_TARGET_DB=test_target_db
```


```python
python3 test/full_test.py -v
>>
	Ran N tests in ...
	OK

# If all tests is OK then application ready to use

```


### Usage cases ###


#### Usage case: full dump/restore ####

Input: sourse database, empty target database, dictionary
Task: copy full structe and all data using dictionary

```bash
chown postgres:postgres -R /home/pg_anon

su - postgres
cd /home/pg_anon


# Common options in any mode:
#   --debug			(default false)
# 	--verbose = [info, debug, error]	(default info)
#   --threads

#---------------------------
# init schema "anon_funcs"
#---------------------------
python3 pg_anon.py \
	--db-host=127.0.0.1 \
	--db-name=test_source_db \
	--db-user=anon_test_user \
	--db-port=5432 \
	--db-user-password=mYy5RexGsZ \
	--mode=init

#---------------------------
# run dump
#---------------------------
python3 pg_anon.py \
	--db-host=127.0.0.1 \
	--db-name=test_source_db \
	--db-user=anon_test_user \
	--db-port=5432 \
	--db-user-password=mYy5RexGsZ \
	--dict-file=some_dict.py \
	--clear-output-dir \
	--mode=dump
# result will be written to "output/some_dict"

# Possible options in mode=dump:
#   --validate-dict			(default false)
# 	--validate-full			(default false)
#   --clear_output_dir		(default true)
#   --pg_dump=...
#   --format=[binary, text]
#   --copy_options=...


#---------------------------
# run restore
#---------------------------
python3 pg_anon.py \
	--db-host=127.0.0.1 \
	--db-name=test_target_db \
	--db-user=anon_test_user \
	--db-port=5432 \
	--db-user-password=mYy5RexGsZ \
	--input-dir=some_dict \
	--mode=restore

# Possible options in mode=restore:
#   --disable-checks 					(default false)
#   --seq-init-by-max-value 			(default false)
#   --drop-custom-check-constr 			(default false)
#   --pg_restore=...

#---------------------------
# If "--db-host" is not local then on database server prepare directory:
# mkdir -p /home/pg_anon/output/some_dict
# chown postgres:postgres -R /home/pg_anon
#---------------------------
```

#### Usage case: partial dump/restore ####


Input: sourse database, empty target database, dictionary
Task: copy partial structe and data of specific tables using dictionary

```
TODO
```

#### Usage case: sync specific tables ####

Input: sourse database, NOT empty target database, dictionary
Task: truncate target tables and copy data using dictionary

```
TODO
```

#### Usage case: dictionary generator ####

Input: sourse database
Task: anonymizer itself walks through the database and all tables, searches based on some algorithm for tables and fields for anonymization, then writes a dictionary itself with substitution of suitable functions

```
TODO
```


### How to escape/unescape complex names of objects ###

```python
python3

import json
j = {"k": "_TBL.$complex#имя;@&* a'2"}
json.dumps(j)
>>
	'{"k": "_TBL.$complex#\\u0438\\u043c\\u044f;@&* a\'2"}'

s = '{"k": "_TBL.$complex#\\u0438\\u043c\\u044f;@&* a\'2"}'
u = json.loads(s)
print(u['k'])
>>
	_TBL.$complex#имя;@&* a'2

```

### Generate dictionary by table rows ###


```sql
select
	jsonb_pretty(
		json_agg(json_build_object('schema', T.schm, 'table', T.tbl, 'fields', flds ))::jsonb
	)
from (
    select
        T.schm,
        T.tbl,
        JSON_OBJECT_AGG(fld, mrule) as flds
    from (
        select 'schm_1' as schm, 'tbl_a' as tbl, 'fld_1' as fld, 'md5(fld_1)' as mrule
        union all
        select 'schm_1', 'tbl_a', 'fld_2', 'md5(fld_2)'
        union all
        select 'schm_1','tbl_b', 'fld_1', 'md5(fld_1)'
        union all
        select 'schm_1','tbl_b', 'fld_2', 'md5(fld_2)'
    ) T
    group by schm, tbl
) T
>>
	[
	    {
	        "table": "tbl_b",
	        "fields": {
	            "fld_1": "md5(fld_1)",
	            "fld_2": "md5(fld_2)"
	        },
	        "schema": "schm_1"
	    },
	    {
	        "table": "tbl_a",
	        "fields": {
	            "fld_1": "md5(fld_1)",
	            "fld_2": "md5(fld_2)"
	        },
	        "schema": "schm_1"
	    }
	]

```
