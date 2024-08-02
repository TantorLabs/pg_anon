# pg_anon

## Overview

`pg_anon` is an efficient tool for the anonymization of Postgres data specifically designed for IT companies. These companies often store "sensitive" data that includes both commercial secrets and personal user information such as contact numbers, passport details, etc.

The tool comes in handy when it is necessary to transfer the database contents from the production environment to other environments for performance testing or functionality debugging during the development process. With `pg_anon`, no sensitive data is exposed, preventing potential data leaks.

## Features

`pg_anon` works in several modes:

- **`init`**: Creates `anon_funcs` schema with anonymization functions.
- **`create_dict`**: Scans the DB data and creates a metadict with an anonymization profile.
- **`dump`**: Creates a database structure dump using Postgres `pg_dump` tool, and data dumps using `COPY ...` queries with anonymization functions. The data dump step saves data locally in `*.bin.gz` format. During this step, the data is anonymized on the database side by `anon_funcs`.
- **`restore`**: Restores database structure using Postgres `pg_restore` tool and data from the dump to the target DB. `restore` mode can separately restore database structure or data.

## Requirements & Dependencies

`pg_anon` is based on `Python3` and also requires the third-party libraries listed in `requirements.txt`.

It uses the following tools and technologies:

- Postgres [`pg_dump`](https://www.postgresql.org/docs/current/app-pgdump.html) tool for dumping the database structure.
- Postgres [`pg_restore`](https://www.postgresql.org/docs/current/app-pgrestore.html) tool for restoring the database structure.
- Postgres [functions](https://www.postgresql.org/docs/current/functions.html) for the anonymization process.

## Installation Guide

### Preconditions

The tool supports Python3.11 and higher versions. The code is hosted on the following repository: [pg_anon repository on Github](https://github.com/TantorLabs/pg_anon).

### Installation Instructions

Installation processes slightly differ depending on your operating system.

#### macOS

1. Install Python3 if it isn't installed:
   - Install [Homebrew](https://brew.sh/)
   - [`brew install python@3.11`](https://formulae.brew.sh/formula/python@3.11)
2. Clone the repository: `git clone https://github.com/TantorLabs/pg_anon.git`
3. Go to the project directory: `cd pg_anon`
4. Set up a virtual environment:
   - Install the virtual environment: `python3 -m venv venv`
   - Activate the virtual environment: `source venv/bin/activate`
5. Install the dependencies: `pip install -r requirements.txt`

#### Ubuntu/Redhat/CentOS

1. Install Python3 if it isn't installed: `sudo apt-get install python3.11` (for Ubuntu), `sudo yum install python311` (for Redhat/Centos)
2. Clone the repository: `git clone https://github.com/TantorLabs/pg_anon.git`
3. Go to the project directory: `cd pg_anon`
4. Set up a virtual environment:
   - Install the virtual environment: `python3 -m venv venv`
   - Activate the virtual environment: `source venv/bin/activate`
5. Install the dependencies: `pip install -r requirements.txt`

#### Windows 7/Windows 11

1. Install Python3 if it isn't installed: Download it from the official [Python website](https://www.python.org/downloads/)
2. Clone the repository: `git clone https://github.com/TantorLabs/pg_anon.git`
3. Go to the project directory: `cd pg_anon`
4. Set up a virtual environment:
   - Install the virtual environment: `py -m venv venv`
   - Activate the virtual environment: `.\venv\Scripts\activate`
5. Install the dependencies: `pip install -r requirements.txt`

## Testing

To test `pg_anon`, you need to have a local database installed. This section covers the installation of postgres and running the test suite.

### Setting Up PostgreSQL

To facilitate the testing, here are instructions on how to set up PostgreSQL on Ubuntu:

1. Add repository configuration:

   ```commandline
   echo "deb [arch=amd64] http://apt.postgresql.org/pub/repos/apt focal-pgdg main" >> /etc/apt/sources.list.d/pgdg.list
   wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
   ```

2. Update packages and install PostgreSQL:

   ```commandline
   apt -y install postgresql-15 postgresql-client-15
   ```

3. Allow connections to the PostgreSQL server:
   ```commandline
   sed -i  '/listen_addresses/s/^#//g' /etc/postgresql/15/main/postgresql.conf
   sed -ie "s/^listen_addresses.*/listen_addresses = '127.0.0.1'/" /etc/postgresql/15/main/postgresql.conf
   sed -i -e '/local.*peer/s/postgres/all/' -e 's/peer\|md5/trust/g' /etc/postgresql/${PG_VERSION}/main/pg_hba.conf
   ```
4. Restart the PostgreSQL instance for the changes to take effect:
   ```commandline
   pg_ctlcluster 15 main restart
   ```
5. Create a test user with superuser rights to allow running the COPY commands:
   ```commandline
   psql -c "CREATE USER anon_test_user WITH PASSWORD 'mYy5RexGsZ' SUPERUSER;" -U postgres
   ```

### Executing Tests

To validate that your setup is functioning correctly, run the unit tests:

```commandline
export PYTHONPATH=$(pwd)
python tests/test_full.py -v
```

Upon successful execution, the output should resemble the following:

```commandline
Ran N tests in ...
OK
```

If all tests pass, the application is ready to use.

To run a specific test case, use the following pattern:

```commandline
export PYTHONPATH=$(pwd)
python tests/test_full.py -v PGAnonValidateUnitTest
```

### Test Database Configuration

Test database connection settings can be overridden using environment variables:

```commandline
set TEST_DB_USER=anon_test_user
set TEST_DB_USER_PASSWORD=mYy5RexGsZ
set TEST_DB_HOST=127.0.0.1
set TEST_DB_PORT=5432
set TEST_SOURCE_DB=test_source_db
set TEST_TARGET_DB=test_target_db
```

## Usage

To display the help message for the CLI, run:

```commandline
python pg_anon.py --help
```

Common pg_anon options:

| Option        | Description                                                    |
|---------------|----------------------------------------------------------------|
| `--debug`     | Enable debug mode (default false)                              |
| `--verbose`   | Configure verbose mode: [info, debug, error] (default info)    |
| `--threads`   | Amount of threads for IO operations (default 4)                |
| `--processes` | Amount of processes for multiprocessing operations (default 4) |

Database configuration options:

| Option                | Description                                                                                         |
|-----------------------|-----------------------------------------------------------------------------------------------------|
| `--db-host`           | Specifies your database host                                                                        |
| `--db-port`           | Specifies your database port                                                                        |
| `--db-name`           | Specifies your database name                                                                        |
| `--db-user`           | Specifies your database user                                                                        |
| `--db-user-password`  | Specifies your database user password                                                               |
| `--db-passfile`       | Path to the file containing the password to be used when connecting to the database                 |
| `--db-ssl-key-file`   | Path to the client SSL key file for secure connections to the database                              |
| `--db-ssl-cert-file`  | Path to the client SSL certificate file for secure connections to the database                      |
| `--db-ssl-ca-file`    | Path to the root SSL certificate file. This certificate is used to verify the server's certificate  |

### Run init mode

To init schema "anon_funcs", run pg_anon in 'init' mode:

```commandline
python pg_anon.py --mode=init \
                  --db-user=postgres \
                  --db-user-password=postgres \
                  --db-name=test_source_db
```

### Run create_dict mode

#### Prerequisites:

- Generated or manually created dictionary `*.py` file with anonymization profile
- "anon_funcs" created in init mode

To create the dictionary:

```commandline
python pg_anon.py --mode=create-dict \
                  --db-user=postgres \
                  --db-user-password=postgres \
                  --db-name=test_source_db \
                  --meta-dict-file=test_meta_dict.py \
                  --prepared-sens-dict-file=test_sens_dict_output_previous_use.py \
                  --prepared-no-sens-dict-file=test_no_sens_dict_output_previous_use.py \
                  --output-sens-dict-file=test_sens_dict_output.py \
                  --output-no-sens-dict-file=test_no_sens_dict_output.py \
                  --processes=2
```

| Option                         | Description                                                                                                                                                  |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--meta-dict-file`             | Input file or file list with scan rules of sensitive and not sensitive fields. In collision case, priority has first file in list                            |
| `--prepared-sens-dict-file`    | Input file or file list with sensitive fields, which was obtained in previous use by option `--output-sens-dict-file` or prepared manually (Optional)        |
| `--prepared-no-sens-dict-file` | Input file or file list with not sensitive fields, which was obtained in previous use by option `--output-no-sens-dict-file` or prepared manually (Optional) |
| `--output-sens-dict-file`      | Output file with sensitive fields will be saved to this value                                                                                                |
| `--output-no-sens-dict-file`   | Output file with not sensitive fields will be saved to this value (Optional)                                                                                 |
| `--scan-mode`                  | defines whether to scan all data or only part of it ["full", "partial"] (default "partial")                                                                  |
| `--scan-partial-rows`          | In `--scan-mode partial` defines amount of rows to scan (default 10000)                                                                                      |

#### Requirements for input --meta-dict-file (metadict):

Input metadict.py file should contain that type of structure:
```python
var = {
    "field": {  # Which fields to anonymize without scanning the content
        "rules": [  # List of regular expressions to search for fields by name
            "^fld_5_em",
            "^amount"
        ],
        "constants": [  # List of constant field names
            "usd",
            "name"
        ]
    },
    "skip_rules": [  # List of schemas, tables, and fields to skip
        {
            # possibly some schema or table contains a lot of data that is not worth scanning. Skipped objects will not be automatically included in the resulting dictionary. Masks are not supported in this object.
            "schema": "schm_mask_ext_exclude_2",  # Schema specification is mandatory
            "table": "card_numbers",  # Optional. If there is no "table", the entire schema will be skipped.
            "fields": ["val_skip"]  # Optional. If there are no "fields", the entire table will be skipped.
        }
    ],
    "data_regex": {  # List of regular expressions to search for sensitive data
        "rules": [
            """[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",  # email
            "7?[\d]{10}"  # phone 7XXXXXXXXXX 
        ]
    },
    "data_const": {
        # List of constants in lowercase, upon detection of which the field will be included in the resulting dictionary. If a text field contains a value consisting of several words, this value will be split into words, converted to lowercase, and matched with the constants from this list. Words shorter than 5 characters are ignored. Search is performed using set.intersection
        "constants": [  # When reading the meta-dictionary, the values of this list are placed in a set container
            "simpson",
            "account"
        ]
    },
    "sens_pg_types": [
        # List of field types which should be checked (other types won't be checked). If this massive is empty program set default SENS_PG_TYPES = ["text", "integer", "bigint", "character", "json"]
        "text",
        "integer",
        "bigint",
        "varchar",  # better write small names, because checker find substrings in original name. For example types varchar(3) contains varchar, so varchar(3) will be checked in program.
        "json"
    ],
    "funcs": {  # List of field types (int, text, ...) and functions for anonymization
        # If a certain field is found during scanning, a function listed in this list will be used according to its type.
        "text": "anon_funcs.digest(\"%s\", 'salt_word', 'md5')",
        "numeric": "anon_funcs.noise(\"%s\", 10)",
        "timestamp": "anon_funcs.dnoise(\"%s\",  interval '6 month')"
    }
}
```

### Run dump mode

#### Prerequisites:

- `anon_funcs` schema with anonymization functions should be created. See [--mode init](#run-init-mode) example.
- output dict file with meta information about database fields, and it's anonymization should be created.
  See [--mode create-dict](#run-create_dict-mode)

#### Dump modes:

1. To create the structure dump and data dump:

   ```commandline
   python pg_anon.py --mode=dump \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_source_db \
                     --prepared-sens-dict-file=test_sens_dict_output.py
   ```

2. To create only structure dump:

   ```commandline
   python pg_anon.py --mode=sync-struct-dump \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_source_db \
                     --output-dir=test_sync_struct_dump \
                     --prepared-sens-dict-file=test_sens_dict_output.py
   ```

3. To create only data dump:

   ```commandline
   python pg_anon.py --mode=sync-data-dump \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_source_db \
                     --output-dir=test_sync_data_dump \
                     --prepared-sens-dict-file=test_sens_dict_output.py
   ```

   This mode could be useful for scheduling the database synchronization, for example with `cron`.

Possible options in mode=dump:

| Option                         | Description                                                                                                                                |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `--prepared-sens-dict-file`    | Input file or file list with sensitive fields, which was obtained in previous use by option `--output-sens-dict-file` or prepared manually |
| `--dbg-stage-1-validate-dict`  | Validate dictionary, show the tables and run SQL queries without data export (default false)                                               |
| `--dbg-stage-2-validate-data`  | Validate data, show the tables and run SQL queries with data export in prepared database (default false)                                   |
| `--dbg-stage-3-validate-full`  | Makes all logic with "limit" in SQL queries (default false)                                                                                |
| `--clear-output-dir`           | In dump mode clears output dict from previous dump or another files. (default true)                                                        |
| `--pg-dump`                    | Path to the `pg_dump` Postgres tool (default `/usr/bin/pg_dump`).                                                                          |
| `--output-dir`                 | Output directory for dump files. (default "")                                                                                              |

### Run restore mode

#### Prerequisites:

- Each mode requires dump for restore.

#### Restore modes:

1. Restore structure and data:  
   _This mode requires the dump output, created in `--mode=dump`._

   ```commandline
   python pg_anon.py --mode=restore \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_target_db \
                     --input-dir=test_dict_output \
                     --verbose=debug
   ```

2. Restore structure only:  
   _This mode requires the dump output, created in `--mode=sync-struct-dump`._

   ```commandline
   python pg_anon.py --mode=sync-struct-restore \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_target_db \
                     --input-dir=test_sync_struct_dump \
                     --verbose=debug
   ```

3. Restore data only:  
   _This mode requires the dump output, created in `--mode=sync-data-dump`._

   ```commandline
   python pg_anon.py --mode=sync-data-restore \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_target_db \
                     --input-dir=test_sync_data_dump \
                     --verbose=debug
   ```

Possible options in `--mode restore`:

| Option                       | Description                                                                                                                            |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `--input-dir`                | Input directory, with the dump files, created in dump mode                                                                             |
| `--disable-checks`           | Disable checks of disk space and PostgreSQL version (default false)                                                                    |
| `--seq-init-by-max-value`    | Initialize sequences based on maximum values. Otherwise, the sequences will be initialized based on the values of the source database. |
| `--drop-custom-check-constr` | Drop all CHECK constrains containing user-defined procedures to avoid performance degradation at the data loading stage.               |
| `--pg-restore`               | Path to the `pg_dump` Postgres tool.                                                                                                   |

### Generate dictionary from table rows

If you have a table that contains objects and fields for anonymization, you can use this SQL query to generate a dictionary in json format:

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

### Debug stages in dump and restore modes

#### Debug stages:

1. Stage 1: validate dict

This stage validate dictionary, show the tables and run SQL queries without data export into the disk or database.
So if program works without errors => the stage is passed.

```commandline
   python pg_anon.py --mode=dump \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_source_db \
                     --output-dir=test_dbg_stages \
                     --prepared-sens-dict-file=test_dbg_stages.py \
                     --clear-output-dir \
                     --verbose=debug \
                     --debug \
                     --dbg-stage-1-validate-dict
   ```

2. Stage 2: validate data

Validate data, show the tables and run SQL queries with data export and limit 100 in prepared database.
This stage requires database with all structure with only pre-data condition, which described in --prepared-sens-dict-file.

- If you want to create the database with required structure, just run:

One-time structure dump:

```commandline
   python pg_anon.py --mode=sync-struct-dump \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_source_db \
                     --output-dir=test_stage_2 \
                     --prepared-sens-dict-file=test_dbg_stages.py \
                     --clear-output-dir \
                     --verbose=debug \
                     --debug \
                     --dbg-stage-3-validate-full
   ```

And then as many times as you want structure restore:

```commandline
   su - postgres -c "psql -U postgres -d postgres -c \"DROP DATABASE IF EXISTS test_target_db_7\""
   su - postgres -c "psql -U postgres -d postgres -c \"CREATE DATABASE test_target_db_7\""
   python pg_anon.py --mode=sync-struct-restore \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_target_db_7 \
                     --input-dir=test_stage_2 \
                     --verbose=debug \
                     --debug 
   ```

- Validate data stage in dump:

```commandline
   python pg_anon.py --mode=dump \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_source_db \
                     --output-dir=test_dbg_stages \
                     --prepared-sens-dict-file=test_dbg_stages.py \
                     --clear-output-dir \
                     --verbose=debug \
                     --debug \
                     --dbg-stage-2-validate-data
   ```

- Validate data stage in data-restore:

```commandline
   python pg_anon.py --mode=sync-data-restore \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_target_db_7 \
                     --input-dir=test_dbg_stages \
                     --verbose=debug \
                     --debug 
   
   # And for example view all data in every table:
   su - postgres -c "psql -U postgres -d test_target_db_7 -c \"SELECT * FROM public.contracts\""
   ```

3. Stage 3: validate full

Makes all logic with "limit 100" in SQL queries. In this stage you don't need prepared database, just run:

```commandline
   su - postgres -c "psql -U postgres -d postgres -c \"DROP DATABASE IF EXISTS test_target_db_8\""
   su - postgres -c "psql -U postgres -d postgres -c \"CREATE DATABASE test_target_db_8\""
   ```

- Validate full stage in dump:

```commandline
   python pg_anon.py --mode=dump \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_source_db \
                     --output-dir=test_dbg_stages \
                     --prepared-sens-dict-file=test_dbg_stages.py \
                     --clear-output-dir \
                     --verbose=debug \
                     --debug \
                     --dbg-stage-3-validate-full
   ```

- Validate full stage in restore:

```commandline
   python pg_anon.py --mode=restore \
                     --db-host=127.0.0.1 \
                     --db-user=postgres \
                     --db-user-password=postgres \
                     --db-name=test_target_db_8 \
                     --input-dir=test_dbg_stages \
                     --verbose=debug \
                     --debug 
   
   # And for example view all data in every table:
   su - postgres -c "psql -U postgres -d test_target_db_8 -c \"SELECT * FROM public.contracts\""
   ```


### How to escape/unescape complex names of objects

```python

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

## Contributing

### Dependencies

The pg_anon uses [Poetry](https://python-poetry.org/)
dependency management tool for managing dependencies and creating packages.  
For [adding new dependencies](https://python-poetry.org/docs/managing-dependencies/)
install Poetry and run command:

```commandline
poetry add <package_name>
```

For locking the dependencies use command:

```commandline
poetry lock --no-update
```

Additionally, [export](https://python-poetry.org/docs/cli/#export)
the latest packages to _requirements.txt_ using poetry export plugin:

```commandline
poetry export -f requirements.txt --output requirements.txt
```

### Build package

For [building](https://python-poetry.org/docs/libraries/#packaging) the package use command:

```commandline
poetry build
```

Additionally package could be build package using setuptools:

```commandline
python3 setup.py sdist
```

## Future plans:

- `--format`: COPY data format, can be overwritten by --copy-options. Selects the data format to be read or written: text, csv or binary.
- `--copy-options`: Options for COPY command like "with binary".
- Supporting restore after full dump:
  Right now struct restoring the structure of the database possible only after struct dump. So you don't able to restore the structure after full dump.
- Simplify commands and options to improve the user experience.
