# 💡 Contributing
> [🏠 Home](../README.md#-documentation-index) | [⚙️ How it works](how-it-works.md) | [💬 FAQ](faq.md) 

## Development setup

Install the project in editable mode:

```bash
pip install -e ".[dev]"         # CLI + dev tools (ruff, mypy, pytest)
pip install -e ".[api,dev]"     # CLI + REST API + dev tools
```

To add a new dependency, edit the `dependencies` list
in `pyproject.toml` and re-run install command above.

## Build and install from source

Build the wheel:

```bash
pip install build
python -m build
```

Install the built package:

```bash
pip install dist/pg_anon-*.whl           # CLI only
pip install "dist/pg_anon-*.whl[api]"    # CLI + REST API
```

---

## Project and directory structure


Main directories:
- `docker/`: Dir with docker.
- `docs/`: Documentation.
- `pg_anon/`: Main python modules.
- `rest_api/`: Additional python modules for starting rest service.
- `tests/`: Contains tests and data for test.


The main logic of pg_anon is contained within the following Python modules:
- `pg_anon/common/`
 - `constants.py` - Constants for pg_anon.
 - `db_queries.py` - SQL queries templates.
 - `db_utils.py` - Utility functions for working with databases.
 - `dto.py` - Small classes for data transfer.
 - `enums.py` - Enumerations for pg_anon.
 - `errors.py` - Error enumerations and classes for pg_anon.
 - `multiprocessing_utils.py` - Utility functions for multiprocessing.
 - `utils.py` - Common utility functions.
- `pg_anon/modes/`
  - `initialization.py` - Class with logic for mode `init`.
  - `create_dict.py` - Class with logic for mode `create-dict`.
  - `dump.py` - Class with logic for modes `dump`, `sync-struct-dump`, and `sync-data-dump`.
  - `restore.py` - Class with logic for modes `restore`, `sync-struct-restore`, and `sync-data-restore`.
  - `view_fields.py` - Class with logic for mode `view-fields`.
  - `view_data.py` - Class with logic for mode `view-data`.
- `pg_anon/app.py` - Main app class.
- `pg_anon/cli.py` - Entrypoint with argument parser. 
- `pg_anon/context.py` - Contains the Context class.
- `pg_anon/logger.py` - Contains the Logger class.
- `pg_anon/version.py` - Contains version of pg_anon.


The logic of REST API service for pg_anon is contained within the following Python modules:
- `rest_api/runners/background/`
 - `base.py` - BaseRunner class for all background tasks.
 - `init.py` - InitRunner class for run in background pg_anon in mode `init`.
 - `scan.py` - ScanRunner class for run in background pg_anon in modes `create-dict`.
 - `dump.py` - DumpRunner class for run in background pg_anon in modes `dump`, `sync-struct-dump`, and `sync-data-dump`.
 - `restore.py` - RestoreRunner class for run in background pg_anon in modes `restore`, `sync-struct-restore`, and `sync-data-restore`.
- `rest_api/runners/direct/`
  - `preview.py` - PreviewRunner class for get data for preview.
  - `view_fields.py` - ViewFieldsRunner class for run pg_anon in mode `view-fields`.
  - `view_data.py` - ViewDataRunner class for run pg_anon in mode `view-data`.
- `rest_api/api.py` - Contains API routing and app object used as entrypoint for REST service
- `rest_api/callbacks.py` - Callbacks functions used for sending background tasks results to webhook 
- `rest_api/constants.py` - Constants for pg_anon REST API service.
- `rest_api/dependencies.py`- FastAPI dependencies
- `rest_api/enums.py` -  Enumerations for pg_anon REST API service.
- `rest_api/pydantic_models.py` - Small classes for data transfer.
- `rest_api/utils.py` - Common utility functions.


`tree -L 4`:

```commandline
pg_anon/
├── Makefile
├── README.md
├── pyproject.toml
├── docker
│   ├── Dockerfile
│   ├── Makefile
│   ├── README.md
│   ├── entrypoint.sh
│   ├── entrypoint_dbg.sh
│   └── motd
├── docs
│   ├── api.md
│   ├── contributing.md
│   ├── debugging.md
│   ├── dicts
│   │   ├── meta-dict-schema.md
│   │   ├── non-sens-dict-schema.md
│   │   ├── sens-dict-schema.md
│   │   └── tables-dictionary.md
│   ├── faq.md
│   ├── how-it-works.md
│   ├── installation-and-configuring.md
│   ├── operations
│   │   ├── dump.md
│   │   ├── init.md
│   │   ├── restore.md
│   │   ├── scan.md
│   │   ├── view-data.md
│   │   └── view-fields.md
│   └── sql-functions-library.md
├── images
│   ├── Create-dict-Terms.drawio.png
│   ├── Dump-Resore-Terms.drawio.png
│   ├── dbg-stage-1.png
│   ├── dbg-stage-2.png
│   ├── dbg-stage-3.png
│   └── scan_workflow.png
├── pg_anon
│   ├── __init__.py
│   ├── __main__.py
│   ├── app.py
│   ├── cli.py
│   ├── common
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   ├── db_queries.py
│   │   ├── db_utils.py
│   │   ├── dto.py
│   │   ├── enums.py
│   │   ├── errors.py
│   │   ├── multiprocessing_utils.py
│   │   └── utils.py
│   ├── context.py
│   ├── init.sql
│   ├── logger.py
│   ├── modes
│   │   ├── __init__.py
│   │   ├── create_dict.py
│   │   ├── dump.py
│   │   ├── initialization.py
│   │   ├── restore.py
│   │   ├── view_data.py
│   │   └── view_fields.py
│   └── version.py
├── rest_api
│   ├── __init__.py
│   ├── __main__.py
│   ├── api.py
│   ├── callbacks.py
│   ├── constants.py
│   ├── dependencies.py
│   ├── enums.py
│   ├── openapi.json
│   ├── pydantic_models.py
│   ├── runners
│   │   ├── __init__.py
│   │   ├── background
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── dump.py
│   │   │   ├── init.py
│   │   │   ├── restore.py
│   │   │   └── scan.py
│   │   └── direct
│   │       ├── __init__.py
│   │       ├── preview.py
│   │       ├── view_data.py
│   │       └── view_fields.py
│   └── utils.py
└── tests
    ├── __init__.py
    ├── conftest.py
    ├── infrastructure
    │   ├── __init__.py
    │   ├── assertions.py
    │   ├── data.py
    │   ├── db.py
    │   ├── params.py
    │   └── pg_anon.py
    └── suites
        ├── __init__.py
        ├── test_dict_gen
        ├── test_dump_restore
        ├── test_mask
        ├── test_partial
        ├── test_pg_utils_options
        ├── test_restore_clean
        ├── test_stress
        ├── test_validate
        ├── test_view_data
        └── test_view_fields
```

---

## Testing

To test `pg_anon`, you need to have a local database installed. This section covers the installation of postgres and running the test suite.
Your operating system also need have a locale `en_US.UTF-8`, because in tests creating database in this locale.

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

To validate that your setup is functioning correctly, run the test suite:

```bash
pytest
```

Upon successful execution, all tests should pass.

To run a specific test file:

```bash
pytest tests/suites/test_validate.py -v
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
set TEST_CONFIG=/path/to/pg_anon/tests/config.yaml
```
