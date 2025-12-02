# Dump
> [🏠 Home](../../README.md#-operations) | [🔍 Scan](scan.md) | [📂 Restore](restore.md) | [📚 SQL Functions Library](../sql-functions-library.md) | [🛠️ Debugging](../debugging.md) | [🔐 Sensitive Dictionary](../dicts/sens-dict-schema.md) | [📑 Tables Dictionary](../dicts/tables-dictionary.md)

## Overview

This mode creates an anonymized backup using rules from the [sensitive dictionary](../dicts/sens-dict-schema.md). 

> ⚠️ **Note**
> 
> This backup **can only be restored using** `pg_anon` and **cannot** be restored with `pg_restore`

## Prerequisites
- The `anon_funcs` schema with anonymization functions must already exist. See [init mode](init.md).
- A sensitive dictionary containing data about database fields and their anonymization rules must be prepared beforehand. See [create-dict (scan) mode](scan.md).

---

## Full dump (`dump`) mode:
Creates a backup containing both the database structure and anonymized data.

This backup can be restored using the following modes:
- [Full restore (`restore`) mode](restore.md#full-restore-restore-mode)
- [Structure restore (`sync-struct-restore`) mode](restore.md#structure-restore-sync-struct-restore-mode)
- [Data restore (`sync-data-restore`) mode](restore.md#data-restore-sync-data-restore-mode)

### Run example
```commandline
python pg_anon.py --mode=dump \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --prepared-sens-dict-file=sens_dict.py
``` 

---

## Structure dump (`sync-struct-dump`) mode
Creates a backup containing only the database structure without anonymized data.

This backup can be restored in this mode:
- [Structure restore (`sync-struct-restore`) mode](restore.md#structure-restore-sync-struct-restore-mode)

This mode is useful when used together with the [data dump (`sync-data-dump`) mode](#data-dump-sync-data-dump-mode).

### Run example
```commandline
python pg_anon.py --mode=sync-struct-dump \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --output-dir=test_sync_struct_dump \
                 --prepared-sens-dict-file=sens_dict.py
```

---

### Data dump (`sync-data-dump`) mode
Create backup contains only anonymized data without database structure.

This backup can be restored in this mode:
- [Data restore (`sync-data-restore`) mode](restore.md#data-restore-sync-data-restore-mode)

This mode can be useful for scheduling database synchronization, for example using `cron`.

### Run example
```commandline
python pg_anon.py --mode=sync-data-dump \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --output-dir=test_sync_data_dump \
                 --prepared-sens-dict-file=sens_dict.py
```

---


## Create partial dump:

Partial dumps are used to create a backup excluding certain tables from the source database.

Partial dump can be run in all dump modes:
- [Full dump (`dump`) mode](#full-dump-dump-mode)
- [Structure dump (`sync-struct-dump`) mode](#structure-dump-sync-struct-dump-mode)
- [Data dump (`sync-data-dump`) mode](#data-dump-sync-data-dump-mode)

Partial dumps use a tables dictionary containing a list of tables.  
This dictionary can act as either a whitelist or a blacklist.
See [tables dictionary](../dicts/tables-dictionary.md).

### Run example
#### Dump only need tables (whitelist)
```commandline
python pg_anon.py --mode=dump \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --output-dir=partial_dump_white_list \
                 --prepared-sens-dict-file=sens_dict.py
                 --partial-tables-dict-file=include_tables.py
```

#### Dump all tables without some specified tables (blacklist)
```commandline
python pg_anon.py --mode=dump \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --output-dir=partial_dump_black_list \
                 --prepared-sens-dict-file=sens_dict.py
                 --partial-tables-exclude-dict-file=exclude_tables.py
```


#### Dump only specified tables with excluding some of them  (whitelist + blacklist)
```commandline
python pg_anon.py --mode=dump \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --output-dir=partial_dump_white_list_and_black_list \
                 --prepared-sens-dict-file=sens_dict.py
                 --partial-tables-dict-file=include_tables.py
                 --partial-tables-exclude-dict-file=exclude_tables.py
```

---

## Options

### Common pg_anon options:

| Option                         | Description                                                                                                    |
|--------------------------------|----------------------------------------------------------------------------------------------------------------|
| `--config`                     | **Optional.** Path to the config file that can specify `pg_dump` and `pg_restore` utilities. (default: none)   |
| `--processes`                  | **Optional.** Number of processes used for multiprocessing operations. (default: 4)                            |
| `--db-connections-per-process` | **Optional.** Number of database connections per process for I/O operations. (default: 4)                      |
| `--verbose`                    | **Optional.** Sets the log verbosity level: `info`, `debug`, `error`. (default: info)                          |
| `--debug`                      | **Optional.** Enables debug mode (equivalent to `--verbose=debug`) and adds extra debug logs. (default: false) |


### Database configuration options:

| Option               | Description                                                                       |
|----------------------|-----------------------------------------------------------------------------------|
| `--db-host`          | **Required.** Database host.                                                      |
| `--db-port`          | **Required.** Database port.                                                      |
| `--db-name`          | **Required.** Database name.                                                      |
| `--db-user`          | **Required.** Database user.                                                      |
| `--db-user-password` | **Optional.** Database user password.                                             |
| `--db-passfile`      | **Optional.** Path to a file containing the password used for authentication.     |
| `--db-ssl-key-file`  | **Optional.** Path to the client SSL key file for secure connections.             |
| `--db-ssl-cert-file` | **Optional.** Path to the client SSL certificate file.                            |
| `--db-ssl-ca-file`   | **Optional.** Path to the CA certificate used to verify the server’s certificate. |


### Dump mode options:

| Option                               | Description                                                                                                                                                                                                           |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--prepared-sens-dict-file`          | **Required.** Input file or file list with sensitive fields, which was obtained in previous use by option `--output-sens-dict-file` or prepared manually                                                              |
| `--partial-tables-dict-file`         | **Optional.** Input file or file list containing a dict of tables to be included in the dump. All tables **not listed** in these files will be excluded. These files must be prepared manually (acts as a whitelist). |
| `--partial-tables-exclude-dict-file` | **Optional.** Input file or file list containing a dict of tables to be excluded from the dump. All tables **listed** in these files will be excluded. These files must be prepared manually (acts as a blacklist).   |
| `--dbg-stage-1-validate-dict`        | **Optional.** Validate dictionary, show the tables and run SQL queries without data export (default: false)                                                                                                           |
| `--dbg-stage-2-validate-data`        | **Optional.** Validate data, show the tables and run SQL queries with data export in prepared database (default: false)                                                                                               |
| `--dbg-stage-3-validate-full`        | **Optional.** Makes all logic with "limit" in SQL queries (default: false)                                                                                                                                            |
| `--clear-output-dir`                 | **Optional.** Clears the output directory from previous dumps or other files. (default: false)                                                                                                                        |
| `--pg-dump`                          | **Optional.** Path to the `pg_dump` Postgres tool (default `/usr/bin/pg_dump`).                                                                                                                                       |
| `--output-dir`                       | **Optional.** Output directory for dump files. (default "")                                                                                                                                                           |
| `--save-dicts`                       | **Optional.** Duplicate all input dictionaries to dir `runs`. It can be useful for debugging or integration purposes.                                                                                                 |
