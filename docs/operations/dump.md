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
python pg_anon.py dump \
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
python pg_anon.py sync-struct-dump \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --output-dir=test_sync_struct_dump \
                 --prepared-sens-dict-file=sens_dict.py
```

---

## Data dump (`sync-data-dump`) mode
Create backup contains only anonymized data without database structure.

This backup can be restored in this mode:
- [Data restore (`sync-data-restore`) mode](restore.md#data-restore-sync-data-restore-mode)

This mode can be useful for scheduling database synchronization, for example using `cron`.

### Run example
```commandline
python pg_anon.py sync-data-dump \
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
python pg_anon.py dump \
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
python pg_anon.py dump \
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
python pg_anon.py dump \
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

| Option                         | Required | Description                                                                                      |
|--------------------------------|----------|--------------------------------------------------------------------------------------------------|
| `--config`                     | No       | Path to the config file that can specify `pg_dump` and `pg_restore` utilities. (default: none)   |
| `--processes`                  | No       | Number of processes used for multiprocessing operations. (default: 4)                            |
| `--db-connections-per-process` | No       | Number of database connections per process for I/O operations. (default: 4)                      |
| `--verbose`                    | No       | Sets the log verbosity level: `info`, `debug`, `error`. (default: info)                          |
| `--debug`                      | No       | Enables debug mode (equivalent to `--verbose=debug`) and adds extra debug logs. (default: false) |


### Database configuration options:

| Option               | Required | Description                                                         |
|----------------------|----------|---------------------------------------------------------------------|
| `--db-host`          | Yes      | Database host.                                                      |
| `--db-port`          | No       | Database port.                                                      |
| `--db-name`          | Yes      | Database name.                                                      |
| `--db-user`          | Yes      | Database user.                                                      |
| `--db-user-password` | No       | Database user password.                                             |
| `--db-passfile`      | No       | Path to a file containing the password used for authentication.     |
| `--db-ssl-key-file`  | No       | Path to the client SSL key file for secure connections.             |
| `--db-ssl-cert-file` | No       | Path to the client SSL certificate file.                            |
| `--db-ssl-ca-file`   | No       | Path to the CA certificate used to verify the server’s certificate. |


### Dump mode options:

| Option                               | Required | Description                                                                                                                                                                                                                                          |
|--------------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--prepared-sens-dict-file`          | Yes      | Input file or file list contains [sensitive dictionary](../dicts/sens-dict-schema.md), which was generated by the [create-dict (scan) mode](scan.md) or created manually. In rules collision case, priority has rules in last file from the list.    |
| `--partial-tables-dict-file`         | No       | Input file or file list contains [tables dictionary](../dicts/tables-dictionary.md) for include specific tables in the dump. All tables **not listed** in these files will be excluded. These files must be prepared manually (acts as a whitelist). |
| `--partial-tables-exclude-dict-file` | No       | Input file or file list contains [tables dictionary](../dicts/tables-dictionary.md) for exclude specific tables from the dump. All tables **listed** in these files will be excluded. These files must be prepared manually (acts as a blacklist).   |
| `--dbg-stage-1-validate-dict`        | No       | Validate dictionary, show the tables and run SQL queries without data export. (default: false)                                                                                                                                                       |
| `--dbg-stage-2-validate-data`        | No       | Validate data, show the tables and run SQL queries with data export in prepared database. (default: false)                                                                                                                                           |
| `--dbg-stage-3-validate-full`        | No       | Makes all logic with "limit" in SQL queries. (default: false)                                                                                                                                                                                        |
| `--clear-output-dir`                 | No       | Clears the output directory from previous dumps or other files. (default: false)                                                                                                                                                                     |
| `--pg-dump`                          | No       | Path to the `pg_dump` Postgres tool (default `/usr/bin/pg_dump`).                                                                                                                                                                                    |
| `--output-dir`                       | No       | Output directory for dump files. (default "")                                                                                                                                                                                                        |
| `--ignore-privileges`                | No       | Ignore privileges from source db.                                                                                                                                                                                                                    |
| `--save-dicts`                       | No       | Duplicate all input dictionaries to dir `runs`. It can be useful for debugging or integration purposes.                                                                                                                                              |
