# Restore
> [🏠 Home](../../README.md#-operations) | [💾 Dump](dump.md) | [🛠️ Debugging](../debugging.md) | [📑 Tables dictionary](../dicts/tables-dictionary.md) 

## Overview

This mode restores an anonymized backup created using pg_anon in the [dump mode](dump.md). 

> ⚠️ **Note**
> 
> Only backups created with `pg_anon` can be restored. Backups created with `pg_dump` **cannot** be restored.

---

## Full restore (`restore`) mode
Restores both the database structure and data. 

### Prerequisites
- Dump or [partial dump](dump.md#create-partial-dump) created by pg_anon in [full dump (`dump`) mode](dump.md#full-dump-dump-mode).
- The target database must be empty, or the `--clean-db` / `--drop-db` options can be used.

### Run example
```commandline
python pg_anon.py --mode=restore \
                  --db-host=127.0.0.1 \
                  --db-user=postgres \
                  --db-user-password=postgres \
                  --db-name=target_db \
                  --input-dir=path/to/my_full_dump \
                  --verbose=debug
```

---

## Structure restore (`sync-struct-restore`) mode
Restores only the database structure.

### Prerequisites
- Dump or [partial dump](dump.md#create-partial-dump) created in modes:
  - [Full dump (`dump`) mode](dump.md#full-dump-dump-mode)
  - [Structure dump (`sync-struct-dump`) mode](dump.md#structure-dump-sync-struct-dump-mode)
- The target database must be empty, or the `--clean-db` / `--drop-db` options can be used.

### Run example
```commandline
python pg_anon.py --mode=sync-struct-restore \
                  --db-host=127.0.0.1 \
                  --db-user=postgres \
                  --db-user-password=postgres \
                  --db-name=target_db \
                  --input-dir=path/to/my_sync_struct_dump \
                  --verbose=debug
```

---

## Data restore (`sync-data-restore`) mode
Restores data only.

### Prerequisites
- Dump or [partial dump](dump.md#create-partial-dump) created in modes:
  - [Full dump (`dump`) mode](dump.md#full-dump-dump-mode)
  - [Data dump (`sync-data-dump`) mode](dump.md#data-dump-sync-data-dump-mode)
- The target database must already contain the required schema for restoring data.

### Run example
```commandline
python pg_anon.py --mode=sync-data-restore \
                  --db-host=127.0.0.1 \
                  --db-user=postgres \
                  --db-user-password=postgres \
                  --db-name=target_db \
                  --input-dir=path/to/my_sync_data_dump \
                  --verbose=debug
```

---

## Create partial restore:

Partial restores are used to restore only part of a backup, excluding certain tables if needed.

Partial restore can be run in all restore modes:
- [Full restore (`restore`) mode](#full-restore-restore-mode)
- [Structure restore (`sync-struct-restore`) mode](#structure-restore-sync-struct-restore-mode)
- [Data restore (`sync-data-restore`) mode](#data-restore-sync-data-restore-mode)

Partial restores use a tables dictionary containing a list of tables.  
This dictionary can act as either a whitelist (only listed tables are restored) or a blacklist (listed tables are excluded).
See [tables dictionary](../dicts/tables-dictionary.md).

### Run example
#### Restore only need tables (whitelist)
```commandline
python pg_anon.py --mode=restore \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --input-dir=partial_dump_white_list \
                 --prepared-sens-dict-file=sens_dict.py
                 --partial-tables-dict-file=include_tables.py
```

#### Dump all tables without some specified tables (blacklist)
```commandline
python pg_anon.py --mode=restore \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --input-dir=partial_dump_black_list \
                 --prepared-sens-dict-file=sens_dict.py
                 --partial-tables-exclude-dict-file=exclude_tables.py
```


#### Dump only specified tables with excluding some of them  (whitelist + blacklist)
```commandline
python pg_anon.py --mode=restore \
                 --db-host=127.0.0.1 \
                 --db-user=postgres \
                 --db-user-password=postgres \
                 --db-name=source_db \
                 --input-dir=partial_dump_white_list_and_black_list \
                 --prepared-sens-dict-file=sens_dict.py
                 --partial-tables-dict-file=include_tables.py
                 --partial-tables-exclude-dict-file=exclude_tables.py
```

---

## Options

### Common pg_anon options:

| Option                         | Description                                                                                                    |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `--config`                     | **Optional.** Path to the config file that can specify `pg_dump` and `pg_restore` utilities. (default: none)   |
| `--processes`                  | **Optional.** Number of processes used for multiprocessing operations. (default: 4)                            |
| `--db-connections-per-process` | **Optional.** Number of database connections per process for I/O operations. (default: 4)                      |
| `--verbose`                    | **Optional.** Sets the log verbosity level: `info`, `debug`, `error`. (default: info)                          |
| `--debug`                      | **Optional.** Enables debug mode (equivalent to `--verbose=debug`) and adds extra debug logs. (default: false) |


### Database configuration options:

| Option               | Description                                                                       |
| -------------------- | --------------------------------------------------------------------------------- |
| `--db-host`          | **Required.** Database host.                                                      |
| `--db-port`          | **Required.** Database port.                                                      |
| `--db-name`          | **Required.** Database name.                                                      |
| `--db-user`          | **Required.** Database user.                                                      |
| `--db-user-password` | **Optional.** Database user password.                                             |
| `--db-passfile`      | **Optional.** Path to a file containing the password used for authentication.     |
| `--db-ssl-key-file`  | **Optional.** Path to the client SSL key file for secure connections.             |
| `--db-ssl-cert-file` | **Optional.** Path to the client SSL certificate file.                            |
| `--db-ssl-ca-file`   | **Optional.** Path to the CA certificate used to verify the server’s certificate. |


### Restore mode options:

| Option                               | Description                                                                                                                                                                                                          |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--input-dir`                        | **Required.** Path to the directory containing dump files created in dump mode.                                                                                                                                      |
| `--partial-tables-dict-file`         | **Optional.** Input file or file list containing a dict of tables to be included in the dump. All tables **not listed** in these files will be excluded. These files must be prepared manually (acts as a whitelist). |
| `--partial-tables-exclude-dict-file` | **Optional.** Input file or file list containing a dict of tables to be excluded from the dump. All tables **listed** in these files will be excluded. These files must be prepared manually (acts as a blacklist).  |
| `--disable-checks`                   | **Optional.** Disable checks of disk space and PostgreSQL version (default false)                                                                                                                                    |
| `--seq-init-by-max-value`            | **Optional.** Initialize sequences based on maximum values. Otherwise, the sequences will be initialized based on the values of the source database.                                                                 |
| `--drop-custom-check-constr`         | **Optional.** Drops all CHECK constraints that contain user-defined procedures to avoid performance degradation during data loading.                                                                                 |
| `--pg-restore`                       | **Optional.** Path to the `pg_restore` Postgres tool.                                                                                                                                                                |
| `--clean-db`                         | **Optional.** Cleans the database objects before restoring (if they exist in the dump). Mutually exclusive with `--drop-db`.                                                                                         |
| `--drop-db`                          | **Optional.** Drop target database before restore. Mutually exclusive with `--clean-db`.                                                                                                                             |
| `--save-dicts`                       | **Optional.** Duplicate all input dictionaries to dir `runs`. It can be useful for debugging or integration purposes.                                                                                                |
