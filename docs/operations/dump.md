# Dump
> [🏠 Home](../../README.md#-operations) | [🔍 Scan](scan.md) | [📂 Restore](restore.md) | [🛠️ Debugging](../debugging.md) | [🛡️ Security](../security.md) | [🔐 Sensitive Dictionary](../dicts/sens-dict-schema.md) | [📑 Tables Dictionary](../dicts/tables-dictionary.md)

## Overview

This mode creates a masked backup using rules from the [sensitive dictionary](../dicts/sens-dict-schema.md). 

> ⚠️ **Note**
> 
> This backup **can only be restored using** `pg_anon` and **cannot** be restored with `pg_restore`

## Prerequisites
- The `anon_funcs` schema with masking functions must already exist. See [init mode](init.md).
- A sensitive dictionary containing data about database fields and their masking rules must be prepared beforehand. See [create-dict (scan) mode](scan.md).

---

## Full dump (`dump`) mode:
Creates a backup containing both the database structure and masked data.

This backup can be restored using the following modes:
- [Full restore (`restore`) mode](restore.md#full-restore-restore-mode)
- [Structure restore (`sync-struct-restore`) mode](restore.md#structure-restore-sync-struct-restore-mode)
- [Data restore (`sync-data-restore`) mode](restore.md#data-restore-sync-data-restore-mode)

### Run example
```commandline
pg_anon dump \
    --db-host=127.0.0.1 \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=source_db \
    --prepared-sens-dict-file=sens_dict.py
``` 

---

## Structure dump (`sync-struct-dump`) mode
Creates a backup containing only the database structure without masked data.

This backup can be restored in this mode:
- [Structure restore (`sync-struct-restore`) mode](restore.md#structure-restore-sync-struct-restore-mode)

This mode is useful when used together with the [data dump (`sync-data-dump`) mode](#data-dump-sync-data-dump-mode).

### Run example
```commandline
pg_anon sync-struct-dump \
    --db-host=127.0.0.1 \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=source_db \
    --output-dir=test_sync_struct_dump \
    --prepared-sens-dict-file=sens_dict.py
```

---

## Data dump (`sync-data-dump`) mode
Create backup contains only masked data without database structure.

This backup can be restored in this mode:
- [Data restore (`sync-data-restore`) mode](restore.md#data-restore-sync-data-restore-mode)

This mode can be useful for scheduling database synchronization, for example using `cron`.

### Run example
```commandline
pg_anon sync-data-dump \
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
pg_anon dump \
    --db-host=127.0.0.1 \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=source_db \
    --output-dir=partial_dump_white_list \
    --prepared-sens-dict-file=sens_dict.py \
    --partial-tables-dict-file=include_tables.py
```

#### Dump all tables without some specified tables (blacklist)
```commandline
pg_anon dump \
    --db-host=127.0.0.1 \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=source_db \
    --output-dir=partial_dump_black_list \
    --prepared-sens-dict-file=sens_dict.py \
    --partial-tables-exclude-dict-file=exclude_tables.py
```


#### Dump only specified tables with excluding some of them  (whitelist + blacklist)
```commandline
pg_anon dump \
    --db-host=127.0.0.1 \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=source_db \
    --output-dir=partial_dump_white_list_and_black_list \
    --prepared-sens-dict-file=sens_dict.py \
    --partial-tables-dict-file=include_tables.py \
    --partial-tables-exclude-dict-file=exclude_tables.py
```

---

## Exclude schemas:

Schema options are used to create a backup without certain schemas, in all dump modes. An excluded schema has
no structure and no data in the dump.

`--schema-name` and `--schema-mask` keep only the selected schemas, `--exclude-schema-name` and
`--exclude-schema-mask` leave schemas out. If a schema is selected by both, it is excluded.

- Each option takes one value or a comma-separated list. A comma inside brackets, like in `tmp_{1,2}`, does not
  split the value.
- A name matches only the same name. A mask is a regular expression and matches any part of the name. For
  example, `--exclude-schema-name=audit` leaves out only `audit`, and `--exclude-schema-mask=audit` also leaves
  out `audit_old`.
- `anon_funcs` and schemas that belong to extensions are never excluded.
- An event trigger is not restored if its function is in an excluded schema.
- For `sync-struct-dump` and `sync-data-dump`, use the same schema options. Otherwise the data does not fit the
  structure.

If a table that stays in the dump uses a type from an excluded schema, or is a partition or a child of a table
there, the dump stops with an error. Exclude these tables too, or keep the schema. `sync-data-dump` has no
structure, so it does not check this.

### Run example
```commandline
pg_anon dump \
    --db-host=127.0.0.1 \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=source_db \
    --output-dir=dump_without_audit \
    --prepared-sens-dict-file=sens_dict.py \
    --exclude-schema-name=audit \
    --exclude-schema-mask='^tmp_'
```

### Structure or data

| Tool | Leaves out of the dump |
|---|---|
| [Schema options](#exclude-schemas) | Schemas, with structure and data |
| [Tables dictionary](../dicts/tables-dictionary.md) | Tables, with structure and data |
| `dictionary_exclude` in the [sensitive dictionary](../dicts/sens-dict-schema.md) | Data of tables. The structure stays |

---

## Options

### Common pg_anon options:

| Option                         | Required | Description                                                                                      |
|--------------------------------|----------|--------------------------------------------------------------------------------------------------|
| `--config`                     | No       | Path to the config file that can specify `pg_dump` and `pg_restore` utilities. (default: none)   |
| `--db-connections`             | No       | Number of concurrent database connections. (default: 4)                                          |
| `--verbose`                    | No       | Sets the log verbosity level: `info`, `debug`, `error`. (default: info)                          |
| `--debug`                      | No       | Enables debug mode (equivalent to `--verbose=debug`) and adds extra debug logs. (default: false) |
| `--application-name-suffix`    | No       | Appends a suffix to the database connection name. Useful for automation. (default: none)         |
| `--internal-operation-id`      | No       | Pre-generated operation ID. If not set, a random UUID is generated.                              |
| `--version`                    | No       | Show the version number and exit.                                                                |


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
| `--schema-name`                      | No       | One schema name or a comma-separated list to keep in the dump. Other schemas are left out. See [Exclude schemas](#exclude-schemas). |
| `--schema-mask`                      | No       | One regular expression or a comma-separated list to select the schemas to keep in the dump. See [Exclude schemas](#exclude-schemas). |
| `--exclude-schema-name`              | No       | One schema name or a comma-separated list to leave out of the dump. See [Exclude schemas](#exclude-schemas). |
| `--exclude-schema-mask`              | No       | One regular expression or a comma-separated list to select the schemas to leave out of the dump. See [Exclude schemas](#exclude-schemas). |
| `--disable-checks`                   | No       | Disable the pre-flight check for available database connections. (default: false)                                                                       |
| `--dbg-stage-1-validate-dict`        | No       | Validate dictionary, show the tables and run SQL queries without data export. (default: false)                                                                                                                                                       |
| `--dbg-stage-2-validate-data`        | No       | Validate data, show the tables and run SQL queries with data export in prepared database. (default: false)                                                                                                                                           |
| `--dbg-stage-3-validate-full`        | No       | Makes all logic with "limit" in SQL queries. (default: false)                                                                                                                                                                                        |
| `--clear-output-dir`                 | No       | Clears the output directory from previous dumps or other files. (default: false)                                                                                                                                                                     |
| `--pg-dump`                          | No       | Path to the `pg_dump` Postgres tool (default `/usr/bin/pg_dump`).                                                                                                                                                                                    |
| `--pg-dump-options`                  | No       | Additional options passed to `pg_dump` as is, at your own risk. Example: `"--no-comments --encoding=LATIN1"`. See [Options for pg_dump](#options-for-pg_dump). |
| `--allow-fdw-credentials`            | No       | Allow FDW user-mapping credentials (`OPTIONS`) into the dump; blocked by default. (default: false)                                                                                                                                                   |
| `--output-dir`                       | No       | Output directory for dump files. (default: `./<sens-dict-file-name>`)                                                                                                                                                                                                        |
| `--ignore-privileges`                | No       | Ignore privileges from source db.                                                                                                                                                                                                                    |
| `--save-dicts`                       | No       | Duplicate all input dictionaries into the operation's run directory under `pg_anon_runs`. Useful for debugging or integration purposes.                                                                                                                                              |

### Options for pg_dump

`--pg-dump-options` are passed to `pg_dump` as is, at your own risk. They change only the structure of the
dump: `pg_anon` dumps the data itself. If you change the list of dumped objects with them, the structure and
the data may not match. To choose schemas and tables, use schema options and tables dictionaries.
The `sync-data-dump` mode does not run `pg_dump`, so the options have no effect there.

### Security defaults

In dump mode, pg_anon passes a few extra flags to `pg_dump` to leave out data that a
masked backup does not need:

- `--no-subscriptions` is always added (`pg_dump` ≥ 10). A subscription can store a
  password in its `CONNECTION` string.
- `--no-statistics` is added when the `pg_dump` binary is version 18 or newer. Statistics
  can hold real column values.

pg_anon also **stops** the dump when the current user can see FDW user-mapping
credentials (`OPTIONS`), because they would go into the backup. Use
`--allow-fdw-credentials` to dump anyway.

This lowers common leaks but does not make the backup fully clean. Some objects, such as
function or trigger bodies, are still dumped as they are. See the
[Security](../security.md) page for the full list.
