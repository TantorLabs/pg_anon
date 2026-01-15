# 🔍 Scan
> [🏠 Home](../../README.md#-operations) | [💾 Dump](dump.md) | [📂 Restore](restore.md) | [🔬 View Fields](view-fields.md) | [📊 View Data](view-data.md) | [📚 SQL Functions Library](../sql-functions-library.md) 

---

## Overview
The **scan** operation analyzes your PostgreSQL database to detect potentially sensitive data and generate dictionaries files.
It used for dump and repeat scan.

---

## Prerequisites:
- Manually created [meta-dictionary](../dicts/meta-dict-schema.md)
- Already run `init` mode for source database

## Usage:
To scan source database and create dictionary for dump, run pg_anon in `create-dict` mode.
You need:
- **meta-dictionary** file with scan rules.

```commandline
python pg_anon.py create-dict \
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


### Create-dict (scan) mode options

| Option                         | Required | Description                                                                                                                                                                                                                                                            |
|--------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--meta-dict-file`             | Yes      | Input file or file list contains [meta-dictionary](../dicts/meta-dict-schema.md), which was prepared manually. In rules collision case, priority has rules in last file from the list.                                                                                 |
| `--prepared-sens-dict-file`    | No       | Input file or file list contains [sensitive dictionary](../dicts/sens-dict-schema.md), which was obtained in previous use by option `--output-sens-dict-file` or prepared manually. In rules collision case, priority has rules in last file from the list.            |
| `--prepared-no-sens-dict-file` | No       | Input file or file list contains [not sensitive dictionary](../dicts/non-sens-dict-schema.md), which was obtained in previous use by option `--output-no-sens-dict-file` or prepared manually. In rules collision case, priority has rules in last file from the list. |
| `--output-sens-dict-file`      | Yes      | Output file path for saving sensitive dictionary.                                                                                                                                                                                                                      |
| `--output-no-sens-dict-file`   | No       | Output file path for saving not sensitive dictionary.                                                                                                                                                                                                                  |
| `--scan-mode`                  | No       | Defines whether to scan all data or only part of it ["full", "partial"] (default "partial").                                                                                                                                                                           |
| `--scan-partial-rows`          | No       | In `--scan-mode partial` defines amount of rows to scan (default 10000). Actual rows count can be smaller after getting unique values.                                                                                                                                 |
| `--save-dicts`                 | No       | Duplicate all input and output dictionaries to dir `runs`. It can be useful for debugging or integration purposes.                                                                                                                                                     |
