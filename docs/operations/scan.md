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


### Create-dict (scan) mode options

| Option                         | Description                                                                                                                                       |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `--meta-dict-file`             | Input file or file list with scan rules of sensitive and not sensitive fields. In collision case, priority has first file in list                 |
| `--prepared-sens-dict-file`    | Input file or file list with sensitive fields, which was obtained in previous use by option `--output-sens-dict-file` or prepared manually        |
| `--prepared-no-sens-dict-file` | Input file or file list with not sensitive fields, which was obtained in previous use by option `--output-no-sens-dict-file` or prepared manually |
| `--output-sens-dict-file`      | Output file with sensitive fields will be saved to this value                                                                                     |
| `--output-no-sens-dict-file`   | Output file with not sensitive fields will be saved to this value                                                                                 |
| `--scan-mode`                  | defines whether to scan all data or only part of it ["full", "partial"] (default "partial")                                                       |
| `--scan-partial-rows`          | In `--scan-mode partial` defines amount of rows to scan (default 10000). Actual rows count can be smaller after getting unique values             |
| `--save-dicts`                 | Duplicate all input and output dictionaries to dir `runs`. It can be useful for debugging or integration purposes.                                |
