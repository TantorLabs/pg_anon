# 🏗️ Init

> [🏠 Home](../../README.md#-operations) | [🔍 Scan](scan.md) | [💾 Dump](dump.md) | [📂 Restore](restore.md) | [🔬 View Fields](view-fields.md) | [📊 View Data](view-data.md) | [📚 SQL Functions Library](../sql-functions-library.md)

## Overview

This mode creates the `anon_funcs` schema in the source database and loads the predefined SQL functions from [init.sql](../../init.sql).
These functions are required for processing data in the source database.

## Run example

```commandline
python -m pg_anon --mode=init \
                  --db-user=postgres \
                  --db-user-password=postgres \
                  --db-name=source_db
```

---

## Options

### Common pg_anon options:

| Option                         | Description                                                                                                    |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------- |
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
