# 🏗️ Init

> [🏠 Home](../../README.md#-operations) | [🔍 Scan](scan.md) | [💾 Dump](dump.md) | [📂 Restore](restore.md) | [🔬 View Fields](view-fields.md) | [📊 View Data](view-data.md) | [📚 SQL Functions Library](../sql-functions-library.md)

## Overview

This mode creates the `anon_funcs` schema in the source database and loads the predefined SQL functions from [init.sql](../../pg_anon/init.sql).
These functions are required for processing data in the source database.

## Run example

```commandline
pg_anon init \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=source_db
```

---

## Options

### Common pg_anon options:

| Option      | Required | Description                                                                                      |
|-------------|----------|--------------------------------------------------------------------------------------------------|
| `--verbose` | No       | Sets the log verbosity level: `info`, `debug`, `error`. (default: info)                          |
| `--debug`   | No       | Enables debug mode (equivalent to `--verbose=debug`) and adds extra debug logs. (default: false) |

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
