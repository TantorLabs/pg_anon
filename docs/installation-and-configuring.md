# 💽 Installation & Configuration
> [🏠 Home](../README.md#-documentation-index) | [⚙️ How it works](how-it-works.md) | [💬 FAQ](faq.md) 

## Before you install

**Requires Python 3.11+**

pg_anon provides 2 ways to run: **CLI** and **REST API**

The REST API service is optional to install. This service is designed to integrate `pg_anon` functionality into any system or pipelines via HTTP requests.
It works just as a thin wrapper around the CLI version of `pg_anon`. REST API calls prepare CLI parameters and run the CLI version of pg_anon in the background.

It doesn't keep state or store data in a database, so it can be scaled easily without extra setup.

However, this means that the system that integrates pg_anon must implement its own storage for dictionaries, dump tasks, and restore tasks.

> ⚠️ **Note**
>
> Not suitable for fully autonomous operation.
>
> All operation runs logs and info will be stored in the directory `pg_anon_runs/` (relative to the working directory or `PG_ANON_HOME`).
> All dumps will be stored in the directory `pg_anon_output/` (relative to the working directory or `PG_ANON_HOME`).
> If the REST API service is scaled, you must create a symlink to these directories on a shared disk.
> This is required because restore operations also read dumps from `pg_anon_output/`.

---

## Installing pg_anon

Requires Python 3.11+.

```bash
pip install pg_anon              # CLI only
pip install "pg_anon[api]"       # CLI + REST API service
```

This installs two commands: `pg_anon` (CLI) and, with the `api` extra,
`pg_anon_api` (REST API service).

Where the system Python is externally managed, install it in an environment of
its own: `pipx install "pg_anon[api]"` or `uv tool install "pg_anon[api]"`.

### Installing from source

Needed to get an unreleased version, or to work on pg_anon itself:

```bash
git clone https://github.com/TantorLabs/pg_anon.git
cd pg_anon
pip install .                    # CLI only
pip install ".[api]"             # CLI + REST API service
```

Or build a wheel first and install that:

```bash
pip install build
python -m build
pip install dist/pg_anon-*.whl           # CLI only
pip install "dist/pg_anon-*.whl[api]"    # CLI + REST API service
```

---

## Configuring pg_anon

By default pg_anon calls `/usr/bin/pg_dump` and `/usr/bin/pg_restore`. To use other binaries, pass the `--pg-dump` and `--pg-restore` parameters.

Advanced configuration is also available:
- CLI - use run parameter `--config`
- REST API - config must be placed at `config.yml` in the working directory (or `$PG_ANON_HOME/config.yml`)

This parameter accepts a YAML file in this format:
```yaml
pg-utils-versions:
  <postgres_major_version>:
    pg_dump: "/path/to/<postgres_major_version>/pg_dump"
    pg_restore: "/path/to/<postgres_major_version>/pg_restore"
  <another_postgres_major_version>:
    pg_dump: "/path/to/<postgres_major_version>/pg_dump"
    pg_restore: "/path/to/<postgres_major_version>/pg_restore"
  default:
    pg_dump: "/path/to/default_postgres_version/pg_dump"
    pg_restore: "/path/to/default_postgres_version/pg_restore"
```

For example, you can specify a configuration for postgres 15 and 17:

```yaml
pg-utils-versions:
  15:
    pg_dump: "/usr/lib/postgresql/15/bin/pg_dump"
    pg_restore: "/usr/lib/postgresql/15/bin/pg_restore"
  17:
    pg_dump: "/usr/lib/postgresql/17/bin/pg_dump"
    pg_restore: "/usr/lib/postgresql/17/bin/pg_restore"
  default:
    pg_dump: "/usr/lib/postgresql/17/bin/pg_dump"
    pg_restore: "/usr/lib/postgresql/17/bin/pg_restore"
```

If the current PostgreSQL version does not match any version in this config, the utilities from the default section will be used.
For example, `pg_anon` can be run with this config on Postgres 16. In this case, `pg_dump 17` and `pg_restore 17` will be used.

---

## Working directory & PG_ANON_HOME

pg_anon stores runtime data (operation logs, dumps) relative to the current working directory:

| Directory                                           | Contents                                                 |
|-----------------------------------------------------|----------------------------------------------------------|
| `pg_anon_runs/<year>/<month>/<day>/<operation_id>/` | Operation logs and metadata                              |
| `pg_anon_output/`                                   | Dump files (used by REST API)                            |
| `config.yml`                                        | Optional configuration file for pg_dump/pg_restore paths |

By default, these paths are resolved from the current working directory.

To override the base directory, set the `PG_ANON_HOME` environment variable:

```bash
export PG_ANON_HOME=/opt/pg_anon/data
pg_anon init --db-host=... --db-port=...
```

This is useful for systemd services, Docker containers, or any deployment where the working directory differs from the data directory.

When `PG_ANON_HOME` is set:
- `pg_anon_runs/` → `$PG_ANON_HOME/pg_anon_runs/`
- `pg_anon_output/` → `$PG_ANON_HOME/pg_anon_output/`
- `config.yml` → `$PG_ANON_HOME/config.yml`

---

## Running REST API

Run service command:
```sh
pg_anon_api --host 0.0.0.0 --port 8000 --workers=3
```

To specify a custom data directory:
```sh
PG_ANON_HOME=/opt/pg_anon/data pg_anon_api --host 0.0.0.0 --port 8000 --workers=3
```

- Recommended worker count = `2 * CPU_CORES + 1`
- Service OpenAPI documentation is available at http://0.0.0.0:8000/docs
- Also, you can see [API documentation](api.md) 
