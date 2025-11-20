# 💽 Installation & Configuration
> [🏠 Home](../README.md#-documentation-index) | [⚙️ How it works](how-it-works.md) | [💬 FAQ](faq.md) 

## Before you install
pg_anon provides 2 ways to run: **CLI** and **REST API**

The REST API service is optional to install. This service is designed to integrate `pg_anon` functionality into any system or pipelines via HTTP requests.
It works just as a thin wrapper around the CLI version of `pg_anon`. REST API calls prepare CLI parameters and run the CLI version of pg_anon in the background.

It doesn’t keep state or store data in a database, so it can be scaled easily without extra setup.

However, this means that the system that integrates pg_anon must implement its own storage for dictionaries, dump tasks, and restore tasks.

> ⚠️ **Note**
> 
> Not suitable for fully autonomous operation.
> 
> All operation runs logs and info will be stored in the directory `/path_to_pg_anon/runs`.
> All dumps will be stored in the directory `/path_to_pg_anon/output`.
> If the REST API service is scaled, you must create a symlink to this directory on a shared disk. 
> This is required because restore operations also read dumps from `/path_to_pg_anon/output`.

---

## Linux

1. Install Python 3 if it is not installed: `sudo apt-get install python3.11` (for Ubuntu), `sudo yum install python311` (for Redhat/Centos)
2. Clone the repository: `git clone https://github.com/TantorLabs/pg_anon.git`
3. Go to the project directory: `cd pg_anon`
4. Set up a virtual environment:
    - Install the virtual environment: `python3 -m venv venv`
    - Activate the virtual environment: `source venv/bin/activate`
5. Install the dependencies: `pip install -r requirements.txt`
6. Optional, if you want to use the REST API service, install its dependencies: `pip install -r rest_api/requirements.txt`

## Windows

1. Install Python 3 if it is not installed: Download it from the official [Python website](https://www.python.org/downloads/)
2. Clone the repository: `git clone https://github.com/TantorLabs/pg_anon.git`
3. Go to the project directory: `cd pg_anon`
4. Set up a virtual environment:
    - Install the virtual environment: `py -m venv venv`
    - Activate the virtual environment: `.\venv\Scripts\activate`
5. Install the dependencies: `pip install -r requirements.txt`
6. Optional, if you want to use the REST API service, install its dependencies: `pip install -r rest_api/requirements.txt`

## macOS

1. Install Python 3 if it is not installed:
    - Install [Homebrew](https://brew.sh/)
    - [`brew install python@3.11`](https://formulae.brew.sh/formula/python@3.11)
2. Clone the repository: `git clone https://github.com/TantorLabs/pg_anon.git`
3. Go to the project directory: `cd pg_anon`
4. Set up a virtual environment:
    - Install the virtual environment: `python3 -m venv venv`
    - Activate the virtual environment: `source venv/bin/activate`
5. Install the dependencies: `pip install -r requirements.txt`
6. Optional, if you want to use the REST API service, install its dependencies: `pip install -r rest_api/requirements.txt`

---

## Configuring pg_anon

To specify custom `pg_dump` and `pg_restore` utilities, use the `--pg-dump` and `--pg-restore` parameters.

Advanced configuration is also available:
- CLI - use run parameter `--config`
- REST API - config must be placed at `/path_to_pg_anon/config.yml`

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

## Running REST API
1. Run service - `python -m uvicorn rest_api.api:app --host 0.0.0.0 --port 8000 --workers=3`
   - Recommended worker count = `2 * CPU_CORES + 1`
2. OpenAPI documentation - http://0.0.0.0:8000/docs#/
