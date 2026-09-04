# 🔒 pg_anon

[![PyPI](https://img.shields.io/pypi/v/pg_anon)](https://pypi.org/project/pg_anon/)
[![Python](https://img.shields.io/pypi/pyversions/pg_anon)](https://pypi.org/project/pg_anon/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-9.6%20--%2018-336791)](https://www.postgresql.org/)
[![License](https://img.shields.io/github/license/TantorLabs/pg_anon)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/TantorLabs/pg_anon/ci.yml?branch=master&label=tests)](../../actions)

**Data masking for PostgreSQL.**  
Share realistic data, not the real values.

---

## ✨ Overview

`pg_anon` clones a PostgreSQL database and replaces the sensitive fields on the way.
The copy keeps the structure, the row counts and the relations, so it can be used
where the original must not go — test and staging environments, analytics, support
and training, demos, work with contractors.

This is masking (pseudonymization): it lowers the exposure of personal data, but it
is not irreversible anonymization — see [Security & limitations](#-security--limitations).

---

## ⚙️ Requirements

- **Python:** 3.11+
- **PostgreSQL:** 9.6+
- **PostgreSQL client utilities (must match the server’s major version):**
  - `pg_dump` – used to export the database schema
  - `pg_restore` – used to restore that schema into the target database

The data itself is read and written by pg_anon, not by these utilities.

The target server must be of the same major version as the source or newer: a dump
taken from PostgreSQL 15 restores into 17, but not the other way round.

For details, see: [Installation and configuring](docs/installation-and-configuring.md#configuring-pg_anon)

---

## 📦 Installation

```bash
pip install pg_anon              # CLI only
pip install "pg_anon[api]"       # CLI + REST API service
```

The same works from a clone of the repository: `pip install .` and
`pip install ".[api]"`.

The package installs two commands: `pg_anon`, the CLI used throughout this
readme, and `pg_anon_api`, the REST API service:

```bash
pg_anon_api --host 0.0.0.0 --port 8000 --workers=3
```

The service publishes its OpenAPI documentation at `/docs`. See
[Installation & Configuration](docs/installation-and-configuring.md) for
per-platform notes and the data directory, and [API](docs/api.md) for the
endpoints.

---

## 🧩 Terminology

| Term                                  | Description                                                                                                                                                                                             |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Personal (sensitive) data**         | Data that must not be shared with third parties. Includes personal or confidential business information.                                                                                                |
| **Source database**                   | The original database that contains sensitive data.                                                                                                                                                     |
| **Target database**                   | An empty database where the masked data will be restored.                                                                                                                                               |
| **Meta-dictionary**                   | A Python file describing rules for detecting sensitive data. Created manually and used as the basis for generating the sensitive dictionary during scanning. [See more](docs/dicts/meta-dict-schema.md) |
| **Prepared sensitive dictionary**     | A Python file that defines which tables and fields contain sensitive data and how to mask them. Created automatically or manually. [See more](docs/dicts/sens-dict-schema.md)                      |
| **Prepared non-sensitive dictionary** | A Python file listing schemas, tables, and fields without sensitive data. Used to speed up repeated scans. [See more](docs/dicts/non-sens-dict-schema.md)                                               |
| **Table dictionary**                  | A Python file listing tables. Used to include or exclude tables from dump & restore operations. [See more](docs/dicts/tables-dictionary.md)                                                                |
| **Create-dict (scan)**                | The process of scanning the source database to detect sensitive fields and create dictionary files. [See more](docs/operations/scan.md)                                                                 |
| **Dump**                              | Exporting data from the source database into files using a dictionary. This is where masking happens. [See more](docs/operations/dump.md)                                                          |
| **Restore**                           | Importing masked data from files into the target database. [See more](docs/operations/restore.md)                                                                                                   |
| **Masking (pseudonymization)**        | Full process of cloning and sanitizing data (`dump → restore`), replacing sensitive values with random or hashed ones. It reduces exposure, but is not irreversible anonymization in the GDPR sense — the result stays personal data unless you make it otherwise. |
| **Masking function**                  | A PostgreSQL function (built-in or from the `anon_funcs` schema) that replaces a sensitive value with a random or hashed one. New functions can be added to extend the masking logic.                       |

## 🚀 Quick Start

Find the personal data in a demo database, make a masked dump and restore it
into a second database. The demo database, the dictionary and a container to
run them in are all in [`demo/`](demo).

### 1. Get a PostgreSQL server

You need a PostgreSQL 9.6+ server where you may create databases, `pg_dump`
and `pg_restore` of its major version, and pg_anon itself. The steps below also
use `psql` to prepare the demo and to compare the results — pg_anon itself does
not need it. Clone the repository in any case — the demo files live in it:

```bash
git clone https://github.com/TantorLabs/pg_anon.git
cd pg_anon
pip install .
```

If you have no server at hand, `demo/` starts a container that already has the
server, the client utilities and pg_anon inside. This needs Docker with the
Compose plugin, nothing else:

```bash
docker compose -f demo/docker-compose.yml up -d --build --wait
docker compose -f demo/docker-compose.yml exec pg_anon_demo bash
```

Everything below runs the same way in both cases, from the directory that holds
`demo/` — in the container that is `/pg_anon`, where the demo files are mounted,
and the shell starts there. Set the connection once; on your own server, put
your values here:

```bash
export PGHOST=localhost PGPORT=5432 PGUSER=demo
export DEMO_PASSWORD=demo        # the role's password; `demo` in the container
```

### 2. Prepare the databases and the password file

Two databases: the source with the demo data, and an empty target for the
masked copy. The password goes into a passfile in the current directory, so it
stays out of the shell history and out of `ps` — `psql` and pg_anon both read
it from there.

```bash
printf "$PGHOST:$PGPORT:*:$PGUSER:$DEMO_PASSWORD\n" > pgpass.conf && chmod 600 pgpass.conf
export PGPASSFILE=$PWD/pgpass.conf

psql -d postgres -c "CREATE DATABASE demo_source" -c "CREATE DATABASE demo_target"
psql -d demo_source -f demo/data.sql
```

The source database now holds two schemas — `shop` and `hr`, five tables,
~255 rows. The `chmod` matters: a passfile with wider permissions is ignored.
`psql` picks the file up from `PGPASSFILE`, pg_anon is told about it with
`--db-passfile` in every command below.

Creating the two databases needs the `CREATEDB` privilege; in the container the
`demo` role is a superuser and has it.

### 3. Install the masking functions

`init` creates the `anon_funcs` schema with the SQL functions that replace
values later on. Scan and dump both need it.

> ⚠️ This writes to the **source** database: `init` creates a schema there, so
> the user needs the right to create one. On your own server, remove it when you
> are done — `DROP SCHEMA anon_funcs CASCADE` — the container needs no cleanup.

```bash
pg_anon init \
    --db-host=$PGHOST --db-port=$PGPORT --db-user=$PGUSER \
    --db-passfile=$PWD/pgpass.conf \
    --db-name=demo_source
```

```
Finished pg_anon in mode: init, result_code = done
```

The dump carries everything the source database has, `anon_funcs` included, so
the schema shows up in the target database as well.

### 4. Scan the database for sensitive data

`create-dict` reads [`demo/meta_dict.py`](demo/meta_dict.py) — the rules
describing what counts as sensitive — and writes out the fields it found, with
a masking rule for each. Both output files go to `/tmp/demo/`, a
scratch directory for everything this guide produces.

```bash
mkdir -p /tmp/demo

pg_anon create-dict \
    --db-host=$PGHOST --db-port=$PGPORT --db-user=$PGUSER \
    --db-passfile=$PWD/pgpass.conf \
    --db-name=demo_source \
    --meta-dict-file=demo/meta_dict.py \
    --output-sens-dict-file=/tmp/demo/sens_dict.py \
    --output-no-sens-dict-file=/tmp/demo/no_sens_dict.py
```

The command prints its progress; the result is the file
`/tmp/demo/sens_dict.py`, shortened here to two tables out of four:

```python
{
    "dictionary": [
        {
            "schema": "shop",
            "table": "customer_order",
            "fields": {
                "note": "regexp_replace(\"note\", '[^ ]+@[^ ]+', 'customer@example.com', 'g')"
            }
        },
        {
            "schema": "hr",
            "table": "employee",
            "fields": {
                "full_name": "anon_funcs.random_in(array['Nora Fisher', 'Paul Adler', 'Rita Lang', 'Simon Falk', 'Vera Roth'])",
                "email": "lower(anon_funcs.random_string(8)) || '@example.com'",
                "phone": "anon_funcs.random_phone('+1')",
                "ssn": "anon_funcs.partial(\"ssn\", 0, 'XXX-XX-', 4)",
                "salary": "round(anon_funcs.noise(\"salary\", 0.2), 2)"
            }
        },
        ...
    ]
}
```

`shop.customer_order.note` is in the list although its name gives nothing
away — the `data_regex` rules of the meta-dictionary look at the values, not at
the names. `shop.product` is absent: the catalogue is excluded by
`skip_rules`. The second file, `no_sens_dict.py`, lists the fields that were
checked and found harmless; the rest of this guide does not use it.

Scanning is optional. The file above is an ordinary dictionary: keep it in your
repository, edit it by hand, or write one from scratch — see
[sensitive dictionary](docs/dicts/sens-dict-schema.md).

### 5. Check what will happen

`view-fields` shows the rule chosen for every field, `view-data` applies those
rules to real rows. Neither writes anything.

```bash
pg_anon view-fields \
    --db-host=$PGHOST --db-port=$PGPORT --db-user=$PGUSER \
    --db-passfile=$PWD/pgpass.conf \
    --db-name=demo_source \
    --prepared-sens-dict-file=/tmp/demo/sens_dict.py \
    --schema-name=hr --table-name=employee
```

```
┌────────┬──────────┬────────────┬────────────────────────┬────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ schema │ table    │ field      │ type                   │ dict_file_name         │ rule                                                                                             │
├────────┼──────────┼────────────┼────────────────────────┼────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
│ hr     │ employee │ full_name  │ character varying(120) │ /tmp/demo/sens_dict.py │ anon_funcs.random_in(array['Nora Fisher', 'Paul Adler', 'Rita Lang', 'Simon Falk', 'Vera Roth']) │
│ hr     │ employee │ email      │ character varying(100) │ /tmp/demo/sens_dict.py │ lower(anon_funcs.random_string(8)) || '@example.com'                                             │
│ hr     │ employee │ phone      │ character varying(20)  │ /tmp/demo/sens_dict.py │ anon_funcs.random_phone('+1')                                                                    │
│ hr     │ employee │ ssn        │ character varying(11)  │ /tmp/demo/sens_dict.py │ anon_funcs.partial("ssn", 0, 'XXX-XX-', 4)                                                       │
│ hr     │ employee │ department │ character varying(60)  │ ---                    │ ---                                                                                              │
│ hr     │ employee │ salary     │ numeric(10,2)          │ /tmp/demo/sens_dict.py │ round(anon_funcs.noise("salary", 0.2), 2)                                                        │
│ hr     │ employee │ hired_on   │ date                   │ ---                    │ ---                                                                                              │
└────────┴──────────┴────────────┴────────────────────────┴────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Fields with `---` are dumped as they are. Every rule keeps the shape of the
original value: a name stays a name, an SSN keeps its last four digits, a
salary stays a two-decimal number.

```bash
pg_anon view-data \
    --db-host=$PGHOST --db-port=$PGPORT --db-user=$PGUSER \
    --db-passfile=$PWD/pgpass.conf \
    --db-name=demo_source \
    --prepared-sens-dict-file=/tmp/demo/sens_dict.py \
    --schema-name=hr --table-name=employee --limit=3
```

```
┌────┬─────────────┬──────────────────────┬─────────────┬─────────────┬─────────────┬──────────┬────────────┐
│ id │ * full_name │       * email        │   * phone   │    * ssn    │  department │ * salary │  hired_on  │
├────┼─────────────┼──────────────────────┼─────────────┼─────────────┼─────────────┼──────────┼────────────┤
│ 1  │  Simon Falk │ hmi19nto@example.com │ +1959073807 │ XXX-XX-1001 │   Support   │ 40943.27 │ 2018-02-23 │
│ 2  │  Paul Adler │ 48uvc6zv@example.com │ +1463593776 │ XXX-XX-1002 │  Warehouse  │ 44023.59 │ 2018-04-17 │
│ 3  │  Simon Falk │ lyxoalon@example.com │ +1945863438 │ XXX-XX-1003 │ Engineering │ 48401.45 │ 2018-06-09 │
└────┴─────────────┴──────────────────────┴─────────────┴─────────────┴─────────────┴──────────┴────────────┘
```

A `*` before the column name means the column is masked. Most rules pick
random values, so your output will not match this one literally.

The same table that made the scan interesting is worth a look too — in
`shop.customer_order` the personal data sits inside free text:

```bash
pg_anon view-data \
    --db-host=$PGHOST --db-port=$PGPORT --db-user=$PGUSER \
    --db-passfile=$PWD/pgpass.conf \
    --db-name=demo_source \
    --prepared-sens-dict-file=/tmp/demo/sens_dict.py \
    --schema-name=shop --table-name=customer_order --limit=4
```

```
┌────┬─────────────┬────────────┬────────────┬──────────┬──────────────┬────────┬────────────────────────────────────────────┐
│ id │ customer_id │ product_id │ ordered_at │ quantity │ total_amount │ status │                   * note                   │
├────┼─────────────┼────────────┼────────────┼──────────┼──────────────┼────────┼────────────────────────────────────────────┤
│ 9  │      10     │     10     │ 2025-01-10 │    2     │    38.00     │  paid  │ delivery confirmed by customer@example.com │
│ 69 │      10     │     10     │ 2025-03-11 │    2     │    158.00    │  paid  │ delivery confirmed by customer@example.com │
│ 12 │      13     │     13     │ 2025-01-13 │    1     │    22.00     │  new   │ delivery confirmed by customer@example.com │
│ 72 │      13     │     13     │ 2025-03-14 │    1     │    82.00     │  new   │ delivery confirmed by customer@example.com │
└────┴─────────────┴────────────┴────────────┴──────────┴──────────────┴────────┴────────────────────────────────────────────┘
```

Only `note` carries a `*`: the order itself is business data and stays as it is.
The rule rewrites the address inside the sentence and leaves everything around it
readable — and the notes that hold no address, two thirds of the table, pass
through untouched. Which rows you get here is up to PostgreSQL: the ids and the
order may differ from the output above.

### 6. Make a masked dump

Values are replaced by the source database itself while the data is read, so
the plain ones never leave it.

```bash
pg_anon dump \
    --db-host=$PGHOST --db-port=$PGPORT --db-user=$PGUSER \
    --db-passfile=$PWD/pgpass.conf \
    --db-name=demo_source \
    --prepared-sens-dict-file=/tmp/demo/sens_dict.py \
    --output-dir=/tmp/demo/dump
```

```
Finished pg_anon in mode: dump, result_code = done
```

### 7. Restore it into the target database

The target database has to be empty — `demo_target` was created that way in
step 2.

```bash
pg_anon restore \
    --db-host=$PGHOST --db-port=$PGPORT --db-user=$PGUSER \
    --db-passfile=$PWD/pgpass.conf \
    --db-name=demo_target \
    --input-dir=/tmp/demo/dump
```

```
Finished pg_anon in mode: restore, result_code = done
```

### 8. Compare the two databases

```bash
psql -d demo_source -c "SELECT id, full_name, email, phone, ssn, salary FROM hr.employee ORDER BY id LIMIT 3"
psql -d demo_target -c "SELECT id, full_name, email, phone, ssn, salary FROM hr.employee ORDER BY id LIMIT 3"
```

```
 id |  full_name   |         email         |    phone    |     ssn     |  salary
----+--------------+-----------------------+-------------+-------------+----------
  1 | Hugo Costa   | employee1@example.com | +1202566001 | 101-11-1001 | 45750.00
  2 | Irina Dvorak | employee2@example.com | +1202566002 | 102-12-1002 | 46500.00
  3 | Jonas Egger  | employee3@example.com | +1202566003 | 103-13-1003 | 47250.00

 id |  full_name  |        email         |    phone    |     ssn     |  salary
----+-------------+----------------------+-------------+-------------+----------
  1 | Nora Fisher | i6ev14rk@example.com | +1566461072 | XXX-XX-1001 | 48905.61
  2 | Vera Roth   | sz4li87c@example.com | +1143189653 | XXX-XX-1002 | 49078.04
  3 | Vera Roth   | 6kvn8sbf@example.com | +1485805625 | XXX-XX-1003 | 38450.22
```

Same tables, same row counts, and every field the dictionary lists is masked.
What the dictionary does not list is kept as it is, on purpose: the city, the
department, the identifiers and the hire dates are unchanged, the salary is the
real one shifted by up to 20%, the card number keeps its last four digits. The
product catalogue is identical in both databases, because nothing in it was
marked sensitive.

The masks are random and independent of each other: the same person gets one
name in `shop.customer` and another in `shop.payment_card`. Keep that in mind —
and treat [`demo/meta_dict.py`](demo/meta_dict.py) as an example that shows the
mechanics, not as a dictionary ready for your own database.

Everything left untouched here is a quasi-identifier: city, department, hire
date, the row identifiers, a salary within 20% of the real one, the last four
digits of an SSN. Each is harmless alone, and together they can still point back
at a person — deciding what to keep is the part no tool does for you. See
[Security & limitations](#-security--limitations).

### What's next

- Dump or restore a subset of the tables — [tables dictionary](docs/dicts/tables-dictionary.md)
- Drive the same operations over HTTP — [REST API](docs/api.md)
- Write richer scan rules, including your own SQL scan functions — [meta-dictionary](docs/dicts/meta-dict-schema.md)
- See what the built-in masking functions can do — [SQL functions library](docs/sql-functions-library.md)

Done with the demo? Leave the container shell with `exit`, then:

```bash
docker compose -f demo/docker-compose.yml down -v     # container and databases
docker image rm pg_anon_demo                          # the image itself
```

The container publishes the server on the host as port `55432` (user `demo`,
password `demo`), in case you would rather look at the two databases with your
own client.

---

## 🔐 Security & limitations

- **Masking is not irreversible anonymization.** pg_anon replaces values; it does
  not prove that the result cannot be linked back. Treat masked copies as personal
  data unless your own analysis says otherwise.
- **What you keep can identify people.** Dates, cities, departments, identifiers
  and format-preserving masks are quasi-identifiers, and combining them can be
  enough to re-identify someone. Which fields to mask, and how far, is your call.
- **Deterministic rules need a secret salt.** A hash such as
  `anon_funcs.digest("%s", 'salt', 'sha256')` gives the same output for the same
  input — convenient for keeping relations, but reversible by brute force for
  low-entropy values (phone numbers, e-mails, identifiers). Keep the salt out of
  the dictionary you commit.
- **A dictionary ages with the schema.** It is a snapshot of what the scan saw. New
  tables, new columns or new partitions of a partitioned table are not in it, and a
  dump made with the old dictionary carries them unmasked. Re-run `create-dict` and
  review the result whenever the source schema changes.
- **The source database is written to.** `init` creates the `anon_funcs` schema
  there, and the masking functions run inside the source while the dump is read.
- **Dumps are ordinary files.** Nothing in a dump is encrypted; a masked dump still
  deserves the storage and access rules of a database backup.

A dump can also hold secrets that masking does not change — FDW credentials, function
bodies, planner statistics. pg_anon handles some of these by default and warns about the
rest. See [Security](docs/security.md) for details.

See [How it works](docs/how-it-works.md) and the
[dictionary schemas](docs/dicts/meta-dict-schema.md) for what each rule does.

---

## 📘 Documentation Index
| Section                                                                 | Description                                                       |
|-------------------------------------------------------------------------|-------------------------------------------------------------------|
| [💽 Installation & Configuration](docs/installation-and-configuring.md) | How to install and configure `pg_anon`                            |
| [⚙️ How It Works](docs/how-it-works.md)                                 | How the masking process works in `pg_anon`                        |
| [🛠️ Debugging](docs/debugging.md)                                      | How to debug the masking process                                  |
| [🛡️ Security](docs/security.md)                                         | What can leak besides data, and how pg_anon protects against it   |
| [💬 FAQ](docs/faq.md)                                                   | Common questions and troubleshooting tips                         |
| [📚 SQL Functions Library](docs/sql-functions-library.md)               | Built-in SQL functions for masking                                |
| [🔌 API](docs/api.md)                                                   | Available endpoints, request/response formats, and usage examples |
| [💡 Contributing](docs/contributing.md)                                 | Info about contributing                                           |

### 📘 Operations
| Operation                                         | Description                                                                            |
|---------------------------------------------------|----------------------------------------------------------------------------------------|
| [🏗️ Init](docs/operations/init.md)               | Creates the `anon_funcs` schema with the SQL functions used by scan and dump |
| [🔍 Create-dict (Scan)](docs/operations/scan.md)  | Analyze your database and detect sensitive data                                        |
| [💾 Dump](docs/operations/dump.md)                | Export and mask data using prepared dictionaries                                       |
| [📂 Restore](docs/operations/restore.md)          | Load masked data into a target database                                                |
| [🔬 View Fields](docs/operations/view-fields.md) | Inspect the fields and the rules chosen for them                                       |
| [📊 View Data](docs/operations/view-data.md)     | Inspect masked rows before dumping them                                                |

### 📘 Dictionary Schemas
| Dictionary type                                                   | Description                                                        |
|-------------------------------------------------------------------|--------------------------------------------------------------------|
| [🗂️ Meta Dictionary](docs/dicts/meta-dict-schema.md)             | Structure of the meta-dictionary used for scanning                  |
| [🔐 Sensitive Dictionary](docs/dicts/sens-dict-schema.md)         | Structure of sensitive dictionaries                       |
| [📋 Non-sensitive Dictionary](docs/dicts/non-sens-dict-schema.md) | Structure of non-sensitive dictionaries                   |
| [📑 Tables dictionary](docs/dicts/tables-dictionary.md)           | Dictionary structure for partial dump/restore operations           |
