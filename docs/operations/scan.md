# 🔍 Scan
> [🏠 Home](../../README.md#-operations) | [💾 Dump](dump.md) | [🔬 View Fields](view-fields.md) | [📊 View Data](view-data.md) | [📚 SQL Functions Library](../sql-functions-library.md) 

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
pg_anon create-dict \
    --db-host=127.0.0.1 \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=test_source_db \
    --meta-dict-file=test_meta_dict.py \
    --prepared-sens-dict-file=test_sens_dict_output_previous_use.py \
    --prepared-no-sens-dict-file=test_no_sens_dict_output_previous_use.py \
    --output-sens-dict-file=test_sens_dict_output.py \
    --output-no-sens-dict-file=test_no_sens_dict_output.py
```

---

## Scan modes

**Partial mode** (`--scan-mode partial`, default) reads the first `--scan-partial-rows` rows where the field is not NULL.
It reads rows in the order of table storage. On unchanged data, each run reads the same rows.

**Full mode** (`--scan-mode full`) reads all values of each field. The field gets the function of the highest rule of
the meta-dictionary that matched any of its values, as in partial mode. The result does not depend on the order of
rows in the table and on the number of connections. See the
[order of rules](../dicts/meta-dict-schema.md#order-of-checks) of `data_func`.

For a partitioned table, partial mode reads the first rows of each partition.

New rows are often at the end of a big table. Partial mode can miss them.
Use full mode when you need a full check.

For an automated pipeline:
- Pass the previous sensitive dictionary as `--prepared-sens-dict-file`. Then fields found earlier are not lost.
- Run a full scan from time to time.

Tables and fields in the output dictionaries are sorted. Tables are sorted by schema and table name.
Fields are sorted by column position. Rules with masks from `--prepared-sens-dict-file` keep their order and go
after the rules for table names.

---

## Prepared dictionaries in the scan

- **Rules of `--prepared-sens-dict-file` for tables of the database go to the output dictionary as they are,** also
  the rules of tables that the scan does not read (for example, tables of `skip_rules`). Their fields are not read.
  The scan adds what it finds to the rule for the same table and does not change the fields that the rule already
  has.
- **Rules for tables that the database does not have are removed.** This is the default. Rules with masks that
  match no table are removed too. The scan also removes the fields that a table does not have. A rule with
  `sql_condition` stays, also if none of its fields are left. The scan does not write which rules it
  removed.

  To keep rules for tables that you will create later, use `--keep-unmatched-prepared-rules`. This is important if
  each scan takes the output of the previous scan as `--prepared-sens-dict-file`: without the option, such rules are
  lost after the first scan.
- **`dictionary_exclude` does not limit the scan.** The scan reads these tables like any other ones and puts
  `dictionary_exclude` into the output dictionary as it is. The scan logs it. To leave tables out of the scan, use
  [`skip_rules`](../dicts/meta-dict-schema.md#1-section-skip_rules).
  If the scan finds sensitive fields in such a table, the output has a rule for it. The rule wins over
  `dictionary_exclude`, so the next dump takes the data of this table (masked). A partition that `dictionary_exclude`
  names by its exact name stays without data: the scan writes its findings to the rule of the partitioned table.
- **`--prepared-no-sens-dict-file`** only makes the scan faster: the scan does not read the fields from it. A rule of
  the sensitive dictionary and a match by field name win over it. Its records do not go to the output as they are:
  the output non-sensitive dictionary has the fields of this scan that are not in the output sensitive dictionary.

If you pass the output dictionaries of the previous scan and the database structure did not change, the scan gives
the same dictionaries. New tables and new fields are read and go to one of them.

---

## Partitioned tables and tables with `INHERITS`

pg_anon works with partitions and with child tables of `INHERITS` in different ways. The masking side is described
in the [sensitive dictionary](../dicts/sens-dict-schema.md#partitioned-tables-and-tables-with-inherits).

### Partitioned tables

- **What is read.** Only partitions have rows, so the scan reads only partitions. A partitioned table and a
  partition with its own partitions are checked only by field names.
- **Meta-dictionary and non-sensitive dictionary.** `skip_rules`, `include_rules` and `data_sql_condition` of the
  meta-dictionary and records of the non-sensitive dictionary for a partitioned table work for all its partitions,
  at any depth and in any schema. See [skip_rules](../dicts/meta-dict-schema.md#1-section-skip_rules),
  [include_rules](../dicts/meta-dict-schema.md#2-section-include_rules),
  [data_sql_condition](../dicts/meta-dict-schema.md#5-section-data_sql_condition) and the
  [non-sensitive dictionary](../dicts/non-sens-dict-schema.md).
- **Key columns.** If the scan does not read a column of the partitioned table (a primary key or a column that owns
  a sequence), it does not read this column in the partitions either.
- **Where the findings go.** The scan writes all findings in the partitions to one rule for the top partitioned table,
  also if `skip_rules` or `include_rules` leave this table out of the scan, or if it is in another schema. It does
  not make rules for partitions. So a partition created later is masked too.
- **Different functions for one field.** If partitions give different functions for one field, the rule of the
  partitioned table gets only one of them. The scan writes a warning with all functions and partitions: check the
  rule and fix it by hand if needed. A match by field name wins over a match by data. In one section of the
  meta-dictionary, the rule higher in the list wins.
- **`n_count` of `data_func`.** A `scan_func` rule counts its matches over all read partitions together, and a value
  that is in two partitions counts once. So a rule can match the partitioned table even if it does not match any
  of its partitions alone. Partitions that `skip_rules` or `include_rules` leave out of the scan are not counted.
- **Non-sensitive dictionary.** The output has one record for the top partitioned table: its fields that have no
  sensitive data in any read partition and have no rule for this table or for any of its partitions. The output has
  no records for partitions.

**Dictionaries of pg_anon 1.12 and older.** They have a rule for each partition. You can use such a dictionary for
the dump as it is: each partition takes the fields of the partitioned table that are not in its own rule. If the old
dictionary has no rule for the partitioned table itself, a new partition has no rule, and the dump takes its data
without masking.

To move to one rule for the partitioned table, pass the old dictionary once as `--prepared-sens-dict-file`. In the
rules of partitions the scan keeps only the fields that are masked in another way than in the partitioned table, and
logs them. The dump result does not change. Rules with masks and rules with `sql_condition` stay. Check
that the output has a rule for each partitioned table.

### Tables with `INHERITS`

- Each table is scanned on its own. The scan reads a parent with `ONLY`: the rows of child tables are not in the
  findings of the parent.
- Rules of the meta-dictionary for a parent work only for the parent. To leave out a column of child tables, give
  `skip_rules` for the child tables, by name or by `table_mask`.
- **Key columns.** The scan does not read the columns of a primary key and a column that owns a sequence (`serial`,
  `OWNED BY`). PostgreSQL does not copy a primary key to child tables, and the sequence stays owned by the column of
  the parent: a child table copies only `DEFAULT nextval(...)`. So in a child table such a column is plain data, and
  the scan reads it. As a result, a child table can get a rule for `id`, and the parent cannot. If you mask such a
  column, add the same rule for the parent and for the foreign keys that point to it. Otherwise the values will not
  match.
- A child table created after the scan has no rule until the next scan.

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
| `--keep-unmatched-prepared-rules` | No    | Keep all rules of `--prepared-sens-dict-file` in the output sensitive dictionary as they are. By default the output keeps only the rules that match tables of the database, and only the fields that these tables have. Use it to keep rules for tables that will be created later. (default: false) |
| `--disable-checks`             | No       | Disable the pre-flight check for available database connections. (default: false)                                                                                 |
| `--save-dicts`                 | No       | Duplicate all input and output dictionaries into the operation's run directory under `pg_anon_runs`. Useful for debugging or integration purposes.                                                                                                                                                     |
