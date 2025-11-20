# FAQ
> [🏠 Home](../README.md#-documentation-index) | [⚙️ How it works](how-it-works.md)

### 1. Where can I find operation logs and launch parameters?
All run data is stored in the `/path/to/pg_anon/runs` directory.  
Inside, the structure is: `<year>/<month>/<day>/<operation_id>`.

Each operation folder contains:
- a `logs` directory with all log files  
- a `run_options.json` file with all parameters used to run `pg_anon`

If the `--save-dicts` option was used, the folders `input` and `output` will also appear.  
They contain all input and output dictionaries for that run.

---

### 2. Can I restore a pg_anon dump using pg_dump?

**No.** The pg_anon dump format is not compatible with pg_dump due to the specifics of anonymization.

For the same reason, a regular backup created with pg_dump cannot be restored using pg_anon.

---

### 3. Does pg_anon modify the structure or data of the source database during scan, dump, view-data, or view-fields?

pg_anon does **not** modify either the structure or the data of the source database.

The only thing pg_anon adds is the `anon_funcs` schema, which is required for its internal operations.

---

### 4. Can I use custom functions for anonymization?

**Yes.** You can use any functions and values available in the source database.

You must ensure that anonymized values match the field format.  
For example, if the field type is `varchar(15)`, you must **manually** ensure the generated value does not exceed 15 characters.

If the format is violated, the dump may be created successfully, but restoring it may fail.

---

### 5. Can I use custom functions for scanning?

**Yes.** The meta-dictionary has a `data_func` section.  
In this section, you can use any custom SQL function for sensitivity validation.

This allows you to implement checks using full-text search or any other SQL capabilities.

Such functions must follow this template:

```sql
CREATE OR REPLACE FUNCTION <schema>.<function_name>(
  value TEXT,
  schema_name TEXT,
  table_name TEXT,
  field_name TEXT
)
RETURNS boolean AS $$
...
```

### 6. Is the scanning stage required?

**No**. You can create all required dictionaries manually or reuse previously generated dictionaries.

---

### 7. Why load sensitive and non-sensitive dictionaries during scanning?

They are used only to speed up scanning.

These dictionaries act as a cache, allowing pg_anon to immediately know which fields are sensitive and which are not.

This way, repeated scans of the same database will run very quickly.

If new fields appear that are not present in the dictionaries, pg_anon will evaluate them using the rules from the meta-dictionary.

---

### 8. When should I use `--config` with a configuration file?

If you plan to use pg_anon with different PostgreSQL major versions, you should define a config file.

It is much easier to configure this once rather than repeatedly passing paths to pg_dump and pg_restore.

If you always use a single PostgreSQL version, the system pg_dump and pg_restore will be used, and a config file is unnecessary.

---

### 9. Can I split one large dictionary into multiple smaller ones?

**Yes**. All dictionary-related parameters accept lists of files.

At startup, pg_anon merges them into a single dictionary internally.

This makes it easy to separate different groups of rules into different files and combine them as needed.
This is especially helpful for the meta-dictionary, which contains many optional sections.

---

### 10. Restore error: "Database is not empty"

Restore mode checks that the target database is empty.

This is done to prevent accidental data loss in the target database.

If needed, use the `--drop-db` or `--clean-db` options during restore.

---

### 11. Restore error: "Database is being accessed by other users"

When using the `--drop-db` option, the target database will be recreated using `DROP DATABASE` and `CREATE DATABASE`.

If there are active connections, the `DROP DATABASE` command cannot be executed.

You must terminate all active sessions and run the restore operation again.
