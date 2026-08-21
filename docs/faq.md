# FAQ
> [🏠 Home](../README.md#-documentation-index) | [⚙️ How it works](how-it-works.md)

### 1. Where can I find operation logs and launch parameters?
All run data is stored in the `pg_anon_runs/` directory (relative to the working directory or `$PG_ANON_HOME`).  
Inside, the structure is: `<year>/<month>/<day>/<operation_id>`.

Each operation folder contains:
- a `logs` directory with all log files  
- a `run_options.json` file with all parameters used to run `pg_anon`

If the `--save-dicts` option was used, the folders `input` and `output` will also appear.  
They contain all input and output dictionaries for that run.

---

### 2. Can I restore a pg_anon dump using pg_dump?

**No.** The pg_anon dump format is not compatible with pg_dump due to the specifics of masking.

For the same reason, a regular backup created with pg_dump cannot be restored using pg_anon.

---

### 3. Does pg_anon modify the structure or data of the source database during scan, dump, view-data, or view-fields?

pg_anon does **not** modify either the structure or the data of the source database.

The `anon_funcs` schema is created by the separate `init` mode; the modes listed above only read from the source database.

---

### 4. Can I use custom functions for scanning?

**Yes.** The meta-dictionary has a [`data_func`](dicts/meta-dict-schema.md#6-section-data_func) section.  
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
BEGIN
  <function_logic>;
END;
$$ LANGUAGE plpgsql; 
```

---

### 5. Can I use custom functions for masking?

**Yes.** You can use any functions and values available in the source database.

You must ensure that masked values match the field format.  
For example, if the field type is `varchar(15)`, you must **manually** ensure the generated value does not exceed 15 characters.

If the format is violated, the dump may be created successfully, but restoring it may fail.

Also for this cases can be used [`data_func`](dicts/meta-dict-schema.md#6-section-data_func) section with scan_func for field length comparison and specific anon_function for specific length.

For example, scan function bellow getting only fields with length less than 20 symbols and containing emails:
```sql
CREATE OR REPLACE FUNCTION my_scan_funcs.is_email_field_with_len_20_chars(
  value TEXT,
  schema_name TEXT,
  table_name TEXT,
  field_name TEXT
)
RETURNS boolean AS $$
DECLARE
    max_len integer;
    is_email boolean;
BEGIN
    SELECT c.character_maximum_length
    INTO max_len
    FROM information_schema.columns c
    WHERE c.table_schema = $2
      AND c.table_name = $3
      AND c.column_name = $4;

    -- field length must be 20 characters
    if max_len != 20 then
        return false;
    end if;  	
   
   -- value must be not null for comparison
    if $1 is null then
    	return false;
    end if;  	
   
    -- check email format by regexp
    return $1 ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';
END;
$$ LANGUAGE plpgsql;
```

The meta-dict rule below can be used to detect email fields with a length of 20 characters and mask them while preserving both format and length.
```python
{
    "data_func": {
        "varchar": [
            {
                "scan_func": "my_scan_funcs.is_email_field_with_len_20_chars",
                "anon_func": "lower(anon_funcs.random_string(9)) || '@secret.com'",
                "n_count": 10
            }
        ]
    }
}
```

---

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

---

### 12. Difference between options `--drop-db` and `--clean-db` for restore mode

- `--drop-db` - recreate target database using commands `DROP DATABASE` and `CREATE DATABASE`. After that running restore process on empty db.
- `--clean-db` - Performs a restore similar to pg_restore --clean --if-exists. It creates missing tables from the backup in the target database. It also preserves extra tables that exist in the target DB and are not contained in the restoring backup. This option does not require an empty target database.  

---

### 13. Determining the Optimal Connection Count

pg_anon runs as a single process and opens one pool of `--db-connections` connections,
plus one control connection that stays open for the whole operation.

To choose a value, identify these system parameters:
  - `max_connections` - maximum connections allowed by your PostgreSQL database
  - reserved connections - `superuser_reserved_connections`, plus `reserved_connections` on PostgreSQL 16 and above
  - connections already used by other applications

Important considerations:
  - Exceeding `max_connections` may cause pg_anon failures and affect other database applications
  - Ensure sufficient connection headroom for other services

#### Recommended Configuration:

```bash
--db-connections ≤ max_connections - reserved_connections - connections_used_by_others - 2
```

The trailing `2` covers the control connection and, in restore mode, the extra connection
`pg_restore` opens to coordinate its workers.

#### Example Calculation:
  - `max_connections`: 100
  - reserved connections: 5
  - used by other applications: 20
  - `--db-connections`: 100 - 5 - 20 - 2 = 73

pg_anon performs this check itself before starting and fails early with a clear message
if the database cannot provide the requested number of connections. To skip it, use
`--disable-checks`.
