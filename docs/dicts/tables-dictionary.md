# 📑 Tables dictionary
> [🏠 Home](../../README.md#-dictionary-schemas) | [💾 Dump](../operations/dump.md) | [📂 Restore](../operations/restore.md)

## Overview
The tables dictionary defines which tables participate in the partial dump and partial restore operations.
It can act as either a whitelist (include-only) or a blacklist (exclude-only).

Use this dictionary when you need to:
- dump or restore only specific tables
- exclude unwanted tables from the dump or restore

## Schema
```python
{
    "tables": [
        {
            "schema": "<schema_name: string>",             # Include only this schema
            "schema_mask": "<schema_regex_mask: string>",  # Or include schemas matching regex pattern
            "table": "<table_name: string>",               # Include only this table
            "table_mask": "<table_regex_mask: string>",    # Or include tables matching regex pattern
        }
    ]
}
```
> ⚠️ **Note**
> - You must use either `schema` or `schema_mask` → not both.
> - You must use either `table` or `table_mask` → not both.

---

## ⚙️ Using the Dictionary

You can use the same dictionary in two different roles:
- Whitelist — dump/restore only the matched tables
- Blacklist — dump/restore all tables except the matched ones

Rules match ordinary tables, partitioned tables, foreign tables and tables with `INHERITS`. For partitions and
child tables, see [Parent and child tables](#parent-and-child-tables).


**🏛️ Example Database Structure**

| Schema    | Table       |
|-----------|-------------|
| public    | employees   |
| public    | departments |
| public    | positions   |
| public    | salaries    |
| public    | users       |
| ecommerce | products    |
| ecommerce | categories  |
| ecommerce | orders      |
| ecommerce | order_items |
| tenant_a  | users       | 
| tenant_a  | projects    | 
| tenant_a  | tasks       | 
| tenant_a  | comments    | 
| tenant_b  | users       | 
| tenant_b  | projects    | 
| tenant_b  | tasks       | 
| tenant_b  | comments    | 
| tenant_c  | users       | 
| tenant_c  | projects    | 
| tenant_c  | tasks       | 
| tenant_c  | comments    | 



**📘 Example Tables Dictionary**
```python
{
    "tables": [  
        {
            "schema": "public",
            "table": "employees"
        },
        {
            "schema": "ecommerce",
            "table_mask": "^orders"
        },
        {
            "schema_mask": "_a$",
            "table": "projects"
        },
        {
            "schema_mask": "*",
            "table_mask": "users"
        },
    ]
}
```

**This dictionary matches the following tables:**

| Schema    | Table       | Matched by rule                            |
|-----------|-------------|--------------------------------------------|
| ecommerce | orders      | `schema="ecommerce", table_mask="^orders"` |
| ecommerce | order_items | `schema="ecommerce", table_mask="^orders"` |
| tenant_a  | projects    | `schema_mask="_a$", table="projects"`      |
| tenant_a  | users       | `schema_mask="*", table_mask="users"`      |
| tenant_b  | users       | `schema_mask="*", table_mask="users"`      |
| tenant_c  | users       | `schema_mask="*", table_mask="users"`      |
| public    | users       | `schema_mask="*", table_mask="users"`      |
| public    | employees   | `schema="public", table="employees"`       |

---

## Parent and child tables

Partitions (`PARTITION OF`) and child tables (`INHERITS`) follow different rules.

- **Partitions.** A partitioned table and its partitions are one table in parts. A rule for a partitioned table works
  like a PostgreSQL command on it without `ONLY`: in both lists it covers all its partitions, at any depth.
- **`INHERITS`.** Child tables are separate tables with their own rows. A whitelist rule for a parent covers only
  the parent. A blacklist rule for a parent covers its child tables too: a child table cannot be created without
  its parent.

**🏛️ Example Database Structure**

| Table | Parent table | Kind |
|---|---|---|
| `app.sales` | — | partitioned table |
| `app.sales_2024` | `app.sales` | partition |
| `app.sales_2025` | `app.sales` | partition |
| `app.events` | — | table with its own rows |
| `app.events_old` | `app.events` | child table (`INHERITS`) |
| `app.events_new` | `app.events` | child table (`INHERITS`) |

**What is in the list → what is in the target database**

| Lists | Partitions | `INHERITS` |
|---|---|---|
| Blacklist: `sales_2024` / `events_old` | `sales`, `sales_2025` | `events`, `events_new` |
| Blacklist: `sales` / `events` | no tables | no tables |
| Whitelist: `sales_2024` / `events_old` | `sales`, `sales_2024` | `events` without its own rows, `events_old` |
| Whitelist: `sales` / `events` | `sales`, `sales_2024`, `sales_2025` | `events` |

- **A kept child table keeps all its parents:** a table cannot exist without them. You do not need to list the
  parents in the whitelist. A parent with `INHERITS` is created without its own rows.
- **A left-out parent leaves out all its child tables.** A table with `INHERITS` from two parents is left out if
  one of them is left out. The dump and the restore log the number of child tables that are left out with the
  parent.
- **The blacklist wins.** If a table is in both lists, it is left out.
- **A parent with `INHERITS` and some of its child tables:** put the parent and these child tables in the whitelist.
- **Foreign tables.** A foreign partition and a foreign child table follow their parent. A foreign server stays in
  the dump with any lists.
- **Masking rules.** A rule of the [sensitive dictionary](sens-dict-schema.md) for a partitioned table also works
  for its partitions. Child tables of `INHERITS` do not take the rules of their parent. See
  [Partitioned tables and tables with `INHERITS`](sens-dict-schema.md#partitioned-tables-and-tables-with-inherits).

If only some partitions of a table are left out, foreign keys that point to this table may be created as
`NOT VALID`: see [Foreign keys](../operations/restore.md#foreign-keys).
