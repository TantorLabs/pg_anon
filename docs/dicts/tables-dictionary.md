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
