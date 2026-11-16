# 📋 Sensitive Dictionary
> [🏠 Home](../../README.md#-dictionary-schemas) | [🔍 Scan](../operations/scan.md) | [💾 Dump](../operations/dump.md) | [🔬 View Fields](../operations/view-fields.md) | [📊 View Data](../operations/view-data.md) | [🗂️ Meta Dictionary](meta-dict-schema.md) | [📋 Non-sensitive Dictionary](non-sens-dict-schema.md)

## Overview
The sensitive dictionary defines explicit masking rules for fields.
It is used in four operation modes, and its behavior differs slightly across them:

1. [💾 Dump mode](../operations/dump.md)
    
    Fields listed in the dictionary are masked using the defined rules.
    All other fields are dumped as-is.

2. [🔍 Create-dict (scan) mode](../operations/scan.md)

    Fields listed in the sensitive dictionary are treated as known **sensitive** fields,
    which skips sensitivity detection for them.
    This speeds up scanning process.
    The scan keeps the rules for tables of the database in the output dictionary.
    See [Prepared dictionaries in the scan](../operations/scan.md#prepared-dictionaries-in-the-scan).

3. [🔬 View fields mode](../operations/view-fields.md)

    Shows which masking rules would be applied to fields.

4. [📊 View data mode](../operations/view-data.md)

    Shows how the rules would affect sample data, without performing a dump.

This dictionary can be created manually or generated automatically using [create-dict (scan) mode](../operations/scan.md).

> ⚠️ **Note**
> 
> If a field appears both in the sensitive dictionary and the [non-sensitive](non-sens-dict-schema.md) dictionary, the sensitive dictionary takes priority.


---

## Schema
```python
{    
    "dictionary": [
        {
            "schema": "<schema_name: string>",
            "table": "<table_name: string>",
            "fields": {
                "<field_name: string>": "<masking_rule_for_field: string>",
            },
            "sql_condition": # Optional. Condition in raw SQL format for filtering the data to dump. (This section ignored for create-dict (scan) mode
                """
                <raw_SQL_WHERE_condition: string>
                """
        }
    ],
    # Optional section. It is used to leave the data of tables out of the dump. The structure stays.
    "dictionary_exclude": [
        {
            "schema": "<schema_name: string>",             # Only this schema
            "schema_mask": "<schema_regex_mask: string>",  # Or schemas matching a regular expression
            "table": "<table_name: string>",               # Only this table
            "table_mask": "<table_regex_mask: string>",    # Or tables matching a regular expression
        }
    ]
}
```
> ⚠️ **Note**
> - `sql_condition` in `dictionary` section is optional. It can be used for taking a part of data. Example: getting table data only by last week.
> - `dictionary_exclude` is optional section. It can be used for particular dump and debugging of the masking process. If a table has a rule and `dictionary_exclude`, the order in [Which rule is used](#partitioned-tables) decides: an exact name wins over a mask; for the same name or mask, the rule wins.
> - The scan does not use `dictionary_exclude`. See [Prepared dictionaries in the scan](../operations/scan.md#prepared-dictionaries-in-the-scan).
> - In `dictionary_exclude`, you must use either `schema` or `schema_mask` → not both.
> - In `dictionary_exclude`, you must use either `table` or `table_mask` → not both.
> - `dictionary_exclude` leaves out **data only**, also with `table_mask: "*"`: the tables stay in the dump, but empty. To leave out a whole schema, use [`--exclude-schema-name`](../operations/dump.md#exclude-schemas).
> - A foreign key to a table without data, or to a table dumped with `sql_condition` or `raw_sql`, may be restored as `NOT VALID`: see [Foreign keys](../operations/restore.md#foreign-keys).

---

## Partitioned tables and tables with `INHERITS`

pg_anon works with partitions and with child tables of `INHERITS` in different ways.

### Partitioned tables

A partitioned table and its partitions are one table in parts: the partitions have the same columns, and only the
partitions have rows. So the rules of a partitioned table work for all its partitions, at any depth and in any schema.

- **A rule for a partitioned table masks all its partitions.** This includes partitions that you create or attach
  (`ATTACH PARTITION`) later.
- **A detached partition loses this rule.** After `DETACH PARTITION` the table is an ordinary table, and the rule of
  its former parent does not work for it. A dictionary made by the scan usually has no rule for a partition. So
  until the next scan, the dump takes the data of a detached partition without masking.
- **A rule for a partition is merged with the rules above it, field by field.** A field from the rule of the
  partition uses the function of this rule. Other fields take the function from the next rule that has them, in the
  order below. `sql_condition` also comes from the first rule that has it.
- **Which rule is used.** pg_anon looks for rules in this order:
  1. the exact name of the table;
  2. for a partition: the exact names of the partitioned tables above it, the closest first;
  3. a mask (`schema_mask`, `table_mask`) for the table;
  4. for a partition: masks for the tables above it.

  The first item that has a rule or `dictionary_exclude` decides. If one item has both, the rule wins and the table
  is dumped with masking. So an exact name wins over a mask, also over a mask for the partition itself.
- **To keep a field of one partition as it is,** give this field in the rule of the partition with the field itself as
  the function: `"note": "\"note\""`.

| Dictionary | `sales.orders_2024` (partition of `sales.orders`) |
|---|---|
| `sales.orders {email, note}` | `email` and `note` with the functions of `sales.orders` |
| `sales.orders {email, note}` and `sales.orders_2024 {note}` | `email` of `sales.orders`, `note` of `sales.orders_2024` |
| `sales.orders {email}` and `dictionary_exclude` for `sales.orders_2024` | no data |
| `dictionary_exclude` for `sales.orders` and `sales.orders_2024 {email}` | `email` of `sales.orders_2024` |
| `sales.orders {email}` and `dictionary_exclude` with `table_mask` for `sales.orders_2024` | `email` of `sales.orders`: the exact name wins over the mask |

A dictionary of pg_anon 1.12 and older has a rule for each partition. It works as it is. See
[Dictionaries of pg_anon 1.12 and older](../operations/scan.md#partitioned-tables).

### Tables with `INHERITS`

Child tables of `INHERITS` are separate tables with their own rows and often with more columns than the parent.
That the data of the parent is safe says nothing about a child table. So each table takes only the rules for its
own name:

- A rule or `dictionary_exclude` for the parent does not work for its child tables. Give each child table its own
  rule.
- The dump reads the parent with `ONLY`: the rows of child tables are dumped with the rules of these tables.
- A new child table has no rule. Until the next scan, the dump takes its data without masking.

---

## ⚙️ Using the Dictionary

**🏛️ Example Database Structure**

| Schema    | Table     | Field            |
|-----------|-----------|------------------|
| public    | employees | id               |
| public    | employees | full_name        |
| public    | employees | email            |
| public    | employees | hire_date        |
| public    | salaries  | employee_id      |
| public    | salaries  | monthly_salary   |
| public    | salaries  | currency         |
| ecommerce | orders    | product_id       |
| ecommerce | orders    | count            |
| ecommerce | orders    | client_name      |
| ecommerce | orders    | delivery_address |
| ecommerce | orders    | created          |
| ecommerce | orders    | status           |
| tenant_a  | projects  | title            |
| tenant_a  | projects  | description      |
| tenant_b  | projects  | title            |
| tenant_b  | projects  | description      |
| tenant_c  | projects  | title            |
| tenant_c  | projects  | description      |



**📘 Example Sensitive Dictionary**
```python
{    
    "dictionary": [
        {
            "schema": "public",
            "table": "employees",
            "fields": {
                "full_name": "anon_funcs.digest(\"full_name\", 'salt_word', 'sha256')",  # hashing employees names 
                "email": "md5(\"email\") || '@abc.com'",  # hashing employee emails while preserving email format
            },
        },
        {
            "schema": "public",
            "table": "salaries",
            "fields": {
                "monthly_salary": "10000",  # just defines one value for the field for all rows
            },
        },
        {
            "schema": "ecommerce",
            "table": "orders",
            "fields": {
                "client_name": "anon_funcs.digest(\"client_name\", 'salt_word', 'sha256')",
                "delivery_address": "anon_funcs.digest(\"delivery_address\", 'salt_word', 'sha256')",
            },
            "sql_condition":  # Dumping only the orders completed within the last week
                """
                WHERE created > NOW() - '7 days'::interval
                AND status = 'done'
                """
        }
    ],
    # Leaving out the data of all tables in schemas `tenant_a`, `tenant_b`, `tenant_c`; their structure stays
    "dictionary_exclude": [
        {
            "schema_mask": "tenant_.*",
            "table_mask": "*",
        }
    ]
}
```

**This dictionary matches the following table fields:**

| Schema       | Table     | Field            | Used in `dump` mode       | Used in `create-dict (scan)` mode                       |
|--------------|-----------|------------------|---------------------------|---------------------------------------------------------|
| public       | employees | id               | Dumped as is              | Fields scanned using meta-dictionary rules              |
| public       | employees | full_name        | Dumped with masking | Excluded from sensitivity checks as a "sensitive" field |
| public       | employees | email            | Dumped with masking | Excluded from sensitivity checks as a "sensitive" field |
| public       | employees | hire_date        | Dumped as is              | Fields scanned using meta-dictionary rules              |
| public       | salaries  | employee_id      | Dumped as is              | Fields scanned using meta-dictionary rules              |
| public       | salaries  | monthly_salary   | Dumped with masking | Excluded from sensitivity checks as a "sensitive" field |
| public       | salaries  | currency         | Dumped as is              | Fields scanned using meta-dictionary rules              |
| ecommerce    | orders    | product_id       | Dumped as is              | Fields scanned using meta-dictionary rules              |
| ecommerce    | orders    | client_name      | Dumped with masking | Excluded from sensitivity checks as a "sensitive" field |
| ecommerce    | orders    | delivery_address | Dumped with masking | Excluded from sensitivity checks as a "sensitive" field |
| ecommerce    | orders    | count            | Dumped as is              | Fields scanned using meta-dictionary rules              |
| ecommerce    | orders    | created          | Dumped as is              | Fields scanned using meta-dictionary rules              |
| ecommerce    | orders    | status           | Dumped as is              | Fields scanned using meta-dictionary rules              |
