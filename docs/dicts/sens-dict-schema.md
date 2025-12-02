# 📋 Sensitive Dictionary
> [🏠 Home](../../README.md#-dictionary-schemas) | [🔍 Scan](../operations/scan.md) | [💾 Dump](../operations/dump.md) | [🔬 View Fields](../operations/view-fields.md) | [📊 View Data](../operations/view-data.md) | [🗂️ Meta Dictionary](meta-dict-schema.md) | [📋 Non-sensitive Dictionary](non-sens-dict-schema.md)  

## Overview
The sensitive dictionary defines explicit anonymization rules for fields.
It is used in four operation modes, and its behavior differs slightly across them:

1. [💾 Dump mode](../operations/dump.md)
    
    Fields listed in the dictionary are anonymized using the defined rules.
    All other fields are dumped as-is.

2. [🔍 Create-dict (scan) mode](../operations/scan.md)

    Fields listed in the sensitive dictionary are treated as known **sensitive** fields,
    which skips sensitivity detection for them.
    This speeds up scanning process.

3. [🔬 View fields mode](../operations/view-fields.md)

    Shows which anonymization rules would be applied to fields.

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
                "<field_name: string>": "<anonymization_rule_for_field: string>",
            },
            "sql_condition": # Optional. Condition in raw SQL format for filtering the data to dump. (This section ignored for create-dict (scan) mode
                """
                <raw_SQL_WHERE_condition: string>
                """
        }
    ],
    # Optional section. It is used to exclude schemas and tables from the data dump.  
    "dictionary_exclude": [
        {
            "schema": "<schema_name: string>",             # Exclude only this schema
            "schema_mask": "<schema_regex_mask: string>",  # Or exclude schemas matching regex pattern
            "table": "<table_name: string>",               # Exclude only this table
            "table_mask": "<table_regex_mask: string>",    # Or exclude tables matching regex pattern
        }
    ]
}
```
> ⚠️ **Note**
> - `sql_condition` in `dictionary` section is optional. It can be used for taking a part of data. Example: getting table data only by last week.
> - `dictionary_exclude` is optional section.  If a table appears in both the "dictionary_exclude" and "dictionary" sections, then table will be dumped. It can be used for particular dump and debugging of anonymization process.
> - In `dictionary_exclude`, you must use either `schema` or `schema_mask` → not both.
> - In `dictionary_exclude`, you must use either `table` or `table_mask` → not both.

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
                "email": "md5(\"email\") || @abc.com",  # hashing employee emails while preserving email format
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
    # Excluding all tables from schemas `tenant_a`, `tenant_b`, `tenant_c` 
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
| public       | employees | full_name        | Dumped with anonymization | Excluded from sensitivity checks as a "sensitive" field |
| public       | employees | email            | Dumped with anonymization | Excluded from sensitivity checks as a "sensitive" field |
| public       | employees | hire_date        | Dumped as is              | Fields scanned using meta-dictionary rules              |
| public       | salaries  | employee_id      | Dumped as is              | Fields scanned using meta-dictionary rules              |
| public       | salaries  | monthly_salary   | Dumped with anonymization | Excluded from sensitivity checks as a "sensitive" field |
| public       | salaries  | currency         | Dumped as is              | Fields scanned using meta-dictionary rules              |
| ecommerce    | orders    | product_id       | Dumped as is              | Fields scanned using meta-dictionary rules              |
| ecommerce    | orders    | client_name      | Dumped with anonymization | Excluded from sensitivity checks as a "sensitive" field |
| ecommerce    | orders    | delivery_address | Dumped with anonymization | Excluded from sensitivity checks as a "sensitive" field |
| ecommerce    | orders    | count            | Dumped as is              | Fields scanned using meta-dictionary rules              |
| ecommerce    | orders    | created          | Dumped as is              | Fields scanned using meta-dictionary rules              |
| ecommerce    | orders    | status           | Dumped as is              | Fields scanned using meta-dictionary rules              |
