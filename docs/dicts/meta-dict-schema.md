# 🗂️ Meta Dictionary
> [🏠 Home](../../README.md#-dictionary-schemas) | [🔍 Scan](../operations/scan.md) | [🔐 Sensitive Dictionary](sens-dict-schema.md) | [📋 Non-sensitive Dictionary](non-sens-dict-schema.md)

## Overview

The meta-dictionary defines the rules used by pg_anon to detect sensitive fields during the [create-dict (scan) mode](../operations/scan.md).

This dictionary is not generated automatically — it must be created manually.
It provides powerful configuration options that let you fine-tune scanning behavior. 

During scanning, pg_anon analyzes all database schemas, tables, and fields.

The meta-dictionary allows you to:
- include or exclude specific objects
- detect sensitive fields by:
  - field names
  - data patterns (regex)
  - exact or partial constants
  - custom Python functions
  - SQL filtering conditions
  - select which PostgreSQL types should be scanned
  - specify anonymization functions per data type

To make it easier to navigate, each section of the meta-dictionary is described separately below, with purpose, schema, and example.

At the end, a complete combined schema is shown.

---

## 1. Section: `field`
### Purpose
Detect sensitive fields based solely on field name, without scanning the data inside.

Useful when already known fields contains sensitive data (e.g., email, password)

### Schema
```python
{
    "field": {
        "rules": [
            "<field_name_regex: string>",
        ],
        "constants": [
            "<field_name: string>",
        ]
    }
}
```
| Key          | Meaning                                                                                      |
|--------------|----------------------------------------------------------------------------------------------|
| `rules`      | List of regex patterns. Field names matching these patterns are always considered sensitive. |
| `constants`  | Exact field names to be treated as sensitive.                                                |


### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "field": {
        "rules": [
            "^client_",
            ".*phone.*"
        ],
        "constants": [
            "password",
            "email"
        ]
    }
}
```

**🏛️ Example Tables Structure with following dictionary matches**

| Schema       | Table     | Field                   | Is sensitive | Rule         |
|--------------|-----------|-------------------------|--------------|--------------|
| public       | employees | id                      | No           | -            |
| public       | employees | full_name               | No           | -            |
| public       | employees | email                   | Yes          | email        |
| public       | employees | password                | Yes          | password     |
| public       | employees | phone                   | Yes          | .\*phone.\*  |
| public       | employees | hire_date               | No           | -            |
| public       | salaries  | employee_id             | No           | -            |
| public       | salaries  | monthly_salary          | No           | -            |
| public       | salaries  | currency                | No           | -            |
| ecommerce    | orders    | product_id              | No           | -            |
| ecommerce    | orders    | client_name             | Yes          | ^client_     |
| ecommerce    | orders    | client_phone            | Yes          | .\*phone.\*  |
| ecommerce    | orders    | client_delivery_address | Yes          | ^client_     |
| ecommerce    | orders    | count                   | No           | -            |
| ecommerce    | orders    | created                 | No           | -            |
| ecommerce    | orders    | status                  | No           | -            |

---

## 2. Section: `skip_rules`
### Purpose
Defines what parts of the database should be excluded from scanning.
You can skip entire schemas, specific tables, or individual fields.
Useful for large schemas without sensitive data or for reducing processing time.

### Schema
```python
{
    "skip_rules": [
        {
            "schema": "<schema_name: string>",             # Skip this schema from scanning
            "schema_mask": "<schema_regex_mask: string>",  # Or skip from scanning schemas matching regex pattern
            "table": "<table_name: string>",               # Skip from scanning only this table
            "table_mask": "<table_regex_mask: string>",    # Or skip from scanning tables matching regex pattern
            "fields": [  # Skip from scanning fields of tables
                "<field_name: string>"
            ]
        }
    ]
}
```
### Rule Combinations
1. `schema` or `schema_mask` — required
   - You must specify one of `schema` or `schema_mask`
   - This defines where the rule applies.
2. `table` or `table_mask` — optional
   - You may (but don’t have to) narrow the rule to specific tables.
   - If `table` or `table_mask` is specified → rule applies only to those tables.
   - If both are omitted → the rule applies to the entire schema.
3. `fields` — optional
   - You may also specify particular fields to skip.
   - If fields is specified → only these fields are skipped.
   - If fields is omitted → the entire table is skipped.
   - And if no `table` is specified → the entire schema is skipped.

### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "skip_rules": [
        {
          "schema_msk": "^tmp.*"
        },
      {
        "schema": "ecommerce",
        "table_mask": "^client_.*"
      },
      {
        "schema": "public",
        "fields": ["currency", "id", "employee_id"]
      },
      {
        "schema": "ecommerce",
        "table": "orders",
        "fields": ["count"]
      }
    ]
}
```

**🏛️ Example Tables Structure with following dictionary matches**

| Schema     | Table     | Field                   | Is skipped |
|------------|-----------|-------------------------|------------|
| public     | employees | id                      | Yes        |
| public     | employees | full_name               | No         |
| public     | employees | email                   | No         |
| public     | employees | password                | No         |
| public     | employees | phone                   | No         |
| public     | employees | hire_date               | No         |
| public     | salaries  | employee_id             | No         |
| public     | salaries  | monthly_salary          | No         |
| public     | salaries  | currency                | Yes        |
| ecommerce  | orders    | product_id              | No         |
| ecommerce  | orders    | client_name             | Yes        |
| ecommerce  | orders    | client_phone            | Yes        |
| ecommerce  | orders    | client_delivery_address | Yes        |
| ecommerce  | orders    | count                   | Yes        |
| ecommerce  | orders    | created                 | No         |
| ecommerce  | orders    | status                  | No         |
| tmp_some_1 | tbl_1     | content                 | Yes        |
| tmp_some_2 | tbl_2     | content                 | Yes        |

---

## 3. Section: `include_rules`
### Purpose
Restrict scanning to specific schemas/tables/fields.
Useful when:
- scanning only part of a large database
- debugging detection rules
- running focused scans


### Schema
```python
{
    "include_rules": [
        {
            "schema": "<schema_name: string>",             # Include only this schema
            "schema_mask": "<schema_regex_mask: string>",  # Or include schemas matching regex pattern
            "table": "<table_name: string>",               # Include only this table
            "table_mask": "<table_regex_mask: string>",    # Or include tables matching regex pattern
            "fields": [  # Include only this fields of tables for scanning
                "<field_name: string>"
            ]
        }
    ]
}
```
### Combination rules
1. `schema` or `schema_mask` — required
   - You must specify one of `schema` or `schema_mask`
   - This defines where the rule applies.
2. `table` or `table_mask` — optional
   - You may (but don’t have to) narrow the rule to specific tables.
   - If `table` or `table_mask` is specified → rule applies only to those tables.
   - If both are omitted → the rule applies to the entire schema.
3. `fields` — optional
   - You may also specify particular fields to skip.
   - If fields is specified → only these fields are skipped.
   - If fields is omitted → the entire table is skipped.
   - And if no `table` is specified → the entire schema is skipped.

### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "include_rules": [
        {
          "schema_msk": "^tmp.*"
        },
      {
        "schema": "ecommerce",
        "table_mask": "^client_.*"
      },
      {
        "schema": "public",
        "fields": ["currency", "id", "employee_id"]
      },
      {
        "schema": "ecommerce",
        "table": "orders",
        "fields": ["count"]
      }
    ]
}
```

**🏛️ Example Tables Structure with following dictionary matches**

| Schema     | Table     | Field                   | Is skipped |
|------------|-----------|-------------------------|------------|
| public     | employees | id                      | No         |
| public     | employees | full_name               | Yes        |
| public     | employees | email                   | Yes        |
| public     | employees | password                | Yes        |
| public     | employees | phone                   | Yes        |
| public     | employees | hire_date               | Yes        |
| public     | salaries  | employee_id             | Yes        |
| public     | salaries  | monthly_salary          | Yes        |
| public     | salaries  | currency                | No         |
| ecommerce  | orders    | product_id              | Yes        |
| ecommerce  | orders    | client_name             | No         |
| ecommerce  | orders    | client_phone            | No         |
| ecommerce  | orders    | client_delivery_address | No         |
| ecommerce  | orders    | count                   | No         |
| ecommerce  | orders    | created                 | Yes        |
| ecommerce  | orders    | status                  | Yes        |
| tmp_some_1 | tbl_1     | content                 | No         |
| tmp_some_2 | tbl_2     | content                 | No         |

---

## 4. Section: `data_regex`
### Purpose
Detect sensitive data by scanning field values using regular expressions.

### Schema
```python
{
    "data_regex": {
        "rules": [
            "<regex_rule: string>",
        ]
    }
}
```

### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "data_regex": {
        "rules": [
            """[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",  # email
            "^(7?\d{10})$",				# phone 7XXXXXXXXXX
        ]
    }
}
```

**🏛️ Example Tables Structure with following dictionary matches**

| Schema       | Table     | Field                   | Is sensitive | Rule                                                            |
|--------------|-----------|-------------------------|--------------|-----------------------------------------------------------------|
| public       | employees | id                      | No           | -                                                               |
| public       | employees | full_name               | No           | -                                                               |
| public       | employees | email                   | Yes          | [A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+ |
| public       | employees | password                | No           | -                                                               |
| public       | employees | phone                   | Yes          | ^(7?\d{10})$                                                    |
| public       | employees | hire_date               | No           | -                                                               |
| public       | salaries  | employee_id             | No           | -                                                               |
| public       | salaries  | monthly_salary          | No           | -                                                               |
| public       | salaries  | currency                | No           | -                                                               |
| ecommerce    | orders    | product_id              | No           | -                                                               |
| ecommerce    | orders    | client_name             | No           | -                                                               |
| ecommerce    | orders    | client_phone            | Yes          | ^(7?\d{10})$                                                    |
| ecommerce    | orders    | client_delivery_address | No           | -                                                               |
| ecommerce    | orders    | count                   | No           | -                                                               |
| ecommerce    | orders    | created                 | No           | -                                                               |
| ecommerce    | orders    | status                  | No           | -                                                               |

---

## 5. Section: `data_const`
### Purpose
Detect sensitive fields by matching full or partial constants.

### Schema
```python
{
    "data_const": {
        "constants": [
            "<field_value_full: string>",
        ],
        "partial_constants": [
            "<field_value_partial: string>",
        ]
    }
}
```
| Key                 | Meaning                                                       |
|---------------------|---------------------------------------------------------------|
| `constants`         | **Optional.** Matching full field value.                      |
| `partial_constants` | **Optional.** Matching substring anywhere in the field value. |

> ⚠️ **Note**
> 
> Must be specified one of `constants` or `partial_constants` 

### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "data_const": {
        "constants": [
            "done",
        ],
        "partial_constants": [
            "@example.com",
        ]
    }
}
```

**🏛️ Example Tables Structure with following dictionary matches**

| Schema       | Table     | Field                   | Is sensitive | Rule         |
|--------------|-----------|-------------------------|--------------|--------------|
| public       | employees | id                      | No           | -            |
| public       | employees | full_name               | No           | -            |
| public       | employees | email                   | Yes          | @example.com |
| public       | employees | password                | No           | -            |
| public       | employees | phone                   | No           | -            |
| public       | employees | hire_date               | No           | -            |
| public       | salaries  | employee_id             | No           | -            |
| public       | salaries  | monthly_salary          | No           | -            |
| public       | salaries  | currency                | No           | -            |
| ecommerce    | orders    | product_id              | No           | -            |
| ecommerce    | orders    | client_name             | No           | -            |
| ecommerce    | orders    | client_phone            | No           | -            |
| ecommerce    | orders    | client_delivery_address | No           | -            |
| ecommerce    | orders    | count                   | No           | -            |
| ecommerce    | orders    | created                 | No           | -            |
| ecommerce    | orders    | status                  | Yes          | done         |

---

## 6. Section: `data_func`
### Purpose
Using custom Python functions for detecting and anonymizing sensitive data by type.

### Schema
```python
{
    "data_func": {  
        "<field_type: string>": [ 
            {
                "scan_func": "<scan_function_for_field: string>",  
                "anon_func": "<anonymization_rule_template_for_field: string>", 
                "n_count": "<how_many_checks_must_be_passed: integer>", 
            },
        ],
    }
}
```

| Key          | Meaning                                                                                                     |
|--------------|-------------------------------------------------------------------------------------------------------------|
| `field_type` | PostgreSQL type (or custom type). `"anyelement"` applies to all types.                                      |
| `scan_func`  | Python function called for each field value. Must return Boolean.                                           |
| `anon_func`  | Template anonymization rule. **Must contain `%s` placeholder** for the field name.                          |
| `n_count`    | The field is considered sensitive if the scan function returned `True` at least `n` times for field values. |


> ⚠️ **Note**
> 
> Functions for `scan_func` must follow this template:
> ```sql
> CREATE OR REPLACE FUNCTION <schema>.<function_name>(
>   value TEXT,
>   schema_name TEXT,
>   table_name TEXT,
>   field_name TEXT
> )
> RETURNS boolean AS $$
> ... 

### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "data_func": {
        "anyelement": [
            {
                "scan_func": "custom_funcs.is_employee_email",
                "anon_func": "anon_funcs.digest(\"%s\", 'salt', 'md5')",
                "n_count": 5
            }
        ]
    }
}
```

**🏛️ Example Tables Structure with following dictionary matches**

| Schema       | Table     | Field                   | Is sensitive | Rule                           |
|--------------|-----------|-------------------------|--------------|--------------------------------|
| public       | employees | id                      | No           | -                              |
| public       | employees | full_name               | No           | -                              |
| public       | employees | email                   | Yes          | custom_funcs.is_employee_email |
| public       | employees | password                | No           | -                              |
| public       | employees | phone                   | No           | -                              |
| public       | employees | hire_date               | No           | -                              |
| public       | salaries  | employee_id             | No           | -                              |
| public       | salaries  | monthly_salary          | No           | -                              |
| public       | salaries  | currency                | No           | -                              |
| ecommerce    | orders    | product_id              | No           | -                              |
| ecommerce    | orders    | client_name             | No           | -                              |
| ecommerce    | orders    | client_phone            | No           | -                              |
| ecommerce    | orders    | client_delivery_address | No           | -                              |
| ecommerce    | orders    | count                   | No           | -                              |
| ecommerce    | orders    | created                 | No           | -                              |
| ecommerce    | orders    | status                  | No           | -                              |

---

## 7. Section: `data_sql_condition`
### Purpose
Specify custom SQL WHERE conditions to sample the data instead of scanning the whole table.

### Schema
```python
{
    "data_sql_condition": [
        {
            "schema": "<schema_name: string>",             # Check only this schema
            "schema_mask": "<schema_regex_mask: string>",  # Or check schemas matching regex pattern
            "table": "<table_name: string>",               # Check only this table
            "table_mask": "<table_regex_mask: string>",    # Or check tables matching regex pattern
            "sql_condition": # Condition in raw SQL format for filtering the data to scan
                """
                <raw_SQL_WHERE_condition: string>
                """
        }
    ]
}
```
### Rules
1. `schema` or `schema_mask` — required
   - You must specify one of `schema` or `schema_mask`
2. `table` or `table_mask` — optional
   - You must specify one of `table` or `table_mask`
3. `sql_condition` — required
   - Must specify SQL condition for `WHERE` section
   - Keyword `WHERE` into `sql_condition` is not required

### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "data_sql_condition": [
        {
            "schema": "public",
            "table": "salaries",
            "sql_condition": """
                WHERE hire_date >= '2024-01-01' and hire_date <= '2024-02-01'
            """
        }
    ]
}
```

**Result**

For data scan of table `public.salaries` will be used only data by January 2024.

---

## 8. Section: `sens_pg_types`
### Purpose
Define which PostgreSQL types are scanned. Field types not included in this list are **not scanned**.

If omitted or empty → default types are used: `text`, `integer`, `bigint`, `character`, `json`

### Schema
```python
{
    "sens_pg_types": [
        "<field_type: string>",
    ]
}
```

### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "sens_pg_types": [
        "text",
        "integer",
        "bigint",
        "varchar",
        "mchar",
        "json"
    ]
}
```

Fields with another types are not scanned.

---

## 9. Section: `funcs`
### Purpose
Configure anonymization functions per PostgreSQL type.
- If no specific function is defined, the default is used.
- If no default is set, the following is used automatically: `anon_funcs.digest("%s", 'salt_word', 'md5')`

### Schema
```python
{
    "funcs": {
        "<field_type: string>": "<anonymization_function_for_field_type: string>", 
        "default": "<universal_anonymization_function_for_all_field_types: string>"
    }
}
```

### ⚙️ Using this section

**📘 Example meta-dictionary**
```python
{
    "funcs": {
        "text": "anon_funcs.digest(\"%s\", 'salt_word', 'md5')",
        "numeric": "anon_funcs.noise(\"%s\", 10)",
        "timestamp": "anon_funcs.dnoise(\"%s\",  interval '6 month')",
        "default": "anon_funcs.digest(\"%s\", 'MySecretSaltWord', 'sha256')"
    }
}
```

---

## General meta-dict schema
```python
{
    "field": {
        "rules": [
            "<field_name_regex: string>",
        ],
        "constants": [
            "<field_name: string>",
        ]
    },
    "skip_rules": [
        {
            "schema": "<schema_name: string>",             # Skip this schema from scanning
            "schema_mask": "<schema_regex_mask: string>",  # Or skip from scanning schemas matching regex pattern
            "table": "<table_name: string>",               # Skip from scanning only this table
            "table_mask": "<table_regex_mask: string>",    # Or skip from scanning tables matching regex pattern
            "fields": [  # Skip from scanning fields of tables
                "<field_name: string>"
            ]
        }
    ],
    "include_rules": [
        {
            "schema": "<schema_name: string>",             # Include only this schema
            "schema_mask": "<schema_regex_mask: string>",  # Or include schemas matching regex pattern
            "table": "<table_name: string>",               # Include only this table
            "table_mask": "<table_regex_mask: string>",    # Or include tables matching regex pattern
            "fields": [  # Include only this fields of tables for scanning
                "<field_name: string>"
            ]
        }
    ],
    "data_regex": {
        "rules": [
            "<regex_rule: string>",
        ]
    },
    "data_const": {
        "constants": [
            "<field_value_full: string>",
        ],
        "partial_constants": [
            "<field_value_partial: string>",
        ]
    },
    "data_func": {  
        "<field_type: string>": [ 
            {
                "scan_func": "<scan_function_for_field: string>",  
                "anon_func": "<anonymization_rule_template_for_field: string>", 
                "n_count": "<how_many_checks_must_be_passed: integer>", 
            },
        ],
    },
    "data_sql_condition": [
        {
            "schema": "<schema_name: string>",             # Check only this schema
            "schema_mask": "<schema_regex_mask: string>",  # Or check schemas matching regex pattern
            "table": "<table_name: string>",               # Check only this table
            "table_mask": "<table_regex_mask: string>",    # Or check tables matching regex pattern
            "sql_condition": # Condition in raw SQL format for filtering the data to scan
                """
                <raw_SQL_WHERE_condition: string>
                """
        }
    ],
    "sens_pg_types": [
        "<field_type: string>",
    ],
    "funcs": {
        "<field_type: string>": "<anonymization_function_for_field_type: string>", 
        "default": "<universal_anonymization_function_for_all_field_types: string>"
    }
}
```
