# 📋 Non-Sensitive Dictionary
> [🏠 Home](../../README.md#-dictionary-schemas) | [🔍 Scan](../operations/scan.md) | [🗂️ Meta Dictionary](meta-dict-schema.md) | [🔐 Sensitive Dictionary](sens-dict-schema.md) |  

The non-sensitive dictionary is used only during the [create-dict (scan) mode](../operations/scan.md) to speed up processing.
It defines which fields should be treated as non-sensitive. Fields listed here are **excluded** from all sensitivity checks according to [meta-dictionary](meta-dict-schema.md) rules.

This dictionary can be created manually or generated automatically using [create-dict (scan) mode](../operations/scan.md) with `--output-no-sens-dict-file` option. 

> ⚠️ **Note**
> 
> If a field appears both in the [sensitive dictionary](sens-dict-schema.md) and the non-sensitive dictionary, the sensitive dictionary takes priority.

---

## Schema
```python
{    
    "no_sens_dictionary": [
        {
            "schema": "<schema_name: string>",
            "table": "<table_name: string>",
            "fields": [
                "<field_name: string>",
            ]
        },
    ]
}
```

---

## ⚙️ Using the Dictionary

**🏛️ Example Tables Structure**

| Schema    | Table     | Field            |
|-----------|-----------|------------------|
| public    | employees | id               |
| public    | employees | full_name        |
| public    | employees | email            |
| public    | employees | hire_date        |
| public    | salaries  | employee_id      |
| public    | salaries  | monthly_salary   |
| public    | salaries  | currency         |

**📘 Example Non-Sensitive Dictionary**
```python
{
    "no_sens_dictionary": [
        {
            "schema": "public",
            "table": "employees",
            "fields": [
                "id",
                "hire_date",
            ]
        },
        {
            "schema": "public",
            "table": "salaries",
            "fields": [
                "employee_id",
                "currency",
            ]
        },
    ]
}
```

**This dictionary matches the following table fields:**

| Schema   | Table      | Field            | Used in `create-dict (scan)` mode                          |
|----------|------------|------------------|------------------------------------------------------------|
| public   | employees  | id               | Excluded from sensitivity checks as a "no sensitive" field |
| public   | employees  | full_name        | Fields scanned using meta-dictionary rules                 |
| public   | employees  | email            | Fields scanned using meta-dictionary rules                 |
| public   | employees  | hire_date        | Excluded from sensitivity checks as a "no sensitive" field |
| public   | salaries   | employee_id      | Excluded from sensitivity checks as a "no sensitive" field |
| public   | salaries   | monthly_salary   | Fields scanned using meta-dictionary rules                 |
| public   | salaries   | currency         | Excluded from sensitivity checks as a "no sensitive" field |
