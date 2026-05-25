{
    "dictionary": [
        {"schema": "hr", "table": "department", "fields": {"name": "'Anonymized_' || id::text"}}
    ],
    "dictionary_exclude": [
        {"schema_mask": ".*", "table_mask": "*"}
    ],
    "validate_tables": [
        {"schema": "hr", "table": "department"}
    ]
}
