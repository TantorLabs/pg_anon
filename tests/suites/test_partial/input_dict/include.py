{
    "tables": [
        {"schema": "hr",      "table": "department"},
        {"schema": "hr",      "table": "employee"},
        {"schema": "billing", "table_mask": "^(customer|invoice)$"},
        {"schema_mask": "^ecommerce$", "table": "product"},
        {"schema_mask": "^quirks$",    "table_mask": ".*"}
    ]
}
