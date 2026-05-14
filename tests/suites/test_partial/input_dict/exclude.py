{
    "tables": [
        {"schema_mask": "^analytics.*", "table_mask": ".*"},
        {"schema_mask": "^audit$",      "table_mask": ".*"},
        {"schema_mask": "^content$",    "table_mask": ".*"},
        {"schema_mask": "^data_types$", "table_mask": ".*"},
        {"schema_mask": "^security$",   "table_mask": ".*"},
        {"schema": "hr",       "table": "performance_review"},
        {"schema": "hr",       "table": "salary_history"},
        {"schema": "billing",  "table": "payment_card"},
        {"schema": "billing",  "table": "transaction"},
        {"schema": "ecommerce","table_mask": "^(order|order_item|review|category)$"}
    ]
}
