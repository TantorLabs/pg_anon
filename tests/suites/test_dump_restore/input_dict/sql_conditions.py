{
    "dictionary": [
        {
            "schema": "hr",
            "table": "employee",
            "sql_condition": "WHERE id <= 5",
            "fields": {"first_name": "'X'"}
        },
        {
            "schema": "hr",
            "table": "salary_history",
            "sql_condition": "WHERE employee_id <= 5",
            "fields": {}
        },
        {
            "schema": "hr",
            "table": "performance_review",
            "sql_condition": "WHERE employee_id <= 5 AND (reviewer_id IS NULL OR reviewer_id <= 5)",
            "fields": {}
        },
        {
            "schema": "billing",
            "table": "customer",
            "sql_condition": "WHERE id <= 3",
            "fields": {"name": "'X'"}
        },
        {
            "schema": "billing",
            "table": "payment_card",
            "sql_condition": "WHERE customer_id <= 3",
            "fields": {}
        },
        {
            "schema": "billing",
            "table": "invoice",
            "sql_condition": "WHERE customer_id <= 3",
            "fields": {}
        },
        {
            "schema": "billing",
            "table": "transaction",
            "sql_condition": "WHERE invoice_id IN (SELECT id FROM billing.invoice WHERE customer_id <= 3)",
            "fields": {}
        },
        {
            "schema": "ecommerce",
            "table": "order",
            "sql_condition": "WHERE customer_id <= 3",
            "fields": {}
        },
        {
            "schema": "ecommerce",
            "table": "order_item",
            "sql_condition": "WHERE order_id IN (SELECT id FROM ecommerce.\"order\" WHERE customer_id <= 3)",
            "fields": {}
        },
        {
            "schema": "ecommerce",
            "table": "review",
            "sql_condition": "WHERE customer_id <= 3",
            "fields": {}
        }
    ]
}
