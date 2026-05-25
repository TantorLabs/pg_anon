{
    "dictionary": [
        {
            "schema": "hr",
            "table":  "employee",
            "fields": {
                "email": "anon_funcs.partial_email(email::text)::citext",
                "phone": "anon_funcs.random_phone('+7')",
                "ssn":   "anon_funcs.digest(ssn, 'salt', 'sha256')"
            }
        },
        {
            "schema": "billing",
            "table":  "customer",
            "raw_sql": "SELECT id, 'anon' AS name, email, tax_id, billing_address, metadata, created_at FROM billing.customer"
        },
        {
            "schema_mask": "^audit$",
            "table_mask":  "^login_attempt$",
            "fields": {
                "username":  "anon_funcs.digest(username, 'salt', 'md5')",
                "client_ip": "'0.0.0.0'::inet"
            }
        }
    ],
    "dictionary_exclude": [
        {"schema": "quirks", "table": "with_nulls"}
    ]
}
