{
    "dictionary": [
        {
            "schema": "hr",
            "table": "employee",
            "fields": {
                "first_name":  "anon_funcs.digest(first_name, 'salt', 'md5')",
                "last_name":   "anon_funcs.digest(last_name, 'salt', 'md5')",
                "email":       "(anon_funcs.digest(email::text, 'salt', 'md5') || '@anon.example')::citext",
                "phone":       "anon_funcs.random_phone('+7')",
                "ssn":         "left(anon_funcs.digest(ssn, 'salt', 'md5'), 11)",
                "birth_date":  "anon_funcs.dnoise(birth_date::timestamp, interval '6 months')::date",
                "salary":      "anon_funcs.noise(salary, 0.2)"
            }
        },
        {
            "schema": "billing",
            "table": "customer",
            "fields": {
                "name":   "anon_funcs.digest(name, 'salt', 'md5')",
                "email":  "(anon_funcs.digest(email::text, 'salt', 'md5') || '@anon.example')::billing.email_t",
                "tax_id": "left(anon_funcs.digest(tax_id, 'salt', 'md5'), 20)"
            }
        },
        {
            "schema": "billing",
            "table": "payment_card",
            "fields": {
                "cardholder_name": "'ANONYMIZED'",
                "pan_last4":       "'0000'"
            }
        },
        {
            "schema": "billing",
            "table": "invoice",
            "fields": {
                "notes": "'redacted'"
            }
        },
        {
            "schema": "audit",
            "table": "log_entry",
            "fields": {
                "client_ip":  "'0.0.0.0'::inet",
                "client_mac": "'00:00:00:00:00:00'::macaddr"
            }
        },
        {
            "schema": "audit",
            "table": "login_attempt",
            "fields": {
                "username":  "anon_funcs.digest(username, 'salt', 'md5')",
                "client_ip": "'0.0.0.0'::inet"
            }
        },
        {
            "schema": "content",
            "table": "comment",
            "fields": {
                "author_email": "(anon_funcs.digest(author_email::text, 'salt', 'md5') || '@anon.example')::citext"
            }
        },
        {
            "schema_mask": "^quirks$",
            "table_mask":  ".*",
            "fields": {
                "val": "'masked'"
            }
        }
    ],
    "dictionary_exclude": [
        {"schema": "analytics_archive"}
    ]
}
