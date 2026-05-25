{
    "field": {
        "rules": [
            "^email$",
            "^phone$",
            "^ssn$",
            "^tax_id$",
            "^cardholder_name$",
            "^pan_",
            "^client_ip$",
            "^birth_date$"
        ],
        "constants": [
            "secret",
            "password"
        ]
    },
    "skip_rules": [
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"},
        {"schema": "hr", "table": "department", "fields": ["budget"]}
    ],
    "data_regex": {
        "rules": [
            r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",
            r"""^\+?[0-9]{10,15}$""",
            r"""^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}$""",
            r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"""
        ]
    },
    "data_const": {
        "constants": ["secret", "redacted"]
    },
    "sens_pg_types": [
        "text",
        "varchar",
        "citext",
        "inet",
        "macaddr",
        "uuid"
    ],
    "funcs": {
        "text":     "anon_funcs.digest(\"%s\", 'salt', 'md5')",
        "citext":   "anon_funcs.digest(\"%s\"::text, 'salt', 'md5')::citext",
        "varchar":  "anon_funcs.digest(\"%s\", 'salt', 'md5')",
        "inet":     "'0.0.0.0'::inet",
        "macaddr":  "'00:00:00:00:00:00'::macaddr",
        "uuid":     "uuid_generate_v4()",
        "numeric":  'anon_funcs.noise("%s", 0.1)',
        "date":     "anon_funcs.dnoise(\"%s\"::timestamp, interval '1 year')::date",
        "integer":  "anon_funcs.random_int_between(1, 1000)"
    }
}
