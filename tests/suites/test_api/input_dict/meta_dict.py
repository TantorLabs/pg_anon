{
    "field": {
        "rules": [
            "^email$",
            "^phone$",
            "^ssn$",
            "^tax_id$",
            "^cardholder_name$",
            "^pan_",
            "^birth_date$"
        ],
        "constants": []
    },
    "skip_rules": [
        {"schema": "hr", "table": "department", "fields": ["budget"]}
    ],
    "data_regex": {
        "rules": [
            r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",
            r"""^\+?[0-9]{10,15}$"""
        ]
    },
    "data_const": {
        "constants": []
    },
    "sens_pg_types": [
        "text",
        "varchar",
        "citext"
    ],
    "funcs": {
        "text":     "anon_funcs.digest(\"%s\", 'salt', 'md5')",
        "citext":   "anon_funcs.digest(\"%s\"::text, 'salt', 'md5')::citext",
        "varchar":  "anon_funcs.digest(\"%s\", 'salt', 'md5')",
        "numeric":  'anon_funcs.noise("%s", 0.1)',
        "date":     "anon_funcs.dnoise(\"%s\"::timestamp, interval '1 year')::date"
    }
}
