{
    "field": {
        "rules": ["^email$", "^phone$", "^ssn$", "^tax_id$", "^cardholder_name$"]
    },
    "skip_rules": [
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"}
    ],
    "data_regex": {
        "rules": [
            r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",
            r"""^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}$"""
        ]
    },
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 'salt', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 'salt', 'md5')::citext",
        "varchar": "anon_funcs.digest(\"%s\", 'salt', 'md5')"
    }
}
