{
    "field": {
        "rules": ["^email$", "^phone$"]
    },
    "skip_rules": [
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"}
    ],
    "sens_pg_types": ["text", "varchar", "citext", "numeric"],
    "funcs": {
        "default": "anon_funcs.digest(\"%s\"::text, 'default', 'sha256')"
    }
}
