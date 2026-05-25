{
    "include_rules": [
        {"schema_mask": "*", "fields": ["email"]},
        {"schema": "hr", "table": "employee", "fields": ["phone"]}
    ],
    "skip_rules": [
        {"schema_mask": "audit", "table_mask": ".*"},
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"},
        {"schema": "constraints_quirks"},
        {"schema": "fdw_ext"}
    ],
    "field": {
        "rules": ["^email$", "^phone$"]
    },
    "data_regex": {"rules": []},
    "data_const": {"constants": []},
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 's', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 's', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 's', 'md5')::citext"
    }
}
