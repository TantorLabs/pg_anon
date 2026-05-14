{
    "skip_rules": [
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"},
        {"schema": "constraints_quirks"},
        {"schema": "fdw_ext"}
    ],
    "data_const": {
        "partial_constants": ["@acme.com", "@example.org"]
    },
    "field": {"rules": []},
    "data_regex": {"rules": []},
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 's', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 's', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 's', 'md5')::citext"
    }
}
