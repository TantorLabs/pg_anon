{
    "skip_rules": [
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"},
        {"schema": "constraints_quirks"},
        {"schema": "fdw_ext"}
    ],
    "data_func": {
        "anyelement": [
            {
                "scan_func": "anon_ext.is_email_value",
                "anon_func": "anon_funcs.partial_email(\"%s\")",
                "n_count": 1
            }
        ]
    },
    "field": {"rules": []},
    "data_regex": {"rules": []},
    "data_const": {"constants": []},
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 's', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 's', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 's', 'md5')::citext"
    }
}
