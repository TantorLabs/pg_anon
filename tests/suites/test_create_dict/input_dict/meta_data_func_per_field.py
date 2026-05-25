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
                "scan_func_per_field": "anon_ext.match_pii_field_name",
                "anon_func": "anon_funcs.digest(\"%s\", 'pii_marker', 'md5')"
            }
        ]
    },
    "field": {"rules": []},
    "data_regex": {"rules": []},
    "data_const": {"constants": []},
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {}
}
