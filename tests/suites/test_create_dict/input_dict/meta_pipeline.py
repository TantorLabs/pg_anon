{
    "field": {
        "rules": ["^salary$"]
    },
    "skip_rules": [
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"},
        {"schema": "constraints_quirks"},
        {"schema": "fdw_ext"}
    ],
    "data_regex": {"rules": []},
    "data_const": {"constants": []},
    "sens_pg_types": ["numeric"],
    "funcs": {
        "numeric": "anon_funcs.noise(\"%s\", 0.5)"
    }
}
