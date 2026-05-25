{
    "skip_rules": [
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"},
        {"schema": "constraints_quirks"},
        {"schema": "fdw_ext"}
    ],
    "data_const": {
        "constants": ["Engineering", "Отдел аналитики"]
    },
    "field": {"rules": []},
    "data_regex": {"rules": []},
    "sens_pg_types": ["text", "varchar"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 's', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 's', 'md5')"
    }
}
