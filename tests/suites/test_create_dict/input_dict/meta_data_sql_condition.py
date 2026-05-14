{
    "skip_rules": [
        {"schema": "analytics_archive"},
        {"schema": "data_types"},
        {"schema": "security"},
        {"schema": "constraints_quirks"},
        {"schema": "fdw_ext"}
    ],
    "data_sql_condition": [
        {
            "schema": "hr",
            "table": "employee",
            "sql_condition": "WHERE phone IS NULL"
        }
    ],
    "field": {"rules": []},
    "data_regex": {
        "rules": [
            r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+"""
        ]
    },
    "data_const": {"constants": []},
    "sens_pg_types": ["text", "citext", "varchar"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 's', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 's', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 's', 'md5')::citext"
    }
}
