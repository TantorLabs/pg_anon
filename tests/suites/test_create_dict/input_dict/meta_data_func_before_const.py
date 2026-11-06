{
    "include_rules": [
        {"schema": "hr", "table": "employee", "fields": ["email"]}
    ],
    "data_func": {
        "anyelement": [
            {
                "scan_func": "anon_ext.is_email_value",
                "anon_func": "anon_funcs.digest(\"%s\"::text, 'data_func', 'md5')::citext",
                "n_count": 1
            }
        ]
    },
    "data_const": {
        "partial_constants": [
            {"rule": "@acme.com", "anon_func": "anon_funcs.digest(\"%s\"::text, 'partial_const', 'md5')::citext"}
        ]
    },
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {}
}
