{
    "include_rules": [
        {"schema": "hr", "table": "employee", "fields": ["phone"]}
    ],
    "data_regex": {
        "rules": [
            {"rule": r"""^\+7[0-9]{10}$""", "anon_func": "anon_funcs.digest(\"%s\", 'regex_first', 'md5')"},
            {"rule": r"""^\+[0-9]+$""", "anon_func": "anon_funcs.digest(\"%s\", 'regex_second', 'md5')"}
        ]
    },
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 'type_salt', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 'type_salt', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 'type_salt', 'md5')::citext"
    }
}
