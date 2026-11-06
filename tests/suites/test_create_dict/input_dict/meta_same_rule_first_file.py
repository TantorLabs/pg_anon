{
    "include_rules": [
        {"schema": "hr", "table": "employee", "fields": ["email"]}
    ],
    "data_regex": {
        "rules": [r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+"""]
    },
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 'type_salt', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 'type_salt', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 'type_salt', 'md5')::citext"
    }
}
