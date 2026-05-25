{
    "include_rules": [
        {
            "schema": "hr",
            "table": "employee",
            "fields": ["email"]
        }
    ],
    "field": {"rules": []},
    "data_regex": {
        "rules": [
            r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+"""
        ]
    },
    "data_const": {"constants": []},
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 's', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 's', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 's', 'md5')::citext"
    }
}
