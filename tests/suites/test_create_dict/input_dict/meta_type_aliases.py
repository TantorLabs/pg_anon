{
    "include_rules": [
        {"schema": "data_types", "table": "sample"}
    ],
    "field": {"rules": [".*"]},
    "data_regex": {"rules": []},
    "data_const": {"constants": []},
    "funcs": {
        "default":     "anon_funcs.digest(\"%s\"::text, 'default', 'md5')",
        "int":         "anon_funcs.digest(\"%s\"::text, 'int_alias', 'md5')",
        "int4":        "anon_funcs.digest(\"%s\"::text, 'int4_alias', 'md5')",
        "int8":        "anon_funcs.digest(\"%s\"::text, 'int8_alias', 'md5')",
        "bool":        "anon_funcs.digest(\"%s\"::text, 'bool_alias', 'md5')",
        "varchar":     "anon_funcs.digest(\"%s\"::text, 'varchar_alias', 'md5')",
        "text":        "anon_funcs.digest(\"%s\", 'text_alias', 'md5')",
        "timestamp":   "anon_funcs.digest(\"%s\"::text, 'timestamp_alias', 'md5')",
        "timestamptz": "anon_funcs.digest(\"%s\"::text, 'timestamptz_alias', 'md5')"
    }
}
