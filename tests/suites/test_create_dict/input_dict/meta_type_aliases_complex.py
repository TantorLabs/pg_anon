{
    "include_rules": [
        {"schema": "data_types", "table": "sample"}
    ],
    "field": {"rules": [".*"]},
    "data_regex": {"rules": []},
    "data_const": {"constants": []},
    "funcs": {
        "default":                                   "anon_funcs.digest(\"%s\"::text, 'default', 'md5')",
        "character varying  	(20)":                  "anon_funcs.digest(\"%s\", 'varchar20_complex', 'md5')",
        "double precision":                          "anon_funcs.digest(\"%s\"::text, 'double_complex', 'md5')",
        "timestamp        	 		(3) without time zone": "anon_funcs.digest(\"%s\"::text, 'timestamp3_complex', 'md5')",
        "time        	 		(3) with time zone":         "anon_funcs.digest(\"%s\"::text, 'timetz3_complex', 'md5')"
    }
}
