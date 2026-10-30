{
    "include_rules": [
        {"schema": "hr", "table": "employee", "fields": ["email"]}
    ],
    "data_func": {
        "anyelement": [
            {
                "scan_func_per_field": "anon_ext.is_seqscan_sync_off",
                "anon_func": "anon_funcs.digest(\"%s\"::text, 's', 'md5')"
            }
        ]
    },
    "field": {"rules": []},
    "data_regex": {"rules": []},
    "data_const": {"constants": []},
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {}
}
