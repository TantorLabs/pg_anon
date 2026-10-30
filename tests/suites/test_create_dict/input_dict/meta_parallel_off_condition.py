{
    "include_rules": [
        {"schema": "hr", "table": "employee", "fields": ["phone"]},
        {"schema": "billing", "table": "customer", "fields": ["tax_id"]}
    ],
    "data_sql_condition": [
        {
            "schema": "hr",
            "table": "employee",
            "sql_condition": "WHERE id > 0"
        }
    ],
    "data_func": {
        "anyelement": [
            {
                "scan_func_per_field": "anon_ext.is_parallel_scan_off",
                "anon_func": "anon_funcs.digest(\"%s\", 's', 'md5')"
            }
        ]
    },
    "field": {"rules": []},
    "data_regex": {"rules": []},
    "data_const": {"constants": []},
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {}
}
