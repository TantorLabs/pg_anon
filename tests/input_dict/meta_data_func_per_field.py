{
    "skip_rules": [
        {"schema": "schm_customer"},
        {"schema": "schm_mask_include_1"},
        {"schema": "schm_mask_exclude_1"},
        {"schema": "schm_mask_ext_include_2"},
        {"schema": "schm_mask_ext_exclude_2"},
        {"schema": "columnar_internal"}
    ],
    "data_func": {
        "anyelement": [
            {
                "scan_func_per_field": "test_anon_funcs.test_check_by_fts_is_include_organization_title",
                "anon_func": "anon_funcs.digest(\"%s\", 'scan_func_per_field', 'md5')",
            },
        ],
    }
}
