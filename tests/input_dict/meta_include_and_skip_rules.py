{
    "skip_rules": [
        {
            "schema_mask": "*",
            "table": "customer_company",
            "fields": ["inn"]
        },
        {
            "schema_mask": "mask",
            "fields": ["val"]
        },
        {
            "schema_mask": "*",
            "table_mask": "complex",
            "fields": ["fld_key"],
        },
    ],
    "include_rules": [
        {
            "schema_mask": "*",
            "fields": ["email", "inn", "phone", "val", "site"]
        },
        {
            "schema_mask": "*",
            "table": "_TBL.$complex#имя;@&* a'",
        },
        {
            "schema_mask": "mask",
            "table_mask": "^card",
        },
        {
            "schema": "schm_other_2",
            "table_mask": "anon",
        },
    ]
}
