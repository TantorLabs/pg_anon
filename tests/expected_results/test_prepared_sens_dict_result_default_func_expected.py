{
    "dictionary": [
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'",
            "fields": {
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'by_default_func', 'sha256')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "public",
            "table": "key_value",
            "fields": {
                "fld_value": "anon_funcs.digest(\"fld_value\", 'by_default_func', 'sha256')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "public",
            "table": "tbl_100",
            "fields": {
                "другое_поле": "anon_funcs.digest(\"другое_поле\", 'by_default_func', 'sha256')",
                "amount": "anon_funcs.digest(\"amount\", 'by_default_func', 'sha256')",
                "имя_поля": "anon_funcs.digest(\"имя_поля\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "schm_mask_ext_exclude_2",
            "table": "other_ext_tbl_2",
            "fields": {
                "val_2": "anon_funcs.digest(\"val_2\", 'by_default_func', 'sha256')",
                "val_1": "anon_funcs.digest(\"val_1\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'3",
            "fields": {
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'by_default_func', 'sha256')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "schm_customer",
            "table": "customer_manager",
            "fields": {
                "phone": "anon_funcs.digest(\"phone\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "schm_mask_ext_exclude_2",
            "table": "card_numbers",
            "fields": {
                "val": "anon_funcs.digest(\"val\", 'by_default_func', 'sha256')",
                "другое_поле": "anon_funcs.digest(\"другое_поле\", 'by_default_func', 'sha256')",
                "usd": "anon_funcs.digest(\"usd\", 'by_default_func', 'sha256')",
                "имя_поля": "anon_funcs.digest(\"имя_поля\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'2",
            "fields": {
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'by_default_func', 'sha256')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "schm_other_3",
            "table": "data_types_test",
            "fields": {
                "field_type_int8": "anon_funcs.digest(\"field_type_int8\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "schm_customer",
            "table": "customer_company",
            "fields": {
                "phone": "anon_funcs.digest(\"phone\", 'by_default_func', 'sha256')",
                "inn": "anon_funcs.digest(\"inn\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "public",
            "table": "contracts",
            "fields": {
                "amount": "anon_funcs.digest(\"amount\", 'by_default_func', 'sha256')",
                "contract_expires": "anon_funcs.digest(\"contract_expires\", 'by_default_func', 'sha256')",
                "details": "anon_funcs.digest(\"details\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "public",
            "table": "inn_info",
            "fields": {
                "inn": "anon_funcs.digest(\"inn\", 'by_default_func', 'sha256')"
            }
        },
        {
            "schema": "schm_other_2",
            "table": "tbl_test_anon_functions",
            "fields": {
                "fld_5_email": "anon_funcs.digest(\"fld_5_email\", 'by_default_func', 'sha256')"
            }
        }
    ]
}