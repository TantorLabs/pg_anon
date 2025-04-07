{
    "dictionary": [
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'",
            "fields": {
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "schm_customer",
            "table": "customer_company",
            "fields": {
                "phone": "anon_funcs.digest(\"phone\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "schm_mask_ext_exclude_2",
            "table": "card_numbers",
            "fields": {
                "другое_поле": "anon_funcs.digest(\"другое_поле\", 'salt_word', 'md5')",
                "usd": "anon_funcs.noise(\"usd\", 10)",
                "имя_поля": "anon_funcs.digest(\"имя_поля\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "schm_customer",
            "table": "customer_manager",
            "fields": {
                "phone": "anon_funcs.digest(\"phone\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "public",
            "table": "inn_info",
            "fields": {
                "inn": "LPAD((10000000 + ROW_NUMBER() OVER (ORDER BY inn))::TEXT, 8, '0')"
            }
        },
        {
            "schema": "schm_other_2",
            "table": "tbl_test_anon_functions",
            "fields": {
                "fld_5_email": "anon_funcs.digest(\"fld_5_email\", 'salt_word', 'md5')"
            }
        }
    ]
}