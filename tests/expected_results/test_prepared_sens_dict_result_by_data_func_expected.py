{
    "dictionary": [
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'",
            "fields": {
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'salt_word', 'md5')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "public",
            "table": "key_value",
            "fields": {
                "fld_value": "anon_funcs.digest(\"fld_value\", 'salt_word', 'md5')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "public",
            "table": "tbl_100",
            "fields": {
                "другое_поле": "anon_funcs.digest(\"другое_поле\", 'salt_word', 'md5')",
                "amount": "anon_funcs.noise(\"amount\", 10)",
                "имя_поля": "anon_funcs.digest(\"имя_поля\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "schm_mask_ext_exclude_2",
            "table": "other_ext_tbl_2",
            "fields": {
                "val_2": "anon_funcs.digest(\"val_2\", 'salt_word', 'md5')",
                "val_1": "anon_funcs.digest(\"val_1\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'3",
            "fields": {
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'salt_word', 'md5')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "schm_customer",
            "table": "customer_manager",
            "fields": {
                "phone": "anon_funcs.digest(\"phone\", 'salt_word', 'md5')",
                "email": "anon_funcs.partial_email(\"%s\")"
            }
        },
        {
            "schema": "schm_mask_ext_exclude_2",
            "table": "card_numbers",
            "fields": {
                "val": "anon_funcs.digest(\"val\", 'salt_word', 'md5')",
                "другое_поле": "anon_funcs.digest(\"другое_поле\", 'salt_word', 'md5')",
                "usd": "anon_funcs.noise(\"usd\", 10)",
                "имя_поля": "anon_funcs.digest(\"имя_поля\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'2",
            "fields": {
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'salt_word', 'md5')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "schm_customer",
            "table": "customer_company",
            "fields": {
                "email": "anon_funcs.partial_email(\"%s\")",
                "phone": "anon_funcs.digest(\"phone\", 'salt_word', 'md5')",
                "inn": "anon_funcs.random_inn()"
            }
        },
        {
            "schema": "public",
            "table": "contracts",
            "fields": {
                "amount": "anon_funcs.noise(\"amount\", 10)",
                "contract_expires": "anon_funcs.dnoise(\"contract_expires\",  interval '6 month')",
                "details": "anon_funcs.digest(\"details\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "public",
            "table": "inn_info",
            "fields": {
                "inn": "anon_funcs.random_inn()"
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