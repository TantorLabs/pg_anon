{
    "dictionary": [
        {
            "schema": "_SCHM.$complex#\u0438\u043c\u044f;@&* a'",
            "table": "_TBL.$complex#\u0438\u043c\u044f;@&* a'",
            "fields": {
                "_FLD.$complex#\u0438\u043c\u044f;@&* a'": "anon_funcs.digest(\"_FLD.$complex#\u0438\u043c\u044f;@&* a'\", 'salt_word', 'md5')",
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
                "\u0434\u0440\u0443\u0433\u043e\u0435_\u043f\u043e\u043b\u0435": "anon_funcs.digest(\"\u0434\u0440\u0443\u0433\u043e\u0435_\u043f\u043e\u043b\u0435\", 'salt_word', 'md5')",
                "amount": "anon_funcs.noise(\"amount\", 10)",
                "\u0438\u043c\u044f_\u043f\u043e\u043b\u044f": "anon_funcs.digest(\"\u0438\u043c\u044f_\u043f\u043e\u043b\u044f\", 'salt_word', 'md5')"
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
            "schema": "_SCHM.$complex#\u0438\u043c\u044f;@&* a'",
            "table": "_TBL.$complex#\u0438\u043c\u044f;@&* a'3",
            "fields": {
                "_FLD.$complex#\u0438\u043c\u044f;@&* a'": "anon_funcs.digest(\"_FLD.$complex#\u0438\u043c\u044f;@&* a'\", 'salt_word', 'md5')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')"
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
            "schema": "schm_mask_ext_exclude_2",
            "table": "card_numbers",
            "fields": {
                "val": "anon_funcs.digest(\"val\", 'salt_word', 'md5')",
                "\u0434\u0440\u0443\u0433\u043e\u0435_\u043f\u043e\u043b\u0435": "anon_funcs.digest(\"\u0434\u0440\u0443\u0433\u043e\u0435_\u043f\u043e\u043b\u0435\", 'salt_word', 'md5')",
                "usd": "anon_funcs.noise(\"usd\", 10)",
                "\u0438\u043c\u044f_\u043f\u043e\u043b\u044f": "anon_funcs.digest(\"\u0438\u043c\u044f_\u043f\u043e\u043b\u044f\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "_SCHM.$complex#\u0438\u043c\u044f;@&* a'",
            "table": "_TBL.$complex#\u0438\u043c\u044f;@&* a'2",
            "fields": {
                "_FLD.$complex#\u0438\u043c\u044f;@&* a'": "anon_funcs.digest(\"_FLD.$complex#\u0438\u043c\u044f;@&* a'\", 'salt_word', 'md5')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')"
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
            "schema": "schm_customer",
            "table": "customer_company",
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