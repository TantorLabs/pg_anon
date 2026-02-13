{
    "dictionary": [
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'2",
            "fields": {
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'salt_word', 'md5')",
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'3",
            "fields": {
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')",
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "public",
            "table": "key_value",
            "fields": {
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')",
                "fld_value": "anon_funcs.digest(\"fld_value\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "_SCHM.$complex#имя;@&* a'",
            "table": "_TBL.$complex#имя;@&* a'",
            "fields": {
                "fld_key": "anon_funcs.digest(\"fld_key\", 'salt_word', 'md5')",
                "_FLD.$complex#имя;@&* a'": "anon_funcs.digest(\"_FLD.$complex#имя;@&* a'\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "public",
            "table": "tbl_100",
            "fields": {
                "другое_поле": "anon_funcs.digest(\"другое_поле\", 'salt_word', 'md5')",
                "amount": "anon_funcs.noise(\"amount\", 30)",
                "имя_поля": "anon_funcs.digest(\"имя_поля\", 'salt_word', 'md5')"
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
                "inn": "LPAD((10000000 + ROW_NUMBER() OVER (ORDER BY inn))::TEXT, 8, '0')"
            }
        },
        {
            "schema": "schm_other_2",
            "table": "tbl_test_anon_functions",
            "fields": {
                "fld_5_email": "anon_funcs.digest(\"fld_5_email\", 'salt_word', 'md5')"
            }
        },
        {
            "schema": "schm_other_4",
            "table": "partitioned_table",
            "fields": {
                "amount": "anon_funcs.noise(\"amount\", 10)"
            }
        },
        {
            "schema": "schm_other_4",
            "table": "partitioned_table_2025_01",
            "fields": {
                "amount": "anon_funcs.noise(\"amount\", 10)"
            }
        },
        {
            "schema": "schm_other_4",
            "table": "partitioned_table_2025_02",
            "fields": {
                "amount": "anon_funcs.noise(\"amount\", 10)"
            }
        },
        {
            "schema": "schm_other_4",
            "table": "partitioned_table_2025_03",
            "fields": {
                "amount": "anon_funcs.noise(\"amount\", 10)"
            }
        },
        {
            "schema": "schm_other_4",
            "table": "partitioned_table_default",
            "fields": {
                "amount": "anon_funcs.noise(\"amount\", 10)"
            }
        }
    ]
}