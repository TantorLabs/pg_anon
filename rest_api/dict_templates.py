TEMPLATE_META_DICT = """{
    "field": {  # Which fields to anonymize without scanning the content
        "rules": [  # List of regular expressions to search for fields by name
            # "^fld_5_em",
            # "^amount"
        ],
        "constants": [  # List of constant field names
            # "usd",
            # "name"
        ]
    },
    "skip_rules": [  # List of schemas, tables, and fields to skip
        # {
        #     # possibly some schema or table contains a lot of data that is not worth scanning. Skipped objects will not be automatically included in the resulting dictionary. Masks are not supported in this object.
        #     "schema": "schm_mask_ext_exclude_2",  # Schema specification is mandatory
        #     "table": "card_numbers",  # Optional. If there is no "table", the entire schema will be skipped.
        #     "fields": ["val_skip"]  # Optional. If there are no "fields", the entire table will be skipped.
        # }
    ],
    "include_rules": [ # List of schemas, tables, and fields which will be scanning
        # {
        #     # possibly you need specific fields for scanning or you can debug some functions on specific field
        #     "schema": "schm_other_2", # Required. Schema specification is mandatory
        #     "table": "tbl_test_anon_functions", # Optional. If there is no "table", the entire schema will be included.
        #     "fields": ["fld_5_email"] # Optional. If there are no "fields", the entire table will be included.
        # }
    ],
    "data_regex": {  # List of regular expressions to search for sensitive data
        # "rules": [
        #     \"\"\"[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+\"\"\",  # email
        #     "7?[\d]{10}"  # phone 7XXXXXXXXXX
        # ]
    },
    "data_const": {
        # # List of constants in lowercase, upon detection of which the field will be included in the resulting dictionary. If a text field contains a value consisting of several words, this value will be split into words, converted to lowercase, and matched with the constants from this list. Words shorter than 5 characters are ignored. Search is performed using set.intersection
        # "constants": [  # When reading the meta-dictionary, the values of this list are placed in a set container
        #     "simpson",
        #     "account"
        # ],
        # # List of partial constants. If field value has substring from this list, it will considered as sensitive
        # "partial_constants": [ # When reading the meta-dictionary, the values of this list are placed in a set container
        #     "@example.com",
        #     "login_"
        # ]
    },
    "data_func": { # List of functions for specific field types
        # "text": [ # Field type, which will be checked by functions bellow. Can use custom types. Also, can use common type "anyelement" for all field types. Rules for "anyelement" will be added for all types rules after their own rules.
        #     {
        #         "scan_func": "my_custom_functions.check_by_users_table", # Function for scanning field value. Scan function has fixed call signature: (value, schema_name, table_name, field_name). Also , this function must return boolean result.
        #         "anon_func": "anon_funcs.digest(\"%s\", 'salt_word', 'md5')", # Function will be called for anonymization in dump step
        #         "n_count": 100, # How many times "scan_func" have to returns "True" by values in one field. If this count will be reached, then this field will be anonymized by "anon_func"
        #     },
        # ],
    },
    "data_sql_condition": [ # List of rules for define data sampling for specific tables by custom conditions
        # {
        #    "schema": "schm_mask_ext_exclude_2", # Can use "schema" for full name matching or "schema_mask" for regexp matching. Required one of them
        #    "table_mask": "*", # Can use "table" for full name matching or "table_mask" for regexp matching. Required one of them
        #    "sql_condition": # Condition in raw SQL format. For example, we need data sample created by 2024 year
        #    \"\"\"
        #    WHERE created > '2024-01-01' AND created < '2024-12-31'
        #    \"\"\"
        # }
    ],
    "sens_pg_types": [
        # # List of field types which should be checked (other types won't be checked). If this massive is empty program set default SENS_PG_TYPES = ["text", "integer", "bigint", "character", "json"]
        # "text",
        # "integer",
        # "bigint",
        # "varchar",  # better write small names, because checker find substrings in original name. For example types varchar(3) contains varchar, so varchar(3) will be checked in program.
        # "json"
    ],
    "funcs": {  # List of field types (int, text, ...) and functions for anonymization
        # # If a certain field is found during scanning, a function listed in this list will be used according to its type.
        # "text": "anon_funcs.digest(\"%s\", 'salt_word', 'md5')",
        # "numeric": "anon_funcs.noise(\"%s\", 10)",
        # "timestamp": "anon_funcs.dnoise(\"%s\",  interval '6 month')"
    }
}
"""

TEMPLATE_SENS_DICT = """
{
	"dictionary": [ # List of schemas, tables, fields and their rules for anonymization in dump step
		# {
		# 	"schema":"some_schema",
		# 	"table":"some_tbl",
		# 	"fields": {
     	#		"email":"'***'"
		# 	}
		# }
	],
	"dictionary_exclude": [ # List of schemas and tables or their masks, which need to exclude from dump
		# {
		# 	"schema_mask": "*",
		# 	"table_mask": "*",
		# }
	],
}
"""

TEMPLATE_NO_SENS_DICT = """
{
    "no_sens_dictionary": [ # List of schemas, tables, fields for fast rescan. This fields will be excluded for data scanning
        {
            "schema": "schm_customer",
            "table": "customer_company",
            "fields": [
                "email",
                "company_name",
                "site"
            ]
        }
    ]
}
"""
