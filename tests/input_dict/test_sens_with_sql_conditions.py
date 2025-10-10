{
    "dictionary": [
        {
            "schema": "schm_other_4",
            "table": "goods",
            "fields": {
                "title": "anon_funcs.digest(\"title\", 'salt_word', 'sha256')",
                "description": "anon_funcs.digest(\"description\", 'salt_word', 'sha256')",
                "quantity": "10",
            },
            "sql_condition":
            """
            WHERE release_date > NOW() - '15 days'::interval
            AND valid_until < NOW() + '15 days'::interval
            """
        }
    ],
    "dictionary_exclude": [
        {
            "schema_mask": "*",
            "table_mask": "*",
        }
    ]
}
