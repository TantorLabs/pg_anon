{
    "include_rules": [
        {"schema": "hr", "table": "employee", "fields": ["first_name", "last_name", "email", "phone", "ssn"]},
        {"schema": "hr", "table": "department", "fields": ["name"]},
        {"schema": "hr", "table": "performance_review", "fields": ["notes"]},
        {"schema": "hr", "table": "salary_history", "fields": ["reason"]}
    ],
    "field": {
        "rules": [
            "^first_name$",
            {"rule": "^last_name$", "anon_func": "anon_funcs.digest(\"%s\", 'field_rule', 'md5')"}
        ],
        "constants": [
            {"rule": "ssn", "anon_func": "anon_funcs.digest(\"%s\", 'field_constant', 'md5')"}
        ]
    },
    "data_const": {
        "constants": [
            {"rule": "Engineering", "anon_func": "anon_funcs.digest(\"%s\", 'const_word', 'md5')"},
            {"rule": "Квартальная оценка", "anon_func": "anon_funcs.digest(\"%s\", 'const_phrase', 'md5')"}
        ],
        "partial_constants": [
            {"rule": "raise", "anon_func": "anon_funcs.digest(\"%s\", 'partial_const', 'md5')"}
        ]
    },
    "data_regex": {
        "rules": [
            {
                "rule": r"""^[A-Za-z]+_\d+$""",
                "anon_func": "anon_funcs.digest(\"%s\", 'regex_for_last_name', 'md5')"
            },
            {
                "rule": r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",
                "anon_func": "anon_funcs.digest(\"%s\"::text, 'regex_email', 'md5')::citext"
            },
            {
                "rule": r"""^\+7[0-9]{10}$""",
                "anon_func": "anon_funcs.digest(\"%s\", 'regex_phone', 'md5')"
            }
        ]
    },
    "sens_pg_types": ["text", "varchar", "citext"],
    "funcs": {
        "text":    "anon_funcs.digest(\"%s\", 'type_salt', 'md5')",
        "varchar": "anon_funcs.digest(\"%s\", 'type_salt', 'md5')",
        "citext":  "anon_funcs.digest(\"%s\"::text, 'type_salt', 'md5')::citext"
    }
}
