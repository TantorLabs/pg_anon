{
    "field": {"constants": ["email"]},
    "data_const": {"constants": [{"rule": "secret", "anon_func": "'F_CONST'"}]},
    "data_regex": {
        "rules": [
            r"""^\+7 \d{3} \d{3}-\d{2}-\d{2}$""",
            {"rule": r"""^S-\d+$""", "anon_func": "'F_REGEX'"}
        ]
    },
    "sens_pg_types": ["text"],
    "funcs": {"default": "md5(\"%s\")"}
}
