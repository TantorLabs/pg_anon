{
    "data_regex": {
        "rules": [
            {
                "rule": r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",
                "anon_func": "anon_funcs.digest(\"%s\"::text, 'second_file', 'md5')::citext"
            }
        ]
    }
}
