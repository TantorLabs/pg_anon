{
    "data_regex": {
        "rules": [
            r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+"""
        ]
    },
    "sens_pg_types": ["varchar"],
    "funcs": {
        "varchar": "anon_funcs.digest(\"%s\", 'salt', 'md5')"
    }
}
