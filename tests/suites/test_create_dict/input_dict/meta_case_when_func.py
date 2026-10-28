{
    "field": {
        "rules": ["^phone$"]
    },
    "sens_pg_types": ["varchar"],
    "funcs": {
        "varchar": "CASE WHEN \"%s\" IS NULL THEN NULL ELSE anon_funcs.partial(\"%s\", 2, '0000000000', 2) END"
    }
}
