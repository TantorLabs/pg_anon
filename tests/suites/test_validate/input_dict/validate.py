{
    "dictionary": [
        {
            "schema": "hr",
            "table": "employee",
            "fields": {
                "email": "(anon_funcs.digest(email::text, 'salt', 'md5') || '@anon.example')::citext",
                "phone": "anon_funcs.random_phone('+7')"
            }
        },
        {
            "schema": "billing",
            "table": "customer",
            "fields": {
                "name": "anon_funcs.digest(name, 'salt', 'md5')"
            }
        }
    ]
}
