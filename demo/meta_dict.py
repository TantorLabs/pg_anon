{
    "skip_rules": [
        {"schema": "shop", "table": "product"},
    ],
    "sens_pg_types": ["varchar", "text", "numeric", "date"],
    "field": {
        "rules": [
            "^full_name$",
            "^cardholder_name$",
            "^email$",
            "^phone$",
            "^ssn$",
            "^card_number$",
            "^birth_date$",
            "^salary$",
        ],
    },
    # finds shop.customer_order.note, whose name gives nothing away.
    # The e-mail pattern is unanchored — an address anywhere in the value is
    # enough; a card number has to be the whole value.
    "data_regex": {
        "rules": [
            r"""[A-Za-z0-9._-]+@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",  # e-mail
            r"""^\d{4}-\d{4}-\d{4}-\d{4}$""",  # payment card number
        ]
    },
    # A rule is picked by column type: exact type with its length first, then the
    # bare type, then "default". Column widths are chosen so that every kind of
    # value gets its own rule and keeps the shape of the original.
    "funcs": {
        "varchar(11)": "anon_funcs.partial(\"%s\", 0, 'XXX-XX-', 4)",  # ssn
        "varchar(19)": "anon_funcs.partial(\"%s\", 0, '****-****-****-', 4)",  # card
        "varchar(20)": "anon_funcs.random_phone('+1')",
        "varchar(100)": "lower(anon_funcs.random_string(8)) || '@example.com'",
        "varchar(120)": "anon_funcs.random_in(array['Nora Fisher', 'Paul Adler', 'Rita Lang', 'Simon Falk', 'Vera Roth'])",
        "text": "regexp_replace(\"%s\", '[^ ]+@[^ ]+', 'customer@example.com', 'g')",  # keeps the text readable
        "numeric": "round(anon_funcs.noise(\"%s\", 0.2), 2)",  # +/- 20%
        "date": "anon_funcs.dnoise(\"%s\", interval '3 years')",
        # fallback for sensitive types without a rule of their own: a stable
        # hash, 64 characters long. Unused here — every type above is covered.
        # The salt is a placeholder: a deterministic hash keeps relations but is
        # brute-forceable for low-entropy values, so a real one belongs outside
        # the dictionary you commit.
        "default": "anon_funcs.digest(\"%s\", 'MySecretSaltWord', 'sha256')",
    },
}
