{
    "dictionary": [
        {
            "schema_mask": "^hr$",
            "table_mask":  "^employee$",
            "fields": {
                "email":  "'masked@example.com'::citext",
                "phone":  "'+70000000000'",
                "salary": "0"
            }
        },
        {
            "schema_mask": "^billing$",
            "table_mask":  "^payment_card$",
            "fields": {
                "cardholder_name": "'ANON'",
                "pan_last4":       "'0000'"
            }
        },
        {
            "schema": "billing",
            "table":  "customer",
            "fields": {
                "name":   "'CUSTOMER'",
                "tax_id": "'0000000000'"
            }
        }
    ]
}
