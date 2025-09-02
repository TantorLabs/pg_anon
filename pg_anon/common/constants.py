ANON_UTILS_DB_SCHEMA_NAME = 'anon_funcs'
DEFAULT_HASH_FUNC = f"{ANON_UTILS_DB_SCHEMA_NAME}.digest(\"%s\", 'salt_word', 'md5')"

SERVER_SETTINGS = {
    "application_name": "pg_anon",
    "statement_timeout": "0",
    "lock_timeout": "0",
}

TRANSACTIONS_SERVER_SETTINGS = {
    "idle_in_transaction_session_timeout": "0",
    "idle_session_timeout": "0",
}

DEFAULT_EXCLUDED_SCHEMAS = [
    ANON_UTILS_DB_SCHEMA_NAME,
    "pg_catalog",
    "information_schema"
]
