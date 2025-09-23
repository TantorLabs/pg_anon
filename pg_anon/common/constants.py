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

TYPE_ALIASES = {
    "varbit": "bit varying",
    "bool": "boolean",

    "char": "character",
    "varchar": "character varying",

    "int": "integer",
    "int4": "integer",
    "int2": "smallint",
    "int8": "bigint",

    "float": "double precision",
    "float8": "double precision",
    "float4": "real",
    "decimal": "numeric",

    "serial2": "smallserial",
    "serial4": "serial",
    "serial8": "bigserial",

    "time": "time without time zone",
    "timetz": "time with time zone",

    "timestamp": "timestamp without time zone",
    "timestamptz": "timestamp with time zone",
}

SECRET_RUN_OPTIONS = [
    "db_user_password"
]

TRACEBACK_LINES_COUNT = 100
