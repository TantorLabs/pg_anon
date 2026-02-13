from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RUNS_BASE_DIR = BASE_DIR / 'runs'

LOGS_DIR_NAME = 'logs'
LOGS_FILE_NAME = 'logs.log'
SAVED_RUN_OPTIONS_FILE_NAME = 'run_options.json'
SAVED_RUN_STATUS_FILE_NAME = 'run_status.json'
SAVED_DICTS_INFO_FILE_NAME = 'saved_dicts_info.json'

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
    "pg_catalog",
    "information_schema"
]

BASE_TYPE_ALIASES = {
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
    "dec": "numeric",

    "serial2": "smallserial",
    "serial4": "serial",
    "serial8": "bigserial",

    "time": "time",
    "timetz": "time with time zone",

    "timestamp": "timestamp",
    "timestamptz": "timestamp with time zone",
}

SENS_PG_TYPES = ["text", "character", "varchar", "mvarchar", "json", "integer", "bigint"]

SECRET_RUN_OPTIONS = [
    "db_user_password"
]

TRACEBACK_LINES_COUNT = 100
