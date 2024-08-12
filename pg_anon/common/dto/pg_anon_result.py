from pg_anon.common.enums import ResultCode


class PgAnonResult:
    params = None  # JSON
    result_code = ResultCode.UNKNOWN
    result_data = None
    elapsed = None
