from dataclasses import dataclass
from typing import Optional, Callable, List

from pg_anon.common.enums import ResultCode


class PgAnonResult:
    params = None  # JSON
    result_code = ResultCode.UNKNOWN
    result_data = None
    elapsed = None


@dataclass
class FieldInfo:
    nspname: str
    relname: str
    column_name: str
    type: str
    oid: int
    attnum: int
    obj_id: str
    tbl_id: str
    rule: Optional[Callable] = None  # uses for --mode=create-dict with --prepared-sens-dict-file
    dict_file_name: Optional[List] = None  # uses for --mode=view-fields
