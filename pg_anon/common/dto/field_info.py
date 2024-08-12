from dataclasses import dataclass
from typing import Optional, Callable, List


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
    hash_func: Optional[Callable] = None  # uses for --mode=create-dict with --prepared-sens-dict-file
    dict_file_name: Optional[List] = None  # uses for --mode=view-fields
