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


class ConnectionParams:
    host: str
    database: str
    port: int
    user: str

    password: Optional[str] = None
    passfile: Optional[str] = None

    ssl_cert_file: Optional[str] = None
    ssl_key_file: Optional[str] = None
    ssl_ca_file: Optional[str] = None

    ssl: Optional[str] = None
    ssl_min_protocol_version: Optional[str] = None

    def __init__(self, host: str, port: int, database: str, user: str,
                 password: Optional[str] = None, passfile: Optional[str] = None,
                 ssl_cert_file: Optional[str] = None, ssl_key_file: Optional[str] = None,
                 ssl_ca_file: Optional[str] = None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user

        if password:
            self.password = password

        if passfile:
            self.passfile = passfile

        if ssl_cert_file or ssl_key_file or ssl_ca_file:
            self.ssl = "on"
            self.ssl_min_protocol_version = "TLSv1.2"
            self.ssl_cert_file = ssl_cert_file
            self.ssl_key_file = ssl_key_file
            self.ssl_ca_file = ssl_ca_file

    def as_dict(self) -> dict:
        return self.__dict__
