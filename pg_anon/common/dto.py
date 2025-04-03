import json
import time
from dataclasses import dataclass
from typing import Optional, Callable, Dict, List

from pg_anon.common.enums import ResultCode


class PgAnonResult:
    params = None  # JSON
    result_code = ResultCode.UNKNOWN
    result_data = None
    start_time = None
    end_time = None
    _elapsed = None

    def start(self):
        self.start_time = time.time()

    def fail(self):
        self.end_time = time.time()
        self.result_code = ResultCode.FAIL

    def complete(self):
        self.end_time = time.time()
        self.result_code = ResultCode.DONE

    @property
    def elapsed(self):
        if not self._elapsed:
            if self.start_time is None or self.end_time is None:
                return None

            self._elapsed = round(self.end_time - self.start_time, 2)
        return self._elapsed


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
    dict_file_name: Optional[str] = None  # uses for --mode=view-fields


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


class Metadata:
    created: str
    pg_version: str
    pg_dump_version: str

    dictionary_content_hash: Dict[str, str]
    prepared_sens_dict_files: str
    dbg_stage_2_validate_data: bool = False
    dbg_stage_3_validate_full: bool = False

    # only in data dumps cases
    sequences_last_values: Optional[Dict] = None
    files: Optional[Dict[str, Dict[str, str]]] = None
    total_tables_size: Optional[int] = None
    total_rows: Optional[int] = None
    db_size: Optional[int] = None

    # only in struct dump cases
    schemas: Optional[List[str]] = None

    def _serialize_data(self) -> Dict:
        data = {
            'created': self.created,
            'pg_version': self.pg_version,
            'pg_dump_version': self.pg_dump_version,
            'dictionary_content_hash': self.dictionary_content_hash,
            'prepared_sens_dict_files': self.prepared_sens_dict_files,
            'dbg_stage_2_validate_data': self.dbg_stage_2_validate_data,
            'dbg_stage_3_validate_full': self.dbg_stage_3_validate_full,

            'seq_lastvals': self.sequences_last_values,
            'files': self.files,
            'total_tables_size': self.total_tables_size,
            'total_rows': self.total_rows,
            'db_size': self.db_size,

            'schemas': self.schemas,
        }

        if self.sequences_last_values is None:
            del data['seq_lastvals']
        if self.files is None:
            del data['files']
        if self.total_tables_size is None:
            del data['total_tables_size']
        if self.total_rows is None:
            del data['total_rows']
        if self.db_size is None:
            del data['db_size']

        if self.schemas is None:
            del data['schemas']

        return data

    def _deserialize_data(self, data: Dict):
        self.created = data.get('created')
        self.pg_version = data.get('pg_version')
        self.pg_dump_version = data.get('pg_dump_version')
        self.db_size = data.get('db_size')
        self.dictionary_content_hash = data.get('dictionary_content_hash')
        self.prepared_sens_dict_files = data.get('prepared_sens_dict_files')
        self.dbg_stage_2_validate_data = data.get('dbg_stage_2_validate_data')
        self.dbg_stage_3_validate_full = data.get('dbg_stage_3_validate_full')

        self.sequences_last_values = data.get('seq_lastvals')
        self.files = data.get('files')
        self.total_tables_size = data.get('total_tables_size')
        self.total_rows = data.get('total_rows')

        self.schemas = data.get('schemas')

    def save_into_file(self, file_name: str):
        data = self._serialize_data()
        with open(file_name, "w", encoding='utf-8') as out_file:
            out_file.write(json.dumps(data, indent=4, ensure_ascii=False))

    def load_from_file(self, file_name: str):
        with open(file_name, "r") as metadata_file:
            data = json.loads(metadata_file.read())
            self._deserialize_data(data)
