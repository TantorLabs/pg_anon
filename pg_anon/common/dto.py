import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, UTC
from enum import Enum
from pathlib import Path
from typing import Optional, Callable, Dict, List, Union

from pg_anon.common.constants import SECRET_RUN_OPTIONS
from pg_anon.common.enums import ResultCode, AnonMode, VerboseOptions, ScanMode


@dataclass
class RunOptions:
    pg_anon_version: str
    internal_operation_id: str
    run_dir: str
    debug: bool
    config: Optional[str]
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_user_password: str
    db_passfile: str
    db_ssl_key_file: str
    db_ssl_cert_file: str
    db_ssl_ca_file: str
    mode: AnonMode
    verbose: VerboseOptions
    meta_dict_files: Optional[List[str]]
    db_connections_per_process: int
    processes: int
    pg_dump: str
    pg_restore: str
    output_dir: str
    input_dir: str
    dbg_stage_1_validate_dict: bool
    dbg_stage_2_validate_data: bool
    dbg_stage_3_validate_full: bool
    clear_output_dir: bool
    drop_custom_check_constr: bool
    seq_init_by_max_value: bool
    clean_db: bool
    drop_db: bool
    disable_checks: bool
    scan_mode: ScanMode
    output_sens_dict_file: str
    output_no_sens_dict_file: str
    prepared_sens_dict_files: Optional[List[str]]
    prepared_no_sens_dict_files: Optional[List[str]]
    partial_tables_dict_files: Optional[List[str]]
    partial_tables_exclude_dict_files: Optional[List[str]]
    scan_partial_rows: int
    view_only_sensitive_fields: bool
    schema_name: Optional[str]
    schema_mask: Optional[str]
    table_name: Optional[str]
    table_mask: Optional[str]
    fields_count: int
    limit: int
    offset: int
    application_name_suffix: Optional[str]
    version: bool
    json: bool
    save_dicts: bool

    def to_dict(self):
        return {
            k: v.value if isinstance(v, Enum) else v
            for k, v in asdict(self).items()
            if k not in SECRET_RUN_OPTIONS
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)



class PgAnonResult:
    run_options = None
    result_code = ResultCode.UNKNOWN
    result_data = None
    start_time = None
    end_time = None
    _elapsed = None
    _start_date = None
    _end_date = None
    _exception = None
    _traceback = None

    def start(self, run_options: RunOptions):
        self.run_options = run_options
        self.start_time = time.time()

    def fail(self, exception: Exception = None):
        from pg_anon.common.utils import exception_to_str

        self.end_time = time.time()
        self.result_code = ResultCode.FAIL
        self._exception = exception
        self._traceback = exception_to_str(self._exception)

    def complete(self):
        self.end_time = time.time()
        self.result_code = ResultCode.DONE

    def to_dict(self):
        return {
            "result_code": self.result_code.value,
            "started": self.start_time,
            "ended": self.end_time,
        }

    @property
    def elapsed(self):
        if not self._elapsed:
            if self.start_time is None or self.end_time is None:
                return None

            self._elapsed = round(self.end_time - self.start_time, 2)
        return self._elapsed
    
    @property
    def start_date(self) -> datetime:
        if not self._start_date:
            self._start_date = datetime.fromtimestamp(self.start_time, tz=UTC)
        return self._start_date
    
    @property
    def end_date(self) -> Optional[datetime]:
        if not self._end_date:
            if self.end_time is None:
                return None
            self._end_date = datetime.fromtimestamp(self.end_time, tz=UTC)
        return self._end_date

    @property
    def internal_operation_id(self) -> Optional[str]:
        if not self.run_options:
            return None
        return self.run_options.internal_operation_id

    @property
    def exception(self) -> Optional[Exception]:
        if not self._exception:
            return None
        return self._exception

    @property
    def error_message(self) -> Optional[str]:
        if not self._traceback:
            return None

        return self._traceback


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
        self.password = password
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

    # only in black and white lists cases
    partial_dump_schemas: Optional[List[str]] = None
    partial_dump_functions: Optional[List[str]] = None

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

            'partial_dump_schemas': self.partial_dump_schemas,
            'partial_dump_functions': self.partial_dump_functions,
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

        if self.partial_dump_schemas is None:
            del data['partial_dump_schemas']
            
        if self.partial_dump_functions is None:
            del data['partial_dump_functions']

        return data

    def _serialize_tables(self) -> Dict:
        data = [{k: v for k, v in table_data.items() if k in ("schema", "table")} for table_data in self.files.values()]
        return {"tables": data}

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

        self.partial_dump_schemas = data.get('partial_dump_schemas')
        self.partial_dump_functions = data.get('partial_dump_functions')

    def save_into_file(self, file_path: Path):
        from pg_anon.common.utils import save_json_file
        save_json_file(file_path, self._serialize_data())

    def save_dumped_tables_into_file(self, file_path: Path):
        from pg_anon.common.utils import save_json_file
        save_json_file(file_path, self._serialize_tables())

    def load_from_file(self, file_name: Union[str, Path]):
        file_name = Path(file_name)
        with open(file_name, "r") as metadata_file:
            data = json.loads(metadata_file.read())
            self._deserialize_data(data)
