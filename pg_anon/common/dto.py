import json
import ssl
import time
from dataclasses import asdict, dataclass
from datetime import datetime, UTC
from enum import Enum
from pathlib import Path
from typing import Any

from pg_anon.common.constants import (
    DEFAULT_DB_CONNECTIONS_PER_PROCESS,
    DEFAULT_PG_DUMP_PATH,
    DEFAULT_PG_RESTORE_PATH,
    DEFAULT_PROCESSES,
    DEFAULT_SCAN_PARTIAL_ROWS,
    SECRET_RUN_OPTIONS,
)
from pg_anon.common.enums import AnonMode, ResultCode, ScanMode, VerboseOptions


@dataclass
class RunOptions:
    pg_anon_version: str
    internal_operation_id: str
    run_dir: str
    mode: AnonMode

    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_user_password: str | None = None
    db_passfile: str | None = None
    db_ssl_key_file: str | None = None
    db_ssl_cert_file: str | None = None
    db_ssl_ca_file: str | None = None

    config: str | None = None
    application_name_suffix: str | None = None
    debug: bool = False
    verbose: VerboseOptions = VerboseOptions.INFO
    version: bool = False

    # I/O options (create-dict, dump, restore)
    db_connections_per_process: int = DEFAULT_DB_CONNECTIONS_PER_PROCESS
    processes: int = DEFAULT_PROCESSES
    save_dicts: bool = False

    # create-dict options
    meta_dict_files: list[str] | None = None
    prepared_no_sens_dict_files: list[str] | None = None
    output_sens_dict_file: str | None = None
    output_no_sens_dict_file: str | None = None
    scan_mode: ScanMode | None = None
    scan_partial_rows: int = DEFAULT_SCAN_PARTIAL_ROWS

    # dump options
    prepared_sens_dict_files: list[str] | None = None
    pg_dump: str = DEFAULT_PG_DUMP_PATH
    pg_dump_options: str | None = None
    allow_fdw_credentials: bool = False
    output_dir: str = ""
    clear_output_dir: bool = False
    dbg_stage_1_validate_dict: bool = False
    dbg_stage_2_validate_data: bool = False
    dbg_stage_3_validate_full: bool = False
    partial_tables_dict_files: list[str] | None = None
    partial_tables_exclude_dict_files: list[str] | None = None

    # restore options
    input_dir: str = ""
    pg_restore: str = DEFAULT_PG_RESTORE_PATH
    pg_restore_options: str | None = None
    keep_fdw_user_mappings: bool = False
    drop_custom_check_constr: bool = False
    seq_init_by_max_value: bool = False
    disable_checks: bool = False
    clean_db: bool = False
    drop_db: bool = False

    # dump, restore options
    ignore_privileges: bool = False
    schema_names: list[str] | None = None
    schema_masks: list[str] | None = None
    exclude_schema_names: list[str] | None = None
    exclude_schema_masks: list[str] | None = None

    # view-fields options
    view_only_sensitive_fields: bool = False
    fields_count: int | None = None
    schema_name: str | None = None
    schema_mask: str | None = None
    table_name: str | None = None
    table_mask: str | None = None
    orm_dict_file: str | None = None

    # view-data options
    limit: int | None = None
    offset: int | None = None

    # view-fields, view-data options
    json: bool = False

    def to_dict(self) -> dict:
        """Convert run options to a dictionary, excluding secret fields."""
        return {
            k: v.value if isinstance(v, Enum) else v for k, v in asdict(self).items() if k not in SECRET_RUN_OPTIONS
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize run options to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


class PgAnonResult:
    def __init__(self) -> None:
        self.run_options: RunOptions | None = None
        self.result_code: ResultCode = ResultCode.UNKNOWN
        self.result_data: dict | None = None
        self.start_time: float | None = None
        self.end_time: float | None = None
        self._elapsed: float | None = None
        self._start_date: datetime | None = None
        self._end_date: datetime | None = None
        self._exception: Exception | None = None
        self._traceback: str | None = None

    def start(self, run_options: RunOptions) -> None:
        """Record start time and associate run options with this result."""
        self.run_options = run_options
        self.start_time = time.time()

    def fail(self, exception: Exception | None = None) -> None:
        """Mark the result as failed and capture the exception traceback."""
        from pg_anon.common.utils import exception_to_str  # noqa: PLC0415

        self.end_time = time.time()
        self.result_code = ResultCode.FAIL
        self._exception = exception
        if exception is not None:
            self._traceback = exception_to_str(exception)

    def complete(self) -> None:
        """Mark the result as successfully completed."""
        self.end_time = time.time()
        self.result_code = ResultCode.DONE

    def to_dict(self) -> dict:
        """Convert the result to a dictionary with code and timestamps."""
        return {
            "result_code": self.result_code.value,
            "started": self.start_time,
            "ended": self.end_time,
        }

    @property
    def elapsed(self) -> float | None:
        """Return elapsed time in seconds, or None if not yet finished."""
        if not self._elapsed:
            if self.start_time is None or self.end_time is None:
                return None

            self._elapsed = round(self.end_time - self.start_time, 2)
        return self._elapsed

    @property
    def start_date(self) -> datetime | None:
        """Return the start time as a UTC datetime."""
        if not self._start_date:
            if self.start_time is None:
                return None
            self._start_date = datetime.fromtimestamp(self.start_time, tz=UTC)
        return self._start_date

    @property
    def end_date(self) -> datetime | None:
        """Return the end time as a UTC datetime, or None if not yet finished."""
        if not self._end_date:
            if self.end_time is None:
                return None
            self._end_date = datetime.fromtimestamp(self.end_time, tz=UTC)
        return self._end_date

    @property
    def internal_operation_id(self) -> str | None:
        """Return the internal operation ID from run options, or None."""
        if not self.run_options:
            return None
        return self.run_options.internal_operation_id

    @property
    def exception(self) -> Exception | None:
        """Return the captured exception, or None if no failure occurred."""
        if not self._exception:
            return None
        return self._exception

    @property
    def error_message(self) -> str | None:
        """Return the formatted traceback string, or None if no failure occurred."""
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
    rule: str | None = None  # uses for --mode=create-dict with --prepared-sens-dict-file
    dict_file_name: str | None = None  # uses for --mode=view-fields


class ConnectionParams:
    def __init__(  # noqa: PLR0913
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str | None = None,
        passfile: str | None = None,
        ssl_cert_file: str | None = None,
        ssl_key_file: str | None = None,
        ssl_ca_file: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.passfile = passfile
        self.ssl: ssl.SSLContext | None = None

        if ssl_cert_file or ssl_key_file or ssl_ca_file:
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            if ssl_ca_file:
                ssl_ctx.load_verify_locations(ssl_ca_file)
            else:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE
            if ssl_cert_file:
                ssl_ctx.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)
            self.ssl = ssl_ctx

    def as_dict(self) -> dict:
        """Return connection parameters as a dictionary."""
        return self.__dict__


DUMP_INFO_KEYS = (
    "pg_anon_version",
    "min_pg_anon_version",
    "created",
    "pg_version",
    "pg_dump_version",
    "dbg_stage_2_validate_data",
    "dbg_stage_3_validate_full",
)
STRUCTURE_FILE_KEYS = (
    *DUMP_INFO_KEYS,
    "schemas",
    "extensions",
    "event_triggers",
    "excluded_event_triggers",
    "constraints",
    "indexes",
    "partitions",
    "inherits",
    "dump_dependents_to_skip",
)
DATA_FILE_KEYS = (
    *DUMP_INFO_KEYS,
    "prepared_sens_dict_files",
    "seq_lastvals",
    "files",
    "total_rows",
    "db_size",
)


class Metadata:
    def __init__(self) -> None:
        self.pg_anon_version: str | None = None
        self.min_pg_anon_version: str | None = None
        self.created: str = ""
        self.pg_version: str = ""
        self.pg_dump_version: str = ""
        self.prepared_sens_dict_files: str = ""
        self.dbg_stage_2_validate_data: bool = False
        self.dbg_stage_3_validate_full: bool = False
        # only in data dumps cases
        self.sequences_last_values: dict | None = None
        self.views: dict | None = None
        self.indexes: dict | None = None
        self.constraints: dict | None = None
        self.files: dict[str, dict[str, Any]] | None = None
        self.schemas: list[str] | None = None
        self.total_rows: int | None = None
        self.db_size: int | None = None
        # only in black and white lists cases
        self.partial_dump_schemas: list[str] | None = None
        self.extensions: dict[str, dict[str, Any]] | None = None
        # backward compatibility: DDL of custom objects, only partial dumps of 1.12.0 and older have it
        self.partial_dump_types: list[str] | None = None
        self.partial_dump_domains: list[str] | None = None
        self.partial_dump_ranges: list[str] | None = None
        self.partial_dump_functions: list[str] | None = None
        self.partial_dump_casts: list[str] | None = None
        self.partial_dump_operators: list[str] | None = None
        self.partial_dump_aggregates: list[str] | None = None
        self.event_triggers: dict[str, dict[str, Any]] | None = None
        self.excluded_event_triggers: list[str] | None = None
        # object ("catalog OID:object OID") -> dump tables it depends on; dumps of 1.12.0 and older have none
        self.dependencies: dict[str, list[list[str]]] | None = None
        self.dump_dependents_to_skip: list[str] | None = None
        # partitioned tables and partitions of the dump with the partitioned tables above them, closest first
        self.partitions: list[dict[str, Any]] | None = None
        # INHERITS child tables of the dump with all the parent tables above them, closest first
        self.inherits: list[dict[str, Any]] | None = None

    def _serialize_dump_info(self) -> dict:
        return {
            "pg_anon_version": self.pg_anon_version,
            "min_pg_anon_version": self.min_pg_anon_version,
            "created": self.created,
            "pg_version": self.pg_version,
            "pg_dump_version": self.pg_dump_version,
            "dbg_stage_2_validate_data": self.dbg_stage_2_validate_data,
            "dbg_stage_3_validate_full": self.dbg_stage_3_validate_full,
        }

    def _serialize_structure(self) -> dict:
        from pg_anon.common.dependencies import pack_dependencies  # noqa: PLC0415

        data = {
            **self._serialize_dump_info(),
            "schemas": self.schemas,
            "extensions": self.extensions,
            "event_triggers": self.event_triggers,
            "constraints": self.constraints,
            "indexes": self.indexes,
            "partitions": self.partitions,
            "inherits": self.inherits,
            "dependencies": pack_dependencies(self.dependencies or {}),
            "dump_dependents_to_skip": self.dump_dependents_to_skip or [],
        }
        if self.excluded_event_triggers is not None:
            data["excluded_event_triggers"] = self.excluded_event_triggers
        return data

    def _serialize_data(self) -> dict:
        return {
            **self._serialize_dump_info(),
            "prepared_sens_dict_files": self.prepared_sens_dict_files,
            "seq_lastvals": self.sequences_last_values,
            "files": self.files,
            "total_rows": self.total_rows,
            "db_size": self.db_size,
        }

    def _serialize_tables(self) -> dict:
        if not self.files:
            return {"tables": []}
        data = [{k: v for k, v in table_data.items() if k in ("schema", "table")} for table_data in self.files.values()]
        return {"tables": data}

    def _deserialize_data(self, data: dict) -> None:
        self.pg_anon_version = data.get("pg_anon_version")
        self.min_pg_anon_version = data.get("min_pg_anon_version")
        self.created = data.get("created", "")
        self.pg_version = data.get("pg_version", "")
        self.pg_dump_version = data.get("pg_dump_version", "")
        self.db_size = data.get("db_size")
        self.prepared_sens_dict_files = data.get("prepared_sens_dict_files", "")
        self.dbg_stage_2_validate_data = data.get("dbg_stage_2_validate_data", False)
        self.dbg_stage_3_validate_full = data.get("dbg_stage_3_validate_full", False)

        self.sequences_last_values = data.get("seq_lastvals")
        self.views = data.get("views")
        self.indexes = data.get("indexes")
        self.constraints = data.get("constraints")

        self.files = data.get("files")
        self.schemas = data.get("schemas")
        self.total_rows = data.get("total_rows")

        self.extensions = data.get("extensions")
        self.partial_dump_schemas = data.get("partial_dump_schemas")
        self.partial_dump_types = data.get("partial_dump_types")
        self.partial_dump_domains = data.get("partial_dump_domains")
        self.partial_dump_ranges = data.get("partial_dump_ranges")
        self.partial_dump_functions = data.get("partial_dump_functions")
        self.partial_dump_casts = data.get("partial_dump_casts")
        self.partial_dump_operators = data.get("partial_dump_operators")
        self.partial_dump_aggregates = data.get("partial_dump_aggregates")
        self.event_triggers = data.get("event_triggers")
        self.excluded_event_triggers = data.get("excluded_event_triggers")
        self.dependencies = data.get("dependencies")
        self.dump_dependents_to_skip = data.get("dump_dependents_to_skip")
        self.partitions = data.get("partitions")
        self.inherits = data.get("inherits")

    def save_into_file(self, file_path: Path) -> None:
        """Save the description of the dumped data to a JSON file."""
        from pg_anon.common.utils import save_json_file  # noqa: PLC0415

        save_json_file(file_path, self._serialize_data())

    def save_structure_into_file(self, file_path: Path) -> None:
        """Save the description of the dumped structure to a JSON file without indents."""
        from pg_anon.common.utils import save_json_file  # noqa: PLC0415

        save_json_file(file_path, self._serialize_structure(), compact=True)

    def save_dumped_tables_into_file(self, file_path: Path) -> None:
        """Save the list of dumped tables to a JSON file."""
        from pg_anon.common.utils import save_json_file  # noqa: PLC0415

        save_json_file(file_path, self._serialize_tables())

    def load_from_dump_files(self, structure: dict | None, data: dict | None) -> None:
        """Load a dump of the current format: each file gives only its own keys."""
        from pg_anon.common.dependencies import unpack_dependencies  # noqa: PLC0415

        merged = {key: value for key, value in (data or {}).items() if key in DATA_FILE_KEYS}
        merged.update({key: value for key, value in (structure or {}).items() if key in STRUCTURE_FILE_KEYS})
        self._deserialize_data(merged)
        if structure is not None:
            self.dependencies = unpack_dependencies(structure["dependencies"])

    def load_from_file(self, file_name: str | Path) -> None:
        """Load metadata of a dump of pg_anon 1.12.0 or older from its only JSON file."""
        file_name = Path(file_name)
        with file_name.open() as metadata_file:
            data = json.loads(metadata_file.read())
            self._deserialize_data(data)
