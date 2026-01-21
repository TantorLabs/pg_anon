from datetime import datetime
from typing import List, Any, Optional, Dict

from pydantic import BaseModel, Field, model_validator

from rest_api.enums import DumpMode, ScanMode, RestoreMode


#############################################
# Common
#############################################
class ErrorResponse(BaseModel):
    message: str


class Content(BaseModel):
    content: str

#############################################
# Handbooks
#############################################


class TaskStatus(BaseModel):
    id: int
    title: str
    slug: str


class ScanType(BaseModel):
    title: str
    slug: str


class DumpType(BaseModel):
    title: str
    slug: str


class RestoreType(BaseModel):
    title: str
    slug: str

#############################################
# DB Connections
#############################################


class DbConnection(BaseModel):
    id: int
    title: str
    slug: str

    host: str
    port: int
    database: str

    user: Optional[str] = None
    attributes: Optional[str] = None  # some custom attributes for integrations


class DbConnectionCreate(BaseModel):
    title: str
    slug: str

    host: str
    port: int
    database: str

    user: Optional[str] = None
    password: Optional[str] = None

    attributes: Optional[str] = None  # some custom attributes for integrations


class DbConnectionUpdate(BaseModel):
    title: Optional[str] = None
    slug: str

    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None

    user: Optional[str] = None
    password: Optional[str] = None

    attributes: Optional[str] = None  # some custom attributes for integrations


class DbCheckConnectionStatus(BaseModel):
    status: bool


class DbConnectionCredentials(BaseModel):
    user: str
    password: str


class DbConnectionFullCredentials(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str


#############################################
# Projects
#############################################


class Project(BaseModel):
    id: int
    title: str
    slug: str

    created: datetime
    updated: datetime

    custom_pg_dump_path: Optional[str] = None
    attributes: Optional[str] = None  # some custom attributes for integrations

    last_scan_run: Optional[datetime] = None  # Computed values. Not for manual edit
    last_scan_task_status_id: Optional[int] = None  # Computed values. Not for manual edit
    last_dump_run: Optional[datetime] = None  # Computed values. Not for manual edit
    last_dump_task_status_id: Optional[int] = None  # Computed values. Not for manual edit


class ProjectCreate(BaseModel):
    title: str
    custom_pg_dump_path: Optional[str] = None
    attributes: Optional[str] = None  # some custom attributes for integrations


class ProjectUpdate(BaseModel):
    title: Optional[str] = None
    custom_pg_dump_path: Optional[str] = None
    attributes: Optional[str] = None  # some custom attributes for integrations


#############################################
# Dictionaries
#############################################

class DictionaryShort(BaseModel):
    id: int
    title: str
    slug: str
    project_id: int

    type_id: int
    is_predefined: bool = False
    attributes: Optional[str] = None  # some custom attributes for integrations

    created: datetime
    updated: datetime

    scan_title: Optional[str] = None  # Computed values. Not for manual edit


class DictionaryDetailed(BaseModel):
    id: int
    title: str
    slug: str
    project_id: int

    type_id: int
    is_predefined: bool = False
    content: str
    attributes: Optional[str] = None  # some custom attributes for integrations

    created: datetime
    updated: datetime

    scan_title: Optional[str] = None  # Computed values. Not for manual edit


class DictionaryCreate(BaseModel):
    project_id: int

    title: str
    type_id: int
    content: str

    attributes: Optional[str] = None  # some custom attributes for integrations


class DictionaryUpdate(BaseModel):
    title: str
    content: str

    attributes: Optional[str] = None  # some custom attributes for integrations


class DictionaryDuplicate(BaseModel):
    title: str
    attributes: Optional[str] = None  # some custom attributes for integrations


#############################################
# Scan
#############################################

class Scan(BaseModel):
    id: int
    title: str
    slug: str
    project_id: int

    type_id: int
    depth: Optional[int] = None
    attributes: Optional[str] = None  # some custom attributes for integrations
    source_db: DbConnection

    input_meta_dict_titles: List[int]  # Computed values. Not for manual edit
    input_sens_dict_titles: Optional[List[int]] = None  # Computed values. Not for manual edit
    input_no_sens_dict_titles: Optional[List[int]] = None  # Computed values. Not for manual edit

    status_id: int  # Computed values. Not for manual edit
    created: datetime  # Computed values. Not for manual edit
    updated: datetime  # Computed values. Not for manual edit


class ScanCreate(BaseModel):
    project_id: int

    title: str
    slug: int

    type_id: int
    depth: Optional[int] = None
    source_db_id: int

    input_meta_dict_ids: List[int]
    input_sens_dict_ids: Optional[List[int]] = None
    input_no_sens_dict_ids: Optional[List[int]] = None

    output_sens_dict_name: str
    output_no_sens_dict_name: Optional[str] = None

    attributes: Optional[str] = None  # some custom attributes for integrations


#############################################
# Dump
#############################################


class Dump(BaseModel):
    id: int
    title: str
    slug: str
    project_id: int

    type_id: int
    source_db: DbConnection

    custom_path: Optional[str] = None
    attributes: Optional[str] = None  # some custom attributes for integrations

    input_sens_dict_titles: Optional[List[int]] = None  # Computed values. Not for manual edit

    status: str  # Computed values. Not for manual edit
    created: datetime  # Computed values. Not for manual edit
    updated: datetime  # Computed values. Not for manual edit
    size: Optional[str] = None  # Computed values. Not for manual edit


class DumpCreate(BaseModel):
    project_id: int
    source_db_id: int
    title: str

    type_id: int
    custom_path: Optional[str] = None

    input_sens_dict_ids: List[int]
    attributes: Optional[str] = None  # some custom attributes for integrations


#############################################
# Preview
#############################################


class Preview(BaseModel):
    id: int
    title: str
    slug: str
    project_id: int
    source_db: DbConnection

    input_sens_dict_titles: Optional[List[int]] = None  # Computed values. Not for manual edit
    attributes: Optional[str] = None  # some custom attributes for integrations

    created: datetime  # Computed values. Not for manual edit
    updated: datetime  # Computed values. Not for manual edit


class PreviewCreate(BaseModel):
    project_id: int
    title: str
    source_db_id: int

    input_sens_dict_ids: List[int]
    attributes: Optional[str] = None  # some custom attributes for integrations


class PreviewUpdate(BaseModel):
    title: Optional[str] = None
    source_db_id: Optional[int] = None

    input_sens_dict_ids: Optional[List[int]] = None
    attributes: Optional[str] = None  # some custom attributes for integrations


#############################################
# Stateless
#############################################
class DbConnectionParams(BaseModel):
    host: str
    port: int
    db_name: str

    user_login: str
    user_password: str


class StatelessRunnerRequest(BaseModel):
    operation_id: str
    db_connection_params: DbConnectionParams
    webhook_status_url: str
    webhook_metadata: Optional[Any] = None  # data what will be sent on webhook "as is"
    webhook_extra_headers: Optional[Dict[str, str]] = None
    webhook_verify_ssl: Optional[bool] = True


class StatelessRunnerResponse(BaseModel):
    operation_id: str
    internal_operation_id: Optional[str] = None
    status_id: int
    status: str
    webhook_metadata: Optional[Any] = None  # data what will be sent on webhook "as is"
    started: Optional[str] = None
    ended: Optional[str] = None
    error: Optional[str] = None
    run_options: Optional[Dict[str, Any]] = None


class DictionaryMetadata(BaseModel):
    name: str
    additional_info: Optional[str] = None  # specific data for integrations purposes


class DictionaryContent(DictionaryMetadata):
    content: str


#############################################
# Stateless | Scan
#############################################
class ScanRequest(StatelessRunnerRequest):
    type: ScanMode

    meta_dict_contents: List[DictionaryContent]
    sens_dict_contents: List[DictionaryContent] = Field(default_factory=list)
    no_sens_dict_contents: List[DictionaryContent] = Field(default_factory=list)

    need_no_sens_dict: bool = False

    depth: Optional[int] = None
    proc_count: Optional[int] = None
    proc_conn_count: Optional[int] = None
    save_dicts: bool = False


class ScanStatusResponse(StatelessRunnerResponse):
    sens_dict_content: Optional[str] = None
    no_sens_dict_content: Optional[str] = None


#############################################
# Stateless | Dump
#############################################
class DumpRequest(StatelessRunnerRequest):
    type: DumpMode
    sens_dict_contents: List[DictionaryContent]
    partial_tables_dict_contents: Optional[List[DictionaryContent]] = None
    partial_tables_exclude_dict_contents: Optional[List[DictionaryContent]] = None
    output_path: str
    validated_output_path: Optional[str] = Field(default=None, exclude=True)

    pg_dump_path: Optional[str] = None

    proc_count: Optional[int] = None
    proc_conn_count: Optional[int] = None
    save_dicts: bool = False
    ignore_privileges: bool = False

    @model_validator(mode="after")
    def validate_model(self):
        from rest_api.utils import get_full_dump_path

        if self.output_path:
            self.validated_output_path = get_full_dump_path(self.output_path)
        return self


class DumpStatusResponse(StatelessRunnerResponse):
    size: Optional[int] = None


class DumpDeleteRequest(BaseModel):
    path: str
    validated_path: Optional[str] = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def validate_model(self):
        from rest_api.utils import get_full_dump_path

        if self.path:
            self.validated_path = get_full_dump_path(self.path)
        return self


#############################################
# Stateless | Restore
#############################################
class RestoreRequest(StatelessRunnerRequest):
    type: RestoreMode
    input_path: str
    validated_input_path: Optional[str] = Field(default=None, exclude=True)
    partial_tables_dict_contents: Optional[List[DictionaryContent]] = None
    partial_tables_exclude_dict_contents: Optional[List[DictionaryContent]] = None
    pg_restore_path: Optional[str] = None
    proc_conn_count: Optional[int] = None
    drop_custom_check_constr: bool = False
    clean_db: bool = False
    drop_db: bool = False
    save_dicts: bool = False
    ignore_privileges: bool = False

    @model_validator(mode="after")
    def validate_model(self):
        from rest_api.utils import get_full_dump_path

        if self.clean_db and self.drop_db:
            raise ValueError("Only one of `clean_db` or `drop_db` can be set.")

        if self.input_path:
            self.validated_input_path = get_full_dump_path(self.input_path)

        return self


#############################################
# Stateless | Preview | View fields
#############################################
class ViewFieldsRequest(BaseModel):
    db_connection_params: DbConnectionParams
    sens_dict_contents: List[DictionaryContent]

    schema_name: Optional[str] = None
    schema_mask: Optional[str] = None
    table_name: Optional[str] = None
    table_mask: Optional[str] = None

    view_only_sensitive_fields: bool = False
    fields_limit_count: Optional[int] = None


class ViewFieldsContent(BaseModel):
    schema_name: str
    table_name: str
    field_name: str
    type: str
    dict_data: Optional[DictionaryMetadata] = None
    rule: Optional[str] = None


class ViewFieldsResponse(BaseModel):
    status_id: int
    status: str
    content: Optional[List[ViewFieldsContent]] = None


#############################################
# Stateless | Preview | View data
#############################################
class ViewDataRequest(BaseModel):
    db_connection_params: DbConnectionParams
    sens_dict_contents: List[DictionaryContent]

    schema_name: str
    table_name: str

    limit: int = 10
    offset: int = 0


class ViewDataContent(BaseModel):
    schema_name: str
    table_name: str
    field_names: List[str]
    total_rows_count: int
    rows_before: List[List[str]]
    rows_after: List[List[str]]


class ViewDataResponse(BaseModel):
    status_id: int
    status: str
    content: Optional[ViewDataContent] = None


#############################################
# Stateless | Operations
#############################################
class OperationDataResponse(BaseModel):
    run_status: Dict[str, Any]
    run_options: Dict[str, Any]
    dictionaries: Dict[str, Any]
    extra_data: Optional[Dict[str, Any]] = None

