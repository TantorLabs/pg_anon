from datetime import datetime
from typing import Union, List, Dict

from pydantic import BaseModel


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
    id: int
    title: str
    slug: str


class DumpType(BaseModel):
    id: int
    title: str
    slug: str


class DictionaryType(BaseModel):
    id: int
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

    user: Union[str, None] = None
    attributes: Union[str, None] = None  # some custom attributes for integrations


class DbConnectionCreate(BaseModel):
    title: str
    slug: str

    host: str
    port: int
    database: str

    user: Union[str, None] = None
    password: Union[str, None] = None

    attributes: Union[str, None] = None  # some custom attributes for integrations


class DbConnectionUpdate(BaseModel):
    title: Union[str, None] = None
    slug: str

    host: Union[str, None] = None
    port: Union[int, None] = None
    database: Union[str, None] = None

    user: Union[str, None] = None
    password: Union[str, None] = None

    attributes: Union[str, None] = None  # some custom attributes for integrations


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

    custom_pg_dump_path: Union[str, None] = None
    attributes: Union[str, None] = None  # some custom attributes for integrations

    last_scan_run: Union[datetime, None] = None  # Computed values. Not for manual edit
    last_scan_task_status_id: Union[int, None] = None  # Computed values. Not for manual edit
    last_dump_run: Union[datetime, None] = None  # Computed values. Not for manual edit
    last_dump_task_status_id: Union[int, None] = None  # Computed values. Not for manual edit


class ProjectCreate(BaseModel):
    title: str
    custom_pg_dump_path: Union[str, None] = None
    attributes: Union[str, None] = None  # some custom attributes for integrations


class ProjectUpdate(BaseModel):
    title: Union[str, None] = None
    custom_pg_dump_path: Union[str, None] = None
    attributes: Union[str, None] = None  # some custom attributes for integrations


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
    attributes: Union[str, None] = None  # some custom attributes for integrations

    created: datetime
    updated: datetime

    scan_title: Union[str, None] = None  # Computed values. Not for manual edit


class DictionaryDetailed(BaseModel):
    id: int
    title: str
    slug: str
    project_id: int

    type_id: int
    is_predefined: bool = False
    content: str
    attributes: Union[str, None] = None  # some custom attributes for integrations

    created: datetime
    updated: datetime

    scan_title: Union[str, None] = None  # Computed values. Not for manual edit


class DictionaryCreate(BaseModel):
    project_id: int

    title: str
    type_id: int
    content: str

    attributes: Union[str, None] = None  # some custom attributes for integrations


class DictionaryUpdate(BaseModel):
    title: str
    content: str

    attributes: Union[str, None] = None  # some custom attributes for integrations


class DictionaryDuplicate(BaseModel):
    title: str
    attributes: Union[str, None] = None  # some custom attributes for integrations


#############################################
# Scan
#############################################

class Scan(BaseModel):
    id: int
    title: str
    slug: str
    project_id: int

    type_id: int
    depth: Union[int, None] = None
    attributes: Union[str, None] = None  # some custom attributes for integrations
    source_db: DbConnection

    input_meta_dict_titles: List[int]  # Computed values. Not for manual edit
    input_sens_dict_titles: Union[List[int], None] = None  # Computed values. Not for manual edit
    input_no_sens_dict_titles: Union[List[int], None] = None  # Computed values. Not for manual edit

    status_id: int  # Computed values. Not for manual edit
    created: datetime  # Computed values. Not for manual edit
    updated: datetime  # Computed values. Not for manual edit


class ScanCreate(BaseModel):
    project_id: int

    title: str
    slug: int

    type_id: int
    depth: Union[int, None] = None
    source_db_id: int

    input_meta_dict_ids: List[int]
    input_sens_dict_ids: Union[List[int], None] = None
    input_no_sens_dict_ids: Union[List[int], None] = None

    output_sens_dict_name: str
    output_no_sens_dict_name: Union[str, None] = None

    attributes: Union[str, None] = None  # some custom attributes for integrations


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

    custom_path: Union[str, None] = None
    attributes: Union[str, None] = None  # some custom attributes for integrations

    input_sens_dict_titles: Union[List[int], None] = None  # Computed values. Not for manual edit

    status: str  # Computed values. Not for manual edit
    created: datetime  # Computed values. Not for manual edit
    updated: datetime  # Computed values. Not for manual edit
    size: Union[str, None] = None  # Computed values. Not for manual edit


class DumpCreate(BaseModel):
    project_id: int
    source_db_id: int
    title: str

    type_id: int
    custom_path: Union[str, None] = None

    input_sens_dict_ids: List[int]
    attributes: Union[str, None] = None  # some custom attributes for integrations


#############################################
# Preview
#############################################


class Preview(BaseModel):
    id: int
    title: str
    slug: str
    project_id: int
    source_db: DbConnection

    input_sens_dict_titles: Union[List[int], None] = None  # Computed values. Not for manual edit
    attributes: Union[str, None] = None  # some custom attributes for integrations

    created: datetime  # Computed values. Not for manual edit
    updated: datetime  # Computed values. Not for manual edit


class PreviewCreate(BaseModel):
    project_id: int
    title: str
    source_db_id: int

    input_sens_dict_ids: List[int]
    attributes: Union[str, None] = None  # some custom attributes for integrations


class PreviewUpdate(BaseModel):
    title: Union[str, None] = None
    source_db_id: Union[int, None] = None

    input_sens_dict_ids: Union[List[int], None] = None
    attributes: Union[str, None] = None  # some custom attributes for integrations


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


class DictionaryMetadata(BaseModel):
    name: str
    additional_info: Union[str, None] = None  # specific data for integrations purposes


class DictionaryContent(DictionaryMetadata):
    content: str


#############################################
# Stateless | Scan
#############################################
class ScanRequest(StatelessRunnerRequest):
    type_id: int

    meta_dict_contents: List[DictionaryContent]
    sens_dict_contents: Union[List[DictionaryContent], None] = None
    no_sens_dict_contents: Union[List[DictionaryContent], None] = None

    need_no_sens_dict: bool = False

    depth: Union[int, None] = None
    proc_count: Union[int, None] = None
    proc_conn_count: Union[int, None] = None


class ScanStatusResponse(BaseModel):
    operation_id: str
    status_id: int
    sens_dict_content: Union[str, None] = None
    no_sens_dict_content: Union[str, None] = None


#############################################
# Stateless | Dump
#############################################
class DumpRequest(StatelessRunnerRequest):
    type_id: int
    sens_dict_contents: List[DictionaryContent]
    output_path: str
    pg_dump_path: Union[str, None] = None

    proc_count: Union[int, None] = None
    proc_conn_count: Union[int, None] = None


class DumpStatusResponse(BaseModel):
    operation_id: str
    status_id: int
    size: Union[int, None] = None


class DumpDeleteRequest(BaseModel):
    path: str


#############################################
# Stateless | Preview | View fields
#############################################
class ViewFieldsRequest(BaseModel):
    db_connection_params: DbConnectionParams
    sens_dict_contents: List[DictionaryContent]

    schema_name: Union[str, None] = None
    schema_mask: Union[str, None] = None
    table_name: Union[str, None] = None
    table_mask: Union[str, None] = None

    view_only_sensitive_fields: bool = False
    fields_limit_count: Union[int, None] = None


class ViewFieldsContent(BaseModel):
    schema_name: str
    table_name: str
    field_name: str
    type: str
    dict: Union[DictionaryMetadata, None] = None
    rule: Union[str, None] = None


class ViewFieldsResponse(BaseModel):
    status_id: int
    content: Union[List[ViewFieldsContent], None] = None


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
    content: Union[ViewDataContent, None] = None
