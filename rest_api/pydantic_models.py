from datetime import datetime
from typing import Union, List

from pydantic import BaseModel


#############################################
# Common
#############################################
class ErrorResponse(BaseModel):
    code: int
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



################################################################# nEw LoGiC

# db connection params
class DbConnectionParams(BaseModel):
    host: str
    port: int
    db_name: str

    user_login: Union[str, None] = None
    user_password: Union[str, None] = None


# scan
## scanning request
class ScanRequest(BaseModel):
    operation_id: str #UUID
    type_id: int
    depth: int
    db_connection_params: DbConnectionParams
    proc_cnt: Union[int, None] = None
    proc_conn_cnt: Union[int, None] = None

    input_meta_dict_contents: List[str]
    input_sens_dict_contents: Union[List[str], None] = None
    input_no_sens_dict_contents: Union[List[str], None] = None
## scanning response = HTTP_OK (200)
## scan status request = operation_id (UUID)

## scan status response
class ScanStatusResponse(BaseModel):
    status_id: int
    output_sens_dict_content: Union[str, None] = None
    output_no_sens_dict_content: Union[str, None] = None


# dump
## dump request
class DumpRequest(BaseModel):
    operation_id: str #UUID
    type_id: int
    db_connection_params: DbConnectionParams
    output_path: str
    input_sens_dict_contents: List[str]
## dump response = HTTP_OK(200)

## dump status request = operation_id (UUID)

## dump status response
class DumpStatusResponse(BaseModel):
    status_id: int
    size: Union[int, None] = None


# preview
## preview request
class PreviewRequest(BaseModel):
    operation_id: str #UUID
    db_connection_params: DbConnectionParams
    input_sens_dict_contents: List[str]
## preview response 
class PreviewResponse(BaseModel):
    status_id: int
    data_before:Union[str, None] = None
    data_after:Union[str, None] = None