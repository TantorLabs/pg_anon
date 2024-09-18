from datetime import datetime
from typing import Union, List,Optional

from pydantic import BaseModel,Field


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
    host: str = Field(..., serialization_alias="host")
    port: int = Field(..., serialization_alias="port")
    db_name: str = Field(..., serialization_alias="dbName")

    user_login:str = Field(..., serialization_alias="userLogin")
    user_password: str = Field(..., serialization_alias="userPassword")


# scan
## scanning request
class ScanRequest(BaseModel):
    operation_id: str  = Field(..., serialization_alias="operationID")
    type_id: int = Field(..., serialization_alias="typeID")
    depth: int  = Field(..., serialization_alias="depth")
    db_connection_params: DbConnectionParams  = Field(..., serialization_alias="dbConnectionParams")
    proc_count: Optional[int] = Field(serialization_alias="procCount", default=None)
    proc_conn_count: Optional[int] = Field(serialization_alias="procConnCount", default=None)

    meta_dict_contents: List[str] = Field(..., serialization_alias="metaDictContents")
    sens_dict_contents: Optional[List[str]] = Field(serialization_alias="sensDictContents", default=None)
    no_sens_dict_contents: Optional[List[str]] = Field(serialization_alias="noSensDictContents", default=None)
## scanning response = HTTP_OK (200)

## scan status response
class ScanStatusResponse(BaseModel):
    operation_id: str  = Field(..., serialization_alias="operationID")
    status_id: int = Field(..., serialization_alias="statusID")
    sens_dict_content: Optional[str] = Field(serialization_alias="sensDictContent", default=None)
    no_sens_dict_content: Optional[str] = Field(serialization_alias="noSensDictContent", default=None)


# dump
## dump request
class DumpRequest(BaseModel):
    operation_id: str = Field(..., serialization_alias="operationID")
    type_id: int = Field(..., serialization_alias="typeID")
    db_connection_params: DbConnectionParams  = Field(..., serialization_alias="dbConnectionParams")
    output_path: str = Field(..., serialization_alias="outputPath")
    sens_dict_contents: List[str] = Field(..., serialization_alias="sensDictContents")
## dump response = HTTP_OK(200)


## dump status response
class DumpStatusResponse(BaseModel):
    operation_id: str  = Field(..., serialization_alias="operationID")
    status_id: int = Field(..., serialization_alias="statusID")
    size: Optional[int] = Field(serialization_alias="size", default=None)

# preview
## preview request
class PreviewRequest(BaseModel):
    operation_id: str  = Field(..., serialization_alias="operationID")
    db_connection_params: DbConnectionParams = Field(..., serialization_alias="dbConnectionParams")
    sens_dict_contents: List[str] = Field(..., serialization_alias="sensDictContents")
    
## preview response 
class PreviewDataColumn(BaseModel):
    name: str  = Field(..., serialization_alias="name")
    type_col: str  = Field(..., serialization_alias="type")
    rule: str  = Field(..., serialization_alias="rule")
    example_data_before: Optional[str]  = Field(serialization_alias="exampleDataBefore", default=None)
    example_data_after: Optional[str]  = Field(serialization_alias="exampleDataAfter", default=None)

class PreviewData(BaseModel):
    schema_name: str  = Field(..., serialization_alias="schemaName")
    table_name: str  = Field(..., serialization_alias="tableName")
    columns: List[PreviewDataColumn]  = Field(..., serialization_alias="columns")
    rows_before: List[str] = Field(..., serialization_alias="rowsBefore")
    rows_after: List[str] = Field(..., serialization_alias="rowsAfter")
    total_rows_count: int = Field(..., serialization_alias="totalRowsCount")

class PreviewResponse(BaseModel):
    status_id: int = Field(..., serialization_alias="statusID")
    preview_data: Optional[List[PreviewData]] = Field(serialization_alias="previewData", default=None)