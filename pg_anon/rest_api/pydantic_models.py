from typing import Any, Self

from pydantic import BaseModel, Field, model_validator

from pg_anon.rest_api.enums import DumpMode, RestoreMode, ScanMode


#############################################
# Handbooks
#############################################
class TaskStatus(BaseModel):
    id: int
    title: str


class ScanType(BaseModel):
    title: str


class DumpType(BaseModel):
    title: str


class RestoreType(BaseModel):
    title: str


#############################################
# Stateless | Common
#############################################
class ErrorResponse(BaseModel):
    error_code: str
    message: str


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
    webhook_metadata: Any | None = None  # data what will be sent on webhook "as is"
    webhook_extra_headers: dict[str, str] | None = None
    webhook_verify_ssl: bool = True
    web_debug: bool = False


class StatelessRunnerResponse(BaseModel):
    operation_id: str
    internal_operation_id: str | None = None
    status_id: int
    status: str
    webhook_metadata: Any | None = None  # data what will be sent on webhook "as is"
    started: str | None = None
    ended: str | None = None
    error: str | None = None
    error_code: str | None = None
    run_options: dict[str, Any] | None = None


class DictionaryMetadata(BaseModel):
    name: str
    additional_info: str | None = None  # specific data for integrations purposes


class DictionaryContent(DictionaryMetadata):
    content: str


#############################################
# Stateless | Scan
#############################################
class ScanRequest(StatelessRunnerRequest):
    type: ScanMode

    meta_dict_contents: list[DictionaryContent]
    sens_dict_contents: list[DictionaryContent] = Field(default_factory=list)
    no_sens_dict_contents: list[DictionaryContent] = Field(default_factory=list)

    need_no_sens_dict: bool = False

    depth: int | None = None
    proc_count: int | None = None
    proc_conn_count: int | None = None
    save_dicts: bool = False


class ScanStatusResponse(StatelessRunnerResponse):
    sens_dict_content: str | None = None
    no_sens_dict_content: str | None = None


#############################################
# Stateless | Dump
#############################################
class DumpRequest(StatelessRunnerRequest):
    type: DumpMode
    sens_dict_contents: list[DictionaryContent]
    partial_tables_dict_contents: list[DictionaryContent] | None = None
    partial_tables_exclude_dict_contents: list[DictionaryContent] | None = None
    output_path: str
    validated_output_path: str | None = Field(default=None, exclude=True)

    pg_dump_path: str | None = None
    pg_dump_options: str | None = None

    proc_count: int | None = None
    proc_conn_count: int | None = None
    save_dicts: bool = False
    ignore_privileges: bool = False

    @model_validator(mode="after")
    def validate_model(self) -> Self:
        """Validate and resolve the output path."""
        from pg_anon.rest_api.utils import get_full_dump_path  # noqa: PLC0415

        if self.output_path:
            self.validated_output_path = get_full_dump_path(self.output_path)
        return self


class DumpStatusResponse(StatelessRunnerResponse):
    size: int | None = None


class DumpDeleteRequest(BaseModel):
    path: str
    validated_path: str | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def validate_model(self) -> Self:
        """Validate and resolve the dump path."""
        from pg_anon.rest_api.utils import get_full_dump_path  # noqa: PLC0415

        if self.path:
            self.validated_path = get_full_dump_path(self.path)
        return self


#############################################
# Stateless | Restore
#############################################
class RestoreRequest(StatelessRunnerRequest):
    type: RestoreMode
    input_path: str
    validated_input_path: str | None = Field(default=None, exclude=True)
    partial_tables_dict_contents: list[DictionaryContent] | None = None
    partial_tables_exclude_dict_contents: list[DictionaryContent] | None = None
    pg_restore_path: str | None = None
    pg_restore_options: str | None = None
    proc_conn_count: int | None = None
    drop_custom_check_constr: bool = False
    clean_db: bool = False
    drop_db: bool = False
    save_dicts: bool = False
    ignore_privileges: bool = False

    @model_validator(mode="after")
    def validate_model(self) -> Self:
        """Validate restore options and resolve the input path."""
        from pg_anon.rest_api.utils import get_full_dump_path  # noqa: PLC0415

        if self.clean_db and self.drop_db:
            raise ValueError("Only one of `clean_db` or `drop_db` can be set.")

        if self.input_path:
            self.validated_input_path = get_full_dump_path(self.input_path)

        return self


#############################################
# Stateless | Preview
#############################################
class PreviewSchemasRequest(BaseModel):
    db_connection_params: DbConnectionParams
    schema_filter: str | None = None


class PreviewSchemasResponse(BaseModel):
    status_id: int
    status: str
    content: list[str] | None = None


class PreviewSchemaTablesRequest(BaseModel):
    db_connection_params: DbConnectionParams
    sens_dict_contents: list[DictionaryContent]

    limit: int = 20
    offset: int = 0

    table_filter: str | None = None
    view_only_sensitive_tables: bool = False


class PreviewFieldContent(BaseModel):
    field_name: str
    type: str
    is_sensitive: bool = False
    rule: str | None = None


class PreviewTableContent(BaseModel):
    table_name: str
    is_sensitive: bool
    is_excluded: bool
    fields: list[PreviewFieldContent] | None = None


class PreviewSchemaTablesResponse(BaseModel):
    status_id: int
    status: str
    content: list[PreviewTableContent] | None = None


#############################################
# Stateless | Preview | View fields
#############################################
class ViewFieldsRequest(BaseModel):
    db_connection_params: DbConnectionParams
    sens_dict_contents: list[DictionaryContent]

    schema_name: str | None = None
    schema_mask: str | None = None
    table_name: str | None = None
    table_mask: str | None = None

    view_only_sensitive_fields: bool = False
    fields_limit_count: int | None = None

    orm_dict_content: str | None = None


class ViewFieldsContent(BaseModel):
    schema_name: str
    table_name: str
    field_name: str
    type: str
    dict_data: DictionaryMetadata | None = None
    rule: str | None = None


class ViewFieldsResponse(BaseModel):
    status_id: int
    status: str
    content: list[ViewFieldsContent] | None = None


#############################################
# Stateless | Preview | View data
#############################################
class ViewDataRequest(BaseModel):
    db_connection_params: DbConnectionParams
    sens_dict_contents: list[DictionaryContent]

    schema_name: str
    table_name: str

    limit: int = 10
    offset: int = 0


class ViewDataContent(BaseModel):
    schema_name: str
    table_name: str
    field_names: list[str]
    total_rows_count: int
    rows_before: list[list[str]]
    rows_after: list[list[str]]


class ViewDataResponse(BaseModel):
    status_id: int
    status: str
    content: ViewDataContent | None = None


#############################################
# Stateless | Operations
#############################################
class OperationDataResponse(BaseModel):
    run_status: dict[str, Any]
    run_options: dict[str, Any]
    dictionaries: dict[str, Any]
    extra_data: dict[str, Any] | None = None
