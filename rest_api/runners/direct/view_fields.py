from typing import List, Type

from pg_anon.cli import build_run_options
from pg_anon.common.dto import PgAnonResult
from pg_anon.context import Context
from pg_anon.modes.view_fields import ViewFieldsMode
from rest_api.pydantic_models import ViewFieldsRequest, ViewFieldsContent
from rest_api.utils import write_dictionary_contents


class ViewFieldsRunner:
    request: ViewFieldsRequest
    cli_params: List[str] = None
    result: PgAnonResult = None
    _executor = Type[ViewFieldsMode]

    def __init__(self, request: ViewFieldsRequest):
        self.request = request
        self._prepare_cli_params()
        self._init_context()
        self._init_executor()

    def _prepare_db_credentials_cli_params(self):
        self.cli_params.extend([
            f'--db-host={self.request.db_connection_params.host}',
            f'--db-port={self.request.db_connection_params.port}',
            f'--db-user={self.request.db_connection_params.user_login}',
            f'--db-user-password={self.request.db_connection_params.user_password}',
            f'--db-name={self.request.db_connection_params.db_name}',
        ])

    def _prepare_dictionaries_cli_params(self):
        self._input_sens_dict_file_names = write_dictionary_contents(self.request.sens_dict_contents)
        self.cli_params.append(
            f"--prepared-sens-dict-file={','.join(self._input_sens_dict_file_names.keys())}"
        )

    def _prepare_filters_cli_params(self):
        if self.request.schema_name:
            self.cli_params.append(
                f'--schema-name={self.request.schema_name}',
            )

        if self.request.schema_mask:
            self.cli_params.append(
                f'--schema-mask={self.request.schema_mask}',
            )

        if self.request.table_name:
            self.cli_params.append(
                f'--table-name={self.request.table_name}',
            )

        if self.request.table_mask:
            self.cli_params.append(
                f'--table-mask={self.request.table_mask}',
            )

        if self.request.view_only_sensitive_fields:
            self.cli_params.append(
                f'--view-only-sensitive-fields',
            )

    def _prepare_limit_cli_params(self):
        if self.request.fields_limit_count:
            self.cli_params.append(
                f'--fields-count={self.request.fields_limit_count}',
            )

    def _prepare_json_cli_params(self):
        self.cli_params.append(
            f'--json',
        )

    def _prepare_verbosity_cli_params(self):
        self.cli_params.extend([
            "--verbose=debug",
            "--debug",
        ])

    def _prepare_cli_params(self):
        self.cli_params = []
        self._prepare_db_credentials_cli_params()
        self._prepare_dictionaries_cli_params()
        self._prepare_filters_cli_params()
        self._prepare_limit_cli_params()
        self._prepare_json_cli_params()
        self._prepare_verbosity_cli_params()

    def _init_context(self):
        options = build_run_options(self.cli_params)
        self.context = Context(options)

    def _init_executor(self):
        self._executor = ViewFieldsMode(self.context)

    def _format_output(self) -> List[ViewFieldsContent]:
        result = []
        for field in self._executor.fields:
            dict_data = None
            if field.dict_file_name != self._executor.empty_data_filler:
                dict_data = self._input_sens_dict_file_names[field.dict_file_name]

            field_rule = None
            if field.rule != self._executor.empty_data_filler:
                field_rule = field.rule

            result.append(
                ViewFieldsContent(
                    schema_name=field.nspname,
                    table_name=field.relname,
                    field_name=field.column_name,
                    type=field.type,
                    dict_data=dict_data,
                    rule=field_rule,
                )
            )

        return result

    async def run(self):
        await self._executor.run()
        return self._format_output()
