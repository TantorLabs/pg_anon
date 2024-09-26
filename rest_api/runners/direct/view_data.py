from typing import List, Type

from pg_anon import MainRoutine
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.enums import ResultCode
from pg_anon.context import Context
from pg_anon.view_data import ViewDataMode
from rest_api.pydantic_models import ViewDataRequest, ViewDataContent
from rest_api.utils import write_dictionary_contents


class ViewDataRunner:
    request: ViewDataRequest
    cli_params: List[str] = None
    result: PgAnonResult = None
    _executor = Type[ViewDataMode]

    def __init__(self, request: ViewDataRequest):
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
        self.cli_params.append(
            f'--schema-name={self.request.schema_name}',
        )

        self.cli_params.append(
            f'--table-name={self.request.table_name}',
        )

    def _prepare_pagination_cli_params(self):
        if self.request.limit:
            self.cli_params.append(
                f'--limit={self.request.limit}',
            )

        if self.request.offset:
            self.cli_params.append(
                f'--offset={self.request.offset}',
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
        self._prepare_pagination_cli_params()
        self._prepare_json_cli_params()
        self._prepare_verbosity_cli_params()

    def _init_context(self):
        parser = Context.get_arg_parser()
        run_args = parser.parse_args(self.cli_params)
        self.context = MainRoutine(run_args).ctx

    def _init_executor(self):
        self._executor = ViewDataMode(
            self.context,
            need_raw_data=True
        )

    def _format_output(self) -> ViewDataContent:
        def _format_data_to_str(records: List[List[str]]):
            return [[str(data) for data in record] for record in records]

        rows_before = _format_data_to_str(self._executor.raw_data)
        rows_after = _format_data_to_str(self._executor.data)

        return ViewDataContent(
            schema_name=self.request.schema_name,
            table_name=self.request.table_name,
            field_names=self._executor.raw_field_names,
            total_rows_count=self._executor.rows_count,
            rows_before=rows_before,
            rows_after=rows_after,
        )

    async def run(self):
        self.result = await self._executor.run()
        await self._executor.get_rows_count()

        if not self.result or self.result.result_code == ResultCode.FAIL:
            raise RuntimeError('Operation not completed successfully')

        return self._format_output()
