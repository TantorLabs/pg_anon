from typing import List

from pg_anon.common.enums import AnonMode
from rest_api.enums import DumpModeHandbook
from rest_api.pydantic_models import DumpRequest
from rest_api.runners.background import BaseRunner
from rest_api.utils import write_dictionary_contents, get_full_dump_path


class DumpRunner(BaseRunner):
    mode: str = AnonMode.DUMP.value
    request: DumpRequest
    short_dump_path: str
    full_dump_path: str

    def __init__(self, request: DumpRequest):
        super().__init__(request)
        self._set_mode()

    def _set_mode(self):
        if self.request.type_id == DumpModeHandbook.FULL:
            self.mode = AnonMode.DUMP.value
        elif self.request.type_id == DumpModeHandbook.STRUCT:
            self.mode = AnonMode.SYNC_STRUCT_DUMP.value
        elif self.request.type_id == DumpModeHandbook.DATA:
            self.mode = AnonMode.SYNC_DATA_DUMP.value

    def _prepare_dictionaries_cli_params(self):
        input_sens_dict_file_names = list(write_dictionary_contents(self.request.sens_dict_contents).keys())
        self.cli_params.append(f"--prepared-sens-dict-file={','.join(input_sens_dict_file_names)}")

        if self.request.partial_tables_dict_contents:
            input_partial_tables_dict_file_names = list(
                write_dictionary_contents(self.request.partial_tables_dict_contents).keys()
            )
            self.cli_params.append(
                f"--partial-tables-dict-file={','.join(input_partial_tables_dict_file_names)}"
            )

        if self.request.partial_tables_exclude_dict_contents:
            input_partial_tables_exclude_dict_file_names = list(
                write_dictionary_contents(self.request.partial_tables_exclude_dict_contents).keys()
            )
            self.cli_params.append(
                f"--partial-tables-exclude-dict-file={','.join(input_partial_tables_exclude_dict_file_names)}"
            )

    def _prepare_dump_path_cli_params(self):
        self.short_dump_path = self.request.output_path.lstrip("/")
        self.full_dump_path = get_full_dump_path(self.short_dump_path)
        self.cli_params.extend([
            f'--output-dir={self.full_dump_path}',
            '--clear-output-dir',
        ])

    def _prepare_parallelization_cli_params(self):
        if self.request.proc_count:
            self.cli_params.append(
                f'--processes={self.request.proc_count}'
            )

        if self.request.proc_conn_count:
            self.cli_params.append(
                f'--db-connections-per-process={self.request.proc_conn_count}'
            )

    def _prepare_pg_dump_cli_params(self):
        if self.request.pg_dump_path:
            self.cli_params.append(
                f'--pg-dump={self.request.pg_dump_path}'
            )

    def _prepare_cli_params(self):
        super()._prepare_cli_params()
        self._prepare_dictionaries_cli_params()
        self._prepare_dump_path_cli_params()
        self._prepare_parallelization_cli_params()
        self._prepare_pg_dump_cli_params()
        self._prepare_verbosity_cli_params()
