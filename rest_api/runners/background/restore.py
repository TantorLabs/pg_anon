from typing import List

from pg_anon.common.enums import AnonMode
from rest_api.enums import RestoreModeHandbook
from rest_api.pydantic_models import RestoreRequest
from rest_api.runners.background import BaseRunner
from rest_api.utils import get_full_dump_path, write_dictionary_contents


class RestoreRunner(BaseRunner):
    mode: str = AnonMode.RESTORE.value
    request: RestoreRequest
    short_dump_path: str
    full_input_path: str

    def __init__(self, request: RestoreRequest):
        super().__init__(request)
        self._set_mode()

    def _set_mode(self):
        if self.request.type_id == RestoreModeHandbook.FULL:
            self.mode = AnonMode.RESTORE.value
        elif self.request.type_id == RestoreModeHandbook.STRUCT:
            self.mode = AnonMode.SYNC_STRUCT_RESTORE.value
        elif self.request.type_id == RestoreModeHandbook.DATA:
            self.mode = AnonMode.SYNC_DATA_RESTORE.value

    def _prepare_dictionaries_cli_params(self):
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

    def _prepare_input_dump_path_cli_params(self):
        self.short_input_path = self.request.input_path.lstrip("/")
        self.full_input_path = get_full_dump_path(self.short_input_path)
        self.cli_params.extend([
            f'--input-dir={self.full_input_path}',
        ])

    def _prepare_parallelization_cli_params(self):
        if self.request.proc_conn_count:
            self.cli_params.append(
                f'--db-connections-per-process={self.request.proc_conn_count}'
            )

    def _prepare_pg_restore_cli_params(self):
        if self.request.pg_restore_path:
            self.cli_params.append(
                f'--pg-restore={self.request.pg_restore_path}'
            )

    def _prepare_additional_cli_params(self):
        if self.request.drop_custom_check_constr:
            self.cli_params.append(
                f'--drop-custom-check-constr'
            )
        if self.request.clean_db:
            self.cli_params.append(
                f'--clean-db'
            )
        if self.request.drop_db:
            self.cli_params.append(
                f'--drop-db'
            )

    def _prepare_cli_params(self):
        super()._prepare_cli_params()
        self._prepare_dictionaries_cli_params()
        self._prepare_input_dump_path_cli_params()
        self._prepare_parallelization_cli_params()
        self._prepare_pg_restore_cli_params()
        self._prepare_additional_cli_params()
        self._prepare_verbosity_cli_params()
