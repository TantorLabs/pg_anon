from typing import Optional

from pg_anon.common.enums import AnonMode, ScanMode
from rest_api.enums import ScanModeHandbook
from rest_api.pydantic_models import ScanRequest
from rest_api.runners.background import BaseRunner
from rest_api.utils import write_dictionary_contents


class ScanRunner(BaseRunner):
    mode: str = AnonMode.CREATE_DICT.value
    request: ScanRequest
    output_sens_dict_file_name: str
    output_no_sens_dict_file_name: Optional[str] = None

    def _prepare_dictionaries_cli_params(self):
        input_meta_dict_file_names = list(write_dictionary_contents(self.request.meta_dict_contents).keys())

        input_sens_dict_file_names = None
        if self.request.sens_dict_contents:
            input_sens_dict_file_names = list(write_dictionary_contents(self.request.sens_dict_contents).keys())

        input_no_sens_dict_file_names = None
        if self.request.no_sens_dict_contents:
            input_no_sens_dict_file_names = list(write_dictionary_contents(self.request.no_sens_dict_contents).keys())

        self.output_sens_dict_file_name = f'/tmp/output_sens_dict_{self.request.operation_id}.py'

        self.cli_params.extend([
            f"--meta-dict-file={','.join(input_meta_dict_file_names)}",
            f"--output-sens-dict-file={self.output_sens_dict_file_name}",
        ])

        if self.request.need_no_sens_dict:
            self.output_no_sens_dict_file_name = f'/tmp/output_no_sens_dict_{self.request.operation_id}.py'
            self.cli_params.append(
                f"--output-no-sens-dict-file={self.output_no_sens_dict_file_name}",
            )

        if input_sens_dict_file_names:
            self.cli_params.append(
                f"--prepared-sens-dict-file={','.join(input_sens_dict_file_names)}"
            )

        if input_no_sens_dict_file_names:
            self.cli_params.append(
                f"--prepared-no-sens-dict-file={','.join(input_no_sens_dict_file_names)}"
            )

    def _prepare_parallelization_cli_params(self):
        if self.request.proc_count:
            self.cli_params.append(
                f'--processes={self.request.proc_count}'
            )

        if self.request.proc_conn_count:
            self.cli_params.append(
                f'--db-connections-per-process={self.request.proc_conn_count}'
            )

    def _prepare_scan_mode_cli_params(self):
        if self.request.type_id == ScanModeHandbook.PARTIAL and self.request.depth:
            self.cli_params.extend([
                f'--scan-mode={ScanMode.PARTIAL.value}',
                f'--scan-partial-rows={self.request.depth}',
            ])
        else:
            self.cli_params.append(
                f'--scan-mode={ScanMode.FULL.value}'
            )

    def _prepare_cli_params(self):
        super()._prepare_cli_params()
        self._prepare_dictionaries_cli_params()
        self._prepare_parallelization_cli_params()
        self._prepare_scan_mode_cli_params()
        self._prepare_verbosity_cli_params()
