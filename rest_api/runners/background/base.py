import uuid
from typing import List

from pg_anon.common.constants import BASE_DIR
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.errors import PgAnonError, ErrorCode
from rest_api.constants import BASE_TEMP_DIR
from rest_api.pydantic_models import StatelessRunnerRequest
from rest_api.utils import run_pg_anon_worker


class BaseRunner:
    mode: str
    request: StatelessRunnerRequest
    operation_id: str
    cli_params: List[str] = None
    result: PgAnonResult = None

    def __init__(self, request: StatelessRunnerRequest):
        self.request = request
        self.operation_id = request.operation_id
        self.base_tmp_dir = BASE_TEMP_DIR / f'{self.operation_id}__{uuid.uuid4()}'
        self._prepare_cli_params()

    def _prepare_db_credentials_cli_params(self):
        self.cli_params.extend([
            f'--db-host={self.request.db_connection_params.host}',
            f'--db-port={self.request.db_connection_params.port}',
            f'--db-user={self.request.db_connection_params.user_login}',
            f'--db-user-password={self.request.db_connection_params.user_password}',
            f'--db-name={self.request.db_connection_params.db_name}',
        ])

    def _prepare_config(self):
        config_file_path = BASE_DIR / "config.yml"
        if config_file_path.exists():
            self.cli_params.extend([
                f"--config={str(config_file_path)}",
            ])

    def _prepare_verbosity_cli_params(self):
        self.cli_params.extend([
            "--debug",
        ])

    def _prepare_cli_params(self):
        self.cli_params = []
        self._prepare_db_credentials_cli_params()
        self._prepare_config()

    async def run(self):
        if not self.mode:
            raise PgAnonError(ErrorCode.UNKNOWN_MODE, 'Mode is not set')

        self.result = await run_pg_anon_worker(
            mode=self.mode,
            operation_id=self.operation_id,
            cli_run_params=self.cli_params
        )

        if not self.result:
            raise PgAnonError(ErrorCode.OPERATION_FAILED, 'Operation not completed successfully')

        return self.result
