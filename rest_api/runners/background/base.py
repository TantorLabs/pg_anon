from typing import List

from pg_anon.common.dto import PgAnonResult
from pg_anon.common.enums import ResultCode
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
        self._prepare_cli_params()

    def _prepare_db_credentials_cli_params(self):
        self.cli_params.extend([
            f'--db-host={self.request.db_connection_params.host}',
            f'--db-port={self.request.db_connection_params.port}',
            f'--db-user={self.request.db_connection_params.user_login}',
            f'--db-user-password={self.request.db_connection_params.user_password}',
            f'--db-name={self.request.db_connection_params.db_name}',
        ])

    def _prepare_verbosity_cli_params(self):
        self.cli_params.extend([
            "--verbose=debug",
            "--debug",
        ])

    def _prepare_cli_params(self):
        self.cli_params = []
        self._prepare_db_credentials_cli_params()

    async def run(self):
        if not self.mode:
            raise ValueError(f'Mode is not set')

        self.result = await run_pg_anon_worker(
            mode=self.mode,
            operation_id=self.operation_id,
            cli_run_params=self.cli_params
        )

        if not self.result or self.result.result_code == ResultCode.FAIL:
            raise RuntimeError('Operation not completed successfully')

        return self.result
