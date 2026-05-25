from tests.infrastructure.params import TestParams

from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.dto import PgAnonResult


class PgAnonRunner:
    def __init__(self, params: TestParams) -> None:
        self.params = params

    def _base_args(self, mode: str, db_name: str) -> list[str]:
        return [
            mode,
            f"--db-host={self.params.test_db_host}",
            f"--db-name={db_name}",
            f"--db-user={self.params.test_db_user}",
            f"--db-port={self.params.test_db_port}",
            f"--db-user-password={self.params.test_db_user_password}",
            f"--config={self.params.test_config}",
            "--debug",
        ]

    async def run(self, mode: str, db_name: str, extra_args: list[str] | None = None) -> PgAnonResult:
        args = self._base_args(mode, db_name)
        if extra_args:
            args.extend(extra_args)
        options = build_run_options(args)
        return await PgAnonApp(options).run()
