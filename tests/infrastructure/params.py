import os
from pathlib import Path


ROWS_IN_INIT_ENV = 1512


class TestParams:
    test_db_user = "anon_test_user"  # default value
    test_db_user_password = "mYy5RexGsZ"  # noqa: S105
    test_db_host = "127.0.0.1"
    test_db_port = "5432"
    test_source_db = "test_source_db"
    test_target_db = "test_target_db"
    test_scale = "10"
    keep_test_dbs = False
    db_connections_per_process = 4
    test_processes = 4

    def __init__(self) -> None:  # noqa: C901
        config_path = str(Path(__file__).resolve().parent.parent / "config.yml")

        if os.environ.get("TEST_DB_USER") is not None:
            self.test_db_user = os.environ["TEST_DB_USER"]
        if os.environ.get("PGPASSWORD") is not None:
            self.test_db_user_password = os.environ["TEST_DB_USER_PASSWORD"]
        if os.environ.get("TEST_DB_USER_PASSWORD") is not None:
            self.test_db_user_password = os.environ["TEST_DB_USER_PASSWORD"]
        if os.environ.get("TEST_DB_HOST") is not None:
            self.test_db_host = os.environ["TEST_DB_HOST"]
        if os.environ.get("TEST_DB_PORT") is not None:
            self.test_db_port = os.environ["TEST_DB_PORT"]
        if os.environ.get("TEST_SOURCE_DB") is not None:
            self.test_source_db = os.environ["TEST_SOURCE_DB"]
        if os.environ.get("TEST_TARGET_DB") is not None:
            self.test_target_db = os.environ["TEST_TARGET_DB"]
        if os.environ.get("TEST_SCALE") is not None:
            self.test_scale = os.environ["TEST_SCALE"]
        if os.environ.get("KEEP_TEST_DBS", "").lower() in ("1", "true", "yes"):
            self.keep_test_dbs = True
        if os.environ.get("TEST_DB_CONNECTIONS_PER_PROCESS") is not None:
            self.db_connections_per_process = os.environ["TEST_DB_CONNECTIONS_PER_PROCESS"]
        if os.environ.get("TEST_PROCESSES") is not None:
            self.test_processes = os.environ["TEST_PROCESSES"]
        self.test_config = os.environ.get("TEST_CONFIG", config_path)
