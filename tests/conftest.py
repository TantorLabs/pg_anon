"""Session-level fixtures shared across all tests_new suites."""
from __future__ import annotations

import pytest

from tests.infrastructure.data import Fixtures
from tests.infrastructure.db import DBManager
from tests.infrastructure.params import TestParams
from tests.infrastructure.pg_anon import PgAnonRunner


@pytest.fixture(scope="session")
def db_params() -> TestParams:
    return TestParams()


@pytest.fixture(scope="session")
def pg_anon_runner(db_params: TestParams) -> PgAnonRunner:
    return PgAnonRunner(db_params)


@pytest.fixture(scope="session")
def db_manager(db_params: TestParams) -> DBManager:
    return DBManager(db_params)


@pytest.fixture(scope="session")
def fixtures(db_manager: DBManager) -> Fixtures:
    return Fixtures(db_manager)
