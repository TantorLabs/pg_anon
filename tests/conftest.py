import pytest

from tests.infrastructure.data import TestData
from tests.infrastructure.db import DBManager
from tests.infrastructure.params import TestParams
from tests.infrastructure.pg_anon import PgAnonRunner


@pytest.fixture(scope="session")
def db_params():
    return TestParams()


@pytest.fixture(scope="session")
def pg_anon_runner(db_params):
    return PgAnonRunner(db_params)


@pytest.fixture(scope="session")
def db_manager(db_params):
    return DBManager(db_params)


@pytest.fixture(scope="session")
def test_data(db_manager, db_params):
    return TestData(db_manager)
