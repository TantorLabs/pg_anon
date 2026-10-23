"""Session-level fixtures shared across all tests_new suites."""

from __future__ import annotations

import asyncio
import functools

import pytest

from tests.infrastructure.data import Fixtures
from tests.infrastructure.db import DBManager
from tests.infrastructure.params import TestParams
from tests.infrastructure.pg_anon import PgAnonRunner
from tests.infrastructure.query_guard import QueryArgsGuard


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


@pytest.fixture(autouse=True)
def query_args_guard(monkeypatch: pytest.MonkeyPatch):
    """Fail a test if any query in it has too many bind parameters."""
    guard = QueryArgsGuard()
    guard.install(monkeypatch)
    yield guard
    # pg_anon can catch the error and only log it, so check again after the test.
    assert not guard.violations, "\n".join(guard.violations)


@functools.cache
def _server_version_num() -> int:
    return asyncio.run(DBManager(TestParams()).server_version_num("postgres"))


@pytest.fixture(scope="session")
def pg_version_num() -> int:
    """Server version as `server_version_num`, e.g. 150004."""
    return _server_version_num()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip a test on a server outside `min_pg(N)` (N or newer) and `max_pg(N)` (older than N), before fixtures run."""
    min_marker = item.get_closest_marker("min_pg")
    max_marker = item.get_closest_marker("max_pg")
    if min_marker is None and max_marker is None:
        return
    version_num = _server_version_num()
    if min_marker is not None and version_num < min_marker.args[0]:
        pytest.skip(f"needs PostgreSQL {min_marker.args[0]} or newer, server is {version_num}")
    if max_marker is not None and version_num >= max_marker.args[0]:
        pytest.skip(f"only for PostgreSQL older than {max_marker.args[0]}, server is {version_num}")
