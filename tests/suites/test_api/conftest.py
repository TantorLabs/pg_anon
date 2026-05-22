from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import socket
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

# --------------------------------------------------------------------------
# Repoint pg_anon storage to a per-session temp dir BEFORE importing the app.
# `PG_ANON_HOME` is read by `pg_anon.common.constants` at import time and
# propagates to subprocess workers via the environment, so child processes
# resolve the same dirs we patch in the parent.
# --------------------------------------------------------------------------
_API_TEST_HOME = Path(tempfile.gettempdir()) / f"pg_anon_api_tests_{uuid.uuid4().hex}"
_API_TEST_HOME.mkdir(parents=True, exist_ok=True)
os.environ["PG_ANON_HOME"] = str(_API_TEST_HOME)

_API_TEST_TEMP = _API_TEST_HOME / "tmp"
_API_TEST_TEMP.mkdir(parents=True, exist_ok=True)

import pg_anon.common.constants as _const  # noqa: E402
import pg_anon.common.utils as _common_utils  # noqa: E402
import pg_anon.rest_api.api as _api_module  # noqa: E402
import pg_anon.rest_api.constants as _rest_const  # noqa: E402
import pg_anon.rest_api.dependencies as _deps_module  # noqa: E402
import pg_anon.rest_api.runners.background.base as _runner_base  # noqa: E402
import pg_anon.rest_api.runners.direct.view_data as _view_data_module  # noqa: E402
import pg_anon.rest_api.runners.direct.view_fields as _view_fields_module  # noqa: E402
import pg_anon.rest_api.utils as _rest_utils  # noqa: E402

_const.BASE_DIR = _API_TEST_HOME.resolve()
_const.RUNS_BASE_DIR = _const.BASE_DIR / "pg_anon_runs"
_rest_const.DUMP_STORAGE_BASE_DIR = (_const.BASE_DIR / "pg_anon_output").resolve()
_rest_const.BASE_TEMP_DIR = _API_TEST_TEMP

# `setattr` keeps mypy from flagging these as missing re-exports — the modules
# re-bind the constants under `from ... import X`, which is not the same as a
# public export, but is exactly what we need to override at test time.
for _module, _attr, _value in [
    (_common_utils, "RUNS_BASE_DIR", _const.RUNS_BASE_DIR),
    (_api_module, "RUNS_BASE_DIR", _const.RUNS_BASE_DIR),
    (_deps_module, "RUNS_BASE_DIR", _const.RUNS_BASE_DIR),
    (_rest_utils, "DUMP_STORAGE_BASE_DIR", _rest_const.DUMP_STORAGE_BASE_DIR),
    (_runner_base, "BASE_TEMP_DIR", _rest_const.BASE_TEMP_DIR),
    (_view_data_module, "BASE_TEMP_DIR", _rest_const.BASE_TEMP_DIR),
    (_view_fields_module, "BASE_TEMP_DIR", _rest_const.BASE_TEMP_DIR),
]:
    setattr(_module, _attr, _value)

_const.RUNS_BASE_DIR.mkdir(parents=True, exist_ok=True)
_rest_const.DUMP_STORAGE_BASE_DIR.mkdir(parents=True, exist_ok=True)

import aiohttp  # noqa: E402
import uvicorn  # noqa: E402

from tests.suites.test_api.helpers import WebhookRecorder  # noqa: E402

from pg_anon.common.enums import ResultCode  # noqa: E402
from pg_anon.rest_api.api import app  # noqa: E402


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --------------------------------------------------------------------------
# Storage-layout fixtures.
# --------------------------------------------------------------------------


@pytest.fixture(scope="session")
def api_test_home() -> Path:
    return _API_TEST_HOME


@pytest.fixture(scope="session")
def runs_base_dir() -> Path:
    return _const.RUNS_BASE_DIR


@pytest.fixture(scope="session")
def dump_storage_base_dir() -> Path:
    return _rest_const.DUMP_STORAGE_BASE_DIR


@pytest.fixture(scope="session", autouse=True)
def _cleanup_storage():
    yield
    with contextlib.suppress(FileNotFoundError):
        shutil.rmtree(_API_TEST_HOME)


# --------------------------------------------------------------------------
# Uvicorn server: runs in a dedicated thread with its own asyncio loop, so
# pytest-asyncio's per-test loops can freely create aiohttp sessions.
# --------------------------------------------------------------------------


class _UvicornThread:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        config = uvicorn.Config(
            app=app,
            host=host,
            port=port,
            log_level="warning",
            lifespan="off",
            loop="asyncio",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, name="uvicorn-api", daemon=True)

    def start(self) -> None:
        self._thread.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self._server.started:
                return
            time.sleep(0.05)
        raise RuntimeError("uvicorn did not start in time")

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10)


@pytest.fixture(scope="session")
def api_server() -> Generator[_UvicornThread, None, None]:
    server = _UvicornThread("127.0.0.1", _pick_free_port())
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="session")
def api_base_url(api_server: _UvicornThread) -> str:
    return api_server.base_url


# --------------------------------------------------------------------------
# aiohttp client per test (each test gets its own event loop).
# --------------------------------------------------------------------------


@pytest.fixture
async def api_client(api_base_url: str):
    timeout = aiohttp.ClientTimeout(total=180)
    async with aiohttp.ClientSession(base_url=api_base_url, timeout=timeout) as session:
        yield session


# --------------------------------------------------------------------------
# Mock webhook server, per test.
# --------------------------------------------------------------------------


@pytest.fixture
async def webhook_recorder():
    recorder = WebhookRecorder()
    await recorder.start()
    yield recorder
    await recorder.stop()


# --------------------------------------------------------------------------
# Reusable DB fixtures.
# --------------------------------------------------------------------------

API_SOURCE_DB = "pg_anon_api_source"


@pytest.fixture(scope="session")
def _api_source_db_setup(db_manager, pg_anon_runner, fixtures):
    async def _setup() -> None:
        await db_manager.create_db(API_SOURCE_DB)
        res = await pg_anon_runner.run("init", API_SOURCE_DB)
        assert res.result_code == ResultCode.DONE
        await fixtures.build_minimal_env(API_SOURCE_DB, rows=30)

    async def _teardown() -> None:
        await db_manager.drop_db(API_SOURCE_DB)

    asyncio.run(_setup())
    yield API_SOURCE_DB
    asyncio.run(_teardown())


@pytest.fixture
def api_source_db(_api_source_db_setup: str) -> str:
    return _api_source_db_setup


@pytest.fixture
async def api_target_db(db_manager, request):
    raw = request.node.name.replace("[", "_").replace("]", "").replace("/", "_")
    name = f"pg_anon_api_tgt_{raw}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


# --------------------------------------------------------------------------
# Shared input dict paths.
# --------------------------------------------------------------------------

SUITE = Path(__file__).resolve().parent
INPUT_DICT = SUITE / "input_dict"


def input_dict_text(name: str) -> str:
    return (INPUT_DICT / name).read_text()


@pytest.fixture(scope="session")
def input_dicts() -> dict[str, str]:
    return {p.name: p.read_text() for p in INPUT_DICT.iterdir() if p.is_file()}
