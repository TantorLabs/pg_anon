import logging
import sys
from contextlib import asynccontextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import AsyncIterator, Optional

from concurrent_log_handler import ConcurrentRotatingFileHandler

from pg_anon.common.constants import LOGS_FILE_NAME

REST_API_LOGGER_NAME = "rest_api"
WEBHOOK_LOGGER_NAME = "rest_api.callbacks.webhook"

operation_id_var: ContextVar[Optional[str]] = ContextVar("operation_id", default=None)
web_debug_var: ContextVar[bool] = ContextVar("web_debug", default=False)


class OperationIdFormatter(logging.Formatter):
    """Injects the current operation_id from contextvars into every record."""

    def format(self, record: logging.LogRecord) -> str:
        record.operation_id = operation_id_var.get() or "-"
        return super().format(record)


class OperationFileFilter(logging.Filter):
    """Passes records only for a specific operation — used by per-operation file handlers."""

    def __init__(self, target_operation_id: str):
        super().__init__()
        self.target_operation_id = target_operation_id

    def filter(self, record: logging.LogRecord) -> bool:
        return operation_id_var.get() == self.target_operation_id


class WebhookDebugFilter(logging.Filter):
    """Suppresses webhook logs unless web_debug is explicitly enabled for the current operation.

    Attached to the webhook logger itself so it blocks propagation to every handler
    (stdout + per-operation file) when web_debug is False.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return web_debug_var.get() is True


LOG_FORMAT = (
    "%(asctime)s,%(msecs)03d - %(levelname)8s - [id=%(operation_id)s] - %(message)s"
)
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _build_formatter() -> OperationIdFormatter:
    return OperationIdFormatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)


_setup_done = False


def setup_rest_api_logging(level: int = logging.DEBUG) -> None:
    """Initialise the shared rest_api logger hierarchy.

    Must be called once at service startup. Idempotent — safe to call more than once.
    """
    global _setup_done
    if _setup_done:
        return

    rest_api_logger = logging.getLogger(REST_API_LOGGER_NAME)
    rest_api_logger.setLevel(level)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(_build_formatter())
    rest_api_logger.addHandler(stdout_handler)

    webhook_logger = logging.getLogger(WEBHOOK_LOGGER_NAME)
    webhook_logger.addFilter(WebhookDebugFilter())

    _setup_done = True


def _attach_operation_file_handler(log_dir: Path, operation_id: str) -> logging.Handler:
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = ConcurrentRotatingFileHandler(
        log_dir / LOGS_FILE_NAME,
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
    )
    handler.setFormatter(_build_formatter())
    handler.addFilter(OperationFileFilter(operation_id))
    logging.getLogger(REST_API_LOGGER_NAME).addHandler(handler)
    return handler


def _detach_operation_file_handler(handler: logging.Handler) -> None:
    logging.getLogger(REST_API_LOGGER_NAME).removeHandler(handler)
    handler.close()


@asynccontextmanager
async def operation_logging_context(
    operation_id: str,
    log_dir: Path,
    web_debug: bool,
) -> AsyncIterator[None]:
    """Scope a block of code to a single operation's logging context.

    Sets contextvars for operation_id / web_debug (automatically propagated
    through asyncio tasks) and attaches a per-operation file handler that
    is guaranteed to be detached on exit.
    """
    operation_id_var.set(operation_id)
    web_debug_var.set(web_debug)
    handler = _attach_operation_file_handler(log_dir, operation_id)
    try:
        yield
    finally:
        _detach_operation_file_handler(handler)


webhook_logger = logging.getLogger(WEBHOOK_LOGGER_NAME)
