import asyncio
import json
import logging
from pathlib import Path

import aiohttp
from concurrent_log_handler import ConcurrentRotatingFileHandler
from pydantic import BaseModel

from pg_anon.common.constants import LOGS_FILE_NAME
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import get_folder_size
from pg_anon.rest_api.enums import ResponseStatus
from pg_anon.rest_api.pydantic_models import (
    DumpRequest,
    DumpStatusResponse,
    RestoreRequest,
    ScanRequest,
    ScanStatusResponse,
    StatelessRunnerResponse,
)
from pg_anon.rest_api.runners.background import DumpRunner, InitRunner, RestoreRunner, ScanRunner
from pg_anon.rest_api.utils import normalize_headers, read_dictionary_contents

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOG_FORMATTER = logging.Formatter(
    datefmt="%Y-%m-%d %H:%M:%S",
    fmt="%(asctime)s,%(msecs)03d - %(levelname)8s - %(message)s",
)


def _attach_log_file_handler(log_dir: Path, enabled: bool) -> ConcurrentRotatingFileHandler | None:
    if not enabled:
        return None
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = ConcurrentRotatingFileHandler(
        log_dir / LOGS_FILE_NAME,
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
    )
    handler.setFormatter(LOG_FORMATTER)
    logger.addHandler(handler)
    return handler


def _detach_log_file_handler(handler: ConcurrentRotatingFileHandler | None) -> None:
    if handler:
        logger.removeHandler(handler)
        handler.close()


def _raise_if_failed(result: PgAnonResult) -> None:
    """Re-raise stored exception from background runner result."""
    if result.exception is not None:
        raise result.exception


async def send_webhook(
    url: str,
    response_body: BaseModel,
    extra_headers: dict[str, str] | None = None,
    verify_ssl: bool = True,
    max_retries: int = 5,
    base_delay: float = 1,
) -> None:
    """Send a webhook POST request with exponential backoff retries."""
    payload = response_body.model_dump(by_alias=True)
    logger.info(
        "Starting webhook request to %s with payload: %s",
        url, json.dumps(payload, ensure_ascii=False, default=str)
    )

    headers = normalize_headers(extra_headers)

    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.post(url, json=payload, ssl=verify_ssl, headers=headers) as response:
                    if response.status < 500:  # noqa: PLR2004
                        logger.info("Webhook successfully sent. Status code: %s", response.status)
                        return
                    logger.warning(
                        "Webhook attempt %s to %s failed with server error: %s",
                        attempt + 1,
                        url,
                        response.status,
                    )
            except aiohttp.ClientError as e:
                logger.warning("Webhook attempt %s to %s failed. Request failed: %s", attempt + 1, url, e)

            if attempt < max_retries - 1:
                delay = base_delay * (2**attempt)
                logger.info("Retrying in %.2f seconds...", delay)
                await asyncio.sleep(delay)

    logger.error("All %s webhook attempts to %s have failed", max_retries, url)


async def scan_callback(request: ScanRequest) -> None:
    """Execute a scan operation and send status webhooks on progress and completion."""
    logger.debug("Run scan callback")
    result: PgAnonResult | None = None
    scan_runner = ScanRunner(request)
    _log_handler = _attach_log_file_handler(scan_runner.log_dir, request.web_debug)
    try:
        logger.debug("Run init")
        init_runner = InitRunner(request)
        await init_runner.run()

        logger.info("Send RUN webhook")
        await send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                internal_operation_id=scan_runner.internal_operation_id,
                status_id=ResponseStatus.IN_PROGRESS.value,
                status=ResponseStatus.IN_PROGRESS.name.lower(),
                webhook_metadata=request.webhook_metadata,
            ),
            verify_ssl=request.webhook_verify_ssl,
            extra_headers=request.webhook_extra_headers,
        )

        logger.info("Run SCAN operation")
        result = await scan_runner.run()
        _raise_if_failed(result)

        logger.debug("SCAN completed - OK")
        sens_dict_contents = read_dictionary_contents(scan_runner.output_sens_dict_file_name)
        logger.debug("Read sens_dict_contents")
        no_sens_dict_contents = None
        if scan_runner.output_no_sens_dict_file_name:
            logger.debug("Read no_sens_dict_contents")
            no_sens_dict_contents = read_dictionary_contents(scan_runner.output_no_sens_dict_file_name)
        logger.debug("Complete main operation")
    except Exception as ex:
        logger.debug("SCAN completed - FAIL")
        logger.exception("SCAN failed")
        logger.debug("Send ERROR webhook")

        error_code = ex.code if isinstance(ex, PgAnonError) else ErrorCode.INTERNAL_ERROR
        scan_runner_params: dict = {
            "error": str(ex),
            "error_code": error_code,
            "internal_operation_id": scan_runner.internal_operation_id,
        }
        if result:
            scan_runner_params.update(
                {
                    "started": result.start_date.isoformat(timespec="seconds") if result.start_date else "",
                    "ended": result.end_date.isoformat(timespec="seconds") if result.end_date else "",
                    "run_options": result.run_options.to_dict() if result.run_options else "",
                }
            )
        await send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatus.ERROR.value,
                status=ResponseStatus.ERROR.name.lower(),
                webhook_metadata=request.webhook_metadata,
                **scan_runner_params,
            ),
            verify_ssl=request.webhook_verify_ssl,
            extra_headers=request.webhook_extra_headers,
        )
        _detach_log_file_handler(_log_handler)
        return

    logger.debug("Send COMPLETE webhook")
    await send_webhook(
        url=request.webhook_status_url,
        response_body=ScanStatusResponse(
            operation_id=request.operation_id,
            internal_operation_id=result.internal_operation_id,
            status_id=ResponseStatus.SUCCESS.value,
            status=ResponseStatus.SUCCESS.name.lower(),
            started=result.start_date.isoformat(timespec="seconds") if result.start_date else "",
            ended=result.end_date.isoformat(timespec="seconds") if result.end_date else "",
            run_options=result.run_options.to_dict() if result.run_options else None,
            webhook_metadata=request.webhook_metadata,
            sens_dict_content=sens_dict_contents,
            no_sens_dict_content=no_sens_dict_contents,
        ),
        verify_ssl=request.webhook_verify_ssl,
        extra_headers=request.webhook_extra_headers,
    )
    _detach_log_file_handler(_log_handler)


async def dump_callback(request: DumpRequest) -> None:
    """Execute a dump operation and send status webhooks on progress and completion."""
    logger.debug("Run dump callback")
    result: PgAnonResult | None = None
    dump_runner = DumpRunner(request)
    _log_handler = _attach_log_file_handler(dump_runner.log_dir, request.web_debug)
    try:
        logger.debug("Run init")
        init_runner = InitRunner(request)
        await init_runner.run()

        logger.info("Send RUN webhook")
        await send_webhook(
            url=request.webhook_status_url,
            response_body=DumpStatusResponse(
                operation_id=request.operation_id,
                internal_operation_id=dump_runner.internal_operation_id,
                status_id=ResponseStatus.IN_PROGRESS.value,
                status=ResponseStatus.IN_PROGRESS.name.lower(),
                webhook_metadata=request.webhook_metadata,
            ),
            verify_ssl=request.webhook_verify_ssl,
            extra_headers=request.webhook_extra_headers,
        )

        logger.info("Run DUMP operation")
        result = await dump_runner.run()
        _raise_if_failed(result)

        logger.debug("DUMP completed - OK")
        dump_size = get_folder_size(dump_runner.full_dump_path or "")
    except Exception as ex:
        logger.debug("DUMP completed - FAIL")
        logger.exception("DUMP failed")

        error_code = ex.code if isinstance(ex, PgAnonError) else ErrorCode.INTERNAL_ERROR
        dump_runner_params: dict = {
            "error": str(ex),
            "error_code": error_code,
            "internal_operation_id": dump_runner.internal_operation_id,
        }
        if result:
            dump_runner_params.update(
                {
                    "started": result.start_date.isoformat(timespec="seconds") if result.start_date else "",
                    "ended": result.end_date.isoformat(timespec="seconds") if result.end_date else "",
                    "run_options": result.run_options.to_dict() if result.run_options else "",
                }
            )
        await send_webhook(
            url=request.webhook_status_url,
            response_body=DumpStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatus.ERROR.value,
                status=ResponseStatus.ERROR.name.lower(),
                webhook_metadata=request.webhook_metadata,
                **dump_runner_params,
            ),
            verify_ssl=request.webhook_verify_ssl,
            extra_headers=request.webhook_extra_headers,
        )
        _detach_log_file_handler(_log_handler)
        return

    logger.debug("Send COMPLETE webhook")
    await send_webhook(
        url=request.webhook_status_url,
        response_body=DumpStatusResponse(
            operation_id=request.operation_id,
            internal_operation_id=result.internal_operation_id,
            status_id=ResponseStatus.SUCCESS.value,
            status=ResponseStatus.SUCCESS.name.lower(),
            started=result.start_date.isoformat(timespec="seconds") if result.start_date else "",
            ended=result.end_date.isoformat(timespec="seconds") if result.end_date else "",
            run_options=result.run_options.to_dict() if result.run_options else None,
            webhook_metadata=request.webhook_metadata,
            size=dump_size,
        ),
        verify_ssl=request.webhook_verify_ssl,
        extra_headers=request.webhook_extra_headers,
    )
    _detach_log_file_handler(_log_handler)


async def restore_callback(request: RestoreRequest) -> None:
    """Execute a restore operation and send status webhooks on progress and completion."""
    logger.debug("Run restore callback")
    result: PgAnonResult | None = None
    restore_runner = RestoreRunner(request)
    _log_handler = _attach_log_file_handler(restore_runner.log_dir, request.web_debug)
    try:

        logger.info("Send RUN webhook")
        await send_webhook(
            url=request.webhook_status_url,
            response_body=StatelessRunnerResponse(
                operation_id=request.operation_id,
                internal_operation_id=restore_runner.internal_operation_id,
                status_id=ResponseStatus.IN_PROGRESS.value,
                status=ResponseStatus.IN_PROGRESS.name.lower(),
                webhook_metadata=request.webhook_metadata,
            ),
            verify_ssl=request.webhook_verify_ssl,
            extra_headers=request.webhook_extra_headers,
        )

        logger.info("Run RESTORE operation")
        result = await restore_runner.run()
        _raise_if_failed(result)

        logger.debug("RESTORE completed - OK")
    except Exception as ex:
        logger.debug("RESTORE completed - FAIL")
        logger.exception("RESTORE failed")
        logger.debug("Send ERROR webhook")

        error_code = ex.code if isinstance(ex, PgAnonError) else ErrorCode.INTERNAL_ERROR
        restore_runner_params: dict = {
            "error": str(ex),
            "error_code": error_code,
            "internal_operation_id": restore_runner.internal_operation_id,
        }
        if result:
            restore_runner_params.update(
                {
                    "started": result.start_date.isoformat(timespec="seconds") if result.start_date else "",
                    "ended": result.end_date.isoformat(timespec="seconds") if result.end_date else "",
                    "run_options": result.run_options.to_dict() if result.run_options else "",
                }
            )
        await send_webhook(
            url=request.webhook_status_url,
            response_body=StatelessRunnerResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatus.ERROR.value,
                status=ResponseStatus.ERROR.name.lower(),
                webhook_metadata=request.webhook_metadata,
                **restore_runner_params,
            ),
            verify_ssl=request.webhook_verify_ssl,
            extra_headers=request.webhook_extra_headers,
        )
        _detach_log_file_handler(_log_handler)
        return

    logger.debug("Send COMPLETE webhook")
    await send_webhook(
        url=request.webhook_status_url,
        response_body=StatelessRunnerResponse(
            operation_id=request.operation_id,
            internal_operation_id=result.internal_operation_id,
            status_id=ResponseStatus.SUCCESS.value,
            status=ResponseStatus.SUCCESS.name.lower(),
            started=result.start_date.isoformat(timespec="seconds") if result.start_date else "",
            ended=result.end_date.isoformat(timespec="seconds") if result.end_date else "",
            run_options=result.run_options.to_dict() if result.run_options else None,
            webhook_metadata=request.webhook_metadata,
        ),
        verify_ssl=request.webhook_verify_ssl,
        extra_headers=request.webhook_extra_headers,
    )
    _detach_log_file_handler(_log_handler)
