import asyncio
import logging

import aiohttp
from pydantic import BaseModel

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
    logger.info("Starting webhook request to %s with payload: %s", url, payload)

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
    try:
        logger.debug("Run init")
        init_runner = InitRunner(request)
        await init_runner.run()

        scan_runner = ScanRunner(request)

        logger.info("Send RUN webhook")
        await send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
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
        }
        if result:
            scan_runner_params.update(
                {
                    "internal_operation_id": result.internal_operation_id or "",
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


async def dump_callback(request: DumpRequest) -> None:
    """Execute a dump operation and send status webhooks on progress and completion."""
    logger.debug("Run dump callback")
    result: PgAnonResult | None = None
    try:
        logger.debug("Run init")
        init_runner = InitRunner(request)
        await init_runner.run()

        dump_runner = DumpRunner(request)

        logger.info("Send RUN webhook")
        await send_webhook(
            url=request.webhook_status_url,
            response_body=DumpStatusResponse(
                operation_id=request.operation_id,
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
        }
        if result:
            dump_runner_params.update(
                {
                    "internal_operation_id": result.internal_operation_id or "",
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


async def restore_callback(request: RestoreRequest) -> None:
    """Execute a restore operation and send status webhooks on progress and completion."""
    logger.debug("Run restore callback")
    result: PgAnonResult | None = None
    try:
        restore_runner = RestoreRunner(request)

        logger.info("Send RUN webhook")
        await send_webhook(
            url=request.webhook_status_url,
            response_body=StatelessRunnerResponse(
                operation_id=request.operation_id,
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
        }
        if result:
            restore_runner_params.update(
                {
                    "internal_operation_id": result.internal_operation_id or "",
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
