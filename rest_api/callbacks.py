import asyncio
import logging
from typing import Optional, Dict

import aiohttp
from pydantic import BaseModel

from pg_anon.common.errors import PgAnonError, ErrorCode
from pg_anon.common.utils import get_folder_size
from rest_api.enums import ResponseStatus
from rest_api.pydantic_models import ScanStatusResponse, DumpStatusResponse, DumpRequest, ScanRequest, RestoreRequest, \
    StatelessRunnerResponse
from rest_api.runners.background import ScanRunner, DumpRunner, InitRunner, RestoreRunner
from rest_api.utils import read_dictionary_contents, normalize_headers

logger = logging.getLogger(__name__)


async def send_webhook(url: str, response_body: BaseModel, extra_headers: Optional[Dict[str, str]] = None,
                       verify_ssl: bool = True, max_retries: int = 5, base_delay: float = 1) -> None:
    payload = response_body.model_dump(by_alias=True)
    logger.info(f'Starting webhook request to {url} with payload: {payload}')

    headers = normalize_headers(extra_headers)

    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.post(url, json=payload, ssl=verify_ssl, headers=headers) as response:
                    if response.status < 500:
                        logger.info(f'Webhook successfully sent. Status code: {response.status}')
                        return
                    else:
                        logger.warning(
                            f'Webhook attempt {attempt + 1} to {url} failed with server error: {response.status}'
                        )
            except aiohttp.ClientError as e:
                logger.warning(f'Webhook attempt {attempt + 1} to {url} failed. Request failed: {e}')

            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f'Retrying in {delay:.2f} seconds...')
                await asyncio.sleep(delay)

    logger.error(f'All {max_retries} webhook attempts to {url} have failed')


async def scan_callback(request: ScanRequest):
    logger.debug("Run scan callback")
    scan_runner = None
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
        await scan_runner.run()
        if scan_runner.result.exception is not None:
            raise scan_runner.result.exception

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
        logger.error(ex)
        logger.debug("Send ERROR webhook")

        error_code = ex.code if isinstance(ex, PgAnonError) else ErrorCode.INTERNAL_ERROR
        scan_runner_params = {
            "error": str(ex),
            "error_code": error_code,
        }
        if scan_runner and scan_runner.result:
            scan_runner_params.update({
                "internal_operation_id": scan_runner.result.internal_operation_id,
                "started": scan_runner.result.start_date.isoformat(timespec="seconds"),
                "ended": scan_runner.result.end_date.isoformat(timespec="seconds"),
                "run_options": scan_runner.result.run_options.to_dict(),
            })
        await send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatus.ERROR.value,
                status=ResponseStatus.ERROR.name.lower(),
                webhook_metadata=request.webhook_metadata,
                **scan_runner_params
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
            internal_operation_id=scan_runner.result.internal_operation_id,
            status_id=ResponseStatus.SUCCESS.value,
            status=ResponseStatus.SUCCESS.name.lower(),
            started=scan_runner.result.start_date.isoformat(timespec="seconds"),
            ended=scan_runner.result.end_date.isoformat(timespec="seconds"),
            run_options=scan_runner.result.run_options.to_dict(),
            webhook_metadata=request.webhook_metadata,
            sens_dict_content=sens_dict_contents,
            no_sens_dict_content=no_sens_dict_contents,
        ),
        verify_ssl=request.webhook_verify_ssl,
        extra_headers=request.webhook_extra_headers,
    )


async def dump_callback(request: DumpRequest):
    logger.debug("Run dump callback")
    dump_runner = None
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
        await dump_runner.run()
        if dump_runner.result.exception is not None:
            raise dump_runner.result.exception

        logger.debug("DUMP completed - OK")
        dump_size = get_folder_size(dump_runner.full_dump_path)
    except Exception as ex:
        logger.debug("DUMP completed - FAIL")
        logger.error(ex)

        error_code = ex.code if isinstance(ex, PgAnonError) else ErrorCode.INTERNAL_ERROR
        dump_runner_params = {
            "error": str(ex),
            "error_code": error_code,
        }
        if dump_runner and dump_runner.result:
            dump_runner_params.update({
                "internal_operation_id": dump_runner.result.internal_operation_id,
                "started": dump_runner.result.start_date.isoformat(timespec="seconds"),
                "ended": dump_runner.result.end_date.isoformat(timespec="seconds"),
                "run_options": dump_runner.result.run_options.to_dict(),
            })
        await send_webhook(
            url=request.webhook_status_url,
            response_body=DumpStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatus.ERROR.value,
                status=ResponseStatus.ERROR.name.lower(),
                webhook_metadata=request.webhook_metadata,
                **dump_runner_params
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
            internal_operation_id=dump_runner.result.internal_operation_id,
            status_id=ResponseStatus.SUCCESS.value,
            status=ResponseStatus.SUCCESS.name.lower(),
            started=dump_runner.result.start_date.isoformat(timespec="seconds"),
            ended=dump_runner.result.end_date.isoformat(timespec="seconds"),
            run_options=dump_runner.result.run_options.to_dict(),
            webhook_metadata=request.webhook_metadata,
            size=dump_size,
        ),
        verify_ssl=request.webhook_verify_ssl,
        extra_headers=request.webhook_extra_headers,
    )


async def restore_callback(request: RestoreRequest):
    logger.debug("Run scan callback")
    restore_runner = None
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
        await restore_runner.run()
        if restore_runner.result.exception is not None:
            raise restore_runner.result.exception

        logger.debug("RESTORE completed - OK")
    except Exception as ex:
        logger.debug("RESTORE completed - FAIL")
        logger.error(ex)
        logger.debug("Send ERROR webhook")

        error_code = ex.code if isinstance(ex, PgAnonError) else ErrorCode.INTERNAL_ERROR
        restore_runner_params = {
            "error": str(ex),
            "error_code": error_code,
        }
        if restore_runner and restore_runner.result:
            restore_runner_params.update({
                "internal_operation_id": restore_runner.result.internal_operation_id,
                "started": restore_runner.result.start_date.isoformat(timespec="seconds"),
                "ended": restore_runner.result.end_date.isoformat(timespec="seconds"),
                "run_options": restore_runner.result.run_options.to_dict(),
            })
        await send_webhook(
            url=request.webhook_status_url,
            response_body=StatelessRunnerResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatus.ERROR.value,
                status=ResponseStatus.ERROR.name.lower(),
                webhook_metadata=request.webhook_metadata,
                **restore_runner_params
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
            internal_operation_id=restore_runner.result.internal_operation_id,
            status_id=ResponseStatus.SUCCESS.value,
            status=ResponseStatus.SUCCESS.name.lower(),
            started=restore_runner.result.start_date.isoformat(timespec="seconds"),
            ended=restore_runner.result.end_date.isoformat(timespec="seconds"),
            run_options=restore_runner.result.run_options.to_dict(),
            webhook_metadata=request.webhook_metadata,
        ),
        verify_ssl=request.webhook_verify_ssl,
        extra_headers=request.webhook_extra_headers,
    )
