import asyncio
import logging

import aiohttp
from pydantic import BaseModel

from pg_anon.common.utils import get_folder_size
from rest_api.enums import ResponseStatusesHandbook
from rest_api.pydantic_models import ScanStatusResponse, DumpStatusResponse, DumpRequest, ScanRequest, RestoreRequest, \
    StatelessRunnerResponse
from rest_api.runners.background import ScanRunner, DumpRunner, InitRunner, RestoreRunner
from rest_api.utils import read_dictionary_contents

logger = logging.getLogger(__name__)


async def send_webhook(url: str, response_body: BaseModel, max_retries: int = 5, base_delay: float = 1):
    payload = response_body.model_dump(by_alias=True)
    logger.info(f'Starting webhook request to {url} with payload: {payload}')

    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.post(url, json=payload, ssl=False) as response:
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
    logger.info("[DEBUG] Run scan callback")
    try:
        logger.info("[DEBUG] Run init")
        init_runner = InitRunner(request)
        await init_runner.run()

        scan_runner = ScanRunner(request)

        logger.info("[DEBUG] Send RUN webhook")
        await send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.IN_PROGRESS.value,
                webhook_metadata=request.webhook_metadata,
            )
        )

        logger.info("[DEBUG] Run SCAN operation")
        await scan_runner.run()

        logger.info("[DEBUG] SCAN completed - OK")
        sens_dict_contents = read_dictionary_contents(scan_runner.output_sens_dict_file_name)
        logger.info("[DEBUG] Read sens_dict_contents")
        no_sens_dict_contents = None
        if scan_runner.output_no_sens_dict_file_name:
            logger.info("[DEBUG] Read no_sens_dict_contents")
            no_sens_dict_contents = read_dictionary_contents(scan_runner.output_no_sens_dict_file_name)
        logger.info("[DEBUG] Complete main operation")
    except Exception as ex:
        logger.info("[DEBUG] SCAN completed - FAIL")
        logger.error(ex)
        logger.info("[DEBUG] Send ERROR webhook")
        await send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.ERROR.value,
                webhook_metadata=request.webhook_metadata,
            )
        )
        return

    logger.info("[DEBUG] Send COMPLETE webhook")
    await send_webhook(
        url=request.webhook_status_url,
        response_body=ScanStatusResponse(
            operation_id=request.operation_id,
            status_id=ResponseStatusesHandbook.SUCCESS.value,
            webhook_metadata=request.webhook_metadata,
            sens_dict_content=sens_dict_contents,
            no_sens_dict_content=no_sens_dict_contents,
        )
    )


async def dump_callback(request: DumpRequest):
    try:
        init_runner = InitRunner(request)
        await init_runner.run()

        dump_runner = DumpRunner(request)

        await send_webhook(
            url=request.webhook_status_url,
            response_body=DumpStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.IN_PROGRESS.value,
                webhook_metadata=request.webhook_metadata,
            )
        )

        await dump_runner.run()
        dump_size = get_folder_size(dump_runner.full_dump_path)
    except Exception as ex:
        logger.error(ex)
        await send_webhook(
            url=request.webhook_status_url,
            response_body=DumpStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.ERROR.value,
                webhook_metadata=request.webhook_metadata,
            )
        )
        return

    await send_webhook(
        url=request.webhook_status_url,
        response_body=DumpStatusResponse(
            operation_id=request.operation_id,
            status_id=ResponseStatusesHandbook.SUCCESS.value,
            webhook_metadata=request.webhook_metadata,
            size=dump_size,
        )
    )


async def restore_callback(request: RestoreRequest):
    try:
        restore_runner = RestoreRunner(request)

        await send_webhook(
            url=request.webhook_status_url,
            response_body=StatelessRunnerResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.IN_PROGRESS.value,
                webhook_metadata=request.webhook_metadata,
            )
        )

        await restore_runner.run()
    except Exception as ex:
        logger.error(ex)
        await send_webhook(
            url=request.webhook_status_url,
            response_body=StatelessRunnerResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.ERROR.value,
                webhook_metadata=request.webhook_metadata,
            )
        )
        return

    await send_webhook(
        url=request.webhook_status_url,
        response_body=StatelessRunnerResponse(
            operation_id=request.operation_id,
            status_id=ResponseStatusesHandbook.SUCCESS.value,
            webhook_metadata=request.webhook_metadata,
        )
    )
