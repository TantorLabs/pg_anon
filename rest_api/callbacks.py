import logging

import httpx
from pydantic import BaseModel

from pg_anon.common.utils import get_folder_size
from rest_api.enums import ResponseStatusesHandbook
from rest_api.pydantic_models import ScanStatusResponse, DumpStatusResponse, DumpRequest, ScanRequest, RestoreRequest, \
    RestoreStatusResponse
from rest_api.runners.background import ScanRunner, DumpRunner, InitRunner, RestoreRunner
from rest_api.utils import read_dictionary_contents

logger = logging.getLogger(__name__)


def send_webhook(url: str, response_body: BaseModel):
    logger.info(f'Send webhook on {url} with requst:{response_body.model_dump(by_alias=True)}')
    response = httpx.post(
        url=url,
        json=response_body.model_dump(by_alias=True),
        verify=False
    )
    logger.info(f'Webhook response code: {response.status_code}')


async def scan_callback(request: ScanRequest):
    logger.info("[DEBUG] Run scan callback")
    try:
        logger.info("[DEBUG] Run init")
        init_runner = InitRunner(request)
        await init_runner.run()

        scan_runner = ScanRunner(request)

        logger.info("[DEBUG] Send RUN webhook")
        send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.IN_PROGRESS.value,
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
        send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.ERROR.value,
            )
        )
        return

    logger.info("[DEBUG] Send COMPLETE webhook")
    send_webhook(
        url=request.webhook_status_url,
        response_body=ScanStatusResponse(
            operation_id=request.operation_id,
            status_id=ResponseStatusesHandbook.SUCCESS.value,
            sens_dict_content=sens_dict_contents,
            no_sens_dict_content=no_sens_dict_contents,
        )
    )


async def dump_callback(request: DumpRequest):
    try:
        init_runner = InitRunner(request)
        await init_runner.run()

        dump_runner = DumpRunner(request)

        send_webhook(
            url=request.webhook_status_url,
            response_body=DumpStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.IN_PROGRESS.value,
            )
        )

        await dump_runner.run()
        dump_size = get_folder_size(dump_runner.full_dump_path)
    except Exception as ex:
        logger.error(ex)
        send_webhook(
            url=request.webhook_status_url,
            response_body=DumpStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.ERROR.value,
            )
        )
        return

    send_webhook(
        url=request.webhook_status_url,
        response_body=DumpStatusResponse(
            operation_id=request.operation_id,
            status_id=ResponseStatusesHandbook.SUCCESS.value,
            size=dump_size,
        )
    )


async def restore_callback(request: RestoreRequest):
    try:
        restore_runner = RestoreRunner(request)

        send_webhook(
            url=request.webhook_status_url,
            response_body=RestoreStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.IN_PROGRESS.value,
            )
        )

        await restore_runner.run()
    except Exception as ex:
        logger.error(ex)
        send_webhook(
            url=request.webhook_status_url,
            response_body=RestoreStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.ERROR.value,
            )
        )
        return

    send_webhook(
        url=request.webhook_status_url,
        response_body=RestoreStatusResponse(
            operation_id=request.operation_id,
            status_id=ResponseStatusesHandbook.SUCCESS.value,
        )
    )
