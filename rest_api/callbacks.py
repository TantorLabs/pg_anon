import httpx
from pydantic import BaseModel

from pg_anon.common.utils import get_folder_size
from rest_api.enums import ResponseStatusesHandbook
from rest_api.pydantic_models import ScanStatusResponse, DumpStatusResponse, DumpRequest, ScanRequest
from rest_api.runners.background import ScanRunner, DumpRunner, InitRunner
from rest_api.utils import read_dictionary_contents


def send_webhook(url: str, response_body: BaseModel):
    print(f'Send webhook on {url} with requst:{response_body.model_dump(by_alias=True)}')
    response = httpx.post(
        url=url,
        json=response_body.model_dump(by_alias=True),
        verify=False
    )
    print(f'Webhook response code: {response.status_code}')


async def scan_callback(request: ScanRequest):
    print("[DEBUG] Run scan callback")
    try:
        print("[DEBUG] Run init")
        init_runner = InitRunner(request)
        await init_runner.run()

        scan_runner = ScanRunner(request)

        print("[DEBUG] Send RUN webhook")
        send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.IN_PROGRESS.value,
            )
        )

        print("[DEBUG] Run SCAN operation")
        await scan_runner.run()

        print("[DEBUG] SCAN completed - OK")
        sens_dict_contents = read_dictionary_contents(scan_runner.output_sens_dict_file_name)
        print("[DEBUG] Read sens_dict_contents")
        no_sens_dict_contents = None
        if scan_runner.output_no_sens_dict_file_name:
            print("[DEBUG] Read no_sens_dict_contents")
            no_sens_dict_contents = read_dictionary_contents(scan_runner.output_no_sens_dict_file_name)
        print("[DEBUG] Complete main operation")
    except Exception as ex:
        print("[DEBUG] SCAN completed - FAIL")
        print(ex)
        print("[DEBUG] Send ERROR webhook")
        send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.ERROR.value,
            )
        )
        return

    print("[DEBUG] Send COMPLETE webhook")
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
        print(ex)
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
