import httpx
from pydantic import BaseModel

from pg_anon.common.utils import get_folder_size
from rest_api.enums import ResponseStatusesHandbook
from rest_api.pydantic_models import ScanStatusResponse, DumpStatusResponse, DumpRequest, ScanRequest
from rest_api.runners import ScanRunner, DumpRunner, InitRunner
from rest_api.utils import read_dictionary_contents


def send_webhook(url: str, response_body: BaseModel):
    print(response_body.model_dump(by_alias=True))
    response = httpx.post(
        url=url,
        json=response_body.model_dump(by_alias=True),
        verify=False
    )
    print(response.status_code)


async def scan_callback(request: ScanRequest):
    try:
        init_runner = InitRunner(request)
        await init_runner.run()

        scan_runner = ScanRunner(request)

        send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.IN_PROGRESS.value,
            )
        )

        await scan_runner.run()

        sens_dict_contents = read_dictionary_contents(scan_runner.output_sens_dict_file_name)
        no_sens_dict_contents = None
        if scan_runner.output_no_sens_dict_file_name:
            no_sens_dict_contents = read_dictionary_contents(scan_runner.output_no_sens_dict_file_name)
    except Exception as ex:
        print(ex)
        send_webhook(
            url=request.webhook_status_url,
            response_body=ScanStatusResponse(
                operation_id=request.operation_id,
                status_id=ResponseStatusesHandbook.ERROR.value,
            )
        )
        return

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
