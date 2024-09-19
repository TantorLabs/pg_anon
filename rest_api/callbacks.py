import asyncio
import uuid

import httpx

from rest_api.dict_templates import TEMPLATE_SENS_DICT, TEMPLATE_NO_SENS_DICT
from rest_api.pydantic_models import ScanStatusResponse, DumpStatusResponse, DumpRequest, ScanRequest


async def scan_callback(request: ScanRequest):
    await asyncio.sleep(10)

    scan_status = ScanStatusResponse(
        operation_id=request.operation_id,
        status_id=4,  # in progress
    )
    print(scan_status.model_dump(by_alias=True))
    response = httpx.post(
        url=request.webhook_status_url,
        json=scan_status.model_dump(by_alias=True)
    )
    print(response.status_code)
    
    await asyncio.sleep(10)

    scan_status = ScanStatusResponse(
        operation_id=request.operation_id,
        status_id=2,  # success
        sens_dict_content=TEMPLATE_SENS_DICT,
        no_sens_dict_contents=TEMPLATE_NO_SENS_DICT,
    )
    print(scan_status.model_dump(by_alias=True))
    response = httpx.post(
        url=request.webhook_status_url,
        json=scan_status.model_dump(by_alias=True)
    )
    print(response.status_code)


async def dump_callback(request: DumpRequest):
    if request.output_path:
        path = request.output_path.lstrip("/")
    else:
        dict_name = list(request.sens_dict_contents.keys())[0]
        path = dict_name if dict_name else uuid.uuid4()

    path = f'/some/dumps/{path}'

    await asyncio.sleep(10)

    dump_status = DumpStatusResponse(
        operation_id=request.operation_id,
        status_id=4,  # in progress
        path=path,
    )
    print(dump_status.model_dump(by_alias=True))
    response = httpx.post(
        url=request.webhook_status_url,
        json=dump_status.model_dump(by_alias=True)
    )
    print(response.status_code)

    await asyncio.sleep(10)

    dump_status = DumpStatusResponse(
        operation_id=request.operation_id,
        status_id=2,  # success
        path=path,
        size=4096,
    )
    print(dump_status.model_dump(by_alias=True))
    response = httpx.post(
        url=request.webhook_status_url,
        json=dump_status.model_dump(by_alias=True)
    )
    print(response.status_code)
