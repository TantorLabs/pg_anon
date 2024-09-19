import asyncio

import httpx

from rest_api.dict_templates import TEMPLATE_SENS_DICT, TEMPLATE_NO_SENS_DICT
from rest_api.pydantic_models import ScanStatusResponse, DumpStatusResponse


async def scan_callback(operation_id: str, webhook_status_url: str):
    await asyncio.sleep(10)

    scan_status = ScanStatusResponse(
        operation_id=operation_id,
        status_id=4,  # in progress
    )
    print(scan_status.model_dump(by_alias=True))
    response = httpx.post(
        url=webhook_status_url,
        json=scan_status.model_dump(by_alias=True)
    )
    print(response.status_code)
    
    await asyncio.sleep(10)

    scan_status = ScanStatusResponse(
        operation_id=operation_id,
        status_id=2,  # success
        sens_dict_content=TEMPLATE_SENS_DICT,
        no_sens_dict_contents=TEMPLATE_NO_SENS_DICT,
    )
    print(scan_status.model_dump(by_alias=True))
    response = httpx.post(
        url=webhook_status_url,
        json=scan_status.model_dump(by_alias=True)
    )
    print(response.status_code)


async def dump_callback(operation_id: str, webhook_status_url: str):
    await asyncio.sleep(10)

    dump_status = DumpStatusResponse(
        operation_id=operation_id,
        status_id=4,  # in progress
    )
    print(dump_status.model_dump(by_alias=True))
    response = httpx.post(
        url=webhook_status_url,
        json=dump_status.model_dump(by_alias=True)
    )
    print(response.status_code)

    await asyncio.sleep(10)

    dump_status = DumpStatusResponse(
        operation_id=operation_id,
        status_id=2,  # success
        size=4096,
    )
    print(dump_status.model_dump(by_alias=True))
    response = httpx.post(
        url=webhook_status_url,
        json=dump_status.model_dump(by_alias=True)
    )
    print(response.status_code)
