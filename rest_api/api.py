import json
from pathlib import Path
from typing import List

from fastapi import FastAPI, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from pg_anon.common.db_utils import check_db_connection
from pg_anon.common.dto import ConnectionParams
from rest_api.callbacks import scan_callback, dump_callback, restore_callback
from rest_api.enums import ResponseStatusesHandbook
from rest_api.pydantic_models import ErrorResponse, ScanRequest, DumpRequest, DbConnectionParams, ViewFieldsRequest, \
    ViewFieldsResponse, ViewDataResponse, \
    ViewDataRequest, DumpDeleteRequest, RestoreRequest, ScanType, RestoreType, DumpType, TaskStatus, DictionaryType
from rest_api.runners.direct import ViewFieldsRunner
from rest_api.runners.direct.view_data import ViewDataRunner
from rest_api.utils import get_full_dump_path, delete_folder

app = FastAPI(
    title='Stateless web service for pg_anon'
)


def generate_openapi_doc_file():
    output_path = Path(__file__).parent / "openapi.json"
    with open(output_path, "w") as f:
        json.dump(app.openapi(), f, indent=2)


@app.post(
    '/api/stateless/check_db_connection',
    tags=['Stateless'],
    summary='Check DB connections with credentials',
    description='Check DB connections with credentials',
    status_code=200,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def db_connection_check(request: DbConnectionParams):
    connection_is_ok = await check_db_connection(
        connection_params=ConnectionParams(
            host=request.host,
            port=request.port,
            database=request.db_name,
            user=request.user_login,
            password=request.user_password,
        )
    )

    if not connection_is_ok:
        return JSONResponse(
            status_code=400,
            content=jsonable_encoder(
                ErrorResponse(
                    message='Connection is unreachable'
                )
            )
        )


@app.post(
    '/api/stateless/scan',
    tags=['Stateless'],
    summary='Create new scanning operation',
    description='Create new scanning operation',
    status_code=201,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def stateless_scan_start(request: ScanRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(scan_callback, request)


@app.post(
    '/api/stateless/view-fields',
    tags=['Stateless'],
    summary='Render preview rules by fields',
    description='Rendering of preview rules by fields',
    response_model=ViewFieldsResponse,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def stateless_view_fields(request: ViewFieldsRequest):
    runner = ViewFieldsRunner(request)
    data = await runner.run()

    return ViewFieldsResponse(
        status_id=ResponseStatusesHandbook.SUCCESS.value,
        content=data
    )


@app.post(
    '/api/stateless/view-data',
    tags=['Stateless'],
    summary='Render preview data by rules',
    description='Rendering of preview data by rules',
    response_model=ViewDataResponse,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def stateless_view_data(request: ViewDataRequest):
    runner = ViewDataRunner(request)
    data = await runner.run()

    return ViewDataResponse(
        status_id=ResponseStatusesHandbook.SUCCESS.value,
        content=data
    )


@app.post(
    '/api/stateless/dump',
    tags=['Stateless'],
    summary='Create new dump operation',
    description='Create new dump operation',
    status_code=201,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def stateless_dump_start(request: DumpRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(dump_callback, request)


@app.delete(
    '/api/stateless/dump',
    tags=['Stateless'],
    summary='Delete dump',
    description='Delete dump',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def dump_operation_delete(request: DumpDeleteRequest, background_tasks: BackgroundTasks):
    dump_path = get_full_dump_path(request.path)
    background_tasks.add_task(delete_folder, dump_path)


@app.post(
    '/api/stateless/restore',
    tags=['Stateless'],
    summary='Run restore operation',
    description='Run restore operation',
    status_code=201,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def stateless_dump_start(request: RestoreRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(restore_callback, request)


#############################################
# Handbooks
#############################################

@app.get(
    '/handbook/task-statuses',
    tags=['Handbooks'],
    summary='List of task statuses',
    response_model=List[TaskStatus],
)
async def task_statuses():
    return [
        TaskStatus(
            id=1,
            title="В процессе",
            slug="in_process",
        ),
        TaskStatus(
            id=2,
            title="Завершено",
            slug="done",
        ),
        TaskStatus(
            id=3,
            title="Ошибка",
            slug="error",
        ),
    ]


@app.get(
    '/handbook/dump-types',
    tags=['Handbooks'],
    summary='List of dump types',
    response_model=List[DumpType],
)
async def dump_types():
    return [
        DumpType(
            id=1,
            title="Полный",
            slug="full",
        ),
        DumpType(
            id=2,
            title="Только структура",
            slug="structure",
        ),
        DumpType(
            id=3,
            title="Только данные",
            slug="data",
        ),
    ]

@app.get(
    '/handbook/restore-types',
    tags=['Handbooks'],
    summary='List of restore types',
    response_model=List[RestoreType],
)
async def restore_types():
    return [
        RestoreType(
            id=1,
            title="Полный",
            slug="full",
        ),
        RestoreType(
            id=2,
            title="Только структура",
            slug="structure",
        ),
        RestoreType(
            id=3,
            title="Только данные",
            slug="data",
        ),
    ]


@app.get(
    '/handbook/scan-types',
    tags=['Handbooks'],
    summary='List of scan types',
    response_model=List[ScanType],
)
async def dump_types():
    return [
        ScanType(
            id=1,
            title="Полное сканирование",
            slug="full",
        ),
        ScanType(
            id=2,
            title="Частичное сканирование",
            slug="partial",
        ),
    ]
