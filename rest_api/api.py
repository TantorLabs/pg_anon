import json
from datetime import date, datetime, UTC
from pathlib import Path
from typing import List

from fastapi import FastAPI, BackgroundTasks, Depends, status, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from pg_anon.common.constants import RUNS_BASE_DIR, SAVED_DICTS_INFO_FILE_NAME, SAVED_RUN_STATUS_FILE_NAME, \
    SAVED_RUN_OPTIONS_FILE_NAME, LOGS_DIR_NAME
from pg_anon.common.db_utils import check_db_connection
from pg_anon.common.dto import ConnectionParams
from pg_anon.common.enums import AnonMode
from pg_anon.common.errors import PgAnonError, ErrorCode
from pg_anon.common.utils import get_folder_size
from rest_api.callbacks import scan_callback, dump_callback, restore_callback
from rest_api.dependencies import date_range_filter, get_operation_run_dir
from rest_api.enums import ResponseStatus, DumpMode, RestoreMode, ScanMode
from rest_api.pydantic_models import ErrorResponse, ScanRequest, DumpRequest, DbConnectionParams, ViewFieldsRequest, \
    ViewFieldsResponse, ViewDataResponse, \
    ViewDataRequest, RestoreRequest, ScanType, RestoreType, DumpType, TaskStatus, \
    OperationDataResponse, PreviewSchemasResponse, PreviewSchemasRequest, PreviewSchemaTablesRequest, \
    PreviewSchemaTablesResponse
from rest_api.runners.direct import ViewFieldsRunner
from rest_api.runners.direct.preview import PreviewRunner
from rest_api.runners.direct.view_data import ViewDataRunner
from rest_api.utils import delete_folder, read_json_file, read_logs_from_tail

app = FastAPI(
    title='Stateless web service for pg_anon'
)


@app.exception_handler(PgAnonError)
async def pganon_error_handler(request: Request, exc: PgAnonError):
    return JSONResponse(
        status_code=400,
        content=jsonable_encoder(
            ErrorResponse(error_code=exc.code, message=exc.message)
        ),
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content=jsonable_encoder(
            ErrorResponse(error_code=ErrorCode.INTERNAL_ERROR, message=str(exc))
        ),
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
                    error_code=ErrorCode.DB_CONNECTION_FAILED,
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
        status_id=ResponseStatus.SUCCESS.value,
        status=ResponseStatus.SUCCESS.name.lower(),
        content=data
    )


@app.post(
    '/api/stateless/preview',
    tags=['Stateless | Preview'],
    summary='Render preview rules by fields',  # TODO: Update
    description='Rendering of preview rules by fields',  # TODO: Update
    response_model=PreviewSchemasResponse,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def stateless_preview_schemas(request: PreviewSchemasRequest):
    data = await PreviewRunner.get_schemas(request)

    return PreviewSchemasResponse(
        status_id=ResponseStatus.SUCCESS.value,
        status=ResponseStatus.SUCCESS.name.lower(),
        content=data
    )


@app.post(
    '/api/stateless/preview/{schema}',
    tags=['Stateless | Preview'],
    summary='Preview rules by fields',  # TODO: Update
    description='Rendering of preview rules by fields',  # TODO: Update
    response_model=PreviewSchemaTablesResponse,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def stateless_preview_schema_tables(schema: str, request: PreviewSchemaTablesRequest):
    data = await PreviewRunner.get_schema_tables(schema, request)

    return PreviewSchemaTablesResponse(
        status_id=ResponseStatus.SUCCESS.value,
        status=ResponseStatus.SUCCESS.name.lower(),
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
        status_id=ResponseStatus.SUCCESS.value,
        status=ResponseStatus.SUCCESS.name.lower(),
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
async def stateless_restore_start(request: RestoreRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(restore_callback, request)


#############################################
# Operations
#############################################

@app.get(
    '/operation',
    tags=['Operations'],
    summary='List of operations',
    response_model=List[str],
)
async def stateless_operations_list(
    filters: dict = Depends(date_range_filter)
):
    date_before = filters["date_before"]
    date_after = filters["date_after"]

    operations = []

    if not RUNS_BASE_DIR.exists():
        return operations

    for year_dir in RUNS_BASE_DIR.iterdir():
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue

            for day_dir in sorted(month_dir.iterdir()):
                if not day_dir.is_dir() or not day_dir.name.isdigit():
                    continue

                try:
                    run_date = date(int(year_dir.name), int(month_dir.name), int(day_dir.name))
                except ValueError:
                    continue

                if date_after and run_date < date_after:
                    continue
                if date_before and run_date > date_before:
                    continue

                for operation_dir in day_dir.iterdir():
                    # Return only operations run with "--save-dict"
                    if (operation_dir / SAVED_DICTS_INFO_FILE_NAME).exists():
                        operations.append(str(operation_dir.relative_to(RUNS_BASE_DIR)))

    return operations


@app.get(
    '/operation/{internal_operation_id}',
    tags=['Operations'],
    summary='Operation details',
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Operation run directory not found"},
    },
    response_model=OperationDataResponse,
)
async def stateless_operation_data(operation_run_dir: Path = Depends(get_operation_run_dir)):
    saved_dicts_info_file_path = operation_run_dir / SAVED_DICTS_INFO_FILE_NAME
    run_options_file_path = operation_run_dir / SAVED_RUN_OPTIONS_FILE_NAME
    run_status_file_path = operation_run_dir / SAVED_RUN_STATUS_FILE_NAME

    if not (saved_dicts_info_file_path.exists() and run_options_file_path.exists() and run_status_file_path.exists()):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Operation run directory have a wrong structure",
        )

    run_options_data = read_json_file(run_options_file_path)
    run_status_data = read_json_file(run_status_file_path)
    saved_dicts_info_data = read_json_file(saved_dicts_info_file_path)

    operation_status = ResponseStatus.SUCCESS if run_status_data['result_code'] == 'done' else ResponseStatus.ERROR
    extra_data = None
    if run_options_data['mode'] in (AnonMode.DUMP.value, AnonMode.SYNC_DATA_DUMP.value, AnonMode.SYNC_STRUCT_DUMP.value):
        extra_data = {"dump_size": get_folder_size(run_options_data['output_dir'])}

    return {
        "run_status": {
            "status_id": operation_status.value,
            "status": operation_status.name.lower(),
            "started": datetime.fromtimestamp(float(run_status_data['started']), tz=UTC).replace(microsecond=0).isoformat(),
            "ended": datetime.fromtimestamp(float(run_status_data['ended']), tz=UTC).replace(microsecond=0).isoformat(),
        },
        "run_options": run_options_data,
        "dictionaries": saved_dicts_info_data,
        "extra_data": extra_data
    }


@app.get(
    '/operation/{internal_operation_id}/logs',
    tags=['Operations'],
    summary='Logs of operation',
    response_model=List[str],
)
async def stateless_operation_logs(
        operation_run_dir: Path = Depends(get_operation_run_dir),
        tail_lines: int = Query(1000, gt=0, description="Number of log lines to read from the end of the file"),
):
    logs_file_path = operation_run_dir / LOGS_DIR_NAME
    return read_logs_from_tail(logs_file_path, tail_lines)


@app.delete(
    '/operation/{internal_operation_id}',
    tags=['Operations'],
    summary='Delete operation data',
    description='Delete operation data',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "500": {"model": ErrorResponse},
    }
)
async def remove_operation(background_tasks: BackgroundTasks, operation_run_dir: Path = Depends(get_operation_run_dir)):
    run_options_file_path = operation_run_dir / SAVED_RUN_OPTIONS_FILE_NAME
    run_options_data = read_json_file(run_options_file_path)

    if run_options_data['mode'] in (
        AnonMode.DUMP.value, AnonMode.SYNC_DATA_DUMP.value, AnonMode.SYNC_STRUCT_DUMP.value
    ):
        dump_path = Path(run_options_data['output_dir'])
        background_tasks.add_task(delete_folder, dump_path)
    background_tasks.add_task(delete_folder, operation_run_dir)


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
            title="Полный",
            slug=DumpMode.FULL.value,
        ),
        DumpType(
            title="Только структура",
            slug=DumpMode.STRUCT.value,
        ),
        DumpType(
            title="Только данные",
            slug=DumpMode.DATA.value,
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
            title="Полный",
            slug=RestoreMode.FULL.value,
        ),
        RestoreType(
            title="Только структура",
            slug=RestoreMode.STRUCT.value,
        ),
        RestoreType(
            title="Только данные",
            slug=RestoreMode.DATA.value,
        ),
    ]


@app.get(
    '/handbook/scan-types',
    tags=['Handbooks'],
    summary='List of scan types',
    response_model=List[ScanType],
)
async def scan_types():
    return [
        ScanType(
            title="Полное сканирование",
            slug=ScanMode.FULL.value,
        ),
        ScanType(
            title="Частичное сканирование",
            slug=ScanMode.PARTIAL.value,
        ),
    ]
