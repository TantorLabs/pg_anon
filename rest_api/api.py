import json
from datetime import date, datetime, UTC
from pathlib import Path
from typing import List

from fastapi import FastAPI, BackgroundTasks, Depends, status, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from pg_anon.common.constants import RUNS_BASE_DIR, SAVED_DICTS_INFO_FILE_NAME, SAVED_RUN_STATUS_FILE_NAME, \
    SAVED_RUN_OPTIONS_FILE_NAME
from pg_anon.common.db_utils import check_db_connection
from pg_anon.common.dto import ConnectionParams
from pg_anon.common.enums import AnonMode
from pg_anon.common.utils import get_folder_size
from rest_api.callbacks import scan_callback, dump_callback, restore_callback
from rest_api.dependencies import date_range_filter, get_operation_run_dir
from rest_api.enums import ResponseStatus, DumpMode, RestoreMode, ScanMode
from rest_api.pydantic_models import ErrorResponse, ScanRequest, DumpRequest, DbConnectionParams, ViewFieldsRequest, \
    ViewFieldsResponse, ViewDataResponse, \
    ViewDataRequest, DumpDeleteRequest, RestoreRequest, ScanType, RestoreType, DumpType, TaskStatus, \
    OperationDataResponse
from rest_api.runners.direct import ViewFieldsRunner
from rest_api.runners.direct.view_data import ViewDataRunner
from rest_api.utils import get_full_dump_path, delete_folder, read_json_file

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
        status_id=ResponseStatus.SUCCESS.value,
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
async def stateless_operation_logs(internal_operation_id: str, tail_lines: int = 1000):
    return [
        """2025-09-25 17:27:27,898 -     INFO - ============> Started pg_anon (v1.7.0) in mode: dump""",
        """2025-09-25 17:27:27,898 -    DEBUG - #--------------- Run options""",
        """{""",
        """  "pg_anon_version": "1.7.0",""",
        f"""  "internal_operation_id": "{internal_operation_id}",""",
        """  "debug": true,""",
        """  "config": "config.yml",""",
        """  "db_host": "localhost",""",
        """  "db_port": 5432,""",
        """  "db_name": "postgres",""",
        """  "db_user": "postgres",""",
        """  "db_passfile": "",""",
        """  "db_ssl_key_file": "",""",
        """  "db_ssl_cert_file": "",""",
        """  "db_ssl_ca_file": "",""",
        """  "mode": "dump",""",
        """  "verbose": "debug",""",
        """  "meta_dict_files": null,""",
        """  "db_connections_per_process": 4,""",
        """  "processes": 4,""",
        """  "pg_dump": "/usr/bin/pg_dump",""",
        """  "pg_restore": "/usr/bin/pg_restore",""",
        """  "output_dir": "/pg_anon/output/some/custom/dir",""",
        """  "input_dir": "",""",
        """  "dbg_stage_1_validate_dict": false,""",
        """  "dbg_stage_2_validate_data": false,""",
        """  "dbg_stage_3_validate_full": false,""",
        """  "clear_output_dir": true,""",
        """  "drop_custom_check_constr": false,""",
        """  "seq_init_by_max_value": false,""",
        """  "clean_db": false,""",
        """  "drop_db": false,""",
        """  "disable_checks": false,""",
        """  "scan_mode": "partial",""",
        """  "output_sens_dict_file": "output-sens-dict-file.py",""",
        """  "output_no_sens_dict_file": null,""",
        """  "prepared_sens_dict_files": [""",
        """    "/tmp/some-dict-3605291d-634d-4835-821d-48768c7242b4.py" """,
        """  ],""",
        """  "prepared_no_sens_dict_files": null,""",
        """  "partial_tables_dict_files": [""",
        """    "/tmp/partial-dict-707871bf-5990-4faa-a0b1-f23d5c832a38" """,
        """  ],""",
        """  "partial_tables_exclude_dict_files": [""",
        """    "/tmp/partial-exclude-dict-29d90112-4b67-49ab-97bf-dacd433267ed.py" """,
        """  ],""",
        """  "scan_partial_rows": 10000,""",
        """  "view_only_sensitive_fields": false,""",
        """  "schema_name": null,""",
        """  "schema_mask": null,""",
        """  "table_name": null,""",
        """  "table_mask": null,""",
        """  "fields_count": 5000,""",
        """  "limit": 100,""",
        """  "offset": 0,""",
        """  "application_name_suffix": "worker__dump__789-45-346",""",
        """  "version": false,""",
        """  "json": false""",
        """}""",
        """#-----------------------------------""",
        """2025-09-25 17:27:27,916 -     INFO - Target DB version: 15.12""",
        """2025-09-25 17:27:27,916 -     INFO - pg_dump path: /usr/lib/postgresql/15/bin/pg_dump""",
        """2025-09-25 17:27:27,916 -     INFO - pg_restore path: /usr/lib/postgresql/15/bin/pg_restore""",
        """2025-09-25 17:27:27,916 -     INFO - Postgres utils exists checking""",
        """2025-09-25 17:27:27,946 -     INFO - -------------> Started dump""",
        """2025-09-25 17:27:27,949 -     INFO - -------------> Started dump pre-data (pg_dump)""",
        """2025-09-25 17:27:27,949 -    DEBUG - ['/usr/lib/postgresql/15/bin/pg_dump', '-h', 'localhost', '-p', '5432', '-v', '-w', '-U', 'postgres', '--exclude-schema', 'anon_funcs', '--exclude-schema', 'columnar_internal', '--section', 'pre-data', '-E', 'UTF8', '-F', 'c', '-s', '-f', '/pg_anon/output/some/custom/dir/pre_data.backup', 'postgres']""",
        """2025-09-25 17:27:28,029 -     INFO - pg_dump: последний системный OID: 16383""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: чтение расширений""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: выявление членов расширений""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: чтение схем""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: чтение пользовательских таблиц""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: чтение пользовательских функций""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: чтение пользовательских типов""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: чтение процедурных языков""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: чтение пользовательских агрегатных функций""",
        """2025-09-25 17:27:28,030 -     INFO - pg_dump: чтение пользовательских операторов""",
        """...""",
        """2025-09-25 17:27:28,495 -     INFO - <================ Process [1] finished, elapsed: 0.31 sec. Result 1 item(s)""",
        """2025-09-25 17:27:28,496 -     INFO - <================ Process [2] finished, elapsed: 0.31 sec. Result 1 item(s)""",
        """2025-09-25 17:27:33,199 -    DEBUG - """,
        """        SELECT""",
        """            pn_t.nspname,""",
        """            t.relname AS table_name,""",
        """            a.attname AS column_name,""",
        """            pn_s.nspname,""",
        """            s.relname AS sequence_name""",
        """        FROM pg_class AS t""",
        """        JOIN pg_attribute AS a ON a.attrelid = t.oid""",
        """        JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum""",
        """        JOIN pg_class AS s ON s.oid = d.objid""",
        """        JOIN pg_namespace AS pn_t ON pn_t.oid = t.relnamespace""",
        """        JOIN pg_namespace AS pn_s ON pn_s.oid = s.relnamespace""",
        """        WHERE""",
        """            t.relkind IN ('r', 'p')""",
        """            AND s.relkind = 'S'""",
        """            AND d.deptype = 'a'""",
        """            AND d.classid = 'pg_catalog.pg_class'::regclass""",
        """            AND d.refclassid = 'pg_catalog.pg_class'::regclass""",
        """            """,
        """2025-09-25 17:27:33,213 -     INFO - <------------- Finished dump data""",
        """2025-09-25 17:27:33,235 -     INFO - <------------- Finished dump""",
        """2025-09-25 17:27:33,236 -     INFO - <============ Finished pg_anon in mode: dump, result_code = done, elapsed: 5.34 sec""",
    ][-tail_lines:]


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
