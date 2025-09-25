import json
import uuid
from datetime import date
from enum import Enum
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from pg_anon.common.db_utils import check_db_connection
from pg_anon.common.dto import ConnectionParams
from rest_api.callbacks import scan_callback, dump_callback, restore_callback
from rest_api.enums import ResponseStatusesHandbook
from rest_api.pydantic_models import ErrorResponse, ScanRequest, DumpRequest, DbConnectionParams, ViewFieldsRequest, \
    ViewFieldsResponse, ViewDataResponse, \
    ViewDataRequest, DumpDeleteRequest, RestoreRequest, ScanType, RestoreType, DumpType, TaskStatus, DictionaryType, \
    OperationDataResponse
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
# Operations
#############################################

@app.get(
    '/operation',
    tags=['Operations'],
    summary='List of operations',
    response_model=List[str],
)
async def stateless_operations_list(
    date_before: Optional[date] = Query(None, description="Filter: operations before this date"),
    date_after: Optional[date] = Query(None, description="Filter: operations after this date"),
):
    if date_after:
        mocked_date = date_after
    elif date_before:
        mocked_date = date_before
    else:
        mocked_date = date.today()

    mocked_operations_list = [f'{mocked_date.strftime("%Y/%m/%d")}/{uuid.uuid4()}' for _ in range(0, 10)]

    return mocked_operations_list


class MockOperationType(str, Enum):
    scan = "scan"
    dump = "dump"
    restore = "restore"

@app.get(
    '/operation/{internal_operation_id}',
    tags=['Operations'],
    summary='List of operations',
    response_model=OperationDataResponse,
)
async def stateless_operation_data(
        internal_operation_id: str,
        mock_operation_type: MockOperationType = Query(..., description="Operation type")
):
    data = {
        "scan": {
            "run_status": {
                "status_id": "2",
                "status": "success",
                "started": "2025-09-25T12:27:27+00:00",
                "ended": "2025-09-25T12:27:33+00:00",
            },
            "run_options": {
                "pg_anon_version": "1.7.0",
                "internal_operation_id": internal_operation_id,
                "debug": True,
                "config": "config.yml",
                "db_host": "localhost",
                "db_port": 5432,
                "db_name": "postgres",
                "db_user": "postgres",
                "db_passfile": "",
                "db_ssl_key_file": "",
                "db_ssl_cert_file": "",
                "db_ssl_ca_file": "",
                "mode": "create-dict",
                "verbose": "debug",
                "meta_dict_files": [
                  "/tmp/my-meta-dict-0b45d13a-da16-4e34-88a5-2e16c7db5b36.py"
                ],
                "db_connections_per_process": 4,
                "processes": 4,
                "pg_dump": "/usr/bin/pg_dump",
                "pg_restore": "/usr/bin/pg_restore",
                "output_dir": "",
                "input_dir": "",
                "dbg_stage_1_validate_dict": False,
                "dbg_stage_2_validate_data": False,
                "dbg_stage_3_validate_full": False,
                "clear_output_dir": False,
                "drop_custom_check_constr": False,
                "seq_init_by_max_value": False,
                "clean_db": False,
                "drop_db": False,
                "disable_checks": False,
                "scan_mode": "full",
                "output_sens_dict_file": "/tmp/output_sens_dict_scan-0001.py",
                "output_no_sens_dict_file": "/tmp/output_no_sens_dict_scan-0001.py",
                "prepared_sens_dict_files": None,
                "prepared_no_sens_dict_files": None,
                "partial_tables_dict_files": None,
                "partial_tables_exclude_dict_contents": None,
                "scan_partial_rows": 10000,
                "view_only_sensitive_fields": False,
                "schema_name": None,
                "schema_mask": None,
                "table_name": None,
                "table_mask": None,
                "fields_count": 5000,
                "limit": 100,
                "offset": 0,
                "application_name_suffix": "worker__create-dict__scan-0001",
                "version": False,
                "json": False
            },
            "dictionaries": {
                "/tmp/my-meta-dict-0b45d13a-da16-4e34-88a5-2e16c7db5b36.py": "{\n   \"data_regex\": {\n      \"rules\": [\n         \".*@.*\" \n      ]\n   },\n   \"funcs\": {\n      \"text\": \"md5(%s)\"\n   }\n}\n",
                "/tmp/output_sens_dict_scan-0001.py": "{\n    \"dictionary\": []\n}",
                "/tmp/output_no_sens_dict_scan-0001.py": "{\n    \"no_sens_dictionary\": [\n        {\n            \"schema\": \"public\",\n            \"table\": \"users\",\n            \"fields\": [\n                \"email\",\n                \"login\"\n            ]\n        },\n        {\n            \"schema\": \"public\",\n            \"table\": \"users_anonymized\",\n            \"fields\": [\n                \"email\",\n                \"login\"\n            ]\n        }\n    ]\n}",
            }
        },
        "dump": {
            "run_status": {
                "status_id": "2",
                "status": "success",
                "started": "2025-09-25T12:27:27+00:00",
                "ended": "2025-09-25T12:34:52+00:00",
            },
            "run_options": {
                "pg_anon_version": "1.7.0",
                "internal_operation_id": internal_operation_id,
                "debug": True,
                "config": "config.yml",
                "db_host": "localhost",
                "db_port": 5432,
                "db_name": "postgres",
                "db_user": "postgres",
                "db_passfile": "",
                "db_ssl_key_file": "",
                "db_ssl_cert_file": "",
                "db_ssl_ca_file": "",
                "mode": "dump",
                "verbose": "debug",
                "meta_dict_files": None,
                "db_connections_per_process": 4,
                "processes": 4,
                "pg_dump": "/usr/bin/pg_dump",
                "pg_restore": "/usr/bin/pg_restore",
                "output_dir": "/pg_anon/output/some/custom/dir",
                "input_dir": "",
                "dbg_stage_1_validate_dict": False,
                "dbg_stage_2_validate_data": False,
                "dbg_stage_3_validate_full": False,
                "clear_output_dir": True,
                "drop_custom_check_constr": False,
                "seq_init_by_max_value": False,
                "clean_db": False,
                "drop_db": False,
                "disable_checks": False,
                "scan_mode": "partial",
                "output_sens_dict_file": "output-sens-dict-file.py",
                "output_no_sens_dict_file": None,
                "prepared_sens_dict_files": [
                  "/tmp/emails-sens-dict-8140bbf2-a3af-416b-8712-73e6075c736f.py"
                ],
                "prepared_no_sens_dict_files": None,
                "partial_tables_dict_files": [
                  "/tmp/users_include-caa45a3e-ad1b-463b-9025-380073c46127.py"
                ],
                "partial_tables_exclude_dict_contents": [
                  "/tmp/companies_exclude-299e9de5-f307-4995-897e-c45c1010d2a7.py"
                ],
                "scan_partial_rows": 10000,
                "view_only_sensitive_fields": False,
                "schema_name": None,
                "schema_mask": None,
                "table_name": None,
                "table_mask": None,
                "fields_count": 5000,
                "limit": 100,
                "offset": 0,
                "application_name_suffix": "worker__dump__dump-0002",
                "version": False,
                "json": False
            },
            "dictionaries": {
                "/tmp/emails-sens-dict-8140bbf2-a3af-416b-8712-73e6075c736f.py": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"users_anonymized\", \"fields\": {\"email\": \"md5(email)\"}}]",
                "/tmp/users_include-caa45a3e-ad1b-463b-9025-380073c46127.py": "{\"tables\": [{\"schema\": \"public\",\"table\": \"users\"}]}",
                "/tmp/companies_exclude-299e9de5-f307-4995-897e-c45c1010d2a7.py": "{\"tables\": [{\"schema\": \"public\",\"table\": \"companies\"}]}",
            }
        },
        "restore": {
            "run_status": {
                "status_id": "2",
                "status": "success",
                "started": "2025-09-25T13:25:27+00:00",
                "ended": "2025-09-25T13:25:52+00:00",
            },
            "run_options": {
                "pg_anon_version": "1.7.0",
                "internal_operation_id": internal_operation_id,
                "debug": True,
                "config": "config.yml",
                "db_host": "localhost",
                "db_port": 5432,
                "db_name": "postgres",
                "db_user": "postgres",
                "db_passfile": "",
                "db_ssl_key_file": "",
                "db_ssl_cert_file": "",
                "db_ssl_ca_file": "",
                "mode": "restore",
                "verbose": "debug",
                "meta_dict_files": None,
                "db_connections_per_process": 4,
                "processes": 4,
                "pg_dump": "/usr/bin/pg_dump",
                "pg_restore": "/usr/bin/pg_restore",
                "output_dir": "",
                "input_dir": "/pg_anon/output/some/custom/dir",
                "dbg_stage_1_validate_dict": False,
                "dbg_stage_2_validate_data": False,
                "dbg_stage_3_validate_full": False,
                "clear_output_dir": False,
                "drop_custom_check_constr": False,
                "seq_init_by_max_value": False,
                "clean_db": True,
                "drop_db": False,
                "disable_checks": False,
                "scan_mode": "partial",
                "output_sens_dict_file": "output-sens-dict-file.py",
                "output_no_sens_dict_file": None,
                "prepared_sens_dict_files": None,
                "prepared_no_sens_dict_files": None,
                "partial_tables_dict_files": [
                    "/tmp/users_include-060877e4-f471-4a55-93c9-718141af49fc.py"
                ],
                "partial_tables_exclude_dict_contents": [
                    "/tmp/companies_exclude-95280950-09ba-4e11-acd5-b3850e793921.py"
                ],
                "scan_partial_rows": 10000,
                "view_only_sensitive_fields": False,
                "schema_name": None,
                "schema_mask": None,
                "table_name": None,
                "table_mask": None,
                "fields_count": 5000,
                "limit": 100,
                "offset": 0,
                "application_name_suffix": "worker__restore__restore-0003",
                "version": False,
                "json": False
            },
            "dictionaries": {
                "/tmp/users_include-060877e4-f471-4a55-93c9-718141af49fc.py": "{\"tables\": [{\"schema\": \"public\",\"table\": \"users\"}]}",
                "/tmp/companies_exclude-95280950-09ba-4e11-acd5-b3850e793921.py": "{\"tables\": [{\"schema\": \"public\",\"table\": \"companies\"}]}",
            }
        }
    }

    return data[mock_operation_type]


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
        """  "partial_tables_exclude_dict_contents": [""",
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
