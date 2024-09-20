import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI

from dict_templates import TEMPLATE_META_DICT, TEMPLATE_SENS_DICT, TEMPLATE_NO_SENS_DICT
from pydantic_models import Project, DbConnection, TaskStatus, DumpType, ProjectCreate, \
    DbCheckConnectionStatus, DictionaryShort, DictionaryDetailed, DictionaryCreate, DictionaryType, DictionaryUpdate, \
    DbConnectionCredentials, ScanType, Scan, ScanCreate, DictionaryDuplicate, DumpCreate, Dump, Preview, PreviewCreate, \
    ErrorResponse, ProjectUpdate, DbConnectionCreate, DbConnectionUpdate, DbConnectionFullCredentials, PreviewUpdate, \
    Content, ScanRequest, DumpRequest, DbConnectionParams, ViewFieldsRequest, ViewFieldsResponse, ViewFieldsContent, \
    ViewDataResponse, ViewDataRequest, ViewDataContent, DumpDeleteRequest
from rest_api.callbacks import scan_callback, dump_callback
from utils import simple_slugify, get_full_dump_path

app = FastAPI(
    title='Web service for pg_anon'
)


#############################################
# DB Connections
#############################################


@app.get(
    '/db-connections',
    tags=['DB Connections'],
    summary='List of db connections',
    description='Receive list of db connections',
    response_model=List[DbConnection],
)
async def db_connections_list():
    return [DbConnection(
        id=i,
        title=f"some_db_connection_{i}",
        slug=simple_slugify(f"some_db_connection_{i}"),
        host="127.0.0.1",
        port="5432",
        database=f"some_private_db_{i}",
        user=f"some_user_{i}" if i > 1 else None,
        attributes='{created_user_id: 10, updated_user_id: 11}',
    ) for i in range(1, 3)]


@app.get(
    '/db-connections/{db_connection_id}',
    tags=['DB Connections'],
    summary='DB Connection details',
    description='Receive DB connection details',
    response_model=DbConnection,
    responses={
        "404": {"model": ErrorResponse}
    }
)
async def db_connection_details(db_connection_id: int = None):
    return DbConnection(
        id=db_connection_id,
        title=f"some_db_connection_{db_connection_id}",
        slug=simple_slugify(f"some_db_connection_{db_connection_id}"),
        host="127.0.0.1",
        port="5432",
        database=f"some_private_db_{db_connection_id}",
        user=f"some_user_{db_connection_id}",
        attributes='{created_user_id: 10, updated_user_id: 11}',
    )


@app.post(
    '/db-connections',
    tags=['DB Connections'],
    summary='Create new db connection',
    description='Creating of db connection',
    response_model=DbConnection,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse}
    }
)
async def db_connection_create(db_connection_data: DbConnectionCreate):
    db_connection_id = 14
    return DbConnection(
        id=db_connection_id,
        title=db_connection_data.title,
        slug=simple_slugify(db_connection_data.title),
        host=db_connection_data.host,
        port=db_connection_data.port,
        database=db_connection_data.database,
        user=db_connection_data.user,
    )


@app.put(
    '/db-connections/{db_connection_id}',
    tags=['DB Connections'],
    summary='Update existing db connection',
    description='Updating of existing db connection',
    response_model=DbConnection,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse}
    }
)
async def db_connection_update(db_connection_id: int, db_connection_data: DbConnectionUpdate):
    title = db_connection_data.title if db_connection_data.title else f"some project {db_connection_id}"
    host = db_connection_data.host if db_connection_data.host else "127.0.0.1",
    port = db_connection_data.port if db_connection_data.port else "5432",
    database = db_connection_data.database if db_connection_data.database else f"some_private_db_{db_connection_id}",
    user = db_connection_data.user if db_connection_data.user else f"some_user_{db_connection_id}",
    attributes = db_connection_data.attributes if db_connection_data.attributes else '{created_user_id: 10, updated_user_id: 11}'

    return DbConnection(
        id=db_connection_id,
        title=title,
        slug=simple_slugify(title),
        host=host,
        port=port,
        database=database,
        user=user,
        attributes=attributes,
    )


@app.delete(
    '/db-connections/{db_connection_id}',
    tags=['DB Connections'],
    summary='Delete db connection',
    description='Deleting of db connection',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse}
    }
)
async def db_connection_delete(db_connection_id: int):
    return None


@app.patch(
    '/db-connections/{db_connection_id}',
    tags=['DB Connections'],
    summary='Rename db connection',
    description='Renaming of db connection',
    response_model=DbConnection,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse}
    }
)
async def db_connection_rename(db_connection_id: int, title: str):
    return DbConnection(
        id=db_connection_id,
        title=title,
        slug=simple_slugify(title),
        host="127.0.0.1",
        port="5432",
        database=f"some_private_db_{db_connection_id}",
        user=f"some_user_{db_connection_id}",
        attributes='{created_user_id: 10, updated_user_id: 11}',
    )


@app.post(
    '/db-connections/{db_connection_id}/check',
    tags=['DB Connections'],
    summary='Check existing db connection',
    description='Checking of existing db connection',
    response_model=DbCheckConnectionStatus,
    responses={
        "400": {"model": ErrorResponse},
        "401": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def db_connection_check(db_connection_id: int, credentials: Optional[DbConnectionCredentials] = None):
    return DbCheckConnectionStatus(
        status=True
    )


@app.post(
    '/db-connections/raw-check',
    tags=['DB Connections'],
    summary='Check not already exists db connection',
    description='Checking of not already exists db connection',
    response_model=DbCheckConnectionStatus,
    responses={
        "400": {"model": ErrorResponse},
        "401": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def db_connection_raw_check(credentials: DbConnectionFullCredentials):
    return DbCheckConnectionStatus(
        status=True
    )


#############################################
# Projects
#############################################

@app.get(
    '/project',
    tags=['Projects'],
    summary='List of projects',
    description='Receive list of projects',
    response_model=List[Project],
)
async def projects_list():
    return [Project(
        id=i,
        title=f'Some private project {i}',
        slug=f'some_private_project_{i}',
        created=datetime.now(),
        updated=datetime.now(),
        custom_pg_dump_path='',
        attributes='{created_user_id: 10, updated_user_id: 11}',
        last_scan_run=datetime.now() if i > 0 else None,
        last_scan_task_status_id=2 if i > 0 else None,
        last_dump_run=datetime.now() if i > 1 else None,
        last_dump_task_status_id=1 if i > 1 else None,
    ) for i in range(1, 3)]


@app.get(
    '/project/{project_id}',
    tags=['Projects'],
    summary='Project details',
    description='Receive project details',
    response_model=Project,
    responses={
        "404": {"model": ErrorResponse}
    }
)
async def project_details(project_id: int = None):
    return Project(
        id=project_id,
        title=f'Some private project {project_id}',
        slug=f'some_private_project_{project_id}',
        created=datetime.now(),
        updated=datetime.now(),
        custom_pg_dump_path='',
        attributes='{created_user_id: 10, updated_user_id: 11}',
        last_scan_run=datetime.now(),
        last_scan_task_status_id=2,
        last_dump_run=datetime.now(),
        last_dump_task_status_id=1,
    )


@app.post(
    '/project',
    tags=['Projects'],
    summary='Create new project',
    description='Creating of new project',
    response_model=Project,
    responses={
        "400": {"model": ErrorResponse}
    }
)
async def project_create(project_data: ProjectCreate):
    project_id = 10
    return Project(
        id=project_id,
        title=project_data.title,
        slug=simple_slugify(project_data.title),
        created=datetime.now(),
        updated=datetime.now(),
        custom_pg_dump_path=project_data.custom_pg_dump_path,
        attributes=project_data.attributes,
    )


@app.put(
    '/project/{project_id}',
    tags=['Projects'],
    summary='Update existing project',
    description='Updating of existing project',
    response_model=Project,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse}
    }
)
async def project_update(project_id: int, project_data: ProjectUpdate):
    title = project_data.title if project_data.title else f"some project {project_id}"
    attributes = project_data.attributes if project_data.attributes else '{created_user_id: 10, updated_user_id: 11}'

    return Project(
        id=project_id,
        title=title,
        slug=simple_slugify(title),
        created=datetime.now() - timedelta(days=1),
        updated=datetime.now(),
        custom_pg_dump_path=project_data.custom_pg_dump_path,
        attributes=attributes
    )


@app.delete(
    '/project/{project_id}',
    tags=['Projects'],
    summary='Delete project',
    description='Deleting of project',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def project_delete(project_id: int):
    return None


@app.patch(
    '/project/{project_id}',
    tags=['Projects'],
    summary='Rename project',
    description='Renaming of project',
    response_model=Project,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse}
    }
)
async def project_rename(project_id: int, title: str):
    return Project(
        id=project_id,
        title=title,
        slug=simple_slugify(title),
        created=datetime.now(),
        updated=datetime.now(),
        custom_pg_dump_path="",
        attributes='{created_user_id: 10, updated_user_id: 11}',
    )


#############################################
# Dictionaries
#############################################

@app.get(
    '/dictionary/template',
    tags=['Dictionaries'],
    summary='Get dictionary template by type of dictionary',
    description='Getting of dictionary template by type of dictionary',
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def dictionary_template(type_id: int):
    template = TEMPLATE_META_DICT

    if type_id == 2:
        template = TEMPLATE_SENS_DICT
    elif type_id == 3:
        template = TEMPLATE_NO_SENS_DICT

    return template


#############################################
# Dictionaries
#############################################


@app.get(
    '/dictionary',
    tags=['Dictionaries'],
    summary='List of dictionaries',
    description='Receive list of dictionaries',
    response_model=List[DictionaryShort],
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def dictionary_list(project_id: int, type_id: Optional[int] = None,
                          is_predefined: Optional[bool] = None, scan_id: Optional[int] = None):
    return [DictionaryShort(
        id=i,
        title=f'Some dict {i}',
        slug=f'some_dict_{i}',
        project_id=project_id,
        type_id=type_id if type_id else i % 3 + 1,
        is_predefined=True if is_predefined else bool(i % 2),
        created=datetime.now(),
        updated=datetime.now(),
        attributes='{created_user_id: 10, updated_user_id: 11}',
        scan_title=f"Title of scan with id = {scan_id}" if scan_id else None,
    ) for i in range(1, 8)]


@app.get(
    '/dictionary/{dict_id}',
    tags=['Dictionaries'],
    summary='Project details',
    description='Receive project details',
    response_model=DictionaryDetailed,
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def dictionary_details(dict_id: int = None):
    project_id = 15
    return DictionaryDetailed(
        id=dict_id,
        title=f'Some dict {dict_id}',
        slug=f'some_dict_{dict_id}',
        project_id=project_id,
        type_id=1,
        is_predefined=False,
        created=datetime.now(),
        updated=datetime.now(),
        attributes='{created_user_id: 10, updated_user_id: 11}',
        scan_title=None,
        content=TEMPLATE_META_DICT,
    )


@app.post(
    '/dictionary',
    tags=['Dictionaries'],
    summary='Create new dictionary',
    description='Creating of new dictionary',
    response_model=DictionaryDetailed, # Либо просто возвращать 201
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dictionary_create(dict_data: DictionaryCreate):
    return DictionaryDetailed(
        id=10,
        title=dict_data.title,
        slug=simple_slugify(dict_data.title),
        project_id=dict_data.project_id,
        type_id=dict_data.type_id,
        is_predefined=False,
        created=datetime.now(),
        updated=datetime.now(),
        attributes=dict_data.attributes,
        content=dict_data.content,
    )


@app.put(
    '/dictionary/{dict_id}',
    tags=['Dictionaries'],
    summary='Update existing dictionary',
    description='Updating of existing dictionary',
    response_model=DictionaryDetailed,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dictionary_update(dict_id: int, dict_data: DictionaryUpdate):
    project_id = 15
    return DictionaryDetailed(
        id=dict_id,
        title=dict_data.title,
        slug=simple_slugify(dict_data.title),
        project_id=project_id,
        type_id=1,
        is_predefined=False,
        created=datetime.now() - timedelta(days=1),
        updated=datetime.now(),
        attributes=dict_data.attributes if dict_data.attributes else '{created_user_id: 10, updated_user_id: 11}',
        content=dict_data.content,
    )


@app.delete(
    '/dictionary/{dict_id}',
    tags=['Dictionaries'],
    summary='Delete dictionary',
    description='Deleting of dictionary',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dictionary_delete(dict_id: int):
    return None


@app.patch(
    '/dictionary/{dict_id}',
    tags=['Dictionaries'],
    summary='Rename dictionary',
    description='Renaming of dictionary',
    response_model=DictionaryShort,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dictionary_rename(dict_id: int, title: str):
    project_id = 15
    return DictionaryShort(
        id=dict_id,
        title=title,
        slug=simple_slugify(title),
        project_id=project_id,
        type_id=2,
        is_predefined=False,
        created=datetime.now() - timedelta(days=1),
        updated=datetime.now(),
        attributes='{created_user_id: 10, updated_user_id: 11}',
    )


@app.post(
    '/dictionary/{dict_id}/duplicate',
    tags=['Dictionaries'],
    summary='Duplicate dictionary with another name',
    description='Duplicating of dictionary with another name',
    response_model=DictionaryShort,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dictionary_duplicate(dict_id: int, dict_data: DictionaryDuplicate):
    project_id = 15
    return DictionaryShort(
        id=dict_id,
        title=dict_data.title,
        slug=simple_slugify(dict_data.title),
        project_id=project_id,
        type_id=2,
        is_predefined=False,
        created=datetime.now(),
        updated=datetime.now(),
        attributes=dict_data.attributes
    )


#############################################
# Scans
#############################################

@app.get(
    '/scan',
    tags=['Scans'],
    summary='List of scans',
    description='Receive list of scans',
    response_model=List[Scan],
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def scan_list(project_id: int):
    return [Scan(
        id=i,
        title=f'Some scan {i}',
        slug=f'some_scan_{i}',
        project_id=project_id,
        type_id=1 if i < 2 else 2,
        depth=None if i < 2 else 100 * i,
        source_db=DbConnection(
            id=12,
            title="some_db_connection",
            slug="some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_private_db_{project_id}",
        ),
        input_meta_dict_titles=["meta_dict_1", "meta_dict_2", "meta_dict_3"],
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"] if i > 2 else None,
        input_no_sens_dict_titles=["no_sens_dict_1", "no_sens_dict_2"] if i > 4 else None,
        attributes='{created_user_id: 10, updated_user_id: 11}',
        status_id=i % 3 + 1,
        created=datetime.now(),
        updated=datetime.now(),
    ) for i in range(1, 8)]


@app.get(
    '/scan/{scan_id}',
    tags=['Scans'],
    summary='Scan details',
    description='Receive scan details',
    response_model=Scan,
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def scan_details(scan_id: int = None):
    project_id = 20
    return Scan(
        id=scan_id,
        title=f'Some scan {scan_id}',
        slug=f'some_scan_{scan_id}',
        project_id=project_id,
        type_id=1,
        depth=None,
        source_db=DbConnection(
            id=12,
            title="some_db_connection",
            slug="some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_private_db_{project_id}",
        ),
        input_meta_dict_titles=["meta_dict_1", "meta_dict_2", "meta_dict_3"],
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"],
        input_no_sens_dict_titles=["no_sens_dict_1", "no_sens_dict_2"],
        attributes='{created_user_id: 10, updated_user_id: 11}',
        status_id=1,
        created=datetime.now(),
        updated=datetime.now(),
    )


@app.post(
    '/scan',
    tags=['Scans'],
    summary='Create new scan',
    description='Creating of new scan',
    response_model=Scan,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def scan_create(scan_options: ScanCreate):
    scan_id = 10
    input_meta_dict_titles = ', '.join([f'meta_dict_{id}' for id in scan_options.input_meta_dict_ids])
    input_sens_dict_titles = ', '.join([f'sens_dict_{id}' for id in scan_options.input_sens_dict_ids])
    input_no_sens_dict_titles = ', '.join([f'no_sens_dict_{id}' for id in scan_options.input_no_sens_dict_ids])

    return Scan(
        id=scan_id,
        title=scan_options.title,
        slug=simple_slugify(scan_options.title),
        project_id=scan_options.project_id,
        type_id=scan_options.type_id,
        depth=scan_options.depth,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection_{scan_options.source_db_id}",
            slug=f"some_db_connection_{scan_options.source_db_id}",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection_{scan_options.source_db_id}",
        ),
        input_meta_dict_titles=input_meta_dict_titles,
        input_sens_dict_titles=input_sens_dict_titles,
        input_no_sens_dict_titles=input_no_sens_dict_titles,
        attributes='{created_user_id: 10, updated_user_id: 11}',
        status_id=1,
        created=datetime.now(),
        updated=datetime.now(),
    )


@app.delete(
    '/scan/{scan_id}',
    tags=['Scans'],
    summary='Delete scan',
    description='Deleting of scan',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def scan_delete(scan_id: int):
    return None


@app.patch(
    '/scan/{scan_id}',
    tags=['Scans'],
    summary='Rename scan',
    description='Renaming of scan',
    response_model=Scan,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def scan_rename(scan_id: int, title: str):
    return Scan(
        id=scan_id,
        title=title,
        slug=simple_slugify(title),
        type_id=1,
        depth=None,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection",
            slug=f"some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection",
        ),
        input_meta_dict_titles=["meta_dict_1", "meta_dict_2", "meta_dict_3"],
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"],
        input_no_sens_dict_titles=["no_sens_dict_1", "no_sens_dict_2"],
        attributes='{created_user_id: 10, updated_user_id: 11}',
        status_id=1,
        created=datetime.now(),
        updated=datetime.now(),
    )


@app.post(
    '/scan/{scan_id}/stop',
    tags=['Scans'],
    summary='Stop scan task',
    description='Stopping scan task',
    status_code=202,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def scan_stop(scan_id: int):
    return None


#############################################
# Preview
#############################################

@app.get(
    '/preview',
    tags=['Previews'],
    summary='List of previews',
    description='Receive list of previews',
    response_model=List[Preview],
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def preview_list(project_id: int):
    return [Preview(
        id=i,
        title=f'Some preview {i}',
        slug=f'some_preview_{i}',
        project_id=project_id,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection",
            slug=f"some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection",
        ),
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"],
        attributes='{created_user_id: 10, updated_user_id: 11}',
        created=datetime.now(),
        updated=datetime.now(),
    ) for i in range(1, 8)]


@app.get(
    '/preview/{preview_id}',
    tags=['Previews'],
    summary='Preview details',
    description='Receive preview details',
    response_model=Preview,
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def preview_details(preview_id: int = None):
    project_id = 10
    return Preview(
        id=preview_id,
        title=f'Some preview {preview_id}',
        slug=f'some_preview_{preview_id}',
        project_id=project_id,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection",
            slug=f"some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection",
        ),
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"],
        attributes='{created_user_id: 10, updated_user_id: 11}',
        created=datetime.now(),
        updated=datetime.now(),
    )


@app.post(
    '/preview',
    tags=['Previews'],
    summary='Create new preview',
    description='Creating of new preview',
    response_model=Preview,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def preview_create(preview_options: PreviewCreate):
    preview_id = 10
    input_sens_dict_titles = ', '.join([f'sens_dict_{id}' for id in preview_options.input_sens_dict_ids])

    return Preview(
        id=preview_id,
        title=preview_options.title,
        slug=simple_slugify(preview_options.title),
        project_id=preview_options.project_id,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection",
            slug=f"some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection",
        ),
        input_sens_dict_titles=input_sens_dict_titles,
        attributes=preview_options.attributes,
        created=datetime.now(),
        updated=datetime.now(),
    )


@app.put(
    '/preview/{preview_id}',
    tags=['Previews'],
    summary='Update preview',
    description='Updating preview',
    response_model=Preview,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def preview_update(preview_id: int, preview_options: PreviewUpdate):
    project_id = 15
    title = preview_options.title if preview_options.title else 'some_preview'
    input_sens_dict_titles = ["sens_dict"]
    if preview_options.input_sens_dict_ids:
        input_sens_dict_titles = ', '.join([f'sens_dict_{id}' for id in preview_options.input_sens_dict_ids])

    return Preview(
        id=preview_id,
        title=title,
        slug=simple_slugify(title),
        project_id=project_id,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection_{preview_options.source_db_id if preview_options.source_db_id else 11}",
            slug=f"some_db_connection_{preview_options.source_db_id if preview_options.source_db_id else 11}",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection_{preview_options.source_db_id if preview_options.source_db_id else 11}",
        ),
        input_sens_dict_titles=f"[{input_sens_dict_titles}]",
        attributes=preview_options.attributes,
        created=datetime.now() - timedelta(days=1),
        updated=datetime.now(),
    )


@app.delete(
    '/preview/{preview_id}',
    tags=['Previews'],
    summary='Delete preview',
    description='Deleting of preview',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def preview_delete(preview_id: int):
    return None


@app.patch(
    '/preview/{preview_id}',
    tags=['Previews'],
    summary='Rename preview',
    description='Renaming of preview',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def preview_rename(preview_id: int, title: str):
    project_id = 10
    return Preview(
        id=preview_id,
        title=title,
        slug=simple_slugify(title),
        project_id=project_id,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection",
            slug=f"some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection",
        ),
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"],
        attributes='{created_user_id: 10, updated_user_id: 11}',
        created=datetime.now(),
        updated=datetime.now(),
    )


@app.post(
    '/preview/{preview_id}/fields',
    tags=['Previews'],
    summary='Render preview rules by fields',
    description='Rendering of preview rules by fields',
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def preview_fields(preview_id: int):
    time.sleep(3)  # emulate working
    content = [
        {
            "schema": "public",
            "table": "some_table",
            "field": "non_sens_field",
            "type": "text",
            "dict_file_name": "---",
            "rule": "---"
        },
        {
            "schema": "public",
            "table": "some_table",
            "field": "sens_field",
            "type": "text",
            "dict_file_name": "some_sens_dict.py",
            "rule": "'***'"
        }
    ]
    return Content(
        content=json.dumps(content, ensure_ascii=False),
    )


@app.post(
    '/preview/{preview_id}/data',
    tags=['Previews'],
    summary='Render preview data by rules',
    description='Rendering of preview data by rules',
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def preview_fields(preview_id: int):
    time.sleep(3)  # emulate working
    content = ('{"id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],'
               ' "customer_company_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],'
               ' "customer_manager_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],'
               ' "* amount": ["0", "4.47359410464563", "-15.3125281079801",'
               ' "18.5631230710571", "25.1441671839476", "-20.2687078063081", "-34.950210241733",'
               ' "-44.8546976678868", "-17.9484602231054", "34.660959502514"],'
               ' "* details": ["9367b797b4fffa3635bfa988f49d1cfe", "011c27f81ed321ca29424eaae2cf84f3",'
               ' "b337c3b18dd54e9f3d88b87bf91de8c2", "1b648e0f446f75c4321cac621cd5a3d8",'
               ' "f70fcc3598d32ba4e1372596b9dd9747", "fd2c3a03ef3baa6d564052d61c475180",'
               ' "c8b2ee5f14da86fb88cd305af0b61172", "626aafd6565e8bde69532e310c132c57",'
               ' "bb30e4e0bf498fc74b390b89ddc7f990", "db97f96bbfe578e30b8df9a8f0452d74"],'
               ' "status": ["closed", "closed", "closed", "closed", "closed", "closed", "closed",'
               ' "closed", "closed", "closed"],'
               ' "* contract_expires": ["2026-09-12 07:38:14.342579", "2025-11-21 02:01:21.666190",'
               ' "2026-01-28 17:39:34.510706", "2025-09-12 14:58:20.510293", "2025-11-28 22:46:58.312648",'
               ' "2025-12-20 02:15:36.141231", "2025-08-25 00:53:15.074384", "2026-08-24 08:14:09.941986",'
               ' "2026-07-13 01:34:39.336358", "2026-12-24 14:23:55.405232"]}')
    return Content(
        content=content
    )


#############################################
# Dumps
#############################################

@app.get(
    '/dump',
    tags=['Dumps'],
    summary='List of dumps',
    description='Receive list of dumps',
    response_model=List[Dump],
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def dump_list(project_id: int):
    return [Dump(
        id=i,
        title=f'Some dump {i}',
        slug=f'some_dump_{i}',
        project_id=project_id,
        type_id=1 if i < 2 else 2,
        depth=None if i < 2 else 100 * i,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection",
            slug=f"some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection",
        ),
        input_meta_dict_titles=["meta_dict_1", "meta_dict_2", "meta_dict_3"],
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"] if i > 2 else None,
        input_no_sens_dict_titles=["no_sens_dict_1", "no_sens_dict_2"] if i > 4 else None,
        attributes='{created_user_id: 10, updated_user_id: 11}',
        status_id=i % 3 + 1,
        created=datetime.now(),
        updated=datetime.now(),
    ) for i in range(1, 8)]


@app.get(
    '/dump/{dump_id}',
    tags=['Dumps'],
    summary='Dump details',
    description='Receive dump details',
    response_model=Dump,
    responses={
        "404": {"model": ErrorResponse},
    }
)
async def dump_details(dump_id: int = None):
    project_id = 10
    return Dump(
        id=dump_id,
        title=f'Some dict {dump_id}',
        slug=f'some_dict_{dump_id}',
        project_id=project_id,
        type_id=1,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection",
            slug=f"some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection",
        ),
        custom_path=None,
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"],
        attributes='{created_user_id: 10, updated_user_id: 11}',
        status_id=2,
        created=datetime.now(),
        updated=datetime.now(),
        size="1024000000"
    )


@app.post(
    '/dump',
    tags=['Dumps'],
    summary='Create new dump',
    description='Creating of new dump',
    response_model=Dump,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dump_create(dump_options: DumpCreate):
    dump_id = 10
    input_sens_dict_titles = ', '.join([f'sens_dict_{id}' for id in dump_options.input_sens_dict_ids])
    return Dump(
        id=dump_id,
        title=dump_options.title,
        slug=simple_slugify(dump_options.title),
        type_id=dump_options.type_id,
        source_db=DbConnection(
            id=12,
            title=f"some_db_connection",
            slug=f"some_db_connection",
            host="127.0.0.1",
            port="5432",
            database=f"some_db_connection",
        ),
        custom_path=dump_options.custom_path,
        input_sens_dict_titles=input_sens_dict_titles,
        attributes='{created_user_id: 10, updated_user_id: 11}',
        status_id=1,
        created=datetime.now(),
        updated=datetime.now(),
        size=None
    )


@app.delete(
    '/dump/{dump_id}',
    tags=['Dumps'],
    summary='Delete dump',
    description='Deleting of dump',
    status_code=204,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dump_delete(dump_id: int):
    return None


@app.patch(
    '/dump/{dump_id}',
    tags=['Dumps'],
    summary='Rename dump',
    description='Renaming of dump',
    response_model=Dump,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dump_rename(dump_id: int, title: str):
    return Dump(
        id=dump_id,
        title=title,
        slug=simple_slugify(title),
        type_id=1,
        db_name=f'some_private_db_{dump_id}',
        custom_path=None,
        input_sens_dict_titles=["sens_dict_1", "sens_dict_2"],
        attributes='{created_user_id: 10, updated_user_id: 11}',
        status_id=2,
        created=datetime.now(),
        updated=datetime.now(),
        size="1024000000"
    )


@app.post(
    '/dump/{dump_id}/stop',
    tags=['Dumps'],
    summary='Stop dump task',
    description='Stopping dump task',
    status_code=202,
    responses={
        "400": {"model": ErrorResponse},
        "404": {"model": ErrorResponse},
    }
)
async def dump_stop(dump_id: int):
    return None


#############################################
# Handbooks
#############################################

@app.get(
    '/handbook/dictionary-types',
    tags=['Handbooks'],
    summary='List of dictionary types',
    response_model=List[DictionaryType],
)
async def dictionary_types():
    return [
        TaskStatus(
            id=1,
            title="Мета-словарь",
            slug="meta_dict",
        ),
        TaskStatus(
            id=2,
            title="Сенситивный словарь",
            slug="sens_dict",
        ),
        TaskStatus(
            id=3,
            title="Несенситивный словарь",
            slug="no_sens_dict",
        ),
    ]


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


#############################################
# Stateless API
#############################################

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
    print("DB connection check request=", request)


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
async def stateless_scan_start(request: ScanRequest):
    print("Scan request=", request)

    asyncio.ensure_future(
        scan_callback(request)
    )


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
    print("Preview fields request=", request)

    await asyncio.sleep(2)  # emulate working

    return ViewFieldsResponse(
        status_id=2,  # success
        content=[
            ViewFieldsContent(
                schema='public',
                table='users',
                field='id',
                type='serial',
                dict_file_name='---',
                rule='---',
            ),
            ViewFieldsContent(
                schema='public',
                table='users',
                field='email',
                type='text',
                dict_file_name='---',
                rule="md5(email) || '@abc.com'",
            ),
            ViewFieldsContent(
                schema='public',
                table='users',
                field='login',
                type='text',
                dict_file_name='---',
                rule="---",
            )
        ]
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
    print("Preview data request=", request)

    await asyncio.sleep(2)  # emulate working

    return ViewDataResponse(
        status_id=2,  # success
        content=[
            ViewDataContent(
                schema='public',
                table='users',
                fields=[
                    'id',
                    'email',
                    'login',
                ],
                total_rows_count=3,
                rows_before=[
                    [1, 'user1001@example.com', 'user1001'],
                    [2, 'user1002@example.com', 'user1002'],
                    [3, 'user1003@example.com', 'user1003'],
                ],
                rows_after=[
                    [1, '385513d80895c4c5e19c91d1df9eacae@abc.com', 'user1001'],
                    [2, '9f4c0c30f85b0353c4d5fe3c9cc633e3@abc.com', 'user1002'],
                    [3, 'e4e9fe7090f5be634be77db8f86e453c@abc.com', 'user1003'],
                ],
            ),
        ]
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
async def stateless_dump_start(request: DumpRequest):
    print("Dump request=", request)

    asyncio.ensure_future(
        dump_callback(request)
    )


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
async def dump_operation_delete(request: DumpDeleteRequest):
    print(f'Delete dump dir in path {get_full_dump_path(request.path)}')
