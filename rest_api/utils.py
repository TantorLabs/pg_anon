import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import List, Optional, Dict

import aioprocessing

from pg_anon.cli import run_pg_anon
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.utils import validate_exists_mode, simple_slugify
from rest_api.constants import DUMP_STORAGE_BASE_DIR
from rest_api.pydantic_models import DictionaryContent, DictionaryMetadata


def get_full_dump_path(dump_path: str) -> str:
    return str(DUMP_STORAGE_BASE_DIR / dump_path.lstrip("/"))


def write_dictionary_contents(dictionary_contents: List[DictionaryContent], base_dir: Path) -> Dict[str, DictionaryMetadata]:
    file_names = {}
    base_dir.mkdir(parents=True, exist_ok=True)

    for dictionary_content in dictionary_contents:
        file_name = (base_dir / simple_slugify(dictionary_content.name)).with_suffix('.py')
        with open(file_name, "w") as out_file:
            out_file.write(dictionary_content.content)

        file_names[str(file_name)] = DictionaryMetadata(
            name=dictionary_content.name,
            additional_info=dictionary_content.additional_info,
        )

    return file_names


def read_dictionary_contents(file_path: str | Path) -> str:
    with open(file_path, "r") as dictionary_file:
        data = dictionary_file.read()

    return data


def read_json_file(file_path: str | Path) -> Dict:
    with open(file_path, "r") as file:
        data = json.loads(file.read())

    return data


def delete_folder(folder_path: str):
    try:
        shutil.rmtree(folder_path)
        print(f"Folder {folder_path} deleted successfully.")
    except Exception as e:
        print(f"Error deleting folder {folder_path}: {str(e)}")


def run_pg_anon_subprocess_wrapper(queue: aioprocessing.AioQueue, cli_run_params: List[str]):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Выполняем асинхронную функцию внутри нового event loop
        result = loop.run_until_complete(
            run_pg_anon(cli_run_params)
        )
        queue.put(result)
    except Exception as ex:
        print(ex)
    finally:
        queue.put(None)  # Завершаем процесс
        queue.close()
        loop.close()


async def run_pg_anon_worker(mode: str, operation_id: str, cli_run_params: List[str]) -> Optional[PgAnonResult]:
    if not validate_exists_mode(mode):
        raise ValueError(f'Invalid mode: {mode}')

    application_name_suffix = f'worker__{mode}__{operation_id}'
    cli_run_params.extend([
        f'--mode={mode}',
        f'--application-name-suffix={application_name_suffix}',
    ])

    queue = aioprocessing.AioQueue()

    p = aioprocessing.AioProcess(
        name=f"pg_anon_{application_name_suffix}",
        target=run_pg_anon_subprocess_wrapper,
        args=(queue, cli_run_params),
    )
    p.start()

    result = None
    while True:
        coro_result = await queue.coro_get()
        if coro_result is None:
            break
        result = coro_result
    await p.coro_join()

    return result


def normalize_headers(headers: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not headers:
        return None

    headers = {k.lower(): v for k, v in headers.items()}
    headers.setdefault('content-type', 'application/json')
    return headers
