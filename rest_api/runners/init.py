from typing import List

from pg_anon.common.enums import AnonMode
from rest_api.enums import DumpModeHandbook
from rest_api.pydantic_models import DumpRequest
from rest_api.runners.base import BaseRunner
from rest_api.utils import write_dictionary_contents, get_full_dump_path


class InitRunner(BaseRunner):
    mode: str = AnonMode.INIT.value
