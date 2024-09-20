import os
import re

DUMP_STORAGE_BASE_DIR = '/some/dumps'


def simple_slugify(value: str):
    return re.sub(r'\W+', '-', value).strip('-').lower()


def get_full_dump_path(dump_path: str) -> str:
    return os.path.join(DUMP_STORAGE_BASE_DIR, dump_path)
