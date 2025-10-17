import tempfile
from pathlib import Path

from pg_anon.common.constants import BASE_DIR

BASE_TEMP_DIR = Path(tempfile.gettempdir()) / 'pg_anon'
DUMP_STORAGE_BASE_DIR = BASE_DIR / 'output'
