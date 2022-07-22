import sys
import threading
import time
import queue
from datetime import datetime
import json
import os
import argparse
from pkg_resources import parse_version as version
from enum import Enum
import logging
import traceback
import asyncpg
from functools import wraps, partial
import asyncio
import subprocess
import re
from hashlib import sha256


def get_pg_util_version(util_name):
    command = [util_name, "--version"]
    res = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    return re.findall(r"(\d+\.\d+)", str(res.stdout))[0]


def exception_helper(show_traceback=True):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    return "\n".join(
        [
            v for v in traceback.format_exception(exc_type, exc_value, exc_traceback if show_traceback else None)
        ]
    )


class PgAnonResult:
    params = None       # JSON
    result_code = None
    result_data = None
