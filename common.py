import sys
import traceback
import subprocess
import re
from enum import Enum
from pkg_resources import parse_version as version


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


def exception_handler(func):
    def f(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            print(exception_helper(show_traceback=True))
    return f


class BasicEnum():
    def __str__(self):
        return self.value


class ResultCode(BasicEnum, Enum):
    DONE = 'done'
    FAIL = 'fail'
    UNKNOWN = 'unknown'


class PgAnonResult:
    params = None            # JSON
    result_code = ResultCode.UNKNOWN
    result_data = None


def get_major_version(str_version):
    return version(re.findall(r"(\d+)", str_version)[0])


def pretty_size(bytes_v):
    units = [
        (1 << 50, ' PB'),
        (1 << 40, ' TB'),
        (1 << 30, ' GB'),
        (1 << 20, ' MB'),
        (1 << 10, ' KB'),
        (1, (' byte', ' bytes')),
    ]
    for factor, suffix in units:
        if bytes_v >= factor:
            break
    amount = int(bytes_v / factor)

    if isinstance(suffix, tuple):
        singular, multiple = suffix
        if amount == 1:
            suffix = singular
        else:
            suffix = multiple
    return str(amount) + suffix
