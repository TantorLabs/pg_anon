import decimal
import json
import sys
import traceback
import subprocess
import re
import os.path
from enum import Enum
from pkg_resources import parse_version as version


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


class VerboseOptions(BasicEnum, Enum):
    INFO = 'info'
    DEBUG = 'debug'
    ERROR = 'error'


class AnonMode(BasicEnum, Enum):
    DUMP = 'dump'           # dump table contents to files using dictionary
    RESTORE = 'restore'     # create tables in target database and load data from files
    INIT = 'init'           # create a schema with anonymization helper functions
    SYNC_DATA_DUMP = 'sync-data-dump'            # synchronize the contents of one or more tables (dump stage)
    SYNC_DATA_RESTORE = 'sync-data-restore'      # synchronize the contents of one or more tables (restore stage)
    SYNC_STRUCT_DUMP = 'sync-struct-dump'        # synchronize the structure of one or more tables (dump stage)
    SYNC_STRUCT_RESTORE = 'sync-struct-restore'  # synchronize the structure of one or more tables (restore stage)
    CREATE_DICT = 'create-dict'   # create dictionary


class ScanMode(BasicEnum, Enum):
    FULL = 'full'
    PARTIAL = 'partial'


def get_pg_util_version(util_name):
    command = [util_name, "--version"]
    res = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    return re.findall(r"(\d+\.\d+)", str(res.stdout))[0]


def check_pg_util(ctx, util_name, output_util_res):
    if not os.path.isfile(util_name):
        ctx.logger.error('ERROR: program %s is not exists!' % util_name)
        return False

    command = [util_name, "--version"]
    res = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if str(res.stdout).find(output_util_res) == -1:
        ctx.logger.error('ERROR: program %s is not %s!' % (util_name, output_util_res))
        return False

    return True


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


def chunkify(lst, n):
    return [lst[i::n] for i in range(n)]


def recordset_to_list(rs):
    res = []
    for rec in rs:
        res.append(dict(rec))
    return res


def recordset_to_list_flat(rs):
    res = []
    for rec in rs:
        row = []
        for _, v in dict(rec).items():
            row.append(v)
        res.append(row)
    return res


def setof_to_list(rs):
    res = []
    for rec in rs:
        for _, v in dict(rec).items():
            res.append(v)
    return res


def to_json(obj, formatted=False):
    def type_adapter(o):
        if isinstance(o, decimal.Decimal):
            return float(o)
    if formatted:
        return json.dumps(obj, default=type_adapter, ensure_ascii=False, indent=4, sort_keys=True)
    else:
        return json.dumps(obj, default=type_adapter, ensure_ascii=False).encode('utf8')
