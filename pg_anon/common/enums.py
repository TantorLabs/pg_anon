from enum import Enum


class ResultCode(Enum):
    DONE = "done"
    FAIL = "fail"
    UNKNOWN = "unknown"


class VerboseOptions(Enum):
    INFO = "info"
    DEBUG = "debug"
    ERROR = "error"


class AnonMode(Enum):
    DUMP = "dump"  # dump table contents to files using dictionary
    RESTORE = "restore"  # create tables in target database and load data from files
    INIT = "init"  # create a schema with anonymization helper functions
    SYNC_DATA_DUMP = "sync-data-dump"  # synchronize the contents of one or more tables (dump stage)
    SYNC_DATA_RESTORE = "sync-data-restore"  # synchronize the contents of one or more tables (restore stage)
    SYNC_STRUCT_DUMP = "sync-struct-dump"  # synchronize the structure of one or more tables (dump stage)
    SYNC_STRUCT_RESTORE = "sync-struct-restore"  # synchronize the structure of one or more tables (restore stage)
    CREATE_DICT = "create-dict"  # create dictionary
    VIEW_FIELDS = "view-fields"  # view fields
    VIEW_DATA = "view-data"  # view data using prepared-sens-dict-file


class ScanMode(Enum):
    FULL = "full"
    PARTIAL = "partial"
