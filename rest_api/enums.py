from enum import Enum, StrEnum


class ScanMode(StrEnum):
    FULL = "full"
    PARTIAL = "partial"


class DumpMode(StrEnum):
    FULL = "dump"
    STRUCT = "sync-struct-dump"
    DATA = "sync-data-dump"


class RestoreMode(StrEnum):
    FULL = "restore"
    STRUCT = "sync-struct-restore"
    DATA = "sync-data-restore"


class ResponseStatus(Enum):
    UNKNOWN = 1
    SUCCESS = 2
    ERROR = 3
    IN_PROGRESS = 4
    STARTING = 5
