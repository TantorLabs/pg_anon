from enum import Enum


class ScanModeHandbook(Enum):
    FULL = 1
    PARTIAL = 2


class DumpModeHandbook(Enum):
    FULL = 1
    STRUCT = 2
    DATA = 3


class ResponseStatusesHandbook(Enum):
    UNKNOWN = 1
    SUCCESS = 2
    ERROR = 3
    IN_PROGRESS = 4
    STARTING = 5
