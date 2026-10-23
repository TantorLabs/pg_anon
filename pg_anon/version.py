from importlib.metadata import PackageNotFoundError, version

try:
    # Get version from metadata
    __version__ = version("pg_anon")
except PackageNotFoundError:
    # TMP: if package is not installed, return hardcoded
    __version__ = "unknown"

# the oldest pg_anon that can restore the dumps of this version; raise it together with a breaking change of the dump
DUMP_MIN_PG_ANON_VERSION = "1.13.0"
