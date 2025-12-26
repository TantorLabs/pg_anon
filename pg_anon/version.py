from importlib.metadata import version, PackageNotFoundError


try:
    # Get version from metadata
    __version__ = version("pg_anon")
except PackageNotFoundError:
    # TMP: if package is not installed, return hardcoded
    __version__ = "1.8.6"
