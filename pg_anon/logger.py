from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

from concurrent_log_handler import ConcurrentRotatingFileHandler

if TYPE_CHECKING:
    from pathlib import Path


class Logger:
    _instance: Logger | None = None
    _formatter: logging.Formatter

    logger: logging.Logger

    def __new__(cls) -> Logger:  # noqa: PYI034
        """Create or return the singleton Logger instance."""
        if cls._instance is not None:
            return cls._instance

        cls._instance = super().__new__(cls)
        cls._instance.logger = logging.getLogger("pg_anon.logger")
        cls._instance.logger.setLevel(logging.INFO)

        cls._instance._formatter = logging.Formatter(  # noqa: SLF001
            datefmt="%Y-%m-%d %H:%M:%S",
            fmt="%(asctime)s,%(msecs)03d - %(levelname)8s - %(message)s",
        )

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(cls._instance._formatter)  # noqa: SLF001
        cls._instance.logger.addHandler(handler)

        return cls._instance

    def add_file_handler(self, log_dir: Path, log_file_name: str) -> None:
        """Add a rotating file handler to the logger."""
        for handler in list(self.logger.handlers):
            if isinstance(handler, logging.FileHandler):
                self.logger.removeHandler(handler)
                handler.close()

        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = ConcurrentRotatingFileHandler(
            log_dir / log_file_name,
            maxBytes=10 * 1024 * 1024,
            backupCount=10,
        )
        file_handler.setFormatter(self._formatter)
        self.logger.addHandler(file_handler)

    def set_log_level(self, log_level: int) -> None:
        """Set the logging level."""
        self.logger.setLevel(log_level)

    def __del__(self) -> None:
        """Flush and close all log handlers on instance destruction."""
        for handler in self.logger.handlers.copy():
            try:
                handler.acquire()
                handler.flush()
                handler.close()
            except Exception as ex:
                print(f"Error closing log handler: {ex}")
            finally:
                handler.release()
                self.logger.removeHandler(handler)


def get_logger() -> logging.Logger:
    """Return the singleton logger instance."""
    return Logger().logger


def logger_add_file_handler(log_dir: Path, log_file_name: str) -> None:
    """Add a rotating file handler to the singleton logger."""
    Logger().add_file_handler(
        log_dir=log_dir,
        log_file_name=log_file_name,
    )


def logger_set_log_level(log_level: int) -> None:
    """Set the logging level on the singleton logger."""
    Logger().set_log_level(log_level)
