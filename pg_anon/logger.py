import logging
import os
import sys
from logging.handlers import RotatingFileHandler


class Logger:
    _instance = None
    _formatter: str
    _file_handler_is_setup: bool = False

    logger = None

    def __new__(cls):
        if cls._instance is not None:
            return cls._instance

        cls._instance = super().__new__(cls)
        cls._instance.logger = logging.getLogger('pg_anon.logger')
        cls._instance.logger.setLevel(logging.INFO)

        cls._instance._formatter = logging.Formatter(
            datefmt="%Y-%m-%d %H:%M:%S",
            fmt="%(asctime)s,%(msecs)03d - %(levelname)8s - %(message)s",
        )

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(cls._instance._formatter)
        cls._instance.logger.addHandler(handler)

        return cls._instance

    def add_file_handler(self, log_dir: str, log_file_name: str):
        if self._file_handler_is_setup:
            return

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = RotatingFileHandler(
            os.path.join(log_dir, log_file_name),
            maxBytes=1024 * 10000,
            backupCount=10,
        )
        file_handler.setFormatter(self._formatter)
        self.logger.addHandler(file_handler)

    def set_log_level(self, log_level: int):
        self.logger.setLevel(log_level)

    def __del__(self):
        # Закрытие всех обработчиков при уничтожении экземпляра класса
        for handler in self.logger.handlers.copy():
            try:
                handler.acquire()
                handler.flush()
                handler.close()
            except Exception as e:
                print(f"Error closing log handler: {e}")
            finally:
                handler.release()
                self.logger.removeHandler(handler)


def get_logger():
    return Logger().logger


def logger_add_file_handler(log_dir: str, log_file_name: str):
    Logger().add_file_handler(
        log_dir=log_dir,
        log_file_name=log_file_name,
    )


def logger_set_log_level(log_level: int):
    Logger().set_log_level(log_level)
