from rest_api.runners.background import BaseRunner

from pg_anon.common.enums import AnonMode


class InitRunner(BaseRunner):
    mode: str = AnonMode.INIT.value
