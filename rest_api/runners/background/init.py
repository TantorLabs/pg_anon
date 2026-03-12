from pg_anon.common.enums import AnonMode
from rest_api.runners.background import BaseRunner


class InitRunner(BaseRunner):
    mode: str = AnonMode.INIT.value
