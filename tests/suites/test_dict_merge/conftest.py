from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pg_anon.common.dto import RunOptions
from pg_anon.common.enums import AnonMode
from pg_anon.context import Context

if TYPE_CHECKING:
    from collections.abc import Callable


@pytest.fixture
def write_dict(tmp_path) -> Callable[[str | dict], str]:
    counter = {"n": 0}

    def _write(content: str | dict) -> str:
        counter["n"] += 1
        path = tmp_path / f"dict_{counter['n']}.py"
        path.write_text(content if isinstance(content, str) else repr(content))
        return str(path)

    return _write


@pytest.fixture
def make_context(tmp_path) -> Callable[..., Context]:
    def _make(
        *,
        mode: AnonMode = AnonMode.DUMP,
        prepared_sens_dict_files: list[str] | None = None,
        meta_dict_files: list[str] | None = None,
        prepared_no_sens_dict_files: list[str] | None = None,
        partial_tables_dict_files: list[str] | None = None,
        partial_tables_exclude_dict_files: list[str] | None = None,
    ) -> Context:
        options = RunOptions(
            pg_anon_version="test",
            internal_operation_id="dict-merge-unit",
            run_dir=str(tmp_path),
            mode=mode,
            db_host="localhost",
            db_port=5432,
            db_name="test_db",
            db_user="test_user",
            prepared_sens_dict_files=prepared_sens_dict_files,
            meta_dict_files=meta_dict_files,
            prepared_no_sens_dict_files=prepared_no_sens_dict_files,
            partial_tables_dict_files=partial_tables_dict_files,
            partial_tables_exclude_dict_files=partial_tables_exclude_dict_files,
        )
        return Context(options)

    return _make
