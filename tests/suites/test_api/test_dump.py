from __future__ import annotations

from typing import Any

from .conftest import input_dict_text
from .helpers import build_dump_request, uuid_short
from pg_anon.cli import build_run_options
from pg_anon.rest_api.pydantic_models import DumpRequest
from pg_anon.rest_api.runners.background import DumpRunner


async def _wait_success(recorder, *, max_wait: float = 240) -> dict:
    terminal = await recorder.wait_for_terminal(max_wait=max_wait)
    assert terminal["status"] == "success", terminal
    return terminal


async def test_dump_full_creates_artifact_and_reports_size(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
    dump_storage_base_dir,
):
    output_path = f"/dump_full_{uuid_short()}"
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=output_path,
        webhook_url=webhook_recorder.url,
        dump_type="dump",
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201

    success = await _wait_success(webhook_recorder)
    assert success["size"] > 0
    assert success["run_options"]["mode"] == "dump"
    assert success["started"] and success["ended"]
    dump_dir = dump_storage_base_dir / output_path.lstrip("/")
    assert dump_dir.exists()
    assert any(dump_dir.rglob("*.bin*"))


async def test_dump_partial_whitelist_passes_dict_to_runner(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        partial_tables_dict=input_dict_text("partial_tables.py"),
        output_path=f"/dump_whitelist_{uuid_short()}",
        webhook_url=webhook_recorder.url,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["partial_tables_dict_files"]


async def test_dump_partial_blacklist_passes_exclude_dict(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        partial_tables_exclude_dict=input_dict_text("partial_tables_exclude.py"),
        output_path=f"/dump_blacklist_{uuid_short()}",
        webhook_url=webhook_recorder.url,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["partial_tables_exclude_dict_files"]


async def test_dump_output_path_outside_storage_returns_400(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path="/../escape_me",
        webhook_url=webhook_recorder.url,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 400
    err = await resp.json()
    assert err["error_code"] == "INVALID_PATH"
    assert webhook_recorder.payloads == []


def _run_options(db_params, **request_fields: Any) -> dict:  # noqa: ANN401
    """Run options that the worker gets for this request; no dump is made."""
    body = build_dump_request(
        db_params=db_params,
        db_name="any_db",
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/opts_{uuid_short()}",
        webhook_url="http://example.invalid/hook",
        **request_fields,
    )
    runner = DumpRunner(DumpRequest.model_validate(body))
    return build_run_options([runner.mode, *runner.cli_params]).to_dict()


def test_dump_ignore_privileges_pass_through(db_params):
    assert _run_options(db_params, ignore_privileges=True)["ignore_privileges"] is True


def test_dump_conn_count_replaces_proc_conn_count(db_params):
    assert _run_options(db_params, conn_count=2)["db_connections_per_process"] == 2


def test_dump_deprecated_proc_conn_count_still_works(db_params):
    assert _run_options(db_params, proc_conn_count=3)["db_connections_per_process"] == 3


def test_dump_conn_count_wins_over_deprecated_name(db_params):
    assert _run_options(db_params, conn_count=2, proc_conn_count=5)["db_connections_per_process"] == 2


def test_dump_log_level_is_passed_through(db_params):
    assert _run_options(db_params, log_level="error")["verbose"] == "error"


def test_dump_defaults_to_info_log_level(db_params):
    run_options = _run_options(db_params)
    assert run_options["verbose"] == "info"
    assert run_options["debug"] is False


def test_dump_schema_options_pass_through(db_params):
    run_options = _run_options(db_params, exclude_schema_masks=["no_such_schema", "tmp_{1,2}"])
    assert run_options["exclude_schema_masks"] == ["no_such_schema", "tmp_{1,2}"], (
        "a comma inside regex brackets must not split the value"
    )
