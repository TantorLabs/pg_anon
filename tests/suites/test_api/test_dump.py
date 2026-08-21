from __future__ import annotations

from .conftest import input_dict_text
from .helpers import build_dump_request, uuid_short


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


async def test_dump_struct_uses_sync_struct_mode(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/dump_struct_{uuid_short()}",
        webhook_url=webhook_recorder.url,
        dump_type="sync-struct-dump",
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["mode"] == "sync-struct-dump"


async def test_dump_data_uses_sync_data_mode(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/dump_data_{uuid_short()}",
        webhook_url=webhook_recorder.url,
        dump_type="sync-data-dump",
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["mode"] == "sync-data-dump"


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


async def test_dump_ignore_privileges_pass_through(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/dump_priv_{uuid_short()}",
        webhook_url=webhook_recorder.url,
        ignore_privileges=True,
        proc_conn_count=2,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["ignore_privileges"] is True
    assert success["run_options"]["db_connections_per_process"] == 2


async def test_dump_conn_count_replaces_proc_conn_count(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/dump_conn_count_{uuid_short()}",
        webhook_url=webhook_recorder.url,
        conn_count=2,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["db_connections_per_process"] == 2


async def test_dump_deprecated_proc_conn_count_still_works(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/dump_proc_conn_{uuid_short()}",
        webhook_url=webhook_recorder.url,
        proc_conn_count=3,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["db_connections_per_process"] == 3


async def test_dump_conn_count_wins_over_deprecated_name(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/dump_conn_both_{uuid_short()}",
        webhook_url=webhook_recorder.url,
        conn_count=2,
        proc_conn_count=5,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["db_connections_per_process"] == 2


async def test_dump_log_level_is_passed_through(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/dump_loglevel_{uuid_short()}",
        webhook_url=webhook_recorder.url,
        log_level="error",
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["verbose"] == "error"


async def test_dump_defaults_to_info_log_level(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/dump_loglevel_default_{uuid_short()}",
        webhook_url=webhook_recorder.url,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    success = await _wait_success(webhook_recorder)
    assert success["run_options"]["verbose"] == "info"
    assert success["run_options"]["debug"] is False


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
