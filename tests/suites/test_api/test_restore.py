from __future__ import annotations

from .conftest import input_dict_text
from .helpers import build_restore_request, run_dump_via_api, uuid_short


async def _wait_terminal(recorder, *, max_wait: float = 240) -> dict:
    return await recorder.wait_for_terminal(max_wait=max_wait)


async def _dump_for_restore(
    api_client,
    api_source_db,
    db_params,
    *,
    dump_type: str = "dump",
) -> str:
    output_path = f"/restore_src_{uuid_short()}"
    await run_dump_via_api(
        api_client,
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=output_path,
        dump_type=dump_type,
    )
    return output_path


async def test_restore_full_e2e(
    api_client,
    api_source_db,
    api_target_db,
    db_params,
    webhook_recorder,
):
    input_path = await _dump_for_restore(api_client, api_source_db, db_params)
    body = build_restore_request(
        db_params=db_params,
        db_name=api_target_db,
        input_path=input_path,
        webhook_url=webhook_recorder.url,
        webhook_metadata={"phase": "restore"},
    )
    resp = await api_client.post("/api/stateless/restore", json=body)
    assert resp.status == 201

    terminal = await _wait_terminal(webhook_recorder)
    assert terminal["status"] == "success", terminal
    assert len(webhook_recorder.payloads) == 2
    in_progress, success = webhook_recorder.payloads
    assert in_progress["status"] == "in_progress"
    assert success["operation_id"] == body["operation_id"]
    assert success["internal_operation_id"] == in_progress["internal_operation_id"]
    assert success["webhook_metadata"] == {"phase": "restore"}
    assert success["run_options"]["mode"] == "restore"


async def test_restore_struct_uses_sync_struct_mode(
    api_client,
    api_source_db,
    api_target_db,
    db_params,
    webhook_recorder,
):
    input_path = await _dump_for_restore(
        api_client,
        api_source_db,
        db_params,
        dump_type="sync-struct-dump",
    )
    body = build_restore_request(
        db_params=db_params,
        db_name=api_target_db,
        input_path=input_path,
        webhook_url=webhook_recorder.url,
        restore_type="sync-struct-restore",
    )
    resp = await api_client.post("/api/stateless/restore", json=body)
    assert resp.status == 201
    terminal = await _wait_terminal(webhook_recorder)
    assert terminal["status"] == "success", terminal
    assert terminal["run_options"]["mode"] == "sync-struct-restore"


async def test_restore_data_after_struct(
    api_client,
    api_source_db,
    api_target_db,
    db_params,
    webhook_recorder,
):
    struct_path = await _dump_for_restore(
        api_client,
        api_source_db,
        db_params,
        dump_type="sync-struct-dump",
    )
    data_path = await _dump_for_restore(
        api_client,
        api_source_db,
        db_params,
        dump_type="sync-data-dump",
    )

    struct_body = build_restore_request(
        db_params=db_params,
        db_name=api_target_db,
        input_path=struct_path,
        webhook_url=webhook_recorder.url,
        restore_type="sync-struct-restore",
    )
    resp = await api_client.post("/api/stateless/restore", json=struct_body)
    assert resp.status == 201
    await _wait_terminal(webhook_recorder)
    webhook_recorder.payloads.clear()
    webhook_recorder.headers.clear()

    data_body = build_restore_request(
        db_params=db_params,
        db_name=api_target_db,
        input_path=data_path,
        webhook_url=webhook_recorder.url,
        restore_type="sync-data-restore",
    )
    resp = await api_client.post("/api/stateless/restore", json=data_body)
    assert resp.status == 201
    terminal = await _wait_terminal(webhook_recorder)
    assert terminal["status"] == "success", terminal
    assert terminal["run_options"]["mode"] == "sync-data-restore"


async def test_restore_clean_db_and_drop_db_conflict_returns_422(
    api_client,
    api_target_db,
    db_params,
):
    body = build_restore_request(
        db_params=db_params,
        db_name=api_target_db,
        input_path="/whatever",
        webhook_url="http://example.invalid/hook",
        clean_db=True,
        drop_db=True,
    )
    resp = await api_client.post("/api/stateless/restore", json=body)
    assert resp.status == 422


async def test_restore_invalid_input_path_returns_400(
    api_client,
    api_target_db,
    db_params,
):
    body = build_restore_request(
        db_params=db_params,
        db_name=api_target_db,
        input_path="/../escape_me",
        webhook_url="http://example.invalid/hook",
    )
    resp = await api_client.post("/api/stateless/restore", json=body)
    assert resp.status == 400
    assert (await resp.json())["error_code"] == "INVALID_PATH"


async def test_restore_drop_custom_check_constr_passes_through(
    api_client,
    api_source_db,
    api_target_db,
    db_params,
    webhook_recorder,
):
    input_path = await _dump_for_restore(api_client, api_source_db, db_params)
    body = build_restore_request(
        db_params=db_params,
        db_name=api_target_db,
        input_path=input_path,
        webhook_url=webhook_recorder.url,
        drop_custom_check_constr=True,
        proc_conn_count=2,
    )
    resp = await api_client.post("/api/stateless/restore", json=body)
    assert resp.status == 201
    terminal = await _wait_terminal(webhook_recorder)
    assert terminal["status"] == "success", terminal
    assert terminal["run_options"]["drop_custom_check_constr"] is True
    assert terminal["run_options"]["db_connections_per_process"] == 2


async def test_restore_missing_input_dir_returns_error_webhook(
    api_client,
    api_target_db,
    db_params,
    webhook_recorder,
):
    body = build_restore_request(
        db_params=db_params,
        db_name=api_target_db,
        input_path=f"/missing_dump_{uuid_short()}",
        webhook_url=webhook_recorder.url,
    )
    resp = await api_client.post("/api/stateless/restore", json=body)
    assert resp.status == 201
    terminal = await _wait_terminal(webhook_recorder)
    assert terminal["status"] == "error", terminal
    assert terminal["error"]
    assert terminal["error_code"]
