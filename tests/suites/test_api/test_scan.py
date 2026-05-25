from __future__ import annotations

import json

from .conftest import input_dict_text
from .helpers import build_scan_request


async def test_scan_full_sends_two_webhooks_and_returns_dict(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_scan_request(
        db_params=db_params,
        db_name=api_source_db,
        meta_dict=input_dict_text("meta_dict.py"),
        webhook_url=webhook_recorder.url,
        scan_type="full",
        webhook_metadata={"trace_id": "abc-123", "tags": ["api-test"]},
    )
    resp = await api_client.post("/api/stateless/scan", json=body)
    assert resp.status == 201

    terminal = await webhook_recorder.wait_for_terminal(max_wait=180)
    assert terminal["status"] == "success", terminal
    assert len(webhook_recorder.payloads) == 2

    in_progress, success = webhook_recorder.payloads
    assert in_progress["status"] == "in_progress"
    assert in_progress["internal_operation_id"]
    assert success["status"] == "success"
    assert success["internal_operation_id"] == in_progress["internal_operation_id"]
    assert success["operation_id"] == body["operation_id"]
    assert success["webhook_metadata"] == body["webhook_metadata"]
    assert success["sens_dict_content"]
    assert success["no_sens_dict_content"] is None
    assert success["started"] and success["ended"]
    assert success["run_options"]["mode"] == "create-dict"


async def test_scan_partial_passes_depth_to_runner(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_scan_request(
        db_params=db_params,
        db_name=api_source_db,
        meta_dict=input_dict_text("meta_dict.py"),
        webhook_url=webhook_recorder.url,
        scan_type="partial",
        depth=50,
        proc_count=2,
        proc_conn_count=2,
    )
    resp = await api_client.post("/api/stateless/scan", json=body)
    assert resp.status == 201
    terminal = await webhook_recorder.wait_for_terminal(max_wait=180)
    assert terminal["status"] == "success", terminal
    run_options = terminal["run_options"]
    assert run_options["scan_mode"] == "partial"
    assert run_options["scan_partial_rows"] == 50
    assert run_options["processes"] == 2
    assert run_options["db_connections_per_process"] == 2


async def test_scan_need_no_sens_dict_returns_both_contents(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_scan_request(
        db_params=db_params,
        db_name=api_source_db,
        meta_dict=input_dict_text("meta_dict.py"),
        webhook_url=webhook_recorder.url,
        need_no_sens_dict=True,
    )
    resp = await api_client.post("/api/stateless/scan", json=body)
    assert resp.status == 201
    terminal = await webhook_recorder.wait_for_terminal(max_wait=180)
    assert terminal["status"] == "success", terminal
    assert terminal["sens_dict_content"]
    assert terminal["no_sens_dict_content"] is not None
    assert "dictionary" in terminal["no_sens_dict_content"]


async def test_scan_broken_meta_dict_sends_error_webhook(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_scan_request(
        db_params=db_params,
        db_name=api_source_db,
        meta_dict="{ this is not a valid python dict! }",
        webhook_url=webhook_recorder.url,
    )
    resp = await api_client.post("/api/stateless/scan", json=body)
    assert resp.status == 201
    terminal = await webhook_recorder.wait_for_terminal(max_wait=180)
    assert terminal["status"] == "error", terminal
    assert terminal["error_code"]
    assert terminal["error"]


async def test_scan_extra_headers_reach_webhook(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    body = build_scan_request(
        db_params=db_params,
        db_name=api_source_db,
        meta_dict=input_dict_text("meta_dict.py"),
        webhook_url=webhook_recorder.url,
        webhook_extra_headers={"X-Trace-Id": "trace-007", "Content-Type": "application/json"},
    )
    resp = await api_client.post("/api/stateless/scan", json=body)
    assert resp.status == 201
    await webhook_recorder.wait_for_terminal(max_wait=180)
    headers = webhook_recorder.headers[0]
    assert headers.get("x-trace-id", headers.get("X-Trace-Id")) == "trace-007"


async def test_scan_metadata_preserves_nested_objects(
    api_client,
    api_source_db,
    db_params,
    webhook_recorder,
):
    metadata = {"obj": {"k": [1, 2, 3]}, "n": None, "bool": True}
    body = build_scan_request(
        db_params=db_params,
        db_name=api_source_db,
        meta_dict=input_dict_text("meta_dict.py"),
        webhook_url=webhook_recorder.url,
        webhook_metadata=metadata,
    )
    resp = await api_client.post("/api/stateless/scan", json=body)
    assert resp.status == 201
    terminal = await webhook_recorder.wait_for_terminal(max_wait=180)
    assert json.loads(json.dumps(terminal["webhook_metadata"])) == metadata
