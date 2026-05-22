from __future__ import annotations

from datetime import datetime, timedelta, UTC

from .conftest import input_dict_text
from .helpers import build_dump_request, uuid_short


async def _dump_with_save(api_client, api_source_db, db_params, webhook_recorder) -> dict:
    body = build_dump_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        output_path=f"/ops_{uuid_short()}",
        webhook_url=webhook_recorder.url,
        save_dicts=True,
    )
    resp = await api_client.post("/api/stateless/dump", json=body)
    assert resp.status == 201
    terminal = await webhook_recorder.wait_for_terminal(max_wait=240)
    assert terminal["status"] == "success", terminal
    return terminal


async def test_operations_list_includes_saved_run(
    api_client, api_source_db, db_params, webhook_recorder,
):
    success = await _dump_with_save(api_client, api_source_db, db_params, webhook_recorder)
    op_id = success["internal_operation_id"]

    resp = await api_client.get("/operation")
    assert resp.status == 200
    operations = await resp.json()
    assert any(op_id in path for path in operations), operations


async def test_operations_list_date_filter_validation(api_client):
    today = datetime.now(tz=UTC).date()
    earlier = today - timedelta(days=7)
    resp = await api_client.get(
        f"/operation?date_before={earlier.isoformat()}&date_after={today.isoformat()}",
    )
    assert resp.status == 422


async def test_operation_details_returns_run_options_and_dictionaries(
    api_client, api_source_db, db_params, webhook_recorder,
):
    success = await _dump_with_save(api_client, api_source_db, db_params, webhook_recorder)
    op_id = success["internal_operation_id"]

    resp = await api_client.get(f"/operation/{op_id}")
    assert resp.status == 200
    data = await resp.json()
    assert data["run_status"]["status"] == "success"
    assert data["run_options"]["mode"] == "dump"
    assert "prepared_sens_dict_files" in data["dictionaries"]
    assert data["extra_data"]["dump_size"] > 0


async def test_operation_details_404_for_unknown_id(api_client):
    resp = await api_client.get("/operation/no-such-operation-id-42")
    assert resp.status == 404


async def test_operation_logs_returns_tail(
    api_client, api_source_db, db_params, webhook_recorder,
):
    success = await _dump_with_save(api_client, api_source_db, db_params, webhook_recorder)
    op_id = success["internal_operation_id"]

    resp = await api_client.get(f"/operation/{op_id}/logs?tail_lines=20")
    assert resp.status == 200
    lines = await resp.json()
    assert isinstance(lines, list)
    assert lines


async def test_operation_logs_invalid_tail_lines_422(
    api_client, api_source_db, db_params, webhook_recorder,
):
    success = await _dump_with_save(api_client, api_source_db, db_params, webhook_recorder)
    op_id = success["internal_operation_id"]
    resp = await api_client.get(f"/operation/{op_id}/logs?tail_lines=0")
    assert resp.status == 422


async def test_operation_delete_removes_run_and_dump_dir(
    api_client, api_source_db, db_params, webhook_recorder, runs_base_dir, dump_storage_base_dir,
):
    success = await _dump_with_save(api_client, api_source_db, db_params, webhook_recorder)
    op_id = success["internal_operation_id"]
    output_dir = success["run_options"]["output_dir"]

    resp = await api_client.delete(f"/operation/{op_id}")
    assert resp.status == 204

    await _await_path_disappears(output_dir)
    await _await_path_disappears_glob(runs_base_dir, op_id)


async def test_operation_delete_404_for_unknown_id(api_client):
    resp = await api_client.delete("/operation/no-such-operation-id-42")
    assert resp.status == 404


async def _await_path_disappears(path: str, *, max_wait: float = 10) -> None:
    import asyncio  # noqa: PLC0415
    from pathlib import Path  # noqa: PLC0415

    try:
        async with asyncio.timeout(max_wait):
            while Path(path).exists():  # noqa: ASYNC110
                await asyncio.sleep(0.1)
    except TimeoutError:
        raise AssertionError(f"{path} still exists after {max_wait}s") from None


async def _await_path_disappears_glob(base_dir, op_id: str, *, max_wait: float = 10) -> None:
    import asyncio  # noqa: PLC0415

    try:
        async with asyncio.timeout(max_wait):
            while list(base_dir.glob(f"*/*/*/{op_id}")):  # noqa: ASYNC110
                await asyncio.sleep(0.1)
    except TimeoutError:
        raise AssertionError(f"run dir for {op_id} still exists after {max_wait}s") from None
