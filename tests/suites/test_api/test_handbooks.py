from __future__ import annotations


async def test_task_statuses_match_response_status_enum(api_client):
    resp = await api_client.get("/handbook/task-statuses")
    assert resp.status == 200
    data = await resp.json()
    assert {(item["id"], item["title"]) for item in data} == {
        (1, "unknown"),
        (2, "success"),
        (3, "error"),
        (4, "in_progress"),
        (5, "starting"),
    }


async def test_dump_types_lists_all_modes(api_client):
    resp = await api_client.get("/handbook/dump-types")
    assert resp.status == 200
    data = await resp.json()
    assert {item["title"] for item in data} == {"dump", "sync-struct-dump", "sync-data-dump"}


async def test_restore_types_lists_all_modes(api_client):
    resp = await api_client.get("/handbook/restore-types")
    assert resp.status == 200
    data = await resp.json()
    assert {item["title"] for item in data} == {"restore", "sync-struct-restore", "sync-data-restore"}


async def test_scan_types_lists_full_and_partial(api_client):
    resp = await api_client.get("/handbook/scan-types")
    assert resp.status == 200
    data = await resp.json()
    assert {item["title"] for item in data} == {"full", "partial"}


async def test_openapi_schema_is_generated(api_client):
    resp = await api_client.get("/openapi.json")
    assert resp.status == 200
    schema = await resp.json()
    assert "/api/stateless/dump" in schema["paths"]
    assert "/api/stateless/scan" in schema["paths"]
    assert "/api/stateless/restore" in schema["paths"]
