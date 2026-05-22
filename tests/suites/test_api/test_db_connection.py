from __future__ import annotations

from .helpers import db_creds


async def test_check_connection_ok(api_client, api_source_db, db_params):
    resp = await api_client.post(
        "/api/stateless/check_db_connection",
        json=db_creds(db_params, db_name=api_source_db),
    )
    assert resp.status == 200
    assert await resp.json() == {"status": "ok"}


async def test_check_connection_missing_required_field_422(api_client, db_params):
    creds = db_creds(db_params, db_name="postgres")
    creds.pop("host")
    resp = await api_client.post("/api/stateless/check_db_connection", json=creds)
    assert resp.status == 422


async def test_check_connection_unknown_db_returns_400(api_client, db_params):
    resp = await api_client.post(
        "/api/stateless/check_db_connection",
        json=db_creds(db_params, db_name="no_such_database_here"),
    )
    assert resp.status == 400
    body = await resp.json()
    assert body["error_code"] == "DB_CONNECTION_FAILED"
