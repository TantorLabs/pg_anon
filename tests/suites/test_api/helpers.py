from __future__ import annotations

import asyncio
import socket
from typing import Any

from aiohttp import web


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class WebhookRecorder:
    def __init__(self, host: str = "127.0.0.1") -> None:
        self.host = host
        self.port = _pick_free_port()
        self.payloads: list[dict[str, Any]] = []
        self.headers: list[dict[str, str]] = []
        self.response_queue: list[int] = []
        self._app = web.Application()
        self._app.router.add_post("/hook", self._handle)
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/hook"

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    async def _handle(self, request: web.Request) -> web.Response:
        payload = await request.json()
        self.payloads.append(payload)
        self.headers.append(dict(request.headers))
        status = self.response_queue.pop(0) if self.response_queue else 200
        return web.Response(status=status)

    def queue_responses(self, *codes: int) -> None:
        self.response_queue.extend(codes)

    async def wait_for(self, predicate, *, max_wait: float = 60.0, poll: float = 0.1) -> None:
        try:
            async with asyncio.timeout(max_wait):
                while not predicate(self.payloads):  # noqa: ASYNC110
                    await asyncio.sleep(poll)
        except TimeoutError:
            raise TimeoutError(f"Webhook condition not met within {max_wait}s. Got: {self.payloads!r}") from None

    async def wait_for_count(self, count: int, *, max_wait: float = 60.0) -> None:
        await self.wait_for(lambda p: len(p) >= count, max_wait=max_wait)

    async def wait_for_terminal(self, *, max_wait: float = 120.0) -> dict[str, Any]:
        await self.wait_for(
            lambda payloads: any(p.get("status") in ("success", "error") for p in payloads),
            max_wait=max_wait,
        )
        for payload in self.payloads:
            if payload.get("status") in ("success", "error"):
                return payload
        raise AssertionError("Unreachable")  # pragma: no cover


# --------------------------------------------------------------------------
# Default DB credentials (parametrized per call).
# --------------------------------------------------------------------------


def db_creds(db_params, *, db_name: str) -> dict[str, Any]:
    return {
        "host": db_params.test_db_host,
        "port": int(db_params.test_db_port),
        "db_name": db_name,
        "user_login": db_params.test_db_user,
        "user_password": db_params.test_db_user_password,
    }


def dict_entry(name: str, content: str, *, additional_info: str | None = None) -> dict[str, Any]:
    entry: dict[str, Any] = {"name": name, "content": content}
    if additional_info is not None:
        entry["additional_info"] = additional_info
    return entry


# --------------------------------------------------------------------------
# Request builders for stateless endpoints.
# --------------------------------------------------------------------------


def build_scan_request(
    *,
    db_params,
    db_name: str,
    meta_dict: str,
    webhook_url: str,
    scan_type: str = "full",
    operation_id: str | None = None,
    sens_dicts: list[dict[str, Any]] | None = None,
    no_sens_dicts: list[dict[str, Any]] | None = None,
    need_no_sens_dict: bool = False,
    depth: int | None = None,
    proc_count: int | None = None,
    proc_conn_count: int | None = None,
    conn_count: int | None = None,
    save_dicts: bool = False,
    web_debug: bool = False,
    webhook_metadata: object | None = None,
    webhook_extra_headers: dict[str, str] | None = None,
    webhook_verify_ssl: bool = True,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation_id": operation_id or f"scan-{uuid_short()}",
        "db_connection_params": db_creds(db_params, db_name=db_name),
        "webhook_status_url": webhook_url,
        "webhook_verify_ssl": webhook_verify_ssl,
        "web_debug": web_debug,
        "type": scan_type,
        "meta_dict_contents": [dict_entry("meta_dict.py", meta_dict)],
        "sens_dict_contents": sens_dicts or [],
        "no_sens_dict_contents": no_sens_dicts or [],
        "need_no_sens_dict": need_no_sens_dict,
        "save_dicts": save_dicts,
    }
    if webhook_metadata is not None:
        body["webhook_metadata"] = webhook_metadata
    if webhook_extra_headers is not None:
        body["webhook_extra_headers"] = webhook_extra_headers
    if depth is not None:
        body["depth"] = depth
    if proc_count is not None:
        body["proc_count"] = proc_count
    if proc_conn_count is not None:
        body["proc_conn_count"] = proc_conn_count
    if conn_count is not None:
        body["conn_count"] = conn_count
    return body


def build_dump_request(
    *,
    db_params,
    db_name: str,
    sens_dict: str,
    output_path: str,
    webhook_url: str,
    dump_type: str = "dump",
    operation_id: str | None = None,
    partial_tables_dict: str | None = None,
    partial_tables_exclude_dict: str | None = None,
    pg_dump_path: str | None = None,
    pg_dump_options: str | None = None,
    proc_count: int | None = None,
    proc_conn_count: int | None = None,
    conn_count: int | None = None,
    save_dicts: bool = False,
    ignore_privileges: bool = False,
    web_debug: bool = False,
    webhook_metadata: object | None = None,
    webhook_extra_headers: dict[str, str] | None = None,
    webhook_verify_ssl: bool = True,
    log_level: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation_id": operation_id or f"dump-{uuid_short()}",
        "db_connection_params": db_creds(db_params, db_name=db_name),
        "webhook_status_url": webhook_url,
        "webhook_verify_ssl": webhook_verify_ssl,
        "web_debug": web_debug,
        "type": dump_type,
        "sens_dict_contents": [dict_entry("sens_dict.py", sens_dict)],
        "output_path": output_path,
        "save_dicts": save_dicts,
        "ignore_privileges": ignore_privileges,
    }
    if webhook_metadata is not None:
        body["webhook_metadata"] = webhook_metadata
    if webhook_extra_headers is not None:
        body["webhook_extra_headers"] = webhook_extra_headers
    if partial_tables_dict is not None:
        body["partial_tables_dict_contents"] = [dict_entry("partial.py", partial_tables_dict)]
    if partial_tables_exclude_dict is not None:
        body["partial_tables_exclude_dict_contents"] = [dict_entry("partial_excl.py", partial_tables_exclude_dict)]
    optional = {
        "pg_dump_path": pg_dump_path,
        "pg_dump_options": pg_dump_options,
        "proc_count": proc_count,
        "proc_conn_count": proc_conn_count,
        "conn_count": conn_count,
        "log_level": log_level,
    }
    body.update({key: value for key, value in optional.items() if value is not None})
    return body


def build_restore_request(
    *,
    db_params,
    db_name: str,
    input_path: str,
    webhook_url: str,
    restore_type: str = "restore",
    operation_id: str | None = None,
    partial_tables_dict: str | None = None,
    partial_tables_exclude_dict: str | None = None,
    pg_restore_path: str | None = None,
    pg_restore_options: str | None = None,
    proc_conn_count: int | None = None,
    conn_count: int | None = None,
    drop_custom_check_constr: bool = False,
    clean_db: bool = False,
    drop_db: bool = False,
    save_dicts: bool = False,
    ignore_privileges: bool = False,
    web_debug: bool = False,
    webhook_metadata: object | None = None,
    webhook_extra_headers: dict[str, str] | None = None,
    webhook_verify_ssl: bool = True,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation_id": operation_id or f"restore-{uuid_short()}",
        "db_connection_params": db_creds(db_params, db_name=db_name),
        "webhook_status_url": webhook_url,
        "webhook_verify_ssl": webhook_verify_ssl,
        "web_debug": web_debug,
        "type": restore_type,
        "input_path": input_path,
        "drop_custom_check_constr": drop_custom_check_constr,
        "clean_db": clean_db,
        "drop_db": drop_db,
        "save_dicts": save_dicts,
        "ignore_privileges": ignore_privileges,
    }
    if webhook_metadata is not None:
        body["webhook_metadata"] = webhook_metadata
    if webhook_extra_headers is not None:
        body["webhook_extra_headers"] = webhook_extra_headers
    if partial_tables_dict is not None:
        body["partial_tables_dict_contents"] = [dict_entry("partial.py", partial_tables_dict)]
    if partial_tables_exclude_dict is not None:
        body["partial_tables_exclude_dict_contents"] = [dict_entry("partial_excl.py", partial_tables_exclude_dict)]
    if pg_restore_path is not None:
        body["pg_restore_path"] = pg_restore_path
    if pg_restore_options is not None:
        body["pg_restore_options"] = pg_restore_options
    if proc_conn_count is not None:
        body["proc_conn_count"] = proc_conn_count
    if conn_count is not None:
        body["conn_count"] = conn_count
    return body


def build_view_fields_request(
    *,
    db_params,
    db_name: str,
    sens_dict: str,
    schema_name: str | None = None,
    schema_mask: str | None = None,
    table_name: str | None = None,
    table_mask: str | None = None,
    view_only_sensitive_fields: bool = False,
    fields_limit_count: int | None = None,
    orm_dict_content: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "db_connection_params": db_creds(db_params, db_name=db_name),
        "sens_dict_contents": [dict_entry("sens_dict.py", sens_dict)],
        "view_only_sensitive_fields": view_only_sensitive_fields,
    }
    if schema_name is not None:
        body["schema_name"] = schema_name
    if schema_mask is not None:
        body["schema_mask"] = schema_mask
    if table_name is not None:
        body["table_name"] = table_name
    if table_mask is not None:
        body["table_mask"] = table_mask
    if fields_limit_count is not None:
        body["fields_limit_count"] = fields_limit_count
    if orm_dict_content is not None:
        body["orm_dict_content"] = orm_dict_content
    return body


def build_view_data_request(
    *,
    db_params,
    db_name: str,
    sens_dict: str,
    schema_name: str,
    table_name: str,
    limit: int = 10,
    offset: int = 0,
) -> dict[str, Any]:
    return {
        "db_connection_params": db_creds(db_params, db_name=db_name),
        "sens_dict_contents": [dict_entry("sens_dict.py", sens_dict)],
        "schema_name": schema_name,
        "table_name": table_name,
        "limit": limit,
        "offset": offset,
    }


def build_preview_schemas_request(
    *,
    db_params,
    db_name: str,
    schema_filter: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"db_connection_params": db_creds(db_params, db_name=db_name)}
    if schema_filter is not None:
        body["schema_filter"] = schema_filter
    return body


def build_preview_tables_request(
    *,
    db_params,
    db_name: str,
    sens_dict: str,
    limit: int = 20,
    offset: int = 0,
    table_filter: str | None = None,
    view_only_sensitive_tables: bool = False,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "db_connection_params": db_creds(db_params, db_name=db_name),
        "sens_dict_contents": [dict_entry("sens_dict.py", sens_dict)],
        "limit": limit,
        "offset": offset,
        "view_only_sensitive_tables": view_only_sensitive_tables,
    }
    if table_filter is not None:
        body["table_filter"] = table_filter
    return body


def uuid_short() -> str:
    import uuid

    return uuid.uuid4().hex[:8]


async def run_dump_via_api(
    api_client,
    *,
    db_params,
    db_name: str,
    sens_dict: str,
    output_path: str,
    dump_type: str = "dump",
    webhook_recorder: WebhookRecorder | None = None,
    extra: dict[str, Any] | None = None,
    max_wait: float = 240,
) -> dict[str, Any]:
    own_recorder = webhook_recorder is None
    recorder = webhook_recorder or WebhookRecorder()
    if own_recorder:
        await recorder.start()
    try:
        body = build_dump_request(
            db_params=db_params,
            db_name=db_name,
            sens_dict=sens_dict,
            output_path=output_path,
            webhook_url=recorder.url,
            dump_type=dump_type,
        )
        if extra:
            body.update(extra)
        resp = await api_client.post("/api/stateless/dump", json=body)
        assert resp.status == 201, await resp.text()
        terminal = await recorder.wait_for_terminal(max_wait=max_wait)
        assert terminal["status"] == "success", terminal
        return terminal
    finally:
        if own_recorder:
            await recorder.stop()


__all__ = [
    "WebhookRecorder",
    "build_dump_request",
    "build_preview_schemas_request",
    "build_preview_tables_request",
    "build_restore_request",
    "build_scan_request",
    "build_view_data_request",
    "build_view_fields_request",
    "db_creds",
    "dict_entry",
    "run_dump_via_api",
    "uuid_short",
]
