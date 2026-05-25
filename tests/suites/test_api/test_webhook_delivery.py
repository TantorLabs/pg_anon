from __future__ import annotations

from typing import TYPE_CHECKING

from pg_anon.rest_api import callbacks
from pg_anon.rest_api.pydantic_models import StatelessRunnerResponse

if TYPE_CHECKING:
    from .helpers import WebhookRecorder


def _make_payload(operation_id: str = "wh-test") -> StatelessRunnerResponse:
    return StatelessRunnerResponse(
        operation_id=operation_id,
        internal_operation_id="iop-1",
        status_id=2,
        status="success",
    )


async def test_webhook_retries_until_success(webhook_recorder: WebhookRecorder):
    webhook_recorder.queue_responses(500, 500, 200)
    await callbacks.send_webhook(webhook_recorder.url, _make_payload(), max_retries=5, base_delay=0.01)
    assert len(webhook_recorder.payloads) == 3


async def test_webhook_stops_on_non_5xx(webhook_recorder: WebhookRecorder):
    webhook_recorder.queue_responses(404, 200)
    await callbacks.send_webhook(webhook_recorder.url, _make_payload(), max_retries=5, base_delay=0.01)
    assert len(webhook_recorder.payloads) == 1


async def test_webhook_full_failure_does_not_raise(webhook_recorder: WebhookRecorder):
    webhook_recorder.queue_responses(500, 500, 500)
    await callbacks.send_webhook(webhook_recorder.url, _make_payload(), max_retries=3, base_delay=0.01)
    assert len(webhook_recorder.payloads) == 3


async def test_webhook_unreachable_does_not_raise():
    bad_url = "http://127.0.0.1:1/hook"
    await callbacks.send_webhook(bad_url, _make_payload(), max_retries=2, base_delay=0.01)


async def test_webhook_extra_headers_normalized_to_lowercase(webhook_recorder: WebhookRecorder):
    await callbacks.send_webhook(
        webhook_recorder.url,
        _make_payload(),
        extra_headers={"X-Trace-Id": "abc", "AUTHORIZATION": "Bearer t"},
        max_retries=1,
        base_delay=0.01,
    )
    headers = webhook_recorder.headers[0]
    assert headers.get("x-trace-id", headers.get("X-Trace-Id")) == "abc"
    assert headers.get("authorization", headers.get("Authorization")) == "Bearer t"
    assert headers.get("content-type", headers.get("Content-Type")) == "application/json"


async def test_webhook_metadata_preserves_complex_payload(webhook_recorder: WebhookRecorder):
    payload = StatelessRunnerResponse(
        operation_id="meta-test",
        internal_operation_id="iop-meta",
        status_id=2,
        status="success",
        webhook_metadata={"nested": {"k": [1, 2, {"deep": True}]}, "n": None},
    )
    await callbacks.send_webhook(webhook_recorder.url, payload, max_retries=1, base_delay=0.01)
    assert webhook_recorder.payloads[0]["webhook_metadata"] == {
        "nested": {"k": [1, 2, {"deep": True}]},
        "n": None,
    }


async def test_webhook_verify_ssl_flag_propagates(webhook_recorder: WebhookRecorder):
    await callbacks.send_webhook(
        webhook_recorder.url,
        _make_payload(),
        verify_ssl=False,
        max_retries=1,
        base_delay=0.01,
    )
    assert len(webhook_recorder.payloads) == 1
