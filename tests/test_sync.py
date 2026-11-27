"""A temporary dashboard failure keeps the current permission; a bad reply pauses at once."""

import asyncio
import json
import time
from uuid import uuid4

import httpx
import pytest

from src.control.provider import Provider


class Runtime:
    async def save(self, state):
        pass


def reply(request, **changes):
    """A valid sync response to this request, with the given fields changed."""
    sent = json.loads(request.content)
    return httpx.Response(200, json={
        "account_id": sent["account_id"],
        "boot_id": sent["boot_id"],
        "request_id": sent["request_id"],
        "dashboard_ready": True,
        "permission_ttl_seconds": 15,
        "control_version": 0,
        "configuration_revision": 0,
        "modes": {"paper": {}, "live": {}},
        **changes,
    })


def refused(request):
    raise httpx.ConnectError("Connection refused")


def sync(handler):
    provider = Provider(
        str(uuid4()), str(uuid4()), "test-secret", "https://api.test/stage",
        {"paper": object()}, Runtime(),
        dashboard=httpx.AsyncClient(
            base_url="https://api.test/stage/", transport=httpx.MockTransport(handler)
        ),
    )
    # Paper is running, and the last sync succeeded a moment ago.
    provider.contexts_verified, provider.healthy = True, True
    provider.deadline = time.monotonic() + 15
    provider.modes["paper"]["gate_open"] = True
    assert not asyncio.run(provider.sync_once())
    return provider


@pytest.mark.parametrize(
    "handler, reason",
    [
        (lambda request: httpx.Response(503), "HTTP 503"),
        (refused, "ConnectError"),
        (lambda request: reply(request, dashboard_ready=False), "Dashboard not ready"),
    ],
)
def test_a_temporary_failure_keeps_the_current_permission(handler, reason, caplog):
    provider = sync(handler)
    assert provider.modes["paper"]["gate_open"] and provider.healthy
    assert provider.sync_error == reason
    assert f"Dashboard sync failed ({reason}); permission still valid" in caplog.text


def test_a_mismatched_reply_pauses_both_modes_at_once(caplog):
    provider = sync(lambda request: reply(request, account_id=str(uuid4())))
    assert not provider.modes["paper"]["gate_open"] and not provider.healthy
    assert provider.modes["paper"]["stop_reason"] == "dashboard_sync_failed"
    assert "Dashboard sync failed (Stale or mismatched sync); paused both modes" in caplog.text
