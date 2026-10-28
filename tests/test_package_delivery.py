"""HTTPS policy and provider delivery with an explicit test-only client."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from src.control.provider import Provider
from src.control.packages import validate_package
from src.control.transport import dashboard_client


@pytest.mark.parametrize(
    "url",
    [
        "http://api.test/dev",
        "https://user:secret@api.test",
        "https://api.test?token=secret",
        "https://api.test/#fragment",
        "https:///dev",
    ],
)
def test_dashboard_requires_https_without_embedded_credentials(url):
    with pytest.raises(ValueError):
        dashboard_client(url)


def test_dashboard_preserves_stage_tls_origin_and_retry_policy():
    async def scenario():
        async with dashboard_client("https://api.test/dev/") as client:
            assert (
                str(client.build_request("GET", "internal/v1/accounts/id/sync").url)
                == "https://api.test/dev/internal/v1/accounts/id/sync"
            )
            assert client.follow_redirects is False
            assert client.timeout.connect == 3
            assert client._transport._pool._retries == 0
            assert client._transport._pool._ssl_context.verify_mode == 2
            with pytest.raises(ValueError, match="origin"):
                await client.get("https://other.test/escape")

    asyncio.run(scenario())


class Runtime:
    async def save(self, state):
        self.state = state


class Broker:
    account_id = None

    async def request(self, *args):
        return {"id": "00000000-0000-4000-8000-000000000001"}


def package(version):
    return dict(
        strategy_id="test",
        version=version,
        entrypoint="main.py",
        files={"main.py": 'class Test(Strategy):\n    universe = ["AAPL"]\n'},
    )


@pytest.mark.parametrize("failure", ["missing", "tampered", "timeout"])
def test_failed_download_keeps_cache_and_acknowledges_only_complete_set(failure):
    async def scenario():
        packages = [package(1), package(2)]
        approved = [validate_package(p)[0] for p in packages]
        calls, fail = [], True

        async def respond(request):
            if request.method == "POST":
                body = json.loads(request.content)
                assert body["approved_strategies"] == provider.approved
                return httpx.Response(
                    200,
                    json=dict(
                        account_id=provider.account_id,
                        boot_id=provider.boot_id,
                        request_id=body["request_id"],
                        permission_ttl_seconds=15,
                        dashboard_ready=True,
                        control_version=0,
                        configuration_revision=0,
                        configuration_digest=None,
                        configuration=None,
                        approved_strategies=approved,
                        modes={
                            mode: dict(
                                desired_state="paused",
                                stop_version=0,
                                resume_incident_id=None,
                            )
                            for mode in ("paper", "live")
                        },
                    ),
                )
            digest = request.url.path.split("/")[-1]
            calls.append(digest)
            index = next(
                i for i, item in enumerate(approved) if item["source_digest"] == digest
            )
            if index == 1 and fail:
                if failure == "timeout":
                    await asyncio.sleep(0.2)
                return (
                    httpx.Response(404)
                    if failure == "missing"
                    else httpx.Response(200, json=package(99))
                )
            return httpx.Response(200, json=packages[index])

        async with httpx.AsyncClient(
            base_url="https://api.test/stage/", transport=httpx.MockTransport(respond)
        ) as client:
            provider = Provider(
                str(uuid4()),
                str(uuid4()),
                "test-secret",
                "https://api.test/stage",
                {"paper": Broker()},
                Runtime(),
                timeout=0.1,
                dashboard=client,
            )
            assert not await provider.sync_once()
            assert provider.approved == [] and provider.packages == {}
            assert list(provider.package_cache) == [approved[0]["source_digest"]]
            assert all(not mode["gate_open"] for mode in provider.modes.values())
            fail = False
            assert await provider.sync_once(), provider.sync_error
            assert provider.approved == approved
            assert calls.count(approved[0]["source_digest"]) == 1
            assert calls.count(approved[1]["source_digest"]) == 2
            assert await provider.sync_once()
            assert len(calls) == 3

    asyncio.run(scenario())
