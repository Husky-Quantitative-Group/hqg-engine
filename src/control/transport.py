"""Bounded mock HTTP transport with no SDK defaults, proxies or redirects."""

import httpx
from urllib.parse import urlsplit


class LocalTransport(httpx.AsyncHTTPTransport):
    def __init__(self, origins):
        super().__init__(retries=0)
        self.origins = origins

    async def handle_async_request(self, request):
        origin = (request.url.scheme, request.url.host, request.url.port)

        if origin not in self.origins:
            raise ValueError("Destination outside mock allowlist")

        return await super().handle_async_request(request)


def local_client(url, timeout=3, headers=None):
    parsed = urlsplit(url)
    # Literal loopback avoids DNS rebinding; compose service names are provisioned explicitly.
    if (
        parsed.scheme != "http"
        or parsed.hostname
        not in {
            "127.0.0.1",
            "::1",
            "mock-broker",
            "mock-dashboard",
            "mock-readiness",
            "provider-api",
        }
        or not parsed.port
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Explicit local mock HTTP URL required")

    origin = (parsed.scheme, parsed.hostname, parsed.port)

    return httpx.AsyncClient(
        base_url=url.rstrip("/") + "/",
        timeout=timeout,
        trust_env=False,
        follow_redirects=False,
        headers=headers,
        transport=LocalTransport({origin}),
    )


class Broker:
    def __init__(self, mode, trading_url, data_url, timeout=3, read_only=False):
        if mode not in ("paper", "live"):
            raise ValueError("Explicit mode required")

        self.mode = mode
        headers = {
            "APCA-API-KEY-ID": f"FAKE-{'READ-' if read_only else ''}{mode}",
            "APCA-API-SECRET-KEY": "FAKE-SECRET",
        }
        self.trading = local_client(trading_url, timeout, headers)
        self.data = local_client(data_url, timeout, headers)

    async def request(self, method, path, **kwargs):
        response = await self.trading.request(method, path, **kwargs)
        response.raise_for_status()

        return response.json() if response.content else None

    async def quotes(self, symbols):
        response = await self.data.get(
            "v2/stocks/quotes/latest", params={"symbols": ",".join(symbols)}
        )
        response.raise_for_status()

        return {
            symbol: float(quote["ap"])
            for symbol, quote in response.json()["quotes"].items()
        }

    async def close(self):
        await self.trading.aclose()
        await self.data.aclose()
