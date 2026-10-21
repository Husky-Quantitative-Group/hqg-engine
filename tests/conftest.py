import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/hqg_test"
)
os.environ.setdefault("PROVIDER_API_URL", "http://provider.test")

# Override personal credentials before collecting application modules.
for key, value in {
    "AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_SESSION_TOKEN": "testing", "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_EC2_METADATA_DISABLED": "true", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
    "AWS_CONFIG_FILE": "/dev/null", "ALPACA_API_KEY": "test-key",
    "ALPACA_SECRET_KEY": "test-secret",
}.items():
    os.environ[key] = value
os.environ.pop("AWS_PROFILE", None)
os.environ.pop("AWS_DEFAULT_PROFILE", None)

import socket
import pytest

@pytest.fixture(autouse=True)
def deny_external_network(monkeypatch):
    original = socket.getaddrinfo

    def local_only(host, *args, **kwargs):
        if host not in ("127.0.0.1", "::1", "localhost", b"127.0.0.1"):
            raise AssertionError(f"External destination blocked: {host}")
        return original(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", local_only)
    for cls in (socket.socket,):
        original_connect = cls.connect
        original_connect_ex = cls.connect_ex

        def connect(sock, address):
            if isinstance(address, tuple) and address[0] not in ("127.0.0.1", "::1"):
                raise AssertionError(f"External socket blocked: {address}")
            return original_connect(sock, address)

        def connect_ex(sock, address):
            if isinstance(address, tuple) and address[0] not in ("127.0.0.1", "::1"):
                raise AssertionError(f"External socket blocked: {address}")
            return original_connect_ex(sock, address)

        monkeypatch.setattr(cls, "connect", connect)
        monkeypatch.setattr(cls, "connect_ex", connect_ex)

