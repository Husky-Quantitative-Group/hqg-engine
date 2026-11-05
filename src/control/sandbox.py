"""Persistent, credential-free Docker contexts for dashboard strategy code."""

import json
import math
import os
import struct
import threading
import time
from uuid import uuid4

from hqg_algorithms import get_strategy_metadata

from src.control.packages import declared_lookback, validate_package


def validate_weights(weights, universe):
    if (
        not isinstance(weights, dict)
        or any(symbol not in universe for symbol in weights)
        or any(
            type(w) not in (int, float) or not math.isfinite(w) or w < 0
            for w in weights.values()
        )
        or sum(weights.values()) > 1 + 1e-7
    ):
        raise ValueError(
            "Strategy must return finite, long-only universe weights totaling at most one"
        )
    return weights


class Sandbox:
    def __init__(self, package, image=None, timeout=5):
        self.identity, metadata = validate_package(package)
        self.universe = metadata["universe"]
        self.cadence = get_strategy_metadata(package["files"][package["entrypoint"]]).cadence
        self.lookback = declared_lookback(
            package["files"][package["entrypoint"]], metadata["class_name"]
        )
        self.package, self.timeout = package, timeout
        self.image = image or os.environ.get(
            "STRATEGY_SANDBOX_IMAGE", "hqg-engine-local"
        )
        self.client, self.container, self.stream = None, None, None
        self.buffer = b""
        self.lock = threading.RLock()

    def start(self):
        import docker

        # Only the trusted worker sees this socket. Containers receive no mounts,
        # credentials, host environment, network, or Docker socket.
        self.client = docker.DockerClient(
            base_url="unix:///var/run/docker.sock", timeout=self.timeout
        )
        image_id = self.client.images.get(
            self.image
        ).id  # Local image only; never pull.
        self.container = self.client.containers.create(
            image_id,
            ["python", "-u", "-m", "src.control.sandbox_runner"],
            name="hqg-strategy-" + str(uuid4()),
            stdin_open=True,
            auto_remove=True,
            network_mode="none",
            read_only=True,
            user="65534:65534",
            tmpfs={"/tmp": "rw,noexec,nosuid,size=32m,mode=1777"},
            mem_limit="256m",
            nano_cpus=1_000_000_000,
            pids_limit=32,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
            log_config=docker.types.LogConfig(type="none"),
            environment={
                "HTTP_PROXY": "",
                "HTTPS_PROXY": "",
                "http_proxy": "",
                "https_proxy": "",
                "OPENBLAS_NUM_THREADS": "1",
            },
        )
        self.stream = self.container.attach_socket(
            params={"stdin": 1, "stdout": 1, "stderr": 1, "stream": 1}
        )
        self.socket = self.stream._sock
        self.container.start()
        self.exchange({"package": self.package})

    def read(self, size, deadline):
        chunks = bytearray()
        while len(chunks) < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Strategy execution timed out")
            self.socket.settimeout(remaining)
            chunk = self.socket.recv(size - len(chunks))
            if not chunk:
                raise ValueError("Strategy container exited")
            chunks.extend(chunk)
        return bytes(chunks)

    def exchange(self, message):
        deadline = time.monotonic() + self.timeout
        self.socket.settimeout(self.timeout)
        self.socket.sendall(json.dumps(message, allow_nan=False).encode() + b"\n")
        consumed = 0
        while b"\n" not in self.buffer:
            header = self.read(8, deadline)
            size = struct.unpack(">I", header[4:])[0]
            consumed += size
            if consumed > 256_000:
                raise ValueError("Strategy output exceeds limit")
            chunk = self.read(size, deadline)
            if header[0] == 1:
                self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        result = json.loads(line)
        if "error" in result:
            raise ValueError("Strategy failed: " + str(result["error"])[:500])
        return result

    def on_data(self, bars, portfolio):
        with self.lock:
            try:
                if self.container is None:
                    self.start()
                result = self.exchange({"bars": bars, "portfolio": portfolio})
                weights = result["weights"]
                return (
                    None
                    if weights is None
                    else validate_weights(weights, self.universe)
                )
            except BaseException:
                self.close()
                raise

    def close(self):
        import docker

        with self.lock:
            self.buffer = b""
            if self.stream is not None:
                self.stream.close()
                self.socket.close()
                self.stream = None
            try:
                if self.container is not None:
                    try:
                        self.container.remove(force=True)
                    except docker.errors.NotFound:
                        pass  # EOF may already have triggered automatic removal.
            finally:
                self.container = None
                if self.client is not None:
                    self.client.close()
                    self.client = None
