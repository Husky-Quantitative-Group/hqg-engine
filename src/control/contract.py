import hashlib
import json
from pathlib import Path
from uuid import UUID

from src.strategies.mock_balanced import MockBalanced

MODES = ("paper", "live")
SOURCE_DIGEST = (
    "sha256:"
    + hashlib.sha256(
        Path(__file__).parents[1].joinpath("strategies/mock_balanced.py").read_bytes()
    ).hexdigest()
)
APPROVED = [
    {"strategy_id": "mock-balanced", "version": 1, "source_digest": SOURCE_DIGEST}
]
# A reproducible mock package identity - real image pinning will be done later.
IMAGE_DIGEST = (
    "sha256:"
    + hashlib.sha256(json.dumps(APPROVED, sort_keys=True).encode()).hexdigest()
)
REGISTRY = {("mock-balanced", 1, SOURCE_DIGEST): MockBalanced}


def digest(value):
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                value, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
        ).hexdigest()
    )


def validate_configuration(config):
    if set(config) != {"modes"} or set(config["modes"]) != set(MODES):
        raise ValueError("Both modes required")

    ids = set()
    for spec in config["modes"].values():
        ids.add(str(UUID(spec["portfolio_id"])))

        if (
            set(spec) != {"enabled", "portfolio_id", "strategies"}
            or type(spec["enabled"]) is not bool
        ):
            raise ValueError("Invalid mode")

        if bool(spec["strategies"]) != spec["enabled"]:
            raise ValueError("Enabled mode requires strategies")

        seen, total = set(), 0

        for item in spec["strategies"]:
            identity = (item["strategy_id"], item["version"], item["source_digest"])

            if identity not in REGISTRY or item["strategy_id"] in seen:
                raise ValueError("Unsupported strategy")

            if set(item) != {"strategy_id", "version", "source_digest", "allocation"}:
                raise ValueError("Invalid strategy")

            seen.add(item["strategy_id"])

            if (
                type(item["allocation"]) not in (int, float)
                or not 0 < item["allocation"] <= 1
            ):
                raise ValueError("Invalid allocation")

            total += item["allocation"]

        if total > 1:
            raise ValueError("Allocations exceed one")

    if len(ids) != 2:
        raise ValueError("Distinct mode portfolios required")
