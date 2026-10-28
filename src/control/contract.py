import hashlib
import json
from uuid import UUID

MODES = ("paper", "live")


def digest(value):
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                value, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
        ).hexdigest()
    )


def validate_configuration(config, packages=None):
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

            installed = False
            if packages is not None:
                from src.control.packages import validate_package

                package = packages.get(item["source_digest"])
                if package is not None:
                    approved, _ = validate_package(package)
                    installed = identity == tuple(
                        approved[k] for k in ("strategy_id", "version", "source_digest")
                    )
            if not installed or item["strategy_id"] in seen:
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
