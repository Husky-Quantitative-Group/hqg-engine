"""JSON-line adapter, executed ONLY inside a network-free strategy container."""

import contextlib
import importlib.util
import json
import math
from datetime import datetime, timezone
from types import MappingProxyType
from pathlib import Path
import sys
import tempfile

from hqg_algorithms import (
    Strategy,
    Bar,
    Slice,
    PortfolioView,
    TargetWeights,
    Hold,
    Liquidate,
    get_strategy_metadata,
)

from src.control.packages import validate_package
from src.alt_data import normalize_series


def decode_history(payload, declared):
    if not isinstance(payload, dict) or set(payload) != set(declared):
        raise ValueError("Alternative history differs from declared series")
    result = {}
    for series, history in payload.items():
        values = {}
        for date, value in history.items():
            observation = datetime.fromisoformat(date)
            if observation.tzinfo is not None:
                observation = observation.astimezone(timezone.utc).replace(tzinfo=None)
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError("Invalid alternative history value")
            values[observation] = float(value)
        result[series] = MappingProxyType(dict(sorted(values.items())))
    return result


def main():
    protocol = sys.stdout
    instance = None
    alt_data = []
    with tempfile.TemporaryDirectory() as directory:
        for line in sys.stdin:
            try:
                message = json.loads(line)
                # Strategy prints/logs cannot corrupt the JSON protocol.
                with contextlib.redirect_stdout(sys.stderr):
                    if "package" in message:
                        package = message["package"]
                        _, metadata = validate_package(package)
                        for path, content in package["files"].items():
                            target = Path(directory, path)
                            target.parent.mkdir(parents=True, exist_ok=True)
                            target.write_text(content)
                        sys.path.insert(0, directory)
                        entrypoint = Path(directory, package["entrypoint"])
                        sys.path.insert(0, str(entrypoint.parent))
                        module_name = ".".join(
                            Path(package["entrypoint"]).with_suffix("").parts
                        )
                        spec = importlib.util.spec_from_file_location(
                            module_name, entrypoint
                        )
                        module = importlib.util.module_from_spec(spec)
                        sys.modules[module_name] = module
                        spec.loader.exec_module(module)
                        cls = getattr(module, metadata["class_name"])
                        if (
                            not issubclass(cls, Strategy)
                            or list(cls.universe) != metadata["universe"]
                            or cls.cadence != get_strategy_metadata(
                                package["files"][package["entrypoint"]]
                            ).cadence
                        ):
                            raise ValueError(
                                "Strategy class, universe or cadence differs from registered package"
                            )
                        instance = cls()
                        alt_data = normalize_series(getattr(cls, 'alt_data', []))
                        declared = get_strategy_metadata(package["files"][package["entrypoint"]])
                        if alt_data != normalize_series(getattr(declared, 'alt_data', [])):
                            raise ValueError("Alternative series differ from registered package")
                        result = {"ready": True}
                    else:
                        bars = {
                            symbol: Bar(**bar)
                            for symbol, bar in message["bars"].items()
                        }
                        signal = instance.on_data(
                            Slice(bars, alt_history=decode_history(
                                message.get('alt_history', {}), alt_data,
                            )), PortfolioView(**message["portfolio"])
                        )
                        if isinstance(signal, TargetWeights):
                            weights = dict(signal.weights)
                        elif isinstance(signal, Liquidate):
                            weights = {}
                        elif isinstance(signal, Hold):
                            weights = None
                        else:
                            raise ValueError("Return TargetWeights, Hold, or Liquidate")
                        result = {"weights": weights}
                output = json.dumps(result, allow_nan=False)
            except Exception as exc:
                output = json.dumps({"error": str(exc)[:500]})
            protocol.write(output + "\n")
            protocol.flush()


if __name__ == "__main__":
    main()
