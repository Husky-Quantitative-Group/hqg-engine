"""JSON-line adapter, executed ONLY inside a network-free strategy container."""

import contextlib
import importlib.util
import json
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
)

from src.control.packages import validate_package


def main():
    protocol = sys.stdout
    instance = None
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
                        ):
                            raise ValueError(
                                "Strategy class or universe differs from registered package"
                            )
                        instance = cls()
                        result = {"ready": True}
                    else:
                        bars = {
                            symbol: Bar(**bar)
                            for symbol, bar in message["bars"].items()
                        }
                        signal = instance.on_data(
                            Slice(bars), PortfolioView(**message["portfolio"])
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
