import pytest

from src.control.packages import validate_package
from src.control.sandbox import validate_weights

SOURCE = """from hqg_algorithms import Strategy, TargetWeights
class Example(Strategy):
    universe = ["AAPL"]
    def on_data(self, data, portfolio):
        return TargetWeights({"AAPL": 0.5})
"""


def package(source=SOURCE):
    return {
        "strategy_id": "example",
        "version": 1,
        "entrypoint": "main.py",
        "files": {"main.py": source},
    }


def test_package_identity_covers_version_entrypoint_and_all_files():
    original = package()
    identity, metadata = validate_package(original)
    assert metadata == {"class_name": "Example", "universe": ["AAPL"]}
    for patch in (
        {"version": 2},
        {"files": {"main.py": SOURCE, "helper.py": "X = 1"}},
        {"entrypoint": "other.py", "files": {"main.py": SOURCE, "other.py": SOURCE}},
    ):
        assert (
            validate_package({**original, **patch})[0]["source_digest"]
            != identity["source_digest"]
        )


@pytest.mark.parametrize(
    "path",
    ["../escape.py", "/absolute.py", "a/../../escape.py", "a//b.py", "./main.py"],
)
def test_package_rejects_unsafe_paths(path):
    candidate = package()
    candidate["files"][path] = ""
    with pytest.raises(ValueError):
        validate_package(candidate)


@pytest.mark.parametrize(
    "source",
    [
        SOURCE.replace('"AAPL"', '"SPY"'),
        SOURCE.replace('["AAPL"]', "get_universe()"),
        SOURCE + SOURCE.replace("Example", "Second"),
        "invalid syntax!",
        "raise RuntimeError('never executed')",
    ],
)
def test_package_requires_supported_unambiguous_strategy(source):
    with pytest.raises((ValueError, SyntaxError)):
        validate_package(package(source))


@pytest.mark.parametrize(
    "weights",
    [
        {"AAPL": float("nan")},
        {"AAPL": float("inf")},
        {"AAPL": -0.1},
        {"AAPL": 1.1},
        {"MSFT": 0.5},
        {"AAPL": True},
    ],
)
def test_rejects_unsafe_strategy_outputs(weights):
    with pytest.raises(ValueError):
        validate_weights(weights, ["AAPL"])
