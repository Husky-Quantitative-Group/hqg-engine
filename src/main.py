import asyncio
import logging
from math import trunc

from hqg_algorithms import PortfolioView

logger = logging.getLogger(__name__)

def truncate(value: float) -> float:
    return trunc(float(value) * 1000) / 1000


def build_portfolio_view(equity, positions, market_data):
    for k in list(positions.keys()):
        positions[k] = float(positions[k])
    
    if equity <= 0:
        return PortfolioView(
            equity=float(equity),
            cash=0.0,
            positions=positions,
            weights={},
        )

    holdings_value = 0.0
    weights: dict[str, float] = {}

    for symbol, qty in positions.items():
        reference_price = None
        snapshot = market_data.get(symbol)
        if isinstance(snapshot, dict):
            for key in ("close", "price"):
                v = snapshot.get(key)
                if v is None:
                    continue
                try:
                    reference_price = float(v)
                    break
                except (TypeError, ValueError):
                    continue

        if reference_price is None:
            continue

        position_value = qty * reference_price
        holdings_value += position_value
        weights[symbol] = position_value / equity

    cash = max(float(equity) - holdings_value, 0.0)
    return PortfolioView(
        equity=float(equity),
        cash=cash,
        positions=positions,
        weights=weights,
    )

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.FileHandler("app.log", mode="a")]
    )


async def run():
    from src.control.worker import run as run_controlled_worker
    await run_controlled_worker()


async def main():
    setup_logging()
    await run()


if __name__ == "__main__":
    asyncio.run(main())
