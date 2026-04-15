from pathlib import Path
import importlib
import yaml
import logging
from datetime import datetime, timezone

from hqg_algorithms import Slice, PortfolioView, Cadence, Strategy,BarSize, Bar, TargetWeights, Hold, Liquidate
from src.aggregator import aggregate_allocations

logger = logging.getLogger(__name__)


def _get_strategies(class_name):
    try:
        module = importlib.import_module(f"src.strategies.{class_name}")

    except ModuleNotFoundError:
        raise ValueError(
            f"No module found for strategy '{class_name}'. "
            f"Expected file: src/strategies/{class_name}.py"
        )

    cls = getattr(module, class_name, None)
    
    if cls is None:
        raise ValueError(
            f"Module 'src.strategies.{class_name}' has no class '{class_name}'"
        )

    if not issubclass(cls, Strategy):
        raise TypeError(
            f"'{class_name}' is not a subclass of Strategy"
        )

    return cls


class Portfolio:
    def __init__(self, config_path="config/portfolio.yaml"):
        self.strategies = []
        self.strategy_configs = []
        self.config_path = config_path
        self._strategy_state = {}  # strategy_id -> { current_bar_period, last_output }
        self._bar_aggregate: dict[tuple, dict] = {} # aggregate OHLCV period data for each symbol (for each strategy)
        self.load_config()
        self.init_strategies()
        
    def load_config(self):
        config_file = Path(self.config_path)
        
        # TODO: fix fragile
        if not config_file.exists():
            config_file = Path(__file__).parent / self.config_path
        
        if not config_file.exists():
            logger.error(f"Config file not found: {self.config_path}")
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        self.strategy_configs = config.get('strategies', [])
        
        if not self.strategy_configs:
            logger.error("No strategies configured in the config file")
            raise ValueError("No strategies configured in the config file")
    

    def init_strategies(self):
        strat_map = {}
        
        for config in self.strategy_configs:
            strategy_id = config['id']
            class_name = config['class_name']
            portfolio_weight = config['portfolio_weight']

            StrategyClass = _get_strategies(class_name)
            strategy_instance = StrategyClass()
            universe = list(StrategyClass.universe)

            self.strategies.append({
                'id': strategy_id,
                'instance': strategy_instance,
                'weight': portfolio_weight,
                'universe': universe,
                'cadence': StrategyClass.cadence,
            })
            
            logger.info(f"Initialized strategy: {strategy_id} ({class_name}) with weight {portfolio_weight:.2%}, universe: {universe}")
    

    def get_tickers(self):
        all_tickers = set()
        for strategy in self.strategies:
            all_tickers.update(strategy['universe'])
        logger.debug(f"Universe contains {len(all_tickers)} tickers: {sorted(all_tickers)}")
        return list(all_tickers)
    
    def _create_bar(self, snapshot):
        def _num(key: str):
            v = snapshot.get(key)
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        close = _num("close")
        price = _num("price")
        volume = _num("volume")
        
        if close is None:
            close = price
            if close is None: # both close and price None
                return None

        o = _num("open")
        h = _num("high")
        l = _num("low")
        if o is None:
            o = close
        if h is None:
            h = close
        if l is None:
            l = close

        return Bar(open=o, high=h, low=l, close=close, volume=volume)

    async def on_data(self, data, portfolio_view: PortfolioView):
        strategy_results = []

        event_time = datetime.now(timezone.utc)
        if data:
            snapshot_timestamps = []
            for s in data.values():
                if isinstance(s, dict) and isinstance(s.get('timestamp'), datetime):
                    snapshot_timestamps.append(s['timestamp'])
            if snapshot_timestamps:
                event_time = max(snapshot_timestamps)

        for strategy in self.strategies:
            strategy_id = strategy['id']
            strategy_instance = strategy['instance']
            aum_weight = strategy['weight']
            universe = strategy['universe']
            cadence = strategy['cadence']
            bar_size = cadence.bar_size
            state = self._strategy_state.setdefault(strategy_id, {'current_bar_period': None, 'last_output': None})

            utc = event_time
            if utc.tzinfo is None:
                utc = utc.replace(tzinfo=timezone.utc)
            else:
                utc = utc.astimezone(timezone.utc)

            if bar_size == BarSize.DAILY:
                tick_bar_period = utc.date().isoformat()

            elif bar_size == BarSize.WEEKLY:
                iso_year, iso_week, _iso_weekday = utc.isocalendar()
                tick_bar_period = f"{iso_year}-W{iso_week:02d}"

            elif bar_size == BarSize.MONTHLY:
                tick_bar_period = f"{utc.year}-{utc.month:02d}"

            elif bar_size == BarSize.QUARTERLY:
                calendar_quarter = (utc.month - 1) // 3 + 1
                tick_bar_period = f"{utc.year}-Q{calendar_quarter}"

            else:
                tick_bar_period = utc.date().isoformat()
                logger.warning(
                    "Unknown BarSize %r for strategy %s",
                    bar_size,
                    strategy_id,
                )

            allocations_dict = None

            if state['current_bar_period'] is None:
                state['current_bar_period'] = tick_bar_period

            if tick_bar_period == state['current_bar_period']:
                if data:
                    for sym in universe:
                        snap = data.get(sym)
                        if not isinstance(snap, dict):
                            continue

                        tick = self._create_bar(snap)
                        if tick is None:
                            continue

                        key = (strategy_id, bar_size, tick_bar_period, sym)
                        if key not in self._bar_aggregate:
                            self._bar_aggregate[key] = {
                                "open": None,
                                "high": None,
                                "low": None,
                                "close": None,
                                "volume": None,
                            }

                        acc = self._bar_aggregate[key]
                        if acc["open"] is None:
                            acc["open"] = tick.open
                            acc["high"] = tick.high
                            acc["low"] = tick.low
                            acc["close"] = tick.close
                            acc["volume"] = tick.volume

                        else:
                            acc["high"] = max(acc["high"], tick.high)
                            acc["low"] = min(acc["low"], tick.low)
                            acc["close"] = tick.close
                            if tick.volume is not None:
                                acc["volume"] = (acc["volume"] or 0.0) + tick.volume

                allocations_dict = state['last_output']

            else: # new period; close bar, and move on
                completed_bar_period = state['current_bar_period']
                bars: dict[str, Bar] = {}
                for sym in universe:
                    acc = self._bar_aggregate.get((strategy_id, bar_size, completed_bar_period, sym))
                    if acc is None or acc["open"] is None or acc["close"] is None:
                        continue
                    
                    bars[sym] = Bar(
                        open=acc["open"],
                        high=acc["high"],
                        low=acc["low"],
                        close=acc["close"],
                        volume=acc["volume"],
                    )

                slice_obj = Slice(bars)
                strategy_failed = False
                strategy_out = None
                try:
                    strategy_out = strategy_instance.on_data(slice_obj, portfolio_view)
                except Exception as e:
                    strategy_failed = True
                    logger.error(f"Strategy {strategy_id} failed: {e}", exc_info=True)

                allocations_dict = None
                if not strategy_failed:
                    if isinstance(strategy_out, TargetWeights):
                        allocations_dict = dict(strategy_out.weights)
                    elif isinstance(strategy_out, Hold):
                        allocations_dict = state['last_output']
                    elif isinstance(strategy_out, Liquidate):
                        allocations_dict = {}
                    elif isinstance(strategy_out, dict):
                        allocations_dict = dict(strategy_out)
                    elif strategy_out is None:
                        allocations_dict = None
                    else:
                        logger.warning(
                            "Strategy %s returned unexpected type %s",
                            strategy_id,
                            type(strategy_out).__name__,
                        )

                for sym in universe:
                    self._bar_aggregate.pop(
                        (strategy_id, bar_size, completed_bar_period, sym), None
                    )

                state['current_bar_period'] = tick_bar_period
                if data:
                    for sym in universe:
                        snap = data.get(sym)
                        if not isinstance(snap, dict):
                            continue
                        
                        tick = self._create_bar(snap)
                        if tick is None:
                            continue

                        key = (strategy_id, bar_size, tick_bar_period, sym)
                        if key not in self._bar_aggregate:
                            self._bar_aggregate[key] = {
                                "open": None,
                                "high": None,
                                "low": None,
                                "close": None,
                                "volume": None,
                            }

                        acc = self._bar_aggregate[key]
                        if acc["open"] is None:
                            acc["open"] = tick.open
                            acc["high"] = tick.high
                            acc["low"] = tick.low
                            acc["close"] = tick.close
                            acc["volume"] = tick.volume

                        else:
                            acc["high"] = max(acc["high"], tick.high)
                            acc["low"] = min(acc["low"], tick.low)
                            acc["close"] = tick.close
                            if tick.volume is not None:
                                acc["volume"] = (acc["volume"] or 0.0) + tick.volume

                if allocations_dict is not None:
                    state['last_output'] = allocations_dict
                else:
                    continue

            if allocations_dict is None:
                logger.warning(f"Strategy {strategy_id} returned None allocations")
                continue

            allocations = list(allocations_dict.items())
            strategy_results.append((strategy_id, allocations, aum_weight))
            logger.info(f"Strategy {strategy_id} allocations: {allocations}")

        target_weights = aggregate_allocations(strategy_results)
        logger.info(f"Aggregated target weights: {target_weights}")
        return target_weights