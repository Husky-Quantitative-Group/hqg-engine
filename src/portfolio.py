from pathlib import Path
import yaml
import logging
from enum import Enum
from datetime import datetime, timezone

from hqg_algorithms import Slice, PortfolioView, BarSize
from src.aggregator import aggregate_allocations

logger = logging.getLogger(__name__)

class CadenceDecision(Enum):
    RUN = "run"
    PREV = "prev"
    # WAIT = "wait"

class Portfolio:
    def __init__(self, config_path="config/portfolio.yaml"):
        self.strategies = []
        self.strategy_configs = []
        self.config_path = config_path
        self._strategy_state = {}  # strategy_id -> { last_period, last_output }
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

            if class_name not in strat_map:
                logger.error(f"Unknown strategy class: {class_name}")
                raise ValueError(f"Unknown strategy class: {class_name}")
            
            StrategyClass = strat_map[class_name]
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
    
    def partition_data(universe, data) -> Slice:  #???
        # TODO
        # only gets data in universe
        pass

    async def on_data(self, data):
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

            cadence = strategy['cadence']
            state = self._strategy_state.setdefault(
                strategy_id,
                {'last_period': None, 'last_output': None},
            )

            utc = event_time
            if utc.tzinfo is None:
                utc = utc.replace(tzinfo=timezone.utc)
            else:
                utc = utc.astimezone(timezone.utc)

            bs = cadence.bar_size
            if bs == BarSize.DAILY:
                period_now = utc.date().isoformat()

            elif bs == BarSize.WEEKLY:
                iso_year, iso_week, _iso_weekday = utc.isocalendar()
                period_now = f"{iso_year}-W{iso_week:02d}"

            elif bs == BarSize.MONTHLY:
                period_now = f"{utc.year}-{utc.month:02d}"

            elif bs == BarSize.QUARTERLY:
                calendar_quarter = (utc.month - 1) // 3 + 1
                period_now = f"{utc.year}-Q{calendar_quarter}"
                
            else:
                period_now = utc.date().isoformat()
                logger.warning(
                    "Unknown BarSize %r for strategy %s",
                    bs,
                    strategy_id,
                )

            if state['last_period'] != period_now: # even on None (initial run), strategy should run
                decision = CadenceDecision.RUN
            else:
                decision = CadenceDecision.PREV

            allocations_dict = None

            if decision == CadenceDecision.RUN:
                # TODO send only data in universe

                slice_obj = Slice(data)
                portfolio_obj = PortfolioView(  # TODO: Portfolio should be managing a PortfolioView, not passing in shell
                    equity=0.0,
                    cash=0.0,
                    positions={},
                    weights={}
                )

                try:
                    allocations_dict = strategy_instance.on_data(slice_obj, portfolio_obj)
                except Exception as e:
                    logger.error(f"Strategy {strategy_id} failed: {e}", exc_info=True)
                    continue

                if allocations_dict is None:
                    logger.warning(f"Strategy {strategy_id} returned None allocations")
                    continue

                state['last_output'] = allocations_dict
                state['last_period'] = period_now

            elif decision == CadenceDecision.PREV:
                allocations_dict = state['last_output']
    
            if allocations_dict is None:
                logger.warning(f"Strategy {strategy_id} returned None allocations")
                continue
            
            allocations = list(allocations_dict.items())
            strategy_results.append((strategy_id, allocations, aum_weight))
            logger.info(f"Strategy {strategy_id} allocations: {allocations}")

        target_weights = aggregate_allocations(strategy_results)
        logger.info(f"Aggregated target weights: {target_weights}")
        return target_weights