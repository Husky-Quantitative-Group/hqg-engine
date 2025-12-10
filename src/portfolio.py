from pathlib import Path
import yaml
import logging
from enum import Enum
from datetime import datetime

from hqg_algorithms import Slice, PortfolioView, Cadence
from src.aggregator import aggregate_allocations
from src.strategies import ClassicFinance_SPY_IEF, SMA_AAPL

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
        self._strategy_state = {} # {strategy_id : {cadence=timedelta(), last_called=time.time(), last_output=[(ticker, weight)]} }
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
        strat_map = {
            "ClassicFinance_SPY_IEF": ClassicFinance_SPY_IEF,
            "SMA_AAPL": SMA_AAPL
        }
        
        for config in self.strategy_configs:
            strategy_id = config['id']
            class_name = config['class_name']
            portfolio_weight = config['portfolio_weight']

            if class_name not in strat_map:
                logger.error(f"Unknown strategy class: {class_name}")
                raise ValueError(f"Unknown strategy class: {class_name}")
            
            StrategyClass = strat_map[class_name]
            strategy_instance = StrategyClass()
            universe = strategy_instance.universe()
            
            self.strategies.append({
                'id': strategy_id,
                'instance': strategy_instance,
                'weight': portfolio_weight,
                'universe': universe
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

    def cadence_handler(self, strategy_id, cadence, current_time):
        if strategy_id not in self._strategy_state:
            self._strategy_state[strategy_id] = {
                'cadence': cadence,
                'last_called': None,
                'last_output': []
            }
        
        state = self._strategy_state[strategy_id]
        last_called = state['last_called']
        last_output = state['last_output']
            
        # check if cadence period has elapsed, or this is the first run
        if last_called is None:
            return CadenceDecision.RUN

        next_call = last_called + (cadence.bar_size * cadence.exec_lag_bars)
        if current_time >= next_call:
            return CadenceDecision.RUN
        
        return CadenceDecision.PREV
    
    async def on_data(self, data):
        strategy_results = []
    
        for strategy in self.strategies:
            # call strategy_instance to get cadence & then keep those weights static if cadence has not elapsed
            # ie, if decision is RUN and next_time > cur_time, append prev_weights to strategry_results so we maintain the position
            
            strategy_id = strategy['id']
            strategy_instance = strategy['instance']
            aum_weight = strategy['weight']
            universe = strategy['universe']

            cadence = strategy_instance.cadence()
            current_time = datetime.now()
            decision = self.cadence_handler(strategy_id, cadence, current_time)
            state = self._strategy_state[strategy_id]

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

                state['last_output'] = allocations_dict
                state['last_called'] = current_time
            
            elif decision == CadenceDecision.PREV:  # PREV - use previous output (do not execute strategy)
                allocations_dict = state['last_output']
                state['last_called'] = current_time
    
            if allocations_dict is None:
                logger.warning(f"Strategy {strategy_id} returned None allocations")
                continue
            
            allocations = list(allocations_dict.items())
            strategy_results.append((strategy_id, allocations, aum_weight))
            logger.info(f"Strategy {strategy_id} allocations: {allocations}")

        target_weights = aggregate_allocations(strategy_results)
        logger.info(f"Aggregated target weights: {target_weights}")
        return target_weights