from pathlib import Path
import yaml

from hqg_algorithms import Slice, PortfolioView
from src.aggregator import aggregate_allocations
from src.strategies import ClassicFinance_SPY_IEF, SMA_AAPL

class Portfolio:
    def __init__(self, config_path="config/portfolio.yaml"):
        self.strategies = []
        self.strategy_configs = []
        self.config_path = config_path
        self.load_config()
        self.init_strategies()
        

    def load_config(self):
        config_file = Path(self.config_path)
        
        # TODO: fix fragile
        if not config_file.exists():
            config_file = Path(__file__).parent / self.config_path
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        self.strategy_configs = config.get('strategies', [])
        
        if not self.strategy_configs:
            raise ValueError("No strategies configured in the config file")
    

    def init_strategies(self):
        strat_map = {
            "ClassicFinance_SPY_IEF": ClassicFinance_SPY_IEF,
            "SMA_AAPL": SMA_AAPL
        }
        
        for config in self.strategy_configs:
            strategy_id = config['id']
            class_name = config['class_name']
            #tickers = config['tickers']
            portfolio_weight = config['portfolio_weight']

            if class_name not in strat_map:
                raise ValueError(f"Unknown strategy class: {class_name}")
            
            StrategyClass = strat_map[class_name]
            strategy_instance = StrategyClass()
            universe = strategy_instance.universe()
            
            self.strategies.append({
                'id': strategy_id,
                'instance': strategy_instance,
                'weight': portfolio_weight,
                'tickers': universe
            })
            
            print(f"Initialized strategy: {strategy_id} ({class_name}) with weight {portfolio_weight}")
    

    def get_tickers(self):
        all_tickers = set()
        for strategy in self.strategies:
            all_tickers.update(strategy['tickers'])
        return list(all_tickers)
    
    
    async def on_data(self, data):
        strategy_results = []
    
        for strategy in self.strategies:
            strategy_id = strategy['id']
            strategy_instance = strategy['instance']
            aum_weight = strategy['weight']
            
            slice_obj = Slice(data)
            portfolio_obj = PortfolioView(
                equity=0.0,
                cash=0.0,
                positions={},
                weights={}
            )
            
            allocations_dict = strategy_instance.on_data(slice_obj, portfolio_obj)
            
            if allocations_dict is None:
                continue
            
            allocations = list(allocations_dict.items())
            strategy_results.append((strategy_id, allocations, aum_weight))
            print(f"Strategy {strategy_id} allocations: {allocations}")
        
        target_weights = aggregate_allocations(strategy_results)
        print(f"Aggregated target weights: {target_weights}")
        return target_weights
