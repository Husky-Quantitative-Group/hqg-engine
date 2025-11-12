import asyncio
from pathlib import Path
from typing import List, Dict
from ib_async import IB
import yaml

from hqg_algorithms import Slice, PortfolioView

from src.ingestor.ibkr import IBData
from src.executor import Executor
from src.aggregator import aggregate_allocations
from src.strategies import SMAStrategy, BuyHoldStrategy


class Portfolio:
    def __init__(self, config_path="config/strategies.yaml"):
        self.strategies = []
        self.strategy_configs = []
        self.config_path = config_path
        self.load_config()
        self.init_strategies()
        

    def load_config(self):
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        self.strategy_configs = config.get('strategies', [])
        
        if not self.strategy_configs:
            raise ValueError("No strategies configured in the config file")
    

    def init_strategies(self):
        strategy_classes = {
            "SMAStrategy": SMAStrategy,
            "BuyHoldStrategy": BuyHoldStrategy
        }
        
        for config in self.strategy_configs:
            strategy_id = config['id']
            class_name = config['class_name']
            tickers = config['tickers']
            portfolio_weight = config['portfolio_weight']

            if class_name not in strategy_classes:
                raise ValueError(f"Unknown strategy class: {class_name}")
            
            StrategyClass = strategy_classes[class_name]
            strategy_instance = StrategyClass(tickers=tickers)
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
            portfolio_obj = PortfolioView(      # TODO: Brendan - what is this used for??
                equity=0.0,
                cash=0.0,
                positions={},
                weights={}
            )
            
            allocations_dict = strategy_instance.on_data(slice_obj, portfolio_obj)
            
            if allocations_dict is None:
                continue
            
            allocations = list(allocations_dict.items())    # TODO: don't we need key's too?
            strategy_results.append((strategy_id, allocations, aum_weight))
            print(f"Strategy {strategy_id} allocations: {allocations}")
        
        target_weights = aggregate_allocations(strategy_results)
        print(f"Aggregated target weights: {target_weights}")
        return target_weights


async def run_engine():
    # load execution configuration
    config_file = Path("config/execution.yaml")
    with open(config_file, 'r') as f:
        exec_config = yaml.safe_load(f)
    
    ibkr_config = exec_config['ibkr']
    portfolio_config = exec_config['portfolio']
    
    # initialize portfolio
    portfolio = Portfolio(config_path="config/strategies.yaml")
    
    # obtain all tickers
    tickers = portfolio.get_tickers()
    print(f"Trading universe: {tickers}")
    
    # connect to IBKR
    ib = IB()
    await ib.connectAsync(
        host=ibkr_config['host'],
        port=ibkr_config['port'],
        clientId=ibkr_config['client_id']
    )
    
    # set market data type
    ib.reqMarketDataType(ibkr_config['market_data_type'])
    print(f"Connected to IBKR at {ibkr_config['host']}:{ibkr_config['port']}")
    
    # init data provider and executor
    data_provider = IBData(ib)
    executor = Executor(ib)
    
    try:
        print("Starting trading engine")
        market_data = {}
        ticker_set = set(tickers)
        async for snapshot in data_provider.stream_prices(tickers, 60.0):   # always 1-min granularity
            # obtain data for each ticker
            market_data[snapshot['symbol']] = snapshot
            
            # process only when data for ALL tickers
            if set(market_data.keys()) >= ticker_set:
                print(f"\n Rebalancing with data for {list(market_data.keys())}")

                target_weights = await portfolio.on_data(market_data)
                portfolio_value = await executor.get_account_value()
                print(f"Current account value: ${portfolio_value:,.2f}")
                
                await executor.rebalance(target_weights, portfolio_value)
                market_data = {}
            
    except KeyboardInterrupt:
        print("\n Stopped from keyboard interrupt")
        
    except Exception as e:
        print(f"Error in trading engine: {e}")
        raise

    finally:
        await data_provider.cleanup()
        ib.disconnect()
        print("Disconnected from IBKR")


if __name__ == "__main__":
    asyncio.run(run_engine())

