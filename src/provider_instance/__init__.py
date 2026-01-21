import os
import logging

from src.marketdata_provider.alpaca import AlpacaMarketData
from src.execution_provider.alpaca import AlpacaExecutor
from src.marketdata_provider.ibkr import IBKRMarketData
from src.execution_provider.ibkr import IBKRExecutor

logger = logging.getLogger(__name__)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.FileHandler("app.log", mode="a")]
    )

def singleton(cls):
    instances = {}
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return get_instance

@singleton
class Engine():
    def __init__(self):
        self.use_ib = None
        self.ib_config = {}
        self.alpaca_config = {}
        self.exec_provider = None
        self.data_provider = None
        self.load_config()

    def load_config(self):
        """Load configuration from env"""

        if os.environ.get("PROVIDER") == "ib":
            self.use_ib = True

        elif os.environ.get("PROVIDER") == "alpaca":
            self.use_ib = False

        else:
            raise ValueError("Invalid provider")
        
        if self.use_ib:
            self.ib_config = {
                'host': os.environ["IBKR_HOST"],
                'port': int(os.environ["IBKR_PORT"]),
                'client_id': int(os.environ["IBKR_CLIENT_ID"])
            }
            logger.info(f"Using IBKR. Config: {self.ib_config}")
       
        else:
            self.alpaca_config = {
                'api_key': os.environ["ALPACA_API_KEY"],
                'secret_key': os.environ["ALPACA_SECRET_KEY"],
                'paper': os.environ.get("ALPACA_PAPER", "true")
            }
            logger.info(f"Using Alpaca. Config: {self.alpaca_config}")
    
    async def setup_ib(self):
        """Setup IBKR connection"""
        try:
            from ib_async import IB
            
            ib = IB()
            await ib.connectAsync(
                host=self.ib_config['host'],
                port=self.ib_config['port'],
                clientId=self.ib_config['client_id']
            )
            
            if 'market_data_type' in self.ib_config:
                ib.reqMarketDataType(self.ib_config['market_data_type'])
            
            logger.info(f"Connected to IBKR at {self.ib_config['host']}:{self.ib_config['port']}")
            
            self.data_provider = IBKRMarketData(ib)
            self.exec_provider = IBKRExecutor(ib)
            
        except Exception as e:
            logger.error(f"Error connecting to IBKR: {e}")
            raise

    def setup_alpaca(self):
        """Setup Alpaca connection"""
        try:
            logger.info("Setting up Alpaca connection")
            
            self.data_provider = AlpacaMarketData(
                api_key=self.alpaca_config['api_key'],
                secret_key=self.alpaca_config['secret_key'],
                paper=True
            )

            self.exec_provider = AlpacaExecutor(
                api_key=self.alpaca_config['api_key'],
                secret_key=self.alpaca_config['secret_key'],
                paper=True
            )
            
            logger.info("Alpaca providers initialized")

        except Exception as e:
            logger.error(f"Error setting up Alpaca: {e}")
            raise
    
    def get_data_provider(self):
        return self.data_provider
    
    def get_exec_provider(self):
        return self.exec_provider

engine_instance = Engine()