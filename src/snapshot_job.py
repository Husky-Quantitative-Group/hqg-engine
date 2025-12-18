import asyncio
import logging
from pathlib import Path
from typing import Optional
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.marketdata_provider.alpaca import AlpacaMarketData
from src.execution_provider.alpaca import AlpacaExecutor

logger = logging.getLogger(__name__)

class SnapshotJob:
    def __init__(self, config_path="config/engine.yaml"):
        self.config_path = config_path
        self.alpaca_config = {}
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.alpaca_data: Optional[AlpacaMarketData] = None
        self.alpaca_exec: Optional[AlpacaExecutor] = None
        self.load_config()
    
    def load_config(self):
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            config_file = Path(__file__).parent.parent / self.config_path
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        self.alpaca_config = config.get('alpaca_config', {})
        logger.info(f"Config loaded from {config_file}")