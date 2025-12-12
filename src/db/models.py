from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, BigInteger, Identity, String, Timestamp, Boolean, Date, Numeric,PrimaryKeyConstraint, ForeignKey, CheckConstraint, JSON
from enum import Enum
from sqlalchemy.dialects.postgresql import ENUM as PgEnum

Base = declarative_base()

class Action(Enum):
    BUY = "buy"
    SELL = "sell"

class Portfolio(Base):
    __tablename__ = "portfolios"
    
    portfolio_id = Column(BigInteger, Identity(always=True), primary_key=True)
    name = Column(String(255))
    is_active = Column(Boolean, default=True, nullable=False)

class Instrument(Base):
    __tablename__ = "instruments"
    
    instrument_id = Column(BigInteger, Identity(always=True), primary_key=True)
    ticker = Column(String(35), unique=True, nullable=False)
    asset_class = Column(String(255), nullable=True)
    
    holdings = relationship("HoldingsSnapshot", back_populates="instrument")

class PerformanceSnapshot(Base):
    __tablename__ = "performance_snapshots"
    
    portfolio_id = Column(BigInteger, nullable=False)
    as_of = Column(Date, nullable=False)
    equity = Column(Numeric(18, 4), nullable=False)

    __table_args__ = (PrimaryKeyConstraint('portfolio_id', 'as_of')) # one snapshot per portfolio per day

class HoldingsSnapshot(Base):
    __tablename__ = "holdings_snapshots"
    
    portfolio_id = Column(BigInteger, nullable=False)
    as_of = Column(Date, nullable=False)
    instrument_id = Column(BigInteger, ForeignKey('instruments.instrument_id'), nullable=False)
    quantity = Column(Numeric(18, 6), nullable=False)
    price = Column(Numeric(18, 6), nullable=False)
    market_value = Column(Numeric(18, 4), nullable=False)

    instrument = relationship("Instrument", back_populates="holdings")
    __table_args__ = (PrimaryKeyConstraint('portfolio_id', 'as_of', 'instrument_id')) # one snapshot per portfolio per instrument per day

class StrategyWeightsSnapshot(Base):
    __tablename__ = "strategy_weights_snapshots"
    
    portfolio_id = Column(BigInteger, nullable=False)
    as_of = Column(Date, nullable=False)
    strategy_id = Column(BigInteger, nullable=False)
    weight = Column(Numeric(9, 6), nullable=False)
    
    __table_args__ = (
        PrimaryKeyConstraint('portfolio_id', 'as_of', 'strategy_id'), # one snapshot per portfolio per strategy per day
        CheckConstraint('weight >= 0 AND weight <= 1.0', name='weight_constraint')
    )

class ExecutionEvent(Base):
    __tablename__ = "execution_events"
    
    event_id = Column(BigInteger, Identity(always=True), primary_key=True)
    portfolio_id = Column(BigInteger, nullable=False)
    timestamp = Column(Timestamp, nullable=False)
    action = Column(PgEnum(Action, name="action_enum"), nullable=False)
    symbol = Column(String(35), nullable=False)
    quantity = Column(Numeric(18, 6), nullable=False)
    
    __table_args__ = (CheckConstraint('action IN (0, 1)', name='action_constraint')) # 0 / 1 (sell / buy)

class AllocationEvent(Base):
    __tablename__ = "allocation_events"
    
    event_id = Column(BigInteger, Identity(always=True), primary_key=True)
    portfolio_id = Column(BigInteger, nullable=False)
    timestamp = Column(Timestamp, nullable=False)
    allocations = Column(JSON, nullable=False) # [{"symbol": "TSLA", "weight": 0.6}, ...]