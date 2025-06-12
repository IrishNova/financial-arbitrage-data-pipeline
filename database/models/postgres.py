#!/usr/bin/env python3
"""
models/postgres.py

PostgreSQL data models for trade execution and analysis tracking.
Defines relational data structures for trade tickets, analysis records, and metadata.
"""

import json
import time
import uuid
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class TradeStatus(Enum):
    """Trade execution status"""
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    FAILED = "failed"


class TradeSide(Enum):
    """Trade side"""
    BUY = "buy"
    SELL = "sell"


class TradeVenue(Enum):
    """Trading venue"""
    KALSHI = "kalshi"
    POLYMARKET = "polymarket"


class ArbitrageType(Enum):
    """Type of arbitrage opportunity"""
    KALSHI_YES_POLYMARKET_NO = "kalshi_yes_polymarket_no"
    KALSHI_NO_POLYMARKET_YES = "kalshi_no_polymarket_yes"
    KALSHI_POLYMARKET_SPREAD = "kalshi_polymarket_spread"


@dataclass
class TradeTicket:
    """
    Complete trade execution record.

    This is the primary table for tracking all trade executions,
    with references to the exact market snapshots that influenced the decision.
    """

    # === PRIMARY IDENTIFICATION ===
    trade_id: str = field(default_factory=lambda: f"trade_{uuid.uuid4().hex[:12]}")
    arbitrage_id: str = field(default_factory=lambda: f"arb_{uuid.uuid4().hex[:8]}")  # Links related trades

    # === MARKET IDENTIFICATION ===
    kalshi_ticker: str = ""
    polymarket_condition_id: str = ""
    pair_id: str = ""  # Links to the arbitrage pair configuration
    market_title: str = ""

    # === TRADE DETAILS ===
    venue: TradeVenue = TradeVenue.KALSHI
    side: TradeSide = TradeSide.BUY
    outcome: str = ""  # "YES" or "NO"

    # Order specifications
    order_type: str = "market"  # "market", "limit", "gtc", etc.
    quantity: float = 0.0
    limit_price: Optional[float] = None

    # === EXECUTION RESULTS ===
    status: TradeStatus = TradeStatus.PENDING
    executed_quantity: float = 0.0
    executed_price: Optional[float] = None
    average_fill_price: Optional[float] = None
    total_cost: Optional[float] = None
    fees: Optional[float] = None

    # External system identifiers
    external_order_id: Optional[str] = None  # ID from Kalshi/Polymarket
    execution_report_id: Optional[str] = None

    # === ARBITRAGE CONTEXT ===
    arbitrage_type: ArbitrageType = ArbitrageType.KALSHI_YES_POLYMARKET_NO
    expected_profit: Optional[float] = None
    expected_profit_bps: Optional[float] = None
    leg_number: int = 1  # 1 for first leg, 2 for second leg of arbitrage
    related_trade_id: Optional[str] = None  # ID of the opposing leg

    # === MARKET DATA CORRELATION ===
    # Snapshot IDs that were analyzed for this trade decision
    analyzed_snapshot_ids: List[str] = field(default_factory=list)
    # Snapshot ID that was actually used for execution decision
    executed_snapshot_id: Optional[str] = None

    # Market conditions at time of decision
    kalshi_bid_at_decision: Optional[float] = None
    kalshi_ask_at_decision: Optional[float] = None
    polymarket_bid_at_decision: Optional[float] = None
    polymarket_ask_at_decision: Optional[float] = None
    spread_at_decision: Optional[float] = None

    # === TIMING INFORMATION ===
    # Decision timing
    opportunity_detected_at: Optional[datetime] = None
    decision_made_at: Optional[datetime] = None
    order_submitted_at: Optional[datetime] = None

    # Execution timing
    first_fill_at: Optional[datetime] = None
    last_fill_at: Optional[datetime] = None
    order_completed_at: Optional[datetime] = None

    # Performance metrics
    decision_latency_ms: Optional[float] = None  # Time from detection to decision
    submission_latency_ms: Optional[float] = None  # Time from decision to submission
    fill_latency_ms: Optional[float] = None  # Time from submission to first fill

    # === RISK AND POSITION ===
    position_size_before: float = 0.0
    position_size_after: float = 0.0
    portfolio_exposure_before: Optional[float] = None
    portfolio_exposure_after: Optional[float] = None
    risk_score: Optional[float] = None

    # === METADATA ===
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: str = "arbitrage_system"
    notes: Optional[str] = None

    def __post_init__(self):
        """Calculate derived metrics"""
        # Ensure IDs are set
        if not self.trade_id:
            self.trade_id = f"trade_{uuid.uuid4().hex[:12]}"
        if not self.arbitrage_id:
            self.arbitrage_id = f"arb_{uuid.uuid4().hex[:8]}"

        # Calculate timing metrics
        if self.opportunity_detected_at and self.decision_made_at:
            delta = self.decision_made_at - self.opportunity_detected_at
            self.decision_latency_ms = delta.total_seconds() * 1000

        if self.decision_made_at and self.order_submitted_at:
            delta = self.order_submitted_at - self.decision_made_at
            self.submission_latency_ms = delta.total_seconds() * 1000

        if self.order_submitted_at and self.first_fill_at:
            delta = self.first_fill_at - self.order_submitted_at
            self.fill_latency_ms = delta.total_seconds() * 1000

        # Calculate total cost if not provided
        if self.executed_quantity and self.average_fill_price and self.total_cost is None:
            self.total_cost = self.executed_quantity * self.average_fill_price
            if self.fees:
                self.total_cost += self.fees

    def add_analyzed_snapshot(self, snapshot_id: str):
        """Add a snapshot ID that was analyzed for this trade"""
        if snapshot_id not in self.analyzed_snapshot_ids:
            self.analyzed_snapshot_ids.append(snapshot_id)

    def set_executed_snapshot(self, snapshot_id: str):
        """Set the snapshot ID that was used for execution"""
        self.executed_snapshot_id = snapshot_id
        # Also add to analyzed list if not already there
        self.add_analyzed_snapshot(snapshot_id)

    def mark_as_submitted(self, external_order_id: str = None):
        """Mark trade as submitted to venue"""
        self.status = TradeStatus.SUBMITTED
        self.order_submitted_at = datetime.now(timezone.utc)
        if external_order_id:
            self.external_order_id = external_order_id
        self.updated_at = datetime.now(timezone.utc)

    def add_fill(self, quantity: float, price: float, timestamp: datetime = None):
        """Add a partial or complete fill"""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        # Update execution quantities
        self.executed_quantity += quantity

        # Calculate average fill price
        if self.average_fill_price is None:
            self.average_fill_price = price
        else:
            total_value = (self.executed_quantity - quantity) * self.average_fill_price + quantity * price
            self.average_fill_price = total_value / self.executed_quantity

        # Update timing
        if self.first_fill_at is None:
            self.first_fill_at = timestamp
        self.last_fill_at = timestamp

        # Update status
        if self.executed_quantity >= self.quantity:
            self.status = TradeStatus.FILLED
            self.order_completed_at = timestamp
        else:
            self.status = TradeStatus.PARTIALLY_FILLED

        self.updated_at = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        data = {}

        for field_name, field_def in self.__dataclass_fields__.items():
            value = getattr(self, field_name)

            if isinstance(value, (TradeStatus, TradeSide, TradeVenue, ArbitrageType)):
                data[field_name] = value.value
            elif isinstance(value, datetime):
                data[field_name] = value.isoformat()
            elif isinstance(value, list):
                data[field_name] = json.dumps(value)
            else:
                data[field_name] = value

        return data


@dataclass
class AnalysisRecord:
    """
    Record of arbitrage analysis that didn't result in a trade.

    Tracks opportunities that were detected but passed on,
    with references to the market snapshots that were analyzed.
    """

    # === IDENTIFICATION ===
    analysis_id: str = field(default_factory=lambda: f"analysis_{uuid.uuid4().hex[:12]}")

    # === MARKET CONTEXT ===
    kalshi_ticker: str = ""
    polymarket_condition_id: str = ""
    pair_id: str = ""

    # === ANALYSIS DETAILS ===
    arbitrage_type: ArbitrageType = ArbitrageType.KALSHI_YES_POLYMARKET_NO
    potential_profit: Optional[float] = None
    potential_profit_bps: Optional[float] = None

    # Why the opportunity was passed on
    rejection_reason: str = ""  # "spread_too_small", "insufficient_volume", "risk_limit", etc.
    rejection_details: Optional[str] = None

    # === MARKET DATA CORRELATION ===
    analyzed_snapshot_ids: List[str] = field(default_factory=list)

    # Market conditions at time of analysis
    kalshi_bid: Optional[float] = None
    kalshi_ask: Optional[float] = None
    kalshi_bid_size: Optional[float] = None
    kalshi_ask_size: Optional[float] = None

    polymarket_bid: Optional[float] = None
    polymarket_ask: Optional[float] = None
    polymarket_bid_size: Optional[float] = None
    polymarket_ask_size: Optional[float] = None

    cross_market_spread: Optional[float] = None
    required_minimum_spread: Optional[float] = None

    # === TIMING ===
    analyzed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    analysis_duration_ms: Optional[float] = None

    # === METADATA ===
    created_by: str = "arbitrage_system"
    notes: Optional[str] = None

    def add_analyzed_snapshot(self, snapshot_id: str):
        """Add a snapshot ID that was analyzed"""
        if snapshot_id not in self.analyzed_snapshot_ids:
            self.analyzed_snapshot_ids.append(snapshot_id)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        data = {}

        for field_name, field_def in self.__dataclass_fields__.items():
            value = getattr(self, field_name)

            if isinstance(value, ArbitrageType):
                data[field_name] = value.value
            elif isinstance(value, datetime):
                data[field_name] = value.isoformat()
            elif isinstance(value, list):
                data[field_name] = json.dumps(value)
            else:
                data[field_name] = value

        return data


@dataclass
class ArbitragePair:
    """
    Configuration for an arbitrage pair.

    Links Kalshi ticker to Polymarket condition ID with trading parameters.
    """

    # === IDENTIFICATION ===
    pair_id: str = field(default_factory=lambda: f"pair_{uuid.uuid4().hex[:8]}")

    # === MARKET MAPPING ===
    kalshi_ticker: str = ""
    polymarket_condition_id: str = ""
    market_title: str = ""
    description: Optional[str] = None

    # === TRADING PARAMETERS ===
    is_active: bool = True
    min_spread_bps: float = 100.0  # Minimum spread required for trade
    max_position_size: float = 1000.0
    max_trade_size: float = 100.0

    # Risk parameters
    max_exposure: Optional[float] = None
    stop_loss_bps: Optional[float] = None

    # === PERFORMANCE TRACKING ===
    total_trades: int = 0
    total_volume: float = 0.0
    total_profit: float = 0.0
    win_rate: float = 0.0
    average_profit_per_trade: float = 0.0

    # === METADATA ===
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expiration_date: Optional[datetime] = None  # When the market expires

    def update_performance(self, trade_profit: float, trade_volume: float):
        """Update performance metrics after a trade"""
        self.total_trades += 1
        self.total_volume += trade_volume
        self.total_profit += trade_profit

        # Calculate win rate (profitable trades)
        if trade_profit > 0:
            # This is simplified - in practice you'd need to track wins separately
            pass

        self.average_profit_per_trade = self.total_profit / self.total_trades if self.total_trades > 0 else 0.0
        self.updated_at = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        data = {}

        for field_name, field_def in self.__dataclass_fields__.items():
            value = getattr(self, field_name)

            if isinstance(value, datetime):
                data[field_name] = value.isoformat()
            else:
                data[field_name] = value

        return data


# SQL Schema generation helpers

def get_trade_ticket_schema() -> str:
    """Generate PostgreSQL schema for TradeTicket table"""
    return """
    CREATE TABLE IF NOT EXISTS trade_tickets (
        trade_id VARCHAR(50) PRIMARY KEY,
        arbitrage_id VARCHAR(50) NOT NULL,

        -- Market identification
        kalshi_ticker VARCHAR(100) NOT NULL,
        polymarket_condition_id VARCHAR(100) NOT NULL,
        pair_id VARCHAR(50) NOT NULL,
        market_title TEXT,

        -- Trade details
        venue VARCHAR(20) NOT NULL,
        side VARCHAR(10) NOT NULL,
        outcome VARCHAR(10) NOT NULL,
        order_type VARCHAR(20) DEFAULT 'market',
        quantity DECIMAL(15,6) NOT NULL,
        limit_price DECIMAL(15,6),

        -- Execution results
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        executed_quantity DECIMAL(15,6) DEFAULT 0,
        executed_price DECIMAL(15,6),
        average_fill_price DECIMAL(15,6),
        total_cost DECIMAL(15,6),
        fees DECIMAL(15,6),
        external_order_id VARCHAR(100),
        execution_report_id VARCHAR(100),

        -- Arbitrage context
        arbitrage_type VARCHAR(50) NOT NULL,
        expected_profit DECIMAL(15,6),
        expected_profit_bps DECIMAL(10,2),
        leg_number INTEGER DEFAULT 1,
        related_trade_id VARCHAR(50),

        -- Market data correlation
        analyzed_snapshot_ids JSONB,
        executed_snapshot_id VARCHAR(50),
        kalshi_bid_at_decision DECIMAL(15,6),
        kalshi_ask_at_decision DECIMAL(15,6),
        polymarket_bid_at_decision DECIMAL(15,6),
        polymarket_ask_at_decision DECIMAL(15,6),
        spread_at_decision DECIMAL(15,6),

        -- Timing
        opportunity_detected_at TIMESTAMP WITH TIME ZONE,
        decision_made_at TIMESTAMP WITH TIME ZONE,
        order_submitted_at TIMESTAMP WITH TIME ZONE,
        first_fill_at TIMESTAMP WITH TIME ZONE,
        last_fill_at TIMESTAMP WITH TIME ZONE,
        order_completed_at TIMESTAMP WITH TIME ZONE,
        decision_latency_ms DECIMAL(10,3),
        submission_latency_ms DECIMAL(10,3),
        fill_latency_ms DECIMAL(10,3),

        -- Risk and position
        position_size_before DECIMAL(15,6) DEFAULT 0,
        position_size_after DECIMAL(15,6) DEFAULT 0,
        portfolio_exposure_before DECIMAL(15,6),
        portfolio_exposure_after DECIMAL(15,6),
        risk_score DECIMAL(10,4),

        -- Metadata
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        created_by VARCHAR(50) DEFAULT 'arbitrage_system',
        notes TEXT
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_trade_tickets_arbitrage_id ON trade_tickets(arbitrage_id);
    CREATE INDEX IF NOT EXISTS idx_trade_tickets_pair_id ON trade_tickets(pair_id);
    CREATE INDEX IF NOT EXISTS idx_trade_tickets_status ON trade_tickets(status);
    CREATE INDEX IF NOT EXISTS idx_trade_tickets_created_at ON trade_tickets(created_at);
    CREATE INDEX IF NOT EXISTS idx_trade_tickets_executed_snapshot ON trade_tickets(executed_snapshot_id);
    """


def get_analysis_record_schema() -> str:
    """Generate PostgreSQL schema for AnalysisRecord table"""
    return """
    CREATE TABLE IF NOT EXISTS analysis_records (
        analysis_id VARCHAR(50) PRIMARY KEY,

        -- Market context
        kalshi_ticker VARCHAR(100) NOT NULL,
        polymarket_condition_id VARCHAR(100) NOT NULL,
        pair_id VARCHAR(50) NOT NULL,

        -- Analysis details
        arbitrage_type VARCHAR(50) NOT NULL,
        potential_profit DECIMAL(15,6),
        potential_profit_bps DECIMAL(10,2),
        rejection_reason VARCHAR(100) NOT NULL,
        rejection_details TEXT,

        -- Market data correlation
        analyzed_snapshot_ids JSONB,
        kalshi_bid DECIMAL(15,6),
        kalshi_ask DECIMAL(15,6),
        kalshi_bid_size DECIMAL(15,6),
        kalshi_ask_size DECIMAL(15,6),
        polymarket_bid DECIMAL(15,6),
        polymarket_ask DECIMAL(15,6),
        polymarket_bid_size DECIMAL(15,6),
        polymarket_ask_size DECIMAL(15,6),
        cross_market_spread DECIMAL(15,6),
        required_minimum_spread DECIMAL(15,6),

        -- Timing
        analyzed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        analysis_duration_ms DECIMAL(10,3),

        -- Metadata
        created_by VARCHAR(50) DEFAULT 'arbitrage_system',
        notes TEXT
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_analysis_records_pair_id ON analysis_records(pair_id);
    CREATE INDEX IF NOT EXISTS idx_analysis_records_analyzed_at ON analysis_records(analyzed_at);
    CREATE INDEX IF NOT EXISTS idx_analysis_records_rejection_reason ON analysis_records(rejection_reason);
    """


def get_arbitrage_pair_schema() -> str:
    """Generate PostgreSQL schema for ArbitragePair table"""
    return """
    CREATE TABLE IF NOT EXISTS arbitrage_pairs (
        pair_id VARCHAR(50) PRIMARY KEY,

        -- Market mapping
        kalshi_ticker VARCHAR(100) NOT NULL,
        polymarket_condition_id VARCHAR(100) NOT NULL,
        market_title TEXT NOT NULL,
        description TEXT,

        -- Trading parameters
        is_active BOOLEAN DEFAULT TRUE,
        min_spread_bps DECIMAL(10,2) DEFAULT 100.0,
        max_position_size DECIMAL(15,6) DEFAULT 1000.0,
        max_trade_size DECIMAL(15,6) DEFAULT 100.0,
        max_exposure DECIMAL(15,6),
        stop_loss_bps DECIMAL(10,2),

        -- Performance tracking
        total_trades INTEGER DEFAULT 0,
        total_volume DECIMAL(15,6) DEFAULT 0,
        total_profit DECIMAL(15,6) DEFAULT 0,
        win_rate DECIMAL(5,4) DEFAULT 0,
        average_profit_per_trade DECIMAL(15,6) DEFAULT 0,

        -- Metadata
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        expiration_date TIMESTAMP WITH TIME ZONE,

        UNIQUE(kalshi_ticker, polymarket_condition_id)
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_arbitrage_pairs_kalshi_ticker ON arbitrage_pairs(kalshi_ticker);
    CREATE INDEX IF NOT EXISTS idx_arbitrage_pairs_polymarket_condition ON arbitrage_pairs(polymarket_condition_id);
    CREATE INDEX IF NOT EXISTS idx_arbitrage_pairs_is_active ON arbitrage_pairs(is_active);
    """


# Demo and testing
if __name__ == "__main__":
    print("TESTING POSTGRESQL MODELS")
    print("=" * 50)

    # Test TradeTicket
    trade = TradeTicket(
        kalshi_ticker="KXPRES-25-MM",
        polymarket_condition_id="0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",
        pair_id="irish_election_pair_1",
        market_title="Irish Presidential Election - Mairead McGuinness",
        venue=TradeVenue.KALSHI,
        side=TradeSide.BUY,
        outcome="YES",
        quantity=100.0,
        arbitrage_type=ArbitrageType.KALSHI_YES_POLYMARKET_NO,
        expected_profit=15.50,
        expected_profit_bps=155
    )

    # Add analyzed snapshots
    trade.add_analyzed_snapshot("snapshot_123")
    trade.add_analyzed_snapshot("snapshot_124")
    trade.set_executed_snapshot("snapshot_124")

    # Mark as submitted
    trade.mark_as_submitted("kalshi_order_456")

    # Add fill
    trade.add_fill(quantity=100.0, price=0.65)

    print(f"Trade Ticket:")
    print(f"  Trade ID: {trade.trade_id}")
    print(f"  Status: {trade.status.value}")
    print(f"  Analyzed Snapshots: {len(trade.analyzed_snapshot_ids)}")
    print(f"  Executed Snapshot: {trade.executed_snapshot_id}")
    print(f"  Average Fill Price: {trade.average_fill_price}")
    print(f"  Total Cost: {trade.total_cost}")

    # Test AnalysisRecord
    analysis = AnalysisRecord(
        kalshi_ticker="KXPRES-25-MM",
        polymarket_condition_id="0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",
        pair_id="irish_election_pair_1",
        arbitrage_type=ArbitrageType.KALSHI_YES_POLYMARKET_NO,
        potential_profit=8.25,
        potential_profit_bps=82,
        rejection_reason="spread_too_small",
        rejection_details="Required 100 bps, only 82 bps available"
    )

    analysis.add_analyzed_snapshot("snapshot_125")
    analysis.add_analyzed_snapshot("snapshot_126")

    print(f"\nAnalysis Record:")
    print(f"  Analysis ID: {analysis.analysis_id}")
    print(f"  Rejection Reason: {analysis.rejection_reason}")
    print(f"  Potential Profit: {analysis.potential_profit} ({analysis.potential_profit_bps} bps)")
    print(f"  Analyzed Snapshots: {len(analysis.analyzed_snapshot_ids)}")

    # Test ArbitragePair
    pair = ArbitragePair(
        kalshi_ticker="KXPRES-25-MM",
        polymarket_condition_id="0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",
        market_title="Irish Presidential Election - Mairead McGuinness",
        min_spread_bps=100.0,
        max_position_size=1000.0,
        max_trade_size=100.0
    )

    # Update performance
    pair.update_performance(trade_profit=15.50, trade_volume=100.0)

    print(f"\nArbitrage Pair:")
    print(f"  Pair ID: {pair.pair_id}")
    print(f"  Total Trades: {pair.total_trades}")
    print(f"  Total Profit: {pair.total_profit}")
    print(f"  Average Profit per Trade: {pair.average_profit_per_trade}")

    # Test dictionary conversion
    trade_dict = trade.to_dict()
    print(f"\nDictionary Conversion Test:")
    print(f"  Trade dict keys: {len(trade_dict)}")
    print(f"  Status in dict: {trade_dict['status']}")
    print(f"  Analyzed snapshots in dict: {trade_dict['analyzed_snapshot_ids']}")

    print("\n✅ PostgreSQL models test completed!")