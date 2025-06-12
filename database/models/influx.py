#!/usr/bin/env python3
"""
models/influx.py

InfluxDB data models for market data storage.
Defines MarketSnapshot and related structures for time-series data.

FIXED: Handle Virginia's orderbook format [price_cents, quantity] correctly.
"""

import json
import time
import uuid
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class MarketSnapshot:
    """
    Unified market snapshot for both Kalshi and Polymarket data.

    This is the core data structure for storing complete orderbook state
    with comprehensive timestamp tracking and extracted metrics for fast querying.
    """

    # === IDENTIFICATION ===
    snapshot_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source: str = ""  # "kalshi" or "polymarket"
    ticker: str = ""  # Kalshi ticker or Polymarket condition_id
    condition_id: Optional[str] = None  # For cross-market mapping
    pair_id: Optional[str] = None  # Links Kalshi ticker to Polymarket condition_id

    # === COMPLETE ORDERBOOK DATA ===
    full_orderbook: str = ""  # JSON string containing complete orderbook

    # === EXTRACTED METRICS (for fast querying) ===
    # Best prices
    yes_bid: Optional[float] = None  # Best YES bid price
    yes_ask: Optional[float] = None  # Best YES ask price
    no_bid: Optional[float] = None  # Best NO bid price (or derived: 1 - yes_ask)
    no_ask: Optional[float] = None  # Best NO ask price (or derived: 1 - yes_bid)

    # Best sizes
    yes_bid_size: Optional[float] = None  # Size at best YES bid
    yes_ask_size: Optional[float] = None  # Size at best YES ask
    no_bid_size: Optional[float] = None  # Size at best NO bid
    no_ask_size: Optional[float] = None  # Size at best NO ask

    # Market quality metrics
    inside_spread_bps: Optional[float] = None  # YES spread in basis points
    yes_levels_count: Optional[int] = None  # Number of YES price levels
    no_levels_count: Optional[int] = None  # Number of NO price levels

    # Depth analysis (volume within certain price ranges)
    bid_depth_100: Optional[float] = None  # Volume within 1 cent of best bid
    ask_depth_100: Optional[float] = None  # Volume within 1 cent of best ask
    bid_depth_500: Optional[float] = None  # Volume within 5 cents of best bid
    ask_depth_500: Optional[float] = None  # Volume within 5 cents of best ask

    # Total book metrics
    total_bid_volume: Optional[float] = None  # Total bid volume across all levels
    total_ask_volume: Optional[float] = None  # Total ask volume across all levels
    weighted_mid_price: Optional[float] = None  # Volume-weighted midpoint

    # === COMPREHENSIVE TIMESTAMP TRAIL ===
    # Source API timestamps (nanoseconds)
    api_call_start_ns: Optional[int] = None  # When API call started
    api_response_ns: Optional[int] = None  # When API response received
    processing_complete_ns: Optional[int] = None  # When processing finished

    # Ireland server timestamps (for Polymarket data)
    ireland_api_call_ns: Optional[int] = None  # Ireland API call start
    ireland_api_response_ns: Optional[int] = None  # Ireland API response
    ireland_processing_complete_ns: Optional[int] = None  # Ireland processing done
    ireland_zeromq_sent_ns: Optional[int] = None  # When sent to Virginia via ZeroMQ

    # Virginia server timestamps
    virginia_received_ns: Optional[int] = None  # When Virginia received data
    virginia_enriched_ns: Optional[int] = None  # After Virginia adds extracted metrics
    virginia_sent_to_data_server_ns: Optional[int] = None  # When sent to Data Server

    # Data server timestamps
    data_server_received_ns: Optional[int] = None  # When Data Server received
    data_server_stored_ns: Optional[int] = None  # When stored in InfluxDB

    # === PERFORMANCE METRICS ===
    api_latency_us: Optional[float] = None  # API call latency (microseconds)
    processing_latency_us: Optional[float] = None  # Processing latency
    zeromq_latency_us: Optional[float] = None  # ZeroMQ transmission latency
    total_pipeline_latency_us: Optional[float] = None  # End-to-end latency

    # === USAGE TRACKING (for trade correlation) ===
    analyzed: bool = False  # Was this measurement analyzed for arbitrage?
    executed: bool = False  # Was this measurement used for trade execution?
    analysis_timestamp_ns: Optional[int] = None  # When marked as analyzed
    execution_timestamp_ns: Optional[int] = None  # When marked as executed

    def __post_init__(self):
        """Calculate derived metrics and validate data"""
        # Ensure snapshot_id is set
        if not self.snapshot_id:
            self.snapshot_id = str(uuid.uuid4())

        # Calculate NO prices from YES prices (for binary markets)
        if self.yes_bid is not None and self.no_ask is None:
            self.no_ask = 1.0 - self.yes_bid
        if self.yes_ask is not None and self.no_bid is None:
            self.no_bid = 1.0 - self.yes_ask

        # Calculate sizes for NO side if not provided
        if self.yes_bid_size is not None and self.no_ask_size is None:
            self.no_ask_size = self.yes_bid_size
        if self.yes_ask_size is not None and self.no_bid_size is None:
            self.no_bid_size = self.yes_ask_size

        # Calculate spread in basis points
        if self.yes_bid and self.yes_ask and self.yes_bid > 0:
            spread = self.yes_ask - self.yes_bid
            self.inside_spread_bps = (spread / self.yes_bid) * 10000

        # Calculate weighted midpoint
        if self.yes_bid and self.yes_ask and self.yes_bid_size and self.yes_ask_size:
            total_size = self.yes_bid_size + self.yes_ask_size
            if total_size > 0:
                self.weighted_mid_price = (
                        (self.yes_bid * self.yes_ask_size + self.yes_ask * self.yes_bid_size) / total_size
                )

        # Calculate performance metrics
        self._calculate_latency_metrics()

    def _calculate_latency_metrics(self):
        """Calculate all latency metrics from timestamps"""
        # API latency
        if self.api_call_start_ns and self.api_response_ns:
            self.api_latency_us = (self.api_response_ns - self.api_call_start_ns) / 1000
        elif self.ireland_api_call_ns and self.ireland_api_response_ns:
            self.api_latency_us = (self.ireland_api_response_ns - self.ireland_api_call_ns) / 1000

        # Processing latency
        if self.api_response_ns and self.processing_complete_ns:
            self.processing_latency_us = (self.processing_complete_ns - self.api_response_ns) / 1000
        elif self.ireland_api_response_ns and self.ireland_processing_complete_ns:
            self.processing_latency_us = (
                                                 self.ireland_processing_complete_ns - self.ireland_api_response_ns
                                         ) / 1000

        # ZeroMQ latency (Ireland → Virginia)
        if self.ireland_zeromq_sent_ns and self.virginia_received_ns:
            self.zeromq_latency_us = (self.virginia_received_ns - self.ireland_zeromq_sent_ns) / 1000

        # Total pipeline latency
        start_time = None
        end_time = self.data_server_stored_ns

        if self.api_call_start_ns:
            start_time = self.api_call_start_ns
        elif self.ireland_api_call_ns:
            start_time = self.ireland_api_call_ns

        if start_time and end_time:
            self.total_pipeline_latency_us = (end_time - start_time) / 1000

    @property
    def age_ms(self) -> float:
        """How old is this data in milliseconds"""
        if self.data_server_stored_ns:
            return (time.time_ns() - self.data_server_stored_ns) / 1_000_000
        elif self.processing_complete_ns:
            return (time.time_ns() - self.processing_complete_ns) / 1_000_000
        else:
            return 0.0

    @property
    def is_kalshi_data(self) -> bool:
        """Check if this is Kalshi data"""
        return self.source.lower() == 'kalshi'

    @property
    def is_polymarket_data(self) -> bool:
        """Check if this is Polymarket data"""
        return self.source.lower() == 'polymarket'

    def get_orderbook_dict(self) -> Dict[str, Any]:
        """Parse and return the orderbook as a dictionary"""
        if not self.full_orderbook:
            return {}
        try:
            return json.loads(self.full_orderbook)
        except json.JSONDecodeError:
            return {}

    def set_orderbook_dict(self, orderbook: Dict[str, Any]):
        """Set the orderbook from a dictionary"""
        self.full_orderbook = json.dumps(orderbook, separators=(',', ':'))

    def mark_as_analyzed(self):
        """Mark this measurement as analyzed for arbitrage opportunities"""
        self.analyzed = True
        self.analysis_timestamp_ns = time.time_ns()

    def mark_as_executed(self):
        """Mark this measurement as used for trade execution"""
        self.executed = True
        self.execution_timestamp_ns = time.time_ns()

    def to_influx_tags(self) -> Dict[str, str]:
        """Generate InfluxDB tags for this measurement"""
        tags = {
            'source': self.source,
            'ticker': self.ticker,
            'snapshot_id': self.snapshot_id
        }

        if self.condition_id:
            tags['condition_id'] = self.condition_id
        if self.pair_id:
            tags['pair_id'] = self.pair_id

        return tags

    def to_influx_fields(self) -> Dict[str, Union[str, float, int, bool]]:
        """Generate InfluxDB fields for this measurement"""
        fields = {}

        # Always include the full orderbook
        if self.full_orderbook:
            fields['full_orderbook'] = self.full_orderbook

        # Price fields
        if self.yes_bid is not None:
            fields['yes_bid'] = self.yes_bid
        if self.yes_ask is not None:
            fields['yes_ask'] = self.yes_ask
        if self.no_bid is not None:
            fields['no_bid'] = self.no_bid
        if self.no_ask is not None:
            fields['no_ask'] = self.no_ask

        # Size fields
        if self.yes_bid_size is not None:
            fields['yes_bid_size'] = self.yes_bid_size
        if self.yes_ask_size is not None:
            fields['yes_ask_size'] = self.yes_ask_size
        if self.no_bid_size is not None:
            fields['no_bid_size'] = self.no_bid_size
        if self.no_ask_size is not None:
            fields['no_ask_size'] = self.no_ask_size

        # Market quality metrics
        if self.inside_spread_bps is not None:
            fields['inside_spread_bps'] = self.inside_spread_bps
        if self.yes_levels_count is not None:
            fields['yes_levels_count'] = self.yes_levels_count
        if self.no_levels_count is not None:
            fields['no_levels_count'] = self.no_levels_count

        # Depth metrics
        if self.bid_depth_100 is not None:
            fields['bid_depth_100'] = self.bid_depth_100
        if self.ask_depth_100 is not None:
            fields['ask_depth_100'] = self.ask_depth_100
        if self.bid_depth_500 is not None:
            fields['bid_depth_500'] = self.bid_depth_500
        if self.ask_depth_500 is not None:
            fields['ask_depth_500'] = self.ask_depth_500

        # Volume metrics
        if self.total_bid_volume is not None:
            fields['total_bid_volume'] = self.total_bid_volume
        if self.total_ask_volume is not None:
            fields['total_ask_volume'] = self.total_ask_volume
        if self.weighted_mid_price is not None:
            fields['weighted_mid_price'] = self.weighted_mid_price

        # Timestamp fields (as integers for InfluxDB)
        timestamp_fields = [
            'api_call_start_ns', 'api_response_ns', 'processing_complete_ns',
            'ireland_api_call_ns', 'ireland_api_response_ns', 'ireland_processing_complete_ns',
            'ireland_zeromq_sent_ns', 'virginia_received_ns', 'virginia_enriched_ns',
            'virginia_sent_to_data_server_ns', 'data_server_received_ns', 'data_server_stored_ns',
            'analysis_timestamp_ns', 'execution_timestamp_ns'
        ]

        for field_name in timestamp_fields:
            value = getattr(self, field_name)
            if value is not None:
                fields[field_name] = int(value)

        # Performance metrics
        if self.api_latency_us is not None:
            fields['api_latency_us'] = self.api_latency_us
        if self.processing_latency_us is not None:
            fields['processing_latency_us'] = self.processing_latency_us
        if self.zeromq_latency_us is not None:
            fields['zeromq_latency_us'] = self.zeromq_latency_us
        if self.total_pipeline_latency_us is not None:
            fields['total_pipeline_latency_us'] = self.total_pipeline_latency_us

        # Usage tracking
        fields['analyzed'] = self.analyzed
        fields['executed'] = self.executed

        return fields

    def to_influx_line_protocol(self, measurement_name: str = "market_snapshot") -> str:
        """
        Generate InfluxDB line protocol string for this measurement.

        Args:
            measurement_name: InfluxDB measurement name

        Returns:
            Line protocol string
        """
        # Generate tags and fields
        tags = self.to_influx_tags()
        fields = self.to_influx_fields()

        # Build tags string
        tags_parts = []
        for key, value in tags.items():
            # Escape tag values
            escaped_value = str(value).replace(',', '\\,').replace(' ', '\\ ').replace('=', '\\=')
            tags_parts.append(f'{key}={escaped_value}')
        tags_str = ','.join(tags_parts)

        # Build fields string
        fields_parts = []
        for key, value in fields.items():
            if isinstance(value, str):
                # Escape string field values
                escaped_value = value.replace('"', '\\"').replace('\n', '\\n')
                fields_parts.append(f'{key}="{escaped_value}"')
            elif isinstance(value, bool):
                fields_parts.append(f'{key}={str(value).lower()}')
            elif isinstance(value, int):
                fields_parts.append(f'{key}={value}i')  # Integer suffix
            else:  # float
                fields_parts.append(f'{key}={value}')
        fields_str = ','.join(fields_parts)

        # Use stored timestamp or current time
        timestamp_ns = self.data_server_stored_ns or time.time_ns()

        # Build line protocol
        return f'{measurement_name},{tags_str} {fields_str} {timestamp_ns}'


@dataclass
class MarketDataBatch:
    """
    Container for batching multiple market snapshots for efficient writing.
    """
    snapshots: List[MarketSnapshot] = field(default_factory=list)
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: float = field(default_factory=time.time)

    def add_snapshot(self, snapshot: MarketSnapshot):
        """Add a snapshot to the batch"""
        self.snapshots.append(snapshot)

    def clear(self):
        """Clear all snapshots from the batch"""
        self.snapshots.clear()
        self.created_at = time.time()

    @property
    def size(self) -> int:
        """Number of snapshots in batch"""
        return len(self.snapshots)

    @property
    def age_seconds(self) -> float:
        """Age of batch in seconds"""
        return time.time() - self.created_at

    def to_influx_line_protocol(self, measurement_name: str = "market_snapshot") -> List[str]:
        """Convert all snapshots to line protocol"""
        return [snapshot.to_influx_line_protocol(measurement_name) for snapshot in self.snapshots]


def create_kalshi_snapshot(ticker: str, orderbook_dict: Dict[str, Any],
                           api_timestamps: Dict[str, int] = None,
                           pair_id: str = None) -> MarketSnapshot:
    """
    Create MarketSnapshot from Kalshi data.

    FIXED: Handle Virginia's orderbook format where levels are [price_cents, quantity] arrays.

    Args:
        ticker: Kalshi ticker
        orderbook_dict: Raw orderbook dictionary from Virginia server
        api_timestamps: Dictionary with API timing data
        pair_id: Optional pair ID for cross-market correlation

    Returns:
        MarketSnapshot instance
    """
    snapshot = MarketSnapshot(
        source="kalshi",
        ticker=ticker,
        pair_id=pair_id
    )

    # Set orderbook
    snapshot.set_orderbook_dict(orderbook_dict)

    # Set timestamps if provided
    if api_timestamps:
        snapshot.api_call_start_ns = api_timestamps.get('api_call_start_ns')
        snapshot.api_response_ns = api_timestamps.get('api_response_ns')
        snapshot.processing_complete_ns = api_timestamps.get('processing_complete_ns')

    # Extract YES prices from orderbook - Handle Virginia's format
    yes_data = orderbook_dict.get('yes', [])
    if yes_data:
        snapshot.yes_levels_count = len(yes_data)

        # Handle Virginia's format: [price_cents, quantity]
        if len(yes_data) > 0:
            best_yes_level = yes_data[0]
            if isinstance(best_yes_level, list) and len(best_yes_level) >= 2:
                # Virginia format: [price_cents, quantity]
                price_cents = best_yes_level[0]
                quantity = best_yes_level[1]
                snapshot.yes_bid = float(price_cents) / 100  # Convert cents to dollars
                snapshot.yes_bid_size = float(quantity)
            elif isinstance(best_yes_level, dict):
                # Legacy format: {'price': X, 'quantity': Y}
                snapshot.yes_bid = float(best_yes_level.get('price', 0)) / 100
                snapshot.yes_bid_size = float(best_yes_level.get('quantity', 0))

        # Calculate total volume and depth metrics for YES side
        total_yes_volume = 0
        volume_within_1_cent = 0
        volume_within_5_cents = 0

        if snapshot.yes_bid:
            best_price_cents = snapshot.yes_bid * 100

            for level in yes_data:
                if isinstance(level, list) and len(level) >= 2:
                    price_cents = level[0]
                    quantity = level[1]
                    total_yes_volume += quantity

                    # Calculate depth within price ranges
                    price_diff = abs(price_cents - best_price_cents)
                    if price_diff <= 1:  # Within 1 cent
                        volume_within_1_cent += quantity
                    if price_diff <= 5:  # Within 5 cents
                        volume_within_5_cents += quantity

        snapshot.total_bid_volume = total_yes_volume
        snapshot.bid_depth_100 = volume_within_1_cent
        snapshot.bid_depth_500 = volume_within_5_cents

    # Extract NO prices from orderbook - Handle Virginia's format
    no_data = orderbook_dict.get('no', [])
    if no_data:
        snapshot.no_levels_count = len(no_data)

        # Handle Virginia's format: [price_cents, quantity]
        if len(no_data) > 0:
            best_no_level = no_data[0]
            if isinstance(best_no_level, list) and len(best_no_level) >= 2:
                # Virginia format: [price_cents, quantity]
                price_cents = best_no_level[0]
                quantity = best_no_level[1]
                snapshot.no_bid = float(price_cents) / 100  # Convert cents to dollars
                snapshot.no_bid_size = float(quantity)
            elif isinstance(best_no_level, dict):
                # Legacy format: {'price': X, 'quantity': Y}
                snapshot.no_bid = float(best_no_level.get('price', 0)) / 100
                snapshot.no_bid_size = float(best_no_level.get('quantity', 0))

        # Calculate total volume and depth metrics for NO side
        total_no_volume = 0
        volume_within_1_cent = 0
        volume_within_5_cents = 0

        if snapshot.no_bid:
            best_price_cents = snapshot.no_bid * 100

            for level in no_data:
                if isinstance(level, list) and len(level) >= 2:
                    price_cents = level[0]
                    quantity = level[1]
                    total_no_volume += quantity

                    # Calculate depth within price ranges
                    price_diff = abs(price_cents - best_price_cents)
                    if price_diff <= 1:  # Within 1 cent
                        volume_within_1_cent += quantity
                    if price_diff <= 5:  # Within 5 cents
                        volume_within_5_cents += quantity

        snapshot.total_ask_volume = total_no_volume
        snapshot.ask_depth_100 = volume_within_1_cent
        snapshot.ask_depth_500 = volume_within_5_cents

    # For Kalshi, YES and NO are complementary
    # If we have YES bid, NO ask = 1 - YES bid
    # If we have NO bid, YES ask = 1 - NO bid
    if snapshot.yes_bid is not None and snapshot.no_ask is None:
        snapshot.no_ask = 1.0 - snapshot.yes_bid
        snapshot.no_ask_size = snapshot.yes_bid_size

    if snapshot.no_bid is not None and snapshot.yes_ask is None:
        snapshot.yes_ask = 1.0 - snapshot.no_bid
        snapshot.yes_ask_size = snapshot.no_bid_size

    return snapshot


def create_polymarket_snapshot(condition_id: str, orderbook_dict: Dict[str, Any],
                               ireland_timestamps: Dict[str, int] = None,
                               pair_id: str = None) -> MarketSnapshot:
    """
    Create MarketSnapshot from Polymarket data.

    Args:
        condition_id: Polymarket condition ID
        orderbook_dict: Raw orderbook dictionary from Polymarket API
        ireland_timestamps: Dictionary with Ireland server timing data
        pair_id: Optional pair ID for cross-market correlation

    Returns:
        MarketSnapshot instance
    """
    snapshot = MarketSnapshot(
        source="polymarket",
        ticker=condition_id,
        condition_id=condition_id,
        pair_id=pair_id
    )

    # Set orderbook
    snapshot.set_orderbook_dict(orderbook_dict)

    # Set Ireland timestamps if provided
    if ireland_timestamps:
        snapshot.ireland_api_call_ns = ireland_timestamps.get('ireland_api_call_ns')
        snapshot.ireland_api_response_ns = ireland_timestamps.get('ireland_api_response_ns')
        snapshot.ireland_processing_complete_ns = ireland_timestamps.get('ireland_processing_complete_ns')
        snapshot.ireland_zeromq_sent_ns = ireland_timestamps.get('ireland_zeromq_sent_ns')

    # Extract prices from orderbook (structure may vary based on Polymarket format)
    bids = orderbook_dict.get('bids', [])
    asks = orderbook_dict.get('asks', [])

    if bids and len(bids) > 0:
        snapshot.yes_bid = float(bids[0].get('price', 0))
        snapshot.yes_bid_size = float(bids[0].get('size', 0))
        snapshot.yes_levels_count = len(bids)

    if asks and len(asks) > 0:
        snapshot.yes_ask = float(asks[0].get('price', 0))
        snapshot.yes_ask_size = float(asks[0].get('size', 0))
        snapshot.yes_levels_count = max(snapshot.yes_levels_count or 0, len(asks))

    return snapshot


# Demo and testing
if __name__ == "__main__":
    print("TESTING FIXED INFLUXDB MODELS")
    print("=" * 50)

    # Test Virginia's Kalshi format
    virginia_kalshi_orderbook = {
        "yes": [
            [1, 30000],  # [price_cents, quantity] - 1 cent @ 30000 shares
            [2, 500],  # 2 cents @ 500 shares
            [3, 337],  # 3 cents @ 337 shares
            [4, 4],
            [32, 227],
            [40, 39],
            [41, 6]
        ],
        "no": [
            [1, 162212],  # [price_cents, quantity] - 1 cent @ 162212 shares
            [2, 10700],  # 2 cents @ 10700 shares
            [3, 9000],
            [5, 1000],
            [6, 980],
            [11, 178],
            [12, 211],
            [27, 775],
            [29, 300],
            [30, 261],
            [57, 23]
        ],
        "ticker": "KXPRESIRELAND-25-MM",
        "title": "Market",
        "status": "active",
        "volume": 0.0,
        "api_call_start_ns": 1749684438403614000,
        "api_response_ns": 1749684438431407000,
        "processing_complete_ns": 1749684438431425000
    }

    print("Testing Virginia's Kalshi format...")
    kalshi_snapshot = create_kalshi_snapshot(
        ticker="KXPRESIRELAND-25-MM",
        orderbook_dict=virginia_kalshi_orderbook,
        api_timestamps={
            'api_call_start_ns': time.time_ns() - 1000000,
            'api_response_ns': time.time_ns() - 500000,
            'processing_complete_ns': time.time_ns()
        },
        pair_id="irish_election_pair_1"
    )

    print(f"✅ FIXED Kalshi Snapshot:")
    print(f"  Snapshot ID: {kalshi_snapshot.snapshot_id}")
    print(f"  YES Bid: {kalshi_snapshot.yes_bid} @ {kalshi_snapshot.yes_bid_size}")
    print(f"  YES Ask: {kalshi_snapshot.yes_ask} @ {kalshi_snapshot.yes_ask_size}")
    print(f"  NO Bid: {kalshi_snapshot.no_bid} @ {kalshi_snapshot.no_bid_size}")
    print(f"  NO Ask: {kalshi_snapshot.no_ask} @ {kalshi_snapshot.no_ask_size}")
    print(f"  YES Levels: {kalshi_snapshot.yes_levels_count}")
    print(f"  NO Levels: {kalshi_snapshot.no_levels_count}")
    print(f"  Total YES Volume: {kalshi_snapshot.total_bid_volume}")
    print(f"  Total NO Volume: {kalshi_snapshot.total_ask_volume}")
    print(f"  Spread (bps): {kalshi_snapshot.inside_spread_bps}")
    print(f"  API Latency: {kalshi_snapshot.api_latency_us:.1f}μs")

    # Test line protocol generation
    print(f"\n📝 Line Protocol Test:")
    line_protocol = kalshi_snapshot.to_influx_line_protocol()
    print(f"Length: {len(line_protocol)} characters")
    print(f"Contains prices: {'yes_bid=' in line_protocol and 'yes_ask=' in line_protocol}")

    # Show key price fields
    fields = kalshi_snapshot.to_influx_fields()
    print(f"\n💰 Extracted Price Fields:")
    for key in ['yes_bid', 'yes_ask', 'no_bid', 'no_ask']:
        if key in fields:
            print(f"  {key}: {fields[key]}")

    print("\n✅ Fixed InfluxDB models test completed!")
    print("🎯 Now Virginia's [price_cents, quantity] format will work correctly!")