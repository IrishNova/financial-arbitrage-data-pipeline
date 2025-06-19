#!/usr/bin/env python3
"""
coordinator.py

Enhanced central async coordinator for the arbitrage trading system.
Manages data flows, API calls, and system orchestration with DATA BIFURCATION.
Uses IntervalLogger for high-frequency data to reduce log noise.
Includes Data Flow Monitor for visibility into message flow and structure.

Bifurcates incoming data to both:
- Trading lane: opportunity scanner for arbitrage detection
- Storage lane: database server for historical storage and analysis
"""

import sys
import asyncio
import time
from pathlib import Path
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import after path setup
from utils.logger import get_coordinator_logger, get_coordinator_interval_logger
from utils.config import CONFIG
from database.read_from import DatabaseReader, ArbPair
from api.data_feed import KalshiDataFeed, KalshiMarketData
from broker.publisher import PublisherManager
from broker.receiver import ReceiverManager, PolymarketData, TradeFill, SystemMetrics, ErrorAlert
from broker.database_publisher import DatabasePublisher  # NEW: Database bifurcation
from logic.opportunity_scanner import OpportunityScanner


@dataclass
class SystemHealth:
    """System health status tracking"""
    database_connected: bool
    kalshi_api_healthy: bool
    publisher_healthy: bool
    receiver_healthy: bool
    scanner_healthy: bool
    database_publisher_healthy: bool  # NEW: Database publisher health
    last_ticker_refresh: float
    last_kalshi_data: float
    last_polymarket_data: float
    active_pairs_count: int
    errors_last_hour: int
    uptime_seconds: float


@dataclass
class CoordinatorMetrics:
    """Performance and operational metrics"""
    total_api_calls: int
    api_calls_per_minute: float
    average_response_time: float
    ticker_refresh_count: int
    data_points_processed: int
    polymarket_messages_received: int
    trade_fills_received: int
    # NEW: Bifurcation metrics
    kalshi_snapshots_bifurcated: int
    polymarket_snapshots_bifurcated: int
    database_sends_successful: int
    database_sends_failed: int
    last_reset_time: float


class ArbitrageCoordinator:
    """
    Enhanced central coordinator with DATA BIFURCATION, INTERVAL LOGGING, and DATA FLOW MONITORING.

    This coordinator now splits incoming data into two lanes:
    1. Trading Lane: Real-time arbitrage detection and execution
    2. Storage Lane: Historical data storage for analysis and compliance

    NEW: Uses IntervalLogger to reduce noise from high-frequency data while
    still providing detailed samples and summaries.

    NEW: Data Flow Monitor shows real-time status and message samples every 30 seconds.

    Manages:
    - Ticker refresh from database
    - Kalshi API data feeds with bifurcation
    - ZeroMQ communication with Ireland server
    - Data distribution to opportunity scanner AND database server
    - Health monitoring and error recovery
    - Comprehensive timestamp tracking
    - Data flow visibility and monitoring
    """

    def __init__(self):
        """Initialize the enhanced arbitrage coordinator with interval logging and data flow monitoring"""
        # Regular logger for system events
        self.logger = get_coordinator_logger()

        # NEW: Interval logger for high-frequency data
        self.interval_logger = get_coordinator_interval_logger()

        # Core components
        self.database_reader: Optional[DatabaseReader] = None
        self.kalshi_feed: Optional[KalshiDataFeed] = None
        self.publisher_manager: Optional[PublisherManager] = None
        self.receiver_manager: Optional[ReceiverManager] = None
        self.opportunity_scanner: Optional[OpportunityScanner] = None
        self.database_publisher: Optional[DatabasePublisher] = None  # NEW: Database bifurcation

        # State tracking
        self.active_pairs: Dict[int, ArbPair] = {}
        self.kalshi_tickers: Set[str] = set()
        self.polymarket_condition_ids: Set[str] = set()

        # NEW: Data flow monitoring - store last samples
        self._last_kalshi_sample: Optional[Dict[str, Any]] = None
        self._last_polymarket_sample: Optional[Dict[str, Any]] = None

        # Timing and control
        self.start_time = time.time()
        self.last_ticker_refresh = 0
        self.last_health_check = 0
        self.is_running = False
        self.shutdown_requested = False

        # Enhanced health and metrics
        self.health_status = SystemHealth(
            database_connected=False,
            kalshi_api_healthy=False,
            publisher_healthy=False,
            receiver_healthy=False,
            scanner_healthy=False,
            database_publisher_healthy=False,  # NEW
            last_ticker_refresh=0,
            last_kalshi_data=0,
            last_polymarket_data=0,
            active_pairs_count=0,
            errors_last_hour=0,
            uptime_seconds=0
        )

        self.metrics = CoordinatorMetrics(
            total_api_calls=0,
            api_calls_per_minute=0.0,
            average_response_time=0.0,
            ticker_refresh_count=0,
            data_points_processed=0,
            polymarket_messages_received=0,
            trade_fills_received=0,
            kalshi_snapshots_bifurcated=0,  # NEW
            polymarket_snapshots_bifurcated=0,  # NEW
            database_sends_successful=0,  # NEW
            database_sends_failed=0,  # NEW
            last_reset_time=time.time()
        )

        # Task references for cleanup
        self.running_tasks: List[asyncio.Task] = []

        self.logger.info(
            "Enhanced arbitrage coordinator initialized with data bifurcation, interval logging, and data flow monitoring")
        self.logger.info(f"Sample interval: {getattr(CONFIG, 'log_sample_interval', 100)} messages")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def _initialize_components(self) -> bool:
        """
        Initialize all system components including database publisher.

        Returns:
            True if all components initialized successfully
        """
        try:
            self.logger.info("Initializing enhanced system components...")

            # Initialize database reader
            self.logger.debug("Initializing database connection...")
            self.database_reader = DatabaseReader()
            if not self.database_reader.connect():
                self.logger.error("Failed to connect to database")
                return False

            self.health_status.database_connected = True
            self.logger.info("Database connection established")

            # Initialize Kalshi data feed
            self.logger.debug("Initializing Kalshi data feed...")
            self.kalshi_feed = KalshiDataFeed(
                market_data_interval=CONFIG.kalshi.min_delay
            )
            await self.kalshi_feed.__aenter__()

            # Add callback to receive Kalshi data (enhanced for bifurcation)
            self.kalshi_feed.add_data_callback(self._handle_kalshi_data)

            self.health_status.kalshi_api_healthy = True
            self.logger.info("Kalshi data feed initialized")

            # Initialize ZeroMQ publisher manager (Ireland communication)
            self.logger.debug("Initializing ZeroMQ publisher manager...")
            self.publisher_manager = PublisherManager()
            await self.publisher_manager.start()

            self.health_status.publisher_healthy = True
            self.logger.info("ZeroMQ publisher manager initialized")

            # Initialize ZeroMQ receiver manager (Ireland communication)
            self.logger.debug("Initializing ZeroMQ receiver manager...")
            self.receiver_manager = ReceiverManager()
            await self.receiver_manager.start()

            # Add callbacks to receive data from Ireland (enhanced for bifurcation)
            self.receiver_manager.add_marketdata_callback(self._handle_polymarket_data)
            self.receiver_manager.add_fills_callback(self._handle_trade_fills)
            self.receiver_manager.add_metrics_callback(self._handle_system_metrics)
            self.receiver_manager.add_error_callback(self._handle_error_alerts)

            self.health_status.receiver_healthy = True
            self.logger.info("ZeroMQ receiver manager initialized")

            # NEW: Initialize database publisher (Storage lane)
            self.logger.debug("Initializing database publisher...")
            self.database_publisher = DatabasePublisher()
            await self.database_publisher.start()

            self.health_status.database_publisher_healthy = True
            self.logger.info("Database publisher initialized for data bifurcation")

            # Initialize opportunity scanner
            self.logger.debug("Initializing opportunity scanner...")
            self.opportunity_scanner = OpportunityScanner()

            self.health_status.scanner_healthy = True
            self.logger.info("Opportunity scanner initialized")

            self.logger.info("All enhanced system components initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize enhanced components: {e}")
            return False

    async def _handle_kalshi_data(self, market_data: Dict[str, dict]):
        """
        Enhanced Kalshi data handler with BIFURCATION, INTERVAL LOGGING, and SAMPLE STORAGE.

        Splits data to both:
        1. Trading lane: opportunity scanner
        2. Storage lane: database server

        Uses IntervalLogger to reduce noise while providing detailed samples.
        Stores samples for data flow monitoring.

        Args:
            market_data: Dictionary of ticker -> raw orderbook dict
        """
        if not market_data:
            self.interval_logger.warning('kalshi_data', "Received empty Kalshi market data")
            return

        # Virginia timestamp: when data received
        virginia_received_ns = time.time_ns()

        # NEW: Store sample for data flow monitoring
        try:
            first_ticker = list(market_data.keys())[0]
            first_raw_data = market_data[first_ticker]
            # Create sample snapshot
            sample_snapshot = self._create_kalshi_snapshot(first_ticker, first_raw_data, virginia_received_ns)
            self._last_kalshi_sample = sample_snapshot
        except Exception as e:
            self.logger.error(f"Error storing Kalshi sample: {e}")

        # Use interval logger for high-frequency data
        self.interval_logger.debug(
            'kalshi_data',
            f"Received Kalshi data for {len(market_data)} markets",
            {
                'market_count': len(market_data),
                'tickers': list(market_data.keys()),
                'sample_data': {k: v for k, v in list(market_data.items())[:2]},  # Show first 2 markets
                'virginia_received_ns': virginia_received_ns
            }
        )

        try:
            # Convert raw orderbook data to enhanced format with Virginia timestamps
            enhanced_snapshots = []

            for ticker, raw_data in market_data.items():
                try:
                    # Create enhanced snapshot with comprehensive timestamps
                    snapshot = self._create_kalshi_snapshot(ticker, raw_data, virginia_received_ns)
                    enhanced_snapshots.append(snapshot)

                except Exception as e:
                    # Use regular logger for errors (always shows)
                    self.logger.error(f"Error processing Kalshi {ticker} data: {e}")
                    continue

            # Virginia timestamp: after enrichment processing
            virginia_enriched_ns = time.time_ns()

            # Add enrichment timestamp to all snapshots
            for snapshot in enhanced_snapshots:
                snapshot['virginia_enriched_ns'] = virginia_enriched_ns

            if enhanced_snapshots:
                # BIFURCATION: Split data to both lanes
                await self._bifurcate_kalshi_data(market_data, enhanced_snapshots)

                # Update metrics
                self.metrics.total_api_calls += 1
                self.metrics.data_points_processed += len(market_data)
                self.metrics.kalshi_snapshots_bifurcated += len(enhanced_snapshots)
                self.health_status.last_kalshi_data = time.time()

                # Use interval logger for success summary
                self.interval_logger.debug(
                    'kalshi_bifurcation',
                    f"Successfully bifurcated {len(enhanced_snapshots)} Kalshi snapshots",
                    {
                        'snapshots_count': len(enhanced_snapshots),
                        'processing_time_ns': virginia_enriched_ns - virginia_received_ns,
                        'total_bifurcated': self.metrics.kalshi_snapshots_bifurcated
                    }
                )
            else:
                self.logger.warning("No valid Kalshi snapshots to bifurcate")

        except Exception as e:
            self.logger.error(f"Error in enhanced Kalshi data handler: {e}")

    async def _handle_polymarket_data(self, market_data: Dict[str, PolymarketData]):
        """
        Enhanced Polymarket data handler with BIFURCATION, INTERVAL LOGGING, and SAMPLE STORAGE.

        Splits data to both:
        1. Trading lane: opportunity scanner
        2. Storage lane: database server

        Uses IntervalLogger to reduce noise while providing detailed samples.
        Stores samples for data flow monitoring.

        Args:
            market_data: Dictionary of condition_id -> PolymarketData
        """
        if not market_data:
            return

        # Virginia timestamp: when data received from Ireland
        virginia_received_ns = time.time_ns()

        # NEW: Store sample for data flow monitoring
        try:
            first_condition_id = list(market_data.keys())[0]
            first_polymarket_data = market_data[first_condition_id]
            # Create sample snapshot
            sample_snapshot = self._create_polymarket_snapshot(first_condition_id, first_polymarket_data,
                                                               virginia_received_ns)
            self._last_polymarket_sample = sample_snapshot
        except Exception as e:
            self.logger.error(f"Error storing Polymarket sample: {e}")

        # Use interval logger for high-frequency data
        self.interval_logger.debug(
            'polymarket_data',
            f"Received Polymarket data for {len(market_data)} condition IDs",
            {
                'condition_count': len(market_data),
                'condition_ids': [cid[:10] + "..." for cid in list(market_data.keys())[:3]],  # Show first 3
                'sample_data': {k[:10] + "...": v.__dict__ for k, v in list(market_data.items())[:2]},  # Show first 2
                'virginia_received_ns': virginia_received_ns
            }
        )

        try:
            # Convert PolymarketData to enhanced snapshots with Virginia timestamps
            enhanced_snapshots = []

            for condition_id, polymarket_data in market_data.items():
                try:
                    # Create enhanced snapshot with comprehensive timestamps
                    snapshot = self._create_polymarket_snapshot(condition_id, polymarket_data, virginia_received_ns)
                    enhanced_snapshots.append(snapshot)

                except Exception as e:
                    # Use regular logger for errors (always shows)
                    self.logger.error(f"Error processing Polymarket {condition_id[:10]}... data: {e}")
                    continue

            # Virginia timestamp: after enrichment processing
            virginia_enriched_ns = time.time_ns()

            # Add enrichment timestamp to all snapshots
            for snapshot in enhanced_snapshots:
                snapshot['virginia_enriched_ns'] = virginia_enriched_ns

            if enhanced_snapshots:
                # BIFURCATION: Split data to both lanes
                await self._bifurcate_polymarket_data(market_data, enhanced_snapshots)

                # Update metrics
                self.metrics.polymarket_messages_received += len(market_data)
                self.metrics.polymarket_snapshots_bifurcated += len(enhanced_snapshots)
                self.health_status.last_polymarket_data = time.time()

                # Use interval logger for success summary
                self.interval_logger.debug(
                    'polymarket_bifurcation',
                    f"Successfully bifurcated {len(enhanced_snapshots)} Polymarket snapshots",
                    {
                        'snapshots_count': len(enhanced_snapshots),
                        'processing_time_ns': virginia_enriched_ns - virginia_received_ns,
                        'total_bifurcated': self.metrics.polymarket_snapshots_bifurcated
                    }
                )
            else:
                self.logger.warning("No valid Polymarket snapshots to bifurcate")

        except Exception as e:
            self.logger.error(f"Error in enhanced Polymarket data handler: {e}")

    def _create_kalshi_snapshot(self, ticker: str, raw_data: dict, virginia_received_ns: int) -> Dict[str, Any]:
        """
        Create enhanced Kalshi snapshot with OPTIMIZED SCHEMA.

        Schema:
        - Tags: source=kalshi, ticker=KXPRESIRELAND-25-MM
        - Fields: full_orderbook + timing chain ONLY

        Args:
            ticker: Kalshi ticker
            raw_data: Raw orderbook data from Kalshi API
            virginia_received_ns: Virginia receive timestamp

        Returns:
            Enhanced snapshot dictionary with optimized schema
        """
        import json

        # Extract timing from raw data
        api_call_start_ns = raw_data.get('api_call_start_ns')
        api_response_ns = raw_data.get('api_response_ns')
        processing_complete_ns = time.time_ns()

        # Format orderbook as JSON string
        # Kalshi format: {"yes": [[price_cents, quantity], ...], "no": [[price_cents, quantity], ...]}
        full_orderbook = json.dumps(raw_data, separators=(',', ':'))

        return {
            # Schema identifiers (become tags in InfluxDB)
            'source': 'kalshi',
            'ticker': ticker,

            # Raw data (field)
            'full_orderbook': full_orderbook,

            # Timing chain (fields)
            'api_call_start_ns': api_call_start_ns,
            'api_response_ns': api_response_ns,
            'processing_complete_ns': processing_complete_ns,
            'virginia_received_ns': virginia_received_ns,
            'data_server_stored_ns': None  # Set later by data server
        }

    def _create_polymarket_snapshot(self, condition_id: str, polymarket_data: PolymarketData,
                                    virginia_received_ns: int) -> Dict[str, Any]:
        """
        Create enhanced Polymarket snapshot with OPTIMIZED SCHEMA.

        Schema:
        - Tags: source=polymarket, ticker=0x26d06d9c...
        - Fields: full_orderbook + timing chain ONLY

        Args:
            condition_id: Polymarket condition ID
            polymarket_data: PolymarketData object from Ireland
            virginia_received_ns: Virginia receive timestamp

        Returns:
            Enhanced snapshot dictionary with optimized schema
        """
        import json

        processing_complete_ns = time.time_ns()

        # Format Polymarket data as JSON string
        # Include all raw data from Ireland in structured format
        polymarket_raw = {
            'condition_id': condition_id,
            'yes_price': polymarket_data.yes_price,
            'no_price': polymarket_data.no_price,
            'volume': polymarket_data.volume,
            'liquidity': polymarket_data.liquidity
        }

        # Add any additional fields from PolymarketData
        for attr in dir(polymarket_data):
            if not attr.startswith('_') and attr not in ['yes_price', 'no_price', 'volume', 'liquidity']:
                try:
                    value = getattr(polymarket_data, attr)
                    if not callable(value):
                        polymarket_raw[attr] = value
                except:
                    pass

        full_orderbook = json.dumps(polymarket_raw, separators=(',', ':'))

        return {
            # Schema identifiers (become tags in InfluxDB)
            'source': 'polymarket',
            'ticker': condition_id,  # Use condition_id as ticker

            # Raw data (field)
            'full_orderbook': full_orderbook,

            # Ireland timing chain (fields)
            'ireland_api_call_ns': getattr(polymarket_data, 'ireland_api_call_ns', None),
            'ireland_api_response_ns': getattr(polymarket_data, 'ireland_api_response_ns', None),
            'ireland_processing_complete_ns': getattr(polymarket_data, 'ireland_processing_complete_ns', None),
            'ireland_zeromq_sent_ns': getattr(polymarket_data, 'ireland_zeromq_sent_ns', None),

            # Virginia timing chain (fields)
            'virginia_received_ns': virginia_received_ns,
            'processing_complete_ns': processing_complete_ns,
            'data_server_stored_ns': None  # Set later by data server
        }

    async def _bifurcate_kalshi_data(self, original_data: Dict[str, dict], enhanced_snapshots: List[Dict[str, Any]]):
        """
        Bifurcate Kalshi data to both trading and storage lanes.

        Args:
            original_data: Original Kalshi data for opportunity scanner
            enhanced_snapshots: Enhanced snapshots for database storage
        """
        try:
            # TRADING LANE: Send to opportunity scanner (existing format)
            if self.opportunity_scanner:
                await self.opportunity_scanner.receive_kalshi_data(original_data)
                # Use interval logger for trading lane activity
                self.interval_logger.debug(
                    'trading_lane',
                    f"Sent {len(original_data)} Kalshi markets to trading lane",
                    {'markets_sent': len(original_data), 'lane': 'trading'}
                )

            # STORAGE LANE: Send to database server (enhanced format)
            if self.database_publisher:
                success = await self.database_publisher.send_market_data("kalshi", enhanced_snapshots)
                if success:
                    self.metrics.database_sends_successful += 1
                    # Use interval logger for storage lane activity
                    self.interval_logger.debug(
                        'storage_lane',
                        f"Sent {len(enhanced_snapshots)} Kalshi snapshots to storage lane",
                        {'snapshots_sent': len(enhanced_snapshots), 'lane': 'storage', 'success': True}
                    )
                else:
                    self.metrics.database_sends_failed += 1
                    # Errors always show
                    self.logger.error(f"Failed to send {len(enhanced_snapshots)} Kalshi snapshots to storage lane")

        except Exception as e:
            self.logger.error(f"Error in Kalshi data bifurcation: {e}")
            self.metrics.database_sends_failed += 1

    async def _bifurcate_polymarket_data(self, original_data: Dict[str, PolymarketData],
                                         enhanced_snapshots: List[Dict[str, Any]]):
        """
        Bifurcate Polymarket data to both trading and storage lanes.

        Args:
            original_data: Original Polymarket data for opportunity scanner
            enhanced_snapshots: Enhanced snapshots for database storage
        """
        try:
            # TRADING LANE: Send to opportunity scanner (existing format)
            if self.opportunity_scanner:
                await self.opportunity_scanner.receive_polymarket_data(original_data)
                # Use interval logger for trading lane activity
                self.interval_logger.debug(
                    'trading_lane',
                    f"Sent {len(original_data)} Polymarket markets to trading lane",
                    {'markets_sent': len(original_data), 'lane': 'trading'}
                )

            # STORAGE LANE: Send to database server (enhanced format)
            if self.database_publisher:
                success = await self.database_publisher.send_market_data("polymarket", enhanced_snapshots)
                if success:
                    self.metrics.database_sends_successful += 1
                    # Use interval logger for storage lane activity
                    self.interval_logger.debug(
                        'storage_lane',
                        f"Sent {len(enhanced_snapshots)} Polymarket snapshots to storage lane",
                        {'snapshots_sent': len(enhanced_snapshots), 'lane': 'storage', 'success': True}
                    )
                else:
                    self.metrics.database_sends_failed += 1
                    # Errors always show
                    self.logger.error(f"Failed to send {len(enhanced_snapshots)} Polymarket snapshots to storage lane")

        except Exception as e:
            self.logger.error(f"Error in Polymarket data bifurcation: {e}")
            self.metrics.database_sends_failed += 1

    async def _data_flow_monitor_loop(self):
        """
        NEW: Monitor data flow and show samples.
        Shows status every 30 seconds with actual message samples.
        """
        self.logger.info("Starting data flow monitor - will show samples every 30 seconds")

        last_kalshi_count = 0
        last_polymarket_count = 0
        last_check_time = time.time()

        while not self.shutdown_requested:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds

                current_time = time.time()
                elapsed = current_time - last_check_time

                # Get current counts
                current_kalshi = self.metrics.kalshi_snapshots_bifurcated
                current_polymarket = self.metrics.polymarket_snapshots_bifurcated

                # Calculate flow rates
                kalshi_rate = (current_kalshi - last_kalshi_count) / elapsed if elapsed > 0 else 0
                polymarket_rate = (current_polymarket - last_polymarket_count) / elapsed if elapsed > 0 else 0

                # Data flow status
                kalshi_flowing = kalshi_rate > 0
                polymarket_flowing = polymarket_rate > 0

                self.logger.info("=" * 60)
                self.logger.info("DATA FLOW STATUS REPORT")
                self.logger.info("=" * 60)
                self.logger.info(f"Kalshi Data Flowing: {kalshi_flowing} ({kalshi_rate:.2f} snapshots/sec)")
                self.logger.info(f"Polymarket Data Flowing: {polymarket_flowing} ({polymarket_rate:.2f} snapshots/sec)")
                self.logger.info(f"Total Kalshi Snapshots: {current_kalshi}")
                self.logger.info(f"Total Polymarket Snapshots: {current_polymarket}")
                self.logger.info(f"Active Pairs: {len(self.active_pairs)}")
                self.logger.info(f"Kalshi Tickers: {list(self.kalshi_tickers)}")
                self.logger.info(
                    f"Polymarket IDs: {[cid[:10] + '...' for cid in list(self.polymarket_condition_ids)[:3]]}")

                # Store samples for display
                if hasattr(self, '_last_kalshi_sample') and self._last_kalshi_sample:
                    self.logger.info("\nLAST KALSHI SAMPLE:")
                    self._log_message_sample("KALSHI", self._last_kalshi_sample)
                else:
                    self.logger.info("\nKALSHI: No samples received yet")

                if hasattr(self, '_last_polymarket_sample') and self._last_polymarket_sample:
                    self.logger.info("\nLAST POLYMARKET SAMPLE:")
                    self._log_message_sample("POLYMARKET", self._last_polymarket_sample)
                else:
                    self.logger.info("\nPOLYMARKET: No samples received yet")

                self.logger.info("=" * 60)

                # Update for next iteration
                last_kalshi_count = current_kalshi
                last_polymarket_count = current_polymarket
                last_check_time = current_time

            except Exception as e:
                self.logger.error(f"Error in data flow monitor: {e}")
                await asyncio.sleep(30)

        self.logger.info("Data flow monitor stopped")

    def _log_message_sample(self, source: str, sample_data: dict):
        """NEW: Log a detailed sample of message data"""
        try:
            self.logger.info(f"--- {source} MESSAGE SAMPLE ---")

            if source == "KALSHI":
                # Show Kalshi specific structure
                self.logger.info(f"Ticker: {sample_data.get('ticker', 'MISSING')}")
                self.logger.info(f"Snapshot ID: {sample_data.get('snapshot_id', 'MISSING')}")
                self.logger.info(f"Yes Bid: {sample_data.get('yes_bid')}")
                self.logger.info(f"Yes Ask: {sample_data.get('yes_ask')}")
                self.logger.info(f"No Bid: {sample_data.get('no_bid')}")
                self.logger.info(f"No Ask: {sample_data.get('no_ask')}")
                self.logger.info(f"Yes Levels Count: {sample_data.get('yes_levels_count')}")
                self.logger.info(f"No Levels Count: {sample_data.get('no_levels_count')}")

                # Show orderbook structure
                full_orderbook = sample_data.get('full_orderbook', {})
                if isinstance(full_orderbook, dict):
                    self.logger.info(f"Full Orderbook Keys: {list(full_orderbook.keys())}")
                    if 'yes' in full_orderbook:
                        yes_levels = full_orderbook['yes']
                        self.logger.info(f"Yes Levels (first 3): {yes_levels[:3] if yes_levels else 'None'}")
                    if 'no' in full_orderbook:
                        no_levels = full_orderbook['no']
                        self.logger.info(f"No Levels (first 3): {no_levels[:3] if no_levels else 'None'}")

                # Show timestamps
                self.logger.info(f"API Call Start: {sample_data.get('api_call_start_ns')}")
                self.logger.info(f"API Response: {sample_data.get('api_response_ns')}")
                self.logger.info(f"Virginia Received: {sample_data.get('virginia_received_ns')}")
                self.logger.info(f"Virginia Enriched: {sample_data.get('virginia_enriched_ns')}")

            elif source == "POLYMARKET":
                # Show Polymarket specific structure
                self.logger.info(f"Condition ID: {sample_data.get('condition_id', 'MISSING')[:20]}...")
                self.logger.info(f"Snapshot ID: {sample_data.get('snapshot_id', 'MISSING')}")
                self.logger.info(f"Yes Price: {sample_data.get('yes_bid')}")  # Polymarket uses single price
                self.logger.info(f"No Price: {sample_data.get('no_bid')}")
                self.logger.info(f"Volume: {sample_data.get('volume')}")
                self.logger.info(f"Liquidity: {sample_data.get('liquidity')}")

                # Show full data structure
                full_orderbook = sample_data.get('full_orderbook', {})
                if isinstance(full_orderbook, dict):
                    self.logger.info(f"Full Data Keys: {list(full_orderbook.keys())}")
                    polymarket_data = full_orderbook.get('data', {})
                    if polymarket_data:
                        self.logger.info(
                            f"Polymarket Data Fields: {list(polymarket_data.keys()) if isinstance(polymarket_data, dict) else type(polymarket_data)}")

                # Show timestamps
                self.logger.info(f"Virginia Received: {sample_data.get('virginia_received_ns')}")
                self.logger.info(f"Virginia Enriched: {sample_data.get('virginia_enriched_ns')}")
                self.logger.info(f"Ireland Processing Complete: {sample_data.get('ireland_processing_complete_ns')}")
                self.logger.info(f"Ireland ZeroMQ Sent: {sample_data.get('ireland_zeromq_sent_ns')}")

            self.logger.info(f"--- END {source} SAMPLE ---")

        except Exception as e:
            self.logger.error(f"Error logging {source} sample: {e}")

    async def _refresh_arbitrage_pairs(self) -> bool:
        """
        Refresh arbitrage pairs from database and update routing.
        FIXED: Ensure pairs are loaded before extracting tickers
        """
        try:
            if not self.database_reader:
                self.logger.error("Database reader not initialized")
                return False

            self.logger.debug("Refreshing arbitrage pairs from database...")

            # STEP 1: Load pairs from database
            pairs = self.database_reader.get_active_pairs()
            if not pairs:
                self.logger.warning("No active pairs found in database")
                return True  # Not an error, just no pairs to track

            # STEP 2: Update internal state
            new_pairs = {pair.id: pair for pair in pairs}
            pairs_changed = new_pairs.keys() != self.active_pairs.keys()

            if pairs_changed:
                added = set(new_pairs.keys()) - set(self.active_pairs.keys())
                removed = set(self.active_pairs.keys()) - set(new_pairs.keys())

                if added:
                    self.logger.info(f"Added {len(added)} new arbitrage pairs")
                if removed:
                    self.logger.info(f"Removed {len(removed)} arbitrage pairs")

            # STEP 3: Update active pairs FIRST
            self.active_pairs = new_pairs

            # STEP 4: Extract tickers and condition IDs from the loaded pairs
            new_kalshi_tickers = {pair.kalshi_ticker for pair in pairs}
            new_polymarket_ids = {pair.polymarket_condition_id for pair in pairs}

            # DEBUG: Log what we found
            self.logger.info(f"Loaded {len(pairs)} active pairs from database")
            self.logger.debug(f"Kalshi tickers found: {list(new_kalshi_tickers)}")
            condition_ids_short = [cid[:10] + "..." for cid in list(new_polymarket_ids)[:3]]
            self.logger.debug(f"Polymarket condition IDs (first 3): {condition_ids_short}")

            # STEP 5: Update Kalshi feed if tickers changed
            if new_kalshi_tickers != self.kalshi_tickers:
                self.logger.info(f"Updating Kalshi tickers: {len(new_kalshi_tickers)} active")
                self.kalshi_tickers = new_kalshi_tickers

                # This is the critical fix - only call if kalshi_feed exists and has tickers
                if self.kalshi_feed and self.kalshi_tickers:
                    self.kalshi_feed.set_active_tickers(self.kalshi_tickers)
                    self.logger.info(f"Set active tickers on Kalshi feed: {list(self.kalshi_tickers)}")
                else:
                    self.logger.warning("Cannot set tickers - Kalshi feed not initialized or no tickers")

            # STEP 6: Update opportunity scanner with pairs mapping
            if self.opportunity_scanner:
                pairs_mapping = {pair.kalshi_ticker: pair.polymarket_condition_id for pair in pairs}
                self.opportunity_scanner.set_arbitrage_pairs(pairs_mapping)
                self.logger.debug(f"Updated opportunity scanner with {len(pairs_mapping)} pairs")

            # STEP 7: Update Ireland subscriptions if condition IDs changed
            if new_polymarket_ids != self.polymarket_condition_ids:
                self.logger.info(f"Updating Polymarket condition IDs: {len(new_polymarket_ids)} active")
                self.polymarket_condition_ids = new_polymarket_ids

                if self.publisher_manager and self.polymarket_condition_ids:
                    success = await self.publisher_manager.update_polymarket_subscriptions(new_polymarket_ids)
                    if success:
                        self.logger.info(
                            f"Successfully updated Ireland subscriptions: {len(new_polymarket_ids)} condition IDs")
                    else:
                        self.logger.error("Failed to update Ireland subscriptions")
                else:
                    self.logger.warning("Cannot update Ireland subscriptions - publisher not ready or no condition IDs")
            else:
                self.logger.debug(f"No changes in Polymarket condition IDs: {len(new_polymarket_ids)} active")

            # STEP 8: Update health metrics
            self.health_status.active_pairs_count = len(pairs)
            self.health_status.last_ticker_refresh = time.time()
            self.metrics.ticker_refresh_count += 1
            self.last_ticker_refresh = time.time()

            self.logger.info(f"Arbitrage pairs refresh completed: {len(pairs)} active pairs")
            return True

        except Exception as e:
            self.logger.error(f"Failed to refresh arbitrage pairs: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    async def _handle_trade_fills(self, fill: TradeFill):
        """Handle incoming trade fill notifications from Ireland"""
        # Trade fills are important - always log (not interval)
        self.logger.info(f"Received trade fill: {fill.condition_id[:10]}... "
                         f"{fill.side} {fill.size}@{fill.price}")
        self.metrics.trade_fills_received += 1

    async def _handle_system_metrics(self, metrics: SystemMetrics):
        """Handle incoming system metrics from Ireland server"""
        # Use interval logger for system metrics to reduce noise
        self.interval_logger.debug(
            'ireland_metrics',
            f"Ireland system metrics received",
            {
                'polymarket_api_status': metrics.polymarket_api_status,
                'api_latency_ms': metrics.api_latency_ms,
                'active_subscriptions': metrics.active_subscriptions,
                'timestamp': time.time()
            }
        )

    async def _handle_error_alerts(self, alert: ErrorAlert):
        """Handle error alerts from Ireland server"""
        # Errors always show regardless of interval
        self.logger.error(f"IRELAND ERROR ALERT: {alert.severity} - {alert.error_type}")
        self.logger.error(f"Message: {alert.message}")
        self.logger.error(f"Affected condition IDs: {len(alert.condition_ids_affected)}")
        self.health_status.errors_last_hour += 1

    async def _perform_health_check(self) -> bool:
        """Enhanced health check including database publisher"""
        try:
            current_time = time.time()
            self.health_status.uptime_seconds = current_time - self.start_time

            if self.database_reader:
                self.health_status.database_connected = self.database_reader.health_check()

            if self.kalshi_feed:
                self.health_status.kalshi_api_healthy = self.kalshi_feed.is_running

            if self.publisher_manager:
                publisher_status = self.publisher_manager.get_status()
                self.health_status.publisher_healthy = publisher_status.get('is_active', False)

            if self.receiver_manager:
                receiver_status = self.receiver_manager.get_status()
                self.health_status.receiver_healthy = receiver_status.get('is_active', False)

            # NEW: Check database publisher health
            if self.database_publisher:
                db_health = self.database_publisher.get_health_summary()
                self.health_status.database_publisher_healthy = db_health.get('is_healthy', False)

            if self.opportunity_scanner:
                scanner_status = self.opportunity_scanner.get_status()
                self.health_status.scanner_healthy = scanner_status.get('is_running', False)

            # Enhanced health logging every 5 minutes (use regular logger for system health)
            if int(current_time) % 300 == 0 and int(current_time) != int(self.last_health_check):
                self.logger.info(f"Enhanced System Health - DB: {self.health_status.database_connected}, "
                                 f"Kalshi: {self.health_status.kalshi_api_healthy}, "
                                 f"Publisher: {self.health_status.publisher_healthy}, "
                                 f"Receiver: {self.health_status.receiver_healthy}, "
                                 f"DB Publisher: {self.health_status.database_publisher_healthy}, "
                                 f"Pairs: {self.health_status.active_pairs_count}, "
                                 f"Bifurcated K/P: {self.metrics.kalshi_snapshots_bifurcated}/{self.metrics.polymarket_snapshots_bifurcated}")

            self.last_health_check = current_time
            return True

        except Exception as e:
            self.logger.error(f"Enhanced health check failed: {e}")
            return False

    async def start(self):
        """Start the enhanced arbitrage coordination system"""
        self.logger.info(
            "Starting enhanced arbitrage coordinator with data bifurcation, interval logging, and data flow monitoring...")

        try:
            if not await self._initialize_components():
                raise RuntimeError("Failed to initialize enhanced system components")

            # DEBUG: Check state before refresh
            self.logger.debug(f"Before pairs refresh - Active pairs: {len(self.active_pairs)}")
            self.logger.debug(f"Before pairs refresh - Kalshi tickers: {len(self.kalshi_tickers)}")
            self.logger.debug(f"Before pairs refresh - Polymarket IDs: {len(self.polymarket_condition_ids)}")

            if not await self._refresh_arbitrage_pairs():
                raise RuntimeError("Failed to load initial arbitrage pairs")

            # DEBUG: Check state after refresh
            self.logger.debug(f"After pairs refresh - Active pairs: {len(self.active_pairs)}")
            self.logger.debug(f"After pairs refresh - Kalshi tickers: {len(self.kalshi_tickers)}")
            self.logger.debug(f"After pairs refresh - Polymarket IDs: {len(self.polymarket_condition_ids)}")

            # DEBUG: List actual tickers
            if self.kalshi_tickers:
                self.logger.info(f"Active Kalshi tickers: {list(self.kalshi_tickers)}")
            else:
                self.logger.warning("No Kalshi tickers found - check database pairs!")

            if self.polymarket_condition_ids:
                condition_ids_short = [cid[:10] + "..." for cid in list(self.polymarket_condition_ids)[:3]]
                self.logger.info(f"Active Polymarket condition IDs (first 3): {condition_ids_short}")
            else:
                self.logger.warning("No Polymarket condition IDs found - check database pairs!")

            self.logger.info("Starting enhanced coordination loops...")

            # Ticker refresh loop
            ticker_task = asyncio.create_task(self._ticker_refresh_loop())
            self.running_tasks.append(ticker_task)

            # Health monitoring loop
            health_task = asyncio.create_task(self._health_monitor_loop())
            self.running_tasks.append(health_task)

            # NEW: Data flow monitoring loop
            data_flow_task = asyncio.create_task(self._data_flow_monitor_loop())
            self.running_tasks.append(data_flow_task)

            # Start Kalshi data feed
            if self.kalshi_feed:
                kalshi_task = asyncio.create_task(self.kalshi_feed.run_data_feed())
                self.running_tasks.append(kalshi_task)

            # Start opportunity scanner
            if self.opportunity_scanner:
                scanner_task = asyncio.create_task(self.opportunity_scanner.start_scanner())
                self.running_tasks.append(scanner_task)

            self.is_running = True
            self.logger.info("Enhanced arbitrage coordinator started successfully!")
            self.logger.info(f"Monitoring {len(self.active_pairs)} arbitrage pairs")
            self.logger.info(f"Tracking {len(self.kalshi_tickers)} Kalshi tickers")
            self.logger.info(f"Routing {len(self.polymarket_condition_ids)} Polymarket condition IDs to Ireland")
            self.logger.info("Data bifurcation: Trading lane + Storage lane active")
            self.logger.info(f"Interval logging: Sample every {getattr(CONFIG, 'log_sample_interval', 100)} messages")
            self.logger.info("Data flow monitoring: Status reports every 30 seconds")

        except Exception as e:
            self.logger.error(f"Failed to start enhanced arbitrage coordinator: {e}")
            await self.stop()
            raise

    async def stop(self):
        """Stop the enhanced arbitrage coordination system gracefully"""
        self.logger.info("Stopping enhanced arbitrage coordinator...")
        self.shutdown_requested = True

        # Stop all components
        if self.kalshi_feed:
            self.kalshi_feed.stop()

        if self.publisher_manager:
            await self.publisher_manager.stop()

        if self.receiver_manager:
            await self.receiver_manager.stop()

        # NEW: Stop database publisher
        if self.database_publisher:
            await self.database_publisher.stop()

        if self.opportunity_scanner:
            self.opportunity_scanner.stop_scanner()

        # Cancel all running tasks
        for task in self.running_tasks:
            if not task.done():
                task.cancel()

        if self.running_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.running_tasks, return_exceptions=True),
                    timeout=CONFIG.coordinator.shutdown_timeout
                )
            except asyncio.TimeoutError:
                self.logger.warning("Some tasks did not shutdown within timeout")

        # Cleanup components
        if self.kalshi_feed:
            await self.kalshi_feed.__aexit__(None, None, None)

        if self.database_reader:
            self.database_reader.disconnect()

        self.is_running = False
        self.logger.info("Enhanced arbitrage coordinator stopped")

    async def _ticker_refresh_loop(self):
        """Main ticker refresh loop"""
        self.logger.info(f"Starting ticker refresh loop - interval: {CONFIG.coordinator.ticker_refresh_interval}s")

        while not self.shutdown_requested:
            try:
                current_time = time.time()
                if current_time - self.last_ticker_refresh >= CONFIG.coordinator.ticker_refresh_interval:
                    self.logger.debug("Starting ticker refresh...")
                    success = await self._refresh_arbitrage_pairs()
                    if success:
                        self.logger.debug("Ticker refresh completed successfully")
                    else:
                        self.logger.warning("Ticker refresh failed")
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"Error in ticker refresh loop: {e}")
                await asyncio.sleep(5)

        self.logger.info("Ticker refresh loop stopped")

    async def _health_monitor_loop(self):
        """Health monitoring loop"""
        self.logger.info(
            f"Starting enhanced health monitor loop - interval: {CONFIG.coordinator.health_check_interval}s")

        while not self.shutdown_requested:
            try:
                current_time = time.time()
                if current_time - self.last_health_check >= CONFIG.coordinator.health_check_interval:
                    await self._perform_health_check()
                await asyncio.sleep(10)
            except Exception as e:
                self.logger.error(f"Error in enhanced health monitor loop: {e}")
                await asyncio.sleep(10)

        self.logger.info("Enhanced health monitor loop stopped")

    def is_healthy(self) -> bool:
        """
        Health check that matches the original working logic.
        Keep it simple and forgiving like the old coordinator.
        """
        if not self.is_running:
            return False

        current_time = time.time()
        startup_time = current_time - self.start_time

        if startup_time < 30:
            # Startup phase: Only require database (like original)
            healthy = self.health_status.database_connected

        elif startup_time < 120:
            # Initialization phase: Database + pairs loaded (like original)
            healthy = (self.health_status.database_connected and
                       len(self.active_pairs) > 0)

        else:
            # Operational phase: Same as original - just database, kalshi, publisher, pairs
            healthy = (self.health_status.database_connected and
                       self.health_status.kalshi_api_healthy and
                       self.health_status.publisher_healthy and
                       len(self.active_pairs) > 0)
            # NOTE: We deliberately don't check database_publisher_healthy
            # or receiver_healthy to match original behavior

        # Debug logging (can remove later)
        if not healthy and startup_time > 30:
            self.logger.warning(f"Health check failed at {startup_time:.1f}s: "
                                f"DB={self.health_status.database_connected}, "
                                f"Kalshi={self.health_status.kalshi_api_healthy}, "
                                f"Publisher={self.health_status.publisher_healthy}, "
                                f"Pairs={len(self.active_pairs)}")

        return healthy

    def get_status(self) -> Dict[str, Any]:
        """
        Get enhanced coordinator status including bifurcation metrics and interval logging stats.
        """
        # Get interval logger stats
        interval_stats = self.interval_logger.get_stats() if hasattr(self, 'interval_logger') else {}

        return {
            'is_running': self.is_running,
            'uptime_hours': self.health_status.uptime_seconds / 3600,
            'active_pairs': len(self.active_pairs),
            'kalshi_tickers': len(self.kalshi_tickers),
            'polymarket_ids': len(self.polymarket_condition_ids),
            'health': {
                'database': self.health_status.database_connected,
                'kalshi_api': self.health_status.kalshi_api_healthy,
                'publisher': self.health_status.publisher_healthy,
                'receiver': self.health_status.receiver_healthy,
                'database_publisher': self.health_status.database_publisher_healthy,  # NEW
                'scanner': self.health_status.scanner_healthy
            },
            'metrics': {
                'api_calls': self.metrics.total_api_calls,
                'ticker_refreshes': self.metrics.ticker_refresh_count,
                'data_points': self.metrics.data_points_processed,
                'polymarket_messages': self.metrics.polymarket_messages_received,
                'trade_fills': self.metrics.trade_fills_received,
                # NEW: Bifurcation metrics
                'kalshi_snapshots_bifurcated': self.metrics.kalshi_snapshots_bifurcated,
                'polymarket_snapshots_bifurcated': self.metrics.polymarket_snapshots_bifurcated,
                'database_sends_successful': self.metrics.database_sends_successful,
                'database_sends_failed': self.metrics.database_sends_failed
            },
            'bifurcation': {  # NEW: Bifurcation status
                'is_active': self.database_publisher is not None and self.database_publisher.is_running,
                'success_rate': (self.metrics.database_sends_successful /
                                 max(self.metrics.database_sends_successful + self.metrics.database_sends_failed,
                                     1)) * 100,
                'total_snapshots_sent': self.metrics.kalshi_snapshots_bifurcated + self.metrics.polymarket_snapshots_bifurcated
            },
            'interval_logging': interval_stats,  # NEW: Interval logging statistics
            'last_activities': {
                'ticker_refresh': self.health_status.last_ticker_refresh,
                'kalshi_data': self.health_status.last_kalshi_data,
                'polymarket_data': self.health_status.last_polymarket_data,
                'health_check': self.last_health_check
            }
        }

    async def execute_trade(self, condition_id: str, side: str, size: float, price: float) -> Dict[str, Any]:
        """
        Execute trade via Ireland server.
        """
        if not self.publisher_manager:
            return {"status": "ERROR", "message": "Publisher not available"}

        # Trade execution is important - always log (not interval)
        self.logger.info(f"Executing trade: {condition_id[:10]}... {side} {size}@{price}")

        response = await self.publisher_manager.execute_trade(condition_id, side, size, price)

        self.logger.info(f"Trade execution response: {response.get('status')}")
        return response

    def get_bifurcation_summary(self) -> Dict[str, Any]:
        """
        Get summary of data bifurcation performance.
        NEW method for monitoring bifurcation effectiveness.
        """
        total_snapshots = self.metrics.kalshi_snapshots_bifurcated + self.metrics.polymarket_snapshots_bifurcated
        total_sends = self.metrics.database_sends_successful + self.metrics.database_sends_failed

        success_rate = (self.metrics.database_sends_successful / max(total_sends, 1)) * 100

        return {
            'bifurcation_active': self.database_publisher is not None and self.database_publisher.is_running,
            'total_snapshots_processed': total_snapshots,
            'kalshi_snapshots': self.metrics.kalshi_snapshots_bifurcated,
            'polymarket_snapshots': self.metrics.polymarket_snapshots_bifurcated,
            'database_sends_successful': self.metrics.database_sends_successful,
            'database_sends_failed': self.metrics.database_sends_failed,
            'success_rate_percent': success_rate,
            'trading_lane_active': self.opportunity_scanner is not None and self.opportunity_scanner.is_running,
            'storage_lane_active': self.database_publisher is not None and self.database_publisher.is_running,
            'database_publisher_health': self.database_publisher.get_health_summary() if self.database_publisher else None,
            'interval_logging_stats': self.interval_logger.get_stats() if hasattr(self, 'interval_logger') else None
        }


# Demo and testing
async def run_enhanced_coordinator_demo():
    """Demo the enhanced arbitrage coordinator with data bifurcation, interval logging, and data flow monitoring"""
    print("ENHANCED ARBITRAGE COORDINATOR WITH DATA BIFURCATION, INTERVAL LOGGING, AND DATA FLOW MONITORING DEMO")
    print("=" * 100)

    try:
        async with ArbitrageCoordinator() as coordinator:
            print("✅ Enhanced coordinator started successfully!")

            # Let it run for 90 seconds to test bifurcation, interval logging, and data flow monitoring
            print("🔄 Running enhanced coordinator for 90 seconds...")
            print("   Monitoring data bifurcation to trading + storage lanes...")
            print("   Testing interval logging with sample messages...")
            print("   Data flow monitoring will show status every 30 seconds...")
            await asyncio.sleep(90)

            # Show enhanced status
            status = coordinator.get_status()
            print(f"\n📊 Enhanced Status:")
            for key, value in status.items():
                print(f"  {key}: {value}")

            # Show bifurcation summary
            bifurcation = coordinator.get_bifurcation_summary()
            print(f"\n🔀 Bifurcation Summary:")
            for key, value in bifurcation.items():
                print(f"  {key}: {value}")

            print("\n✅ Enhanced coordinator demo completed!")

    except Exception as e:
        print(f"❌ Enhanced coordinator demo failed: {e}")
        import traceback
        traceback.print_exc()


# DEBUG: Database pairs verification
async def debug_database_pairs():
    """Debug function to check what pairs are in the database"""
    print("🔍 DEBUGGING DATABASE PAIRS")
    print("=" * 40)

    try:
        from database.read_from import DatabaseReader

        db = DatabaseReader()
        if not db.connect():
            print("❌ Failed to connect to database")
            return

        pairs = db.get_active_pairs()
        print(f"✅ Found {len(pairs)} active pairs in database:")

        for pair in pairs:
            print(f"  ID: {pair.id}")
            print(f"  Kalshi: {pair.kalshi_ticker}")
            print(f"  Polymarket: {pair.polymarket_condition_id[:20]}...")
            print(f"  Expiration: {pair.expiration_date}")
            print(f"  Active: {pair.is_active}")
            print("  ---")

        if not pairs:
            print("⚠️ No pairs found! You need to add some pairs to the database first.")
            print("   Run: python market_controller.py")

        db.disconnect()

    except Exception as e:
        print(f"❌ Database debug failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "debug":
        asyncio.run(debug_database_pairs())
    else:
        try:
            asyncio.run(run_enhanced_coordinator_demo())
        except KeyboardInterrupt:
            print("\nDemo interrupted by user")
        except Exception as e:
            # noinspection PyPackageRequirements
            print(f"Demo execution failed: {e}")