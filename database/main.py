#!/usr/bin/env python3
"""

@ rìan

main.py

Production Data Server for Arbitrage Trading System.
Runs continuously until stopped via CTRL+C or system signal.

"""

import asyncio
import time
import signal
import sys
import os
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_logger
from broker.receiver import ZeroMQReceiver, MarketDataMessage, AnalysisMessage, TradeMessage
from influx.writer import InfluxDBWriter
from postgres.writer import PostgreSQLWriter
from models.influx import MarketSnapshot, create_kalshi_snapshot, create_polymarket_snapshot
from models.postgres import TradeTicket, AnalysisRecord, ArbitragePair


class DataServerCoordinator:
    """
    Production Data Server Coordinator.

    Orchestrates data flow between:
    1. ZeroMQ Receiver (receives from Virginia)
    2. InfluxDB Writer (time-series market data)
    3. PostgreSQL Writer (relational trade data)
    """

    def __init__(self):
        """Initialize Data Server coordinator"""
        self.logger = get_logger('coordinator')

        # Core components
        self.receiver: Optional[ZeroMQReceiver] = None
        self.influx_writer: Optional[InfluxDBWriter] = None
        self.postgres_writer: Optional[PostgreSQLWriter] = None

        # State tracking
        self.start_time = time.time()
        self.is_running = False
        self.shutdown_requested = False

        # Task references for cleanup
        self.running_tasks: List[asyncio.Task] = []

        # Performance metrics
        self.stats = {
            'messages_processed': 0,
            'market_data_processed': 0,
            'analysis_updates_processed': 0,
            'trade_data_processed': 0,
            'snapshots_written_influx': 0,
            'records_written_postgres': 0,
            'processing_errors': 0,
            'last_message_time': 0,
            'avg_processing_latency_ms': 0.0,
            'kalshi_snapshots_processed': 0,
            'kalshi_snapshots_written': 0,
            'kalshi_conversion_errors': 0,
            'kalshi_influx_write_errors': 0
        }

        # Processing latency tracking
        self.processing_latencies = []
        self.max_latency_samples = 50

        # Health monitoring
        self.last_health_check = 0
        self.health_check_interval = 60  # seconds

        self.logger.info("Data Server coordinator initialized")

    async def start(self):
        """Start the Data Server coordinator"""
        try:
            self.logger.info("Starting Data Server coordinator...")

            # Initialize all components
            await self._initialize_components()

            # Set up message callbacks
            self._setup_message_callbacks()

            # Start background tasks
            await self._start_background_tasks()

            self.is_running = True
            self.logger.info("Data Server coordinator started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start Data Server coordinator: {e}")
            await self.stop()
            raise

    async def stop(self):
        """Stop the Data Server coordinator gracefully"""
        self.logger.info("Stopping Data Server coordinator...")
        self.shutdown_requested = True

        # Stop all background tasks
        for task in self.running_tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self.running_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.running_tasks, return_exceptions=True),
                    timeout=30
                )
            except asyncio.TimeoutError:
                self.logger.warning("Some tasks did not shutdown within timeout")

        # Stop all components
        if self.receiver:
            await self.receiver.stop()

        if self.influx_writer:
            await self.influx_writer.stop()

        if self.postgres_writer:
            await self.postgres_writer.stop()

        self.is_running = False
        self.logger.info("Data Server coordinator stopped")

    async def _initialize_components(self):
        """Initialize all coordinator components"""
        try:
            # Initialize ZeroMQ receiver
            self.logger.info("Initializing ZeroMQ receiver...")
            self.receiver = ZeroMQReceiver(verbose=False)  # Production: no verbose logging
            await self.receiver.start()

            # Initialize InfluxDB writer
            self.logger.info("Initializing InfluxDB writer...")
            self.influx_writer = InfluxDBWriter()
            await self.influx_writer.start()

            # Initialize PostgreSQL writer
            self.logger.info("Initializing PostgreSQL writer...")
            self.postgres_writer = PostgreSQLWriter()
            await self.postgres_writer.start()

            self.logger.info("All components initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise

    def _setup_message_callbacks(self):
        """Set up message callbacks for the receiver"""
        # Add callbacks to receiver
        self.receiver.add_market_data_callback(self._handle_market_data)
        self.receiver.add_analysis_callback(self._handle_analysis_update)
        self.receiver.add_trade_callback(self._handle_trade_data)

        self.logger.info("Message callbacks configured")

    async def _start_background_tasks(self):
        """Start background coordinator tasks"""
        # Start receiver loop
        receiver_task = asyncio.create_task(self.receiver.run_receiver_loop())
        self.running_tasks.append(receiver_task)

        # Start health monitoring loop
        health_task = asyncio.create_task(self._health_monitor_loop())
        self.running_tasks.append(health_task)

        # Start status reporting loop
        status_task = asyncio.create_task(self._status_reporting_loop())
        self.running_tasks.append(status_task)

        self.logger.info("Background tasks started")

    async def _handle_market_data(self, message: MarketDataMessage):
        """Handle incoming market data message from Virginia"""
        processing_start = time.time()

        try:
            self.stats['messages_processed'] += 1
            self.stats['market_data_processed'] += 1
            self.stats['last_message_time'] = time.time()

            # Convert snapshot dictionaries to MarketSnapshot objects
            snapshots = []
            kalshi_count = 0
            polymarket_count = 0

            for snapshot_data in message.snapshots:
                try:
                    # Track source types
                    source = snapshot_data.get('source', '').lower()
                    if source == 'kalshi':
                        kalshi_count += 1
                        self.stats['kalshi_snapshots_processed'] += 1
                    elif source == 'polymarket':
                        polymarket_count += 1

                    # Create MarketSnapshot from the data
                    snapshot = self._convert_snapshot_data(snapshot_data)
                    if snapshot:
                        # Add Virginia timing information
                        snapshot.virginia_sent_to_data_server_ns = int(message.virginia_sent_timestamp * 1_000_000_000)
                        snapshots.append(snapshot)
                    else:
                        if source == 'kalshi':
                            self.stats['kalshi_conversion_errors'] += 1

                except Exception as e:
                    self.logger.warning(f"Error converting snapshot from {snapshot_data.get('source', 'unknown')}: {e}")
                    if snapshot_data.get('source', '').lower() == 'kalshi':
                        self.stats['kalshi_conversion_errors'] += 1
                    continue

            # Log summary for significant batches
            if len(snapshots) > 0:
                self.logger.debug(f"Processing batch: {kalshi_count} Kalshi, {polymarket_count} Polymarket snapshots")

            if snapshots:
                # Write to InfluxDB
                if len(snapshots) == 1:
                    success = await self.influx_writer.write_snapshot(snapshots[0])
                else:
                    success = await self.influx_writer.write_snapshots_batch(snapshots)

                if success:
                    self.stats['snapshots_written_influx'] += len(snapshots)
                    kalshi_written = len([s for s in snapshots if s.source.lower() == 'kalshi'])
                    self.stats['kalshi_snapshots_written'] += kalshi_written
                else:
                    kalshi_failed = len([s for s in snapshots if s.source.lower() == 'kalshi'])
                    self.stats['kalshi_influx_write_errors'] += kalshi_failed
                    self.logger.error(f"Failed to write {len(snapshots)} snapshots to InfluxDB")

            # Track processing latency
            processing_duration = (time.time() - processing_start) * 1000
            self._track_processing_latency(processing_duration)

        except Exception as e:
            self.stats['processing_errors'] += 1
            self.logger.error(f"Error handling market data: {e}")

    async def _handle_analysis_update(self, message: AnalysisMessage):
        """Handle incoming analysis update message from Virginia"""
        try:
            self.stats['messages_processed'] += 1
            self.stats['analysis_updates_processed'] += 1
            self.stats['last_message_time'] = time.time()

            # Update InfluxDB snapshot usage flags
            success = await self.influx_writer.update_snapshot_usage(
                message.snapshot_id,
                analyzed=message.analyzed if message.analyzed else None,
                executed=message.executed if message.executed else None
            )

            if not success:
                self.logger.warning(f"Failed to update usage flags for snapshot {message.snapshot_id[:8]}...")

        except Exception as e:
            self.stats['processing_errors'] += 1
            self.logger.error(f"Error handling analysis update: {e}")

    async def _handle_trade_data(self, message: TradeMessage):
        """Handle incoming trade data message from Virginia"""
        try:
            self.stats['messages_processed'] += 1
            self.stats['trade_data_processed'] += 1
            self.stats['last_message_time'] = time.time()

            records_written = 0

            # Handle trade ticket
            if message.trade_ticket:
                try:
                    trade_ticket = self._convert_trade_ticket_data(message.trade_ticket)
                    if trade_ticket:
                        success = await self.postgres_writer.write_trade_ticket(trade_ticket)
                        if success:
                            records_written += 1
                            self.logger.info(f"Trade executed: {trade_ticket.trade_id}")
                        else:
                            self.logger.error(f"Failed to write trade ticket: {trade_ticket.trade_id}")
                except Exception as e:
                    self.logger.error(f"Error processing trade ticket: {e}")

            # Handle analysis record
            if message.analysis_record:
                try:
                    analysis_record = self._convert_analysis_record_data(message.analysis_record)
                    if analysis_record:
                        success = await self.postgres_writer.write_analysis_record(analysis_record)
                        if success:
                            records_written += 1
                        else:
                            self.logger.error(f"Failed to write analysis record: {analysis_record.analysis_id}")
                except Exception as e:
                    self.logger.error(f"Error processing analysis record: {e}")

            # Handle arbitrage pair
            if message.arbitrage_pair:
                try:
                    arbitrage_pair = self._convert_arbitrage_pair_data(message.arbitrage_pair)
                    if arbitrage_pair:
                        success = await self.postgres_writer.write_arbitrage_pair(arbitrage_pair)
                        if success:
                            records_written += 1
                        else:
                            self.logger.error(f"Failed to write arbitrage pair: {arbitrage_pair.pair_id}")
                except Exception as e:
                    self.logger.error(f"Error processing arbitrage pair: {e}")

            self.stats['records_written_postgres'] += records_written

        except Exception as e:
            self.stats['processing_errors'] += 1
            self.logger.error(f"Error handling trade data: {e}")

    def _convert_snapshot_data(self, snapshot_data: Dict[str, Any]) -> Optional[MarketSnapshot]:
        """Convert snapshot dictionary to MarketSnapshot object"""
        try:
            source = snapshot_data.get('source', '').lower()

            if source == 'kalshi':
                # Get ticker and orderbook
                ticker = snapshot_data.get('ticker', '')
                full_orderbook = snapshot_data.get('full_orderbook', {})

                # Convert full_orderbook to proper format
                if isinstance(full_orderbook, dict):
                    orderbook_dict = full_orderbook
                elif isinstance(full_orderbook, str):
                    try:
                        orderbook_dict = json.loads(full_orderbook)
                    except Exception:
                        return None
                else:
                    return None

                # Get API timestamps
                api_timestamps = {
                    'api_call_start_ns': snapshot_data.get('api_call_start_ns'),
                    'api_response_ns': snapshot_data.get('api_response_ns'),
                    'processing_complete_ns': snapshot_data.get('processing_complete_ns')
                }

                # Create snapshot
                snapshot = create_kalshi_snapshot(
                    ticker=ticker,
                    orderbook_dict=orderbook_dict,
                    api_timestamps=api_timestamps,
                    pair_id=snapshot_data.get('pair_id')
                )

                # Preserve snapshot ID if present
                if 'snapshot_id' in snapshot_data:
                    snapshot.snapshot_id = snapshot_data['snapshot_id']

                # Add Virginia timestamps
                snapshot.virginia_received_ns = snapshot_data.get('virginia_received_ns')
                snapshot.virginia_enriched_ns = snapshot_data.get('virginia_enriched_ns')

                return snapshot

            elif source == 'polymarket':
                # Convert Polymarket snapshot
                snapshot = create_polymarket_snapshot(
                    condition_id=snapshot_data.get('condition_id', ''),
                    orderbook_dict=snapshot_data.get('full_orderbook', {}),
                    ireland_timestamps={
                        'ireland_api_call_ns': snapshot_data.get('ireland_api_call_ns'),
                        'ireland_api_response_ns': snapshot_data.get('ireland_api_response_ns'),
                        'ireland_processing_complete_ns': snapshot_data.get('ireland_processing_complete_ns'),
                        'ireland_zeromq_sent_ns': snapshot_data.get('ireland_zeromq_sent_ns')
                    },
                    pair_id=snapshot_data.get('pair_id')
                )

                # Add additional fields from the snapshot data
                if 'snapshot_id' in snapshot_data:
                    snapshot.snapshot_id = snapshot_data['snapshot_id']

                # Add Virginia timestamps
                snapshot.virginia_received_ns = snapshot_data.get('virginia_received_ns')
                snapshot.virginia_enriched_ns = snapshot_data.get('virginia_enriched_ns')

                return snapshot

            else:
                self.logger.warning(f"Unknown snapshot source: {source}")
                return None

        except Exception as e:
            self.logger.warning(f"Error converting snapshot data: {e}")
            return None

    def _convert_trade_ticket_data(self, trade_data: Dict[str, Any]) -> Optional[TradeTicket]:
        """Convert trade dictionary to TradeTicket object"""
        try:
            from models.postgres import TradeVenue, TradeSide, ArbitrageType, TradeStatus

            venue = TradeVenue(trade_data.get('venue', 'kalshi'))
            side = TradeSide(trade_data.get('side', 'buy'))
            arbitrage_type = ArbitrageType(trade_data.get('arbitrage_type', 'kalshi_yes_polymarket_no'))
            status = TradeStatus(trade_data.get('status', 'pending'))

            trade_ticket = TradeTicket(
                trade_id=trade_data.get('trade_id', ''),
                arbitrage_id=trade_data.get('arbitrage_id', ''),
                kalshi_ticker=trade_data.get('kalshi_ticker', ''),
                polymarket_condition_id=trade_data.get('polymarket_condition_id', ''),
                pair_id=trade_data.get('pair_id', ''),
                market_title=trade_data.get('market_title', ''),
                venue=venue,
                side=side,
                outcome=trade_data.get('outcome', ''),
                order_type=trade_data.get('order_type', 'market'),
                quantity=float(trade_data.get('quantity', 0)),
                limit_price=float(trade_data['limit_price']) if trade_data.get('limit_price') else None,
                status=status,
                executed_quantity=float(trade_data.get('executed_quantity', 0)),
                executed_price=float(trade_data['executed_price']) if trade_data.get('executed_price') else None,
                arbitrage_type=arbitrage_type,
                expected_profit=float(trade_data['expected_profit']) if trade_data.get('expected_profit') else None
            )

            if 'analyzed_snapshot_ids' in trade_data:
                trade_ticket.analyzed_snapshot_ids = trade_data['analyzed_snapshot_ids']
            if 'executed_snapshot_id' in trade_data:
                trade_ticket.executed_snapshot_id = trade_data['executed_snapshot_id']

            return trade_ticket

        except Exception as e:
            self.logger.error(f"Error converting trade ticket data: {e}")
            return None

    def _convert_analysis_record_data(self, analysis_data: Dict[str, Any]) -> Optional[AnalysisRecord]:
        """Convert analysis dictionary to AnalysisRecord object"""
        try:
            from models.postgres import ArbitrageType

            arbitrage_type = ArbitrageType(analysis_data.get('arbitrage_type', 'kalshi_yes_polymarket_no'))

            analysis_record = AnalysisRecord(
                analysis_id=analysis_data.get('analysis_id', ''),
                kalshi_ticker=analysis_data.get('kalshi_ticker', ''),
                polymarket_condition_id=analysis_data.get('polymarket_condition_id', ''),
                pair_id=analysis_data.get('pair_id', ''),
                arbitrage_type=arbitrage_type,
                potential_profit=float(analysis_data['potential_profit']) if analysis_data.get(
                    'potential_profit') else None,
                rejection_reason=analysis_data.get('rejection_reason', ''),
                rejection_details=analysis_data.get('rejection_details')
            )

            if 'analyzed_snapshot_ids' in analysis_data:
                analysis_record.analyzed_snapshot_ids = analysis_data['analyzed_snapshot_ids']

            return analysis_record

        except Exception as e:
            self.logger.error(f"Error converting analysis record data: {e}")
            return None

    def _convert_arbitrage_pair_data(self, pair_data: Dict[str, Any]) -> Optional[ArbitragePair]:
        """Convert arbitrage pair dictionary to ArbitragePair object"""
        try:
            arbitrage_pair = ArbitragePair(
                pair_id=pair_data.get('pair_id', ''),
                kalshi_ticker=pair_data.get('kalshi_ticker', ''),
                polymarket_condition_id=pair_data.get('polymarket_condition_id', ''),
                market_title=pair_data.get('market_title', ''),
                description=pair_data.get('description'),
                is_active=bool(pair_data.get('is_active', True)),
                min_spread_bps=float(pair_data.get('min_spread_bps', 100.0)),
                max_position_size=float(pair_data.get('max_position_size', 1000.0)),
                max_trade_size=float(pair_data.get('max_trade_size', 100.0))
            )

            return arbitrage_pair

        except Exception as e:
            self.logger.error(f"Error converting arbitrage pair data: {e}")
            return None

    def _track_processing_latency(self, latency_ms: float):
        """Track processing latency for performance monitoring"""
        self.processing_latencies.append(latency_ms)
        if len(self.processing_latencies) > self.max_latency_samples:
            self.processing_latencies = self.processing_latencies[-self.max_latency_samples:]

        if self.processing_latencies:
            self.stats['avg_processing_latency_ms'] = sum(self.processing_latencies) / len(self.processing_latencies)

    async def _health_monitor_loop(self):
        """Background health monitoring loop"""
        self.logger.info("Health monitor started")

        while not self.shutdown_requested:
            try:
                current_time = time.time()

                if current_time - self.last_health_check >= self.health_check_interval:
                    await self._perform_health_check()

                await asyncio.sleep(10)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in health monitor loop: {e}")
                await asyncio.sleep(30)

        self.logger.info("Health monitor stopped")

    async def _status_reporting_loop(self):
        """Background status reporting loop"""
        self.logger.info("Status reporting started")

        while not self.shutdown_requested:
            try:
                await asyncio.sleep(300)  # Report every 5 minutes

                if not self.shutdown_requested:
                    current_time = time.time()
                    uptime_hours = (current_time - self.start_time) / 3600

                    # Calculate rates
                    message_rate = self.stats['messages_processed'] / max(uptime_hours * 3600, 1)

                    # Calculate Kalshi success rate
                    kalshi_success_rate = 0
                    if self.stats['kalshi_snapshots_processed'] > 0:
                        kalshi_success_rate = (self.stats['kalshi_snapshots_written'] /
                                               self.stats['kalshi_snapshots_processed']) * 100

                    self.logger.info(f"STATUS: {uptime_hours:.1f}h uptime | "
                                     f"{self.stats['messages_processed']} msgs ({message_rate:.1f}/min) | "
                                     f"Kalshi: {self.stats['kalshi_snapshots_written']}/{self.stats['kalshi_snapshots_processed']} ({kalshi_success_rate:.1f}%) | "
                                     f"InfluxDB: {self.stats['snapshots_written_influx']} | "
                                     f"PostgreSQL: {self.stats['records_written_postgres']} | "
                                     f"Errors: {self.stats['processing_errors']}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in status reporting loop: {e}")
                await asyncio.sleep(60)

        self.logger.info("Status reporting stopped")

    async def _perform_health_check(self):
        """Perform comprehensive health check"""
        try:
            current_time = time.time()

            # Check component health
            receiver_healthy = self.receiver.get_health_summary()['is_healthy'] if self.receiver else False
            influx_healthy = self.influx_writer.get_health_summary()['is_healthy'] if self.influx_writer else False
            postgres_healthy = self.postgres_writer.get_health_summary()[
                'is_healthy'] if self.postgres_writer else False

            # Check for data staleness (no messages in 5 minutes)
            data_stale = (current_time - self.stats.get('last_message_time', current_time)) > 300

            if not receiver_healthy or not influx_healthy or not postgres_healthy or data_stale:
                self.logger.warning(f"Health issues detected - Receiver: {receiver_healthy}, "
                                    f"InfluxDB: {influx_healthy}, PostgreSQL: {postgres_healthy}, "
                                    f"Data stale: {data_stale}")

            self.last_health_check = current_time

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")

    async def run_forever(self):
        """Run the coordinator continuously until stopped"""
        try:
            await self.start()

            self.logger.info("ARBITRAGE DATA SERVER RUNNING")
            self.logger.info(f"Listening on ports: {CONFIG.zeromq.market_data_port}, "
                             f"{CONFIG.zeromq.analysis_data_port}, {CONFIG.zeromq.trade_data_port}")

            # Run until shutdown requested
            while not self.shutdown_requested:
                await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"Coordinator error: {e}")
            raise
        finally:
            await self.stop()


# Signal handling for graceful shutdown
def setup_signal_handlers(coordinator: DataServerCoordinator):
    """Setup signal handlers for graceful shutdown"""

    def signal_handler(signum, frame):
        signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        coordinator.logger.info(f"Received {signal_name}, initiating graceful shutdown...")
        coordinator.shutdown_requested = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    """Main production entry point"""
    print("ARBITRAGE DATA SERVER - PRODUCTION")
    print("=" * 40)

    # Initialize coordinator
    coordinator = DataServerCoordinator()

    # Setup signal handlers for graceful shutdown
    setup_signal_handlers(coordinator)

    try:
        # Run forever
        asyncio.run(coordinator.run_forever())

    except KeyboardInterrupt:
        print("\nShutdown initiated by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

    print("Data Server shutdown complete")


if __name__ == "__main__":
    main()