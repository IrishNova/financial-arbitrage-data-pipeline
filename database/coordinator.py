#!/usr/bin/env python3
"""
coordinator.py

Central coordinator for the Data Server.
Orchestrates ZeroMQ receiver, InfluxDB writer, and PostgreSQL writer.
"""

import asyncio
import time
import signal
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_logger
from broker.receiver import ZeroMQReceiver, MarketDataMessage, AnalysisMessage, TradeMessage
from influx.writer import InfluxDBWriter
from postgres.writer import PostgreSQLWriter
from models.influx import MarketSnapshot, create_snapshot_from_virginia_data
from models.postgres import TradeTicket, AnalysisRecord, ArbitragePair


class DataServerCoordinator:
    """
    Central coordinator for the Data Server.

    Orchestrates data flow between:
    1. ZeroMQ Receiver (receives from Virginia)
    2. InfluxDB Writer (time-series market data)
    3. PostgreSQL Writer (relational trade data)

    Responsibilities:
    - Message routing and processing
    - Error handling and recovery
    - Performance monitoring
    - Health status management
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

            self.logger.debug(f"Processing market data: {len(message.snapshots)} snapshots")

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

                    # Create MarketSnapshot from the data using unified conversion
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
                    self.logger.debug(f"Successfully wrote {len(snapshots)} snapshots to InfluxDB")
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
        processing_start = time.time()

        try:
            self.stats['messages_processed'] += 1
            self.stats['analysis_updates_processed'] += 1
            self.stats['last_message_time'] = time.time()

            self.logger.debug(f"Processing analysis update: {message.snapshot_id[:8]}... "
                              f"analyzed={message.analyzed}, executed={message.executed}")

            # Update InfluxDB snapshot usage flags
            success = await self.influx_writer.update_snapshot_usage(
                message.snapshot_id,
                analyzed=message.analyzed if message.analyzed else None,
                executed=message.executed if message.executed else None
            )

            if success:
                self.logger.debug(f"Updated usage flags for snapshot {message.snapshot_id[:8]}...")
            else:
                self.logger.error(f"Failed to update usage flags for snapshot {message.snapshot_id[:8]}...")

            # Track processing latency
            processing_duration = (time.time() - processing_start) * 1000
            self._track_processing_latency(processing_duration)

        except Exception as e:
            self.stats['processing_errors'] += 1
            self.logger.error(f"Error handling analysis update: {e}")

    async def _handle_trade_data(self, message: TradeMessage):
        """Handle incoming trade data message from Virginia"""
        processing_start = time.time()

        try:
            self.stats['messages_processed'] += 1
            self.stats['trade_data_processed'] += 1
            self.stats['last_message_time'] = time.time()

            self.logger.debug(f"Processing trade data: trade={bool(message.trade_ticket)}, "
                              f"analysis={bool(message.analysis_record)}, "
                              f"pair={bool(message.arbitrage_pair)}")

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

            # Track processing latency
            processing_duration = (time.time() - processing_start) * 1000
            self._track_processing_latency(processing_duration)

        except Exception as e:
            self.stats['processing_errors'] += 1
            self.logger.error(f"Error handling trade data: {e}")

    def _convert_snapshot_data(self, snapshot_data: Dict[str, Any]) -> Optional[MarketSnapshot]:
        """
        Convert snapshot dictionary to MarketSnapshot object using unified schema.

        Args:
            snapshot_data: Dictionary containing snapshot data from Virginia

        Returns:
            MarketSnapshot object or None if conversion fails
        """
        try:
            # Use the unified conversion function designed for Virginia's optimized format
            # This handles both Kalshi and Polymarket sources automatically
            snapshot = create_snapshot_from_virginia_data(snapshot_data)

            self.logger.debug(f"Successfully converted {snapshot.source} snapshot for {snapshot.ticker}")
            return snapshot

        except Exception as e:
            self.logger.error(f"Error converting snapshot data: {e}")
            self.logger.debug(f"Snapshot data that failed conversion: {snapshot_data}")
            return None

    def _convert_trade_ticket_data(self, trade_data: Dict[str, Any]) -> Optional[TradeTicket]:
        """Convert trade dictionary to TradeTicket object"""
        try:
            from models.postgres import TradeVenue, TradeSide, ArbitrageType, TradeStatus

            # Convert enum fields
            venue = TradeVenue(trade_data.get('venue', 'kalshi'))
            side = TradeSide(trade_data.get('side', 'buy'))
            arbitrage_type = ArbitrageType(trade_data.get('arbitrage_type', 'kalshi_yes_polymarket_no'))
            status = TradeStatus(trade_data.get('status', 'pending'))

            # Create trade ticket
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

            # Set analyzed snapshot IDs
            if 'analyzed_snapshot_ids' in trade_data:
                trade_ticket.analyzed_snapshot_ids = trade_data['analyzed_snapshot_ids']

            # Set executed snapshot ID
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

            # Convert enum fields
            arbitrage_type = ArbitrageType(analysis_data.get('arbitrage_type', 'kalshi_yes_polymarket_no'))

            # Create analysis record
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

            # Set analyzed snapshot IDs
            if 'analyzed_snapshot_ids' in analysis_data:
                analysis_record.analyzed_snapshot_ids = analysis_data['analyzed_snapshot_ids']

            return analysis_record

        except Exception as e:
            self.logger.error(f"Error converting analysis record data: {e}")
            return None

    def _convert_arbitrage_pair_data(self, pair_data: Dict[str, Any]) -> Optional[ArbitragePair]:
        """Convert arbitrage pair dictionary to ArbitragePair object"""
        try:
            # Create arbitrage pair
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

        # Calculate average latency
        if self.processing_latencies:
            self.stats['avg_processing_latency_ms'] = sum(self.processing_latencies) / len(self.processing_latencies)

    async def _health_monitor_loop(self):
        """Background health monitoring loop"""
        self.logger.info("Health monitor started")

        while not self.shutdown_requested:
            try:
                current_time = time.time()

                # Check if it's time for health check
                if current_time - self.last_health_check >= self.health_check_interval:
                    await self._perform_health_check()

                await asyncio.sleep(10)  # Check every 10 seconds

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

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive coordinator status"""
        current_time = time.time()
        uptime = current_time - self.start_time

        # Calculate rates
        message_rate = self.stats['messages_processed'] / max(uptime, 1)

        return {
            'is_running': self.is_running,
            'uptime_hours': uptime / 3600,
            'coordinator_stats': self.stats.copy(),
            'performance': {
                'message_rate_per_second': message_rate,
                'avg_processing_latency_ms': self.stats['avg_processing_latency_ms'],
                'recent_latencies_ms': self.processing_latencies[-5:] if self.processing_latencies else []
            },
            'components': {
                'receiver': self.receiver.get_status() if self.receiver else {},
                'influx_writer': self.influx_writer.get_status() if self.influx_writer else {},
                'postgres_writer': self.postgres_writer.get_status() if self.postgres_writer else {}
            }
        }

    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for monitoring"""
        current_time = time.time()

        # Determine overall health
        component_health = []

        if self.receiver:
            component_health.append(self.receiver.get_health_summary()['is_healthy'])
        if self.influx_writer:
            component_health.append(self.influx_writer.get_health_summary()['is_healthy'])
        if self.postgres_writer:
            component_health.append(self.postgres_writer.get_health_summary()['is_healthy'])

        overall_healthy = self.is_running and all(component_health) if component_health else False

        error_rate = self.stats['processing_errors'] / max(self.stats['messages_processed'], 1)

        return {
            'is_healthy': overall_healthy,
            'is_running': self.is_running,
            'component_count': len(component_health),
            'components_healthy': sum(component_health),
            'uptime_hours': (current_time - self.start_time) / 3600,
            'messages_processed': self.stats['messages_processed'],
            'error_rate_percent': error_rate * 100,
            'last_message_time': self.stats['last_message_time']
        }


# Signal handling for graceful shutdown
def setup_signal_handlers(coordinator: DataServerCoordinator):
    """Setup signal handlers for graceful shutdown"""

    def signal_handler(signum, frame):
        signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        coordinator.logger.info(f"Received {signal_name}, initiating graceful shutdown...")
        coordinator.shutdown_requested = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


# Demo and testing
if __name__ == "__main__":
    import asyncio


    async def test_coordinator():
        """Test coordinator functionality"""
        print("TESTING DATA SERVER COORDINATOR")
        print("=" * 50)

        coordinator = DataServerCoordinator()
        setup_signal_handlers(coordinator)

        try:
            await coordinator.run_forever()
        except KeyboardInterrupt:
            print("\nTest interrupted by user")


    asyncio.run(test_coordinator())