#!/usr/bin/env python3
"""
postgres/writer.py

High-level PostgreSQL writer for Data Server.
Handles trade tickets, analysis records, and arbitrage pair management.
"""

import asyncio
import time
import json
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_postgres_logger
from models.postgres import (
    TradeTicket, AnalysisRecord, ArbitragePair,
    get_trade_ticket_schema, get_analysis_record_schema, get_arbitrage_pair_schema
)
from postgres.client import PostgreSQLClient, PostgreSQLQueryError, PostgreSQLConnectionError


class PostgreSQLWriter:
    """
    High-level PostgreSQL writer for trade data and analysis records.

    Features:
    - Trade ticket management with snapshot correlation
    - Analysis record tracking for passed opportunities
    - Arbitrage pair configuration and performance tracking
    - Schema management and migrations
    - Robust error handling and retry logic
    """

    def __init__(self):
        """Initialize PostgreSQL writer"""
        self.logger = get_postgres_logger()

        # PostgreSQL client
        self.client = PostgreSQLClient()

        # State
        self.is_running = False
        self.shutdown_requested = False

        # Error handling
        self.max_retries = 3
        self.retry_delay_base = 1.0  # seconds

        # Performance tracking
        self.stats = {
            'trade_tickets_written': 0,
            'analysis_records_written': 0,
            'arbitrage_pairs_written': 0,
            'write_errors': 0,
            'retry_attempts': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'last_successful_write': 0,
            'last_error_time': 0,
            'avg_write_latency_ms': 0.0
        }

        # Write latency tracking
        self.write_latencies = []
        self.max_latency_samples = 50

        self.logger.info("PostgreSQL writer initialized")

    async def start(self):
        """Start the PostgreSQL writer"""
        try:
            self.logger.info("Starting PostgreSQL writer...")

            # Connect to PostgreSQL
            await self.client.connect()

            # Initialize database schema
            await self._initialize_schema()

            self.is_running = True
            self.logger.info("PostgreSQL writer started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start PostgreSQL writer: {e}")
            raise

    async def stop(self):
        """Stop the PostgreSQL writer gracefully"""
        self.logger.info("Stopping PostgreSQL writer...")
        self.shutdown_requested = True

        # Disconnect from PostgreSQL
        await self.client.disconnect()

        self.is_running = False
        self.logger.info("PostgreSQL writer stopped")

    async def _initialize_schema(self):
        """Initialize database schema with all required tables"""
        try:
            self.logger.info("Initializing PostgreSQL schema...")

            # Create trade tickets table
            trade_schema = get_trade_ticket_schema()
            await self.client.create_table_if_not_exists(trade_schema)
            self.logger.debug("Trade tickets table initialized")

            # Create analysis records table
            analysis_schema = get_analysis_record_schema()
            await self.client.create_table_if_not_exists(analysis_schema)
            self.logger.debug("Analysis records table initialized")

            # Create arbitrage pairs table
            pairs_schema = get_arbitrage_pair_schema()
            await self.client.create_table_if_not_exists(pairs_schema)
            self.logger.debug("Arbitrage pairs table initialized")

            self.logger.info("PostgreSQL schema initialization completed")

        except Exception as e:
            self.logger.error(f"Failed to initialize schema: {e}")
            raise

    async def write_trade_ticket(self, trade_ticket: TradeTicket) -> bool:
        """
        Write trade ticket to database.

        Args:
            trade_ticket: TradeTicket object to write

        Returns:
            True if write successful
        """
        write_start = time.time()

        try:
            # Convert to dictionary
            trade_data = trade_ticket.to_dict()

            # Write with retry logic
            for attempt in range(self.max_retries + 1):
                try:
                    result = await self.client.insert_record('trade_tickets', trade_data)

                    # Update statistics
                    write_duration = (time.time() - write_start) * 1000
                    self.stats['trade_tickets_written'] += 1
                    self.stats['last_successful_write'] = time.time()
                    self._track_write_latency(write_duration)

                    self.logger.debug(f"Trade ticket {trade_ticket.trade_id} written in {write_duration:.2f}ms")
                    return True

                except Exception as e:
                    self.stats['retry_attempts'] += 1

                    if attempt < self.max_retries:
                        delay = self.retry_delay_base * (2 ** attempt)  # Exponential backoff
                        self.logger.warning(
                            f"Trade ticket write attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                    else:
                        self.stats['write_errors'] += 1
                        self.stats['failed_retries'] += 1
                        self.stats['last_error_time'] = time.time()
                        self.logger.error(
                            f"Failed to write trade ticket {trade_ticket.trade_id} after {self.max_retries} retries: {e}")
                        return False

            return False

        except Exception as e:
            self.stats['write_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Unexpected error writing trade ticket: {e}")
            return False

    async def update_trade_ticket(self, trade_id: str, updates: Dict[str, Any]) -> bool:
        """
        Update an existing trade ticket.

        Args:
            trade_id: Trade ID to update
            updates: Dictionary of fields to update

        Returns:
            True if update successful
        """
        try:
            # Add updated timestamp
            updates['updated_at'] = datetime.now(timezone.utc).isoformat()

            result = await self.client.update_record(
                'trade_tickets',
                updates,
                'trade_id = $1',
                [trade_id]
            )

            self.logger.debug(f"Trade ticket {trade_id} updated: {result}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to update trade ticket {trade_id}: {e}")
            return False

    async def add_trade_fill(self, trade_id: str, quantity: float, price: float,
                             timestamp: datetime = None) -> bool:
        """
        Add a fill to an existing trade ticket.

        Args:
            trade_id: Trade ID to update
            quantity: Fill quantity
            price: Fill price
            timestamp: Fill timestamp

        Returns:
            True if successful
        """
        try:
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)

            # Get current trade ticket
            trades = await self.client.select_records(
                'trade_tickets',
                where_clause='trade_id = $1',
                where_params=[trade_id]
            )

            if not trades:
                self.logger.error(f"Trade ticket {trade_id} not found for fill update")
                return False

            trade = trades[0]

            # Calculate new execution values
            current_executed = float(trade.get('executed_quantity', 0))
            current_avg_price = float(trade.get('average_fill_price', 0))

            new_executed = current_executed + quantity

            # Calculate new average price
            if current_avg_price == 0:
                new_avg_price = price
            else:
                total_value = (current_executed * current_avg_price) + (quantity * price)
                new_avg_price = total_value / new_executed

            # Determine new status
            order_quantity = float(trade.get('quantity', 0))
            if new_executed >= order_quantity:
                new_status = 'filled'
                order_completed_at = timestamp.isoformat()
            else:
                new_status = 'partially_filled'
                order_completed_at = None

            # Prepare updates
            updates = {
                'executed_quantity': new_executed,
                'average_fill_price': new_avg_price,
                'status': new_status,
                'last_fill_at': timestamp.isoformat()
            }

            # Set first fill time if this is the first fill
            if current_executed == 0:
                updates['first_fill_at'] = timestamp.isoformat()

            # Set completion time if fully filled
            if order_completed_at:
                updates['order_completed_at'] = order_completed_at

            # Update the trade ticket
            success = await self.update_trade_ticket(trade_id, updates)

            if success:
                self.logger.info(f"Added fill to trade {trade_id}: {quantity}@{price}, new total: {new_executed}")

            return success

        except Exception as e:
            self.logger.error(f"Failed to add fill to trade {trade_id}: {e}")
            return False

    async def write_analysis_record(self, analysis_record: AnalysisRecord) -> bool:
        """
        Write analysis record to database.

        Args:
            analysis_record: AnalysisRecord object to write

        Returns:
            True if write successful
        """
        write_start = time.time()

        try:
            # Convert to dictionary
            analysis_data = analysis_record.to_dict()

            # Write with retry logic
            for attempt in range(self.max_retries + 1):
                try:
                    result = await self.client.insert_record('analysis_records', analysis_data)

                    # Update statistics
                    write_duration = (time.time() - write_start) * 1000
                    self.stats['analysis_records_written'] += 1
                    self.stats['last_successful_write'] = time.time()
                    self._track_write_latency(write_duration)

                    self.logger.debug(
                        f"Analysis record {analysis_record.analysis_id} written in {write_duration:.2f}ms")
                    return True

                except Exception as e:
                    self.stats['retry_attempts'] += 1

                    if attempt < self.max_retries:
                        delay = self.retry_delay_base * (2 ** attempt)
                        self.logger.warning(
                            f"Analysis record write attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                    else:
                        self.stats['write_errors'] += 1
                        self.stats['failed_retries'] += 1
                        self.stats['last_error_time'] = time.time()
                        self.logger.error(
                            f"Failed to write analysis record {analysis_record.analysis_id} after {self.max_retries} retries: {e}")
                        return False

            return False

        except Exception as e:
            self.stats['write_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Unexpected error writing analysis record: {e}")
            return False

    async def write_arbitrage_pair(self, arbitrage_pair: ArbitragePair) -> bool:
        """
        Write or update arbitrage pair configuration.

        Args:
            arbitrage_pair: ArbitragePair object to write

        Returns:
            True if write successful
        """
        write_start = time.time()

        try:
            # Convert to dictionary
            pair_data = arbitrage_pair.to_dict()

            # Check if pair already exists
            existing_pairs = await self.client.select_records(
                'arbitrage_pairs',
                where_clause='pair_id = $1',
                where_params=[arbitrage_pair.pair_id]
            )

            if existing_pairs:
                # Update existing pair
                # Remove fields that shouldn't be updated
                update_data = pair_data.copy()
                update_data.pop('pair_id', None)
                update_data.pop('created_at', None)

                result = await self.client.update_record(
                    'arbitrage_pairs',
                    update_data,
                    'pair_id = $1',
                    [arbitrage_pair.pair_id]
                )

                operation = "updated"
            else:
                # Insert new pair
                result = await self.client.insert_record('arbitrage_pairs', pair_data)
                operation = "inserted"

            # Update statistics
            write_duration = (time.time() - write_start) * 1000
            self.stats['arbitrage_pairs_written'] += 1
            self.stats['last_successful_write'] = time.time()
            self._track_write_latency(write_duration)

            self.logger.debug(f"Arbitrage pair {arbitrage_pair.pair_id} {operation} in {write_duration:.2f}ms")
            return True

        except Exception as e:
            self.stats['write_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Failed to write arbitrage pair {arbitrage_pair.pair_id}: {e}")
            return False

    async def update_arbitrage_pair_performance(self, pair_id: str, trade_profit: float,
                                                trade_volume: float) -> bool:
        """
        Update performance metrics for an arbitrage pair.

        Args:
            pair_id: Arbitrage pair ID
            trade_profit: Profit from the trade
            trade_volume: Volume of the trade

        Returns:
            True if update successful
        """
        try:
            # Get current pair data
            pairs = await self.client.select_records(
                'arbitrage_pairs',
                where_clause='pair_id = $1',
                where_params=[pair_id]
            )

            if not pairs:
                self.logger.error(f"Arbitrage pair {pair_id} not found for performance update")
                return False

            pair = pairs[0]

            # Calculate new performance metrics
            current_trades = int(pair.get('total_trades', 0))
            current_volume = float(pair.get('total_volume', 0))
            current_profit = float(pair.get('total_profit', 0))

            new_trades = current_trades + 1
            new_volume = current_volume + trade_volume
            new_profit = current_profit + trade_profit
            new_avg_profit = new_profit / new_trades if new_trades > 0 else 0

            # Update the pair
            updates = {
                'total_trades': new_trades,
                'total_volume': new_volume,
                'total_profit': new_profit,
                'average_profit_per_trade': new_avg_profit,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }

            result = await self.client.update_record(
                'arbitrage_pairs',
                updates,
                'pair_id = $1',
                [pair_id]
            )

            self.logger.debug(
                f"Updated performance for pair {pair_id}: {new_trades} trades, ${new_profit:.2f} total profit")
            return True

        except Exception as e:
            self.logger.error(f"Failed to update pair performance for {pair_id}: {e}")
            return False

    async def get_trade_tickets_by_snapshot(self, snapshot_id: str) -> List[Dict[str, Any]]:
        """
        Get all trade tickets that used a specific snapshot.

        Args:
            snapshot_id: Snapshot ID to search for

        Returns:
            List of trade ticket records
        """
        try:
            # Search in both analyzed_snapshot_ids and executed_snapshot_id
            query = """
                SELECT * FROM trade_tickets 
                WHERE executed_snapshot_id = $1 
                   OR analyzed_snapshot_ids::text LIKE $2
                ORDER BY created_at DESC
            """

            like_pattern = f'%"{snapshot_id}"%'

            trades = await self.client.execute_query(query, [snapshot_id, like_pattern])

            return [dict(trade) for trade in trades]

        except Exception as e:
            self.logger.error(f"Failed to get trade tickets for snapshot {snapshot_id}: {e}")
            return []

    async def get_arbitrage_pair_performance(self, pair_id: str) -> Optional[Dict[str, Any]]:
        """
        Get performance metrics for an arbitrage pair.

        Args:
            pair_id: Arbitrage pair ID

        Returns:
            Performance metrics dictionary or None if not found
        """
        try:
            pairs = await self.client.select_records(
                'arbitrage_pairs',
                where_clause='pair_id = $1',
                where_params=[pair_id]
            )

            if pairs:
                return dict(pairs[0])
            else:
                return None

        except Exception as e:
            self.logger.error(f"Failed to get performance for pair {pair_id}: {e}")
            return None

    async def get_recent_analysis_records(self, hours: int = 24, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent analysis records.

        Args:
            hours: Number of hours to look back
            limit: Maximum number of records to return

        Returns:
            List of analysis records
        """
        try:
            query = """
                SELECT * FROM analysis_records 
                WHERE analyzed_at >= NOW() - INTERVAL '%s hours'
                ORDER BY analyzed_at DESC
                LIMIT %s
            """

            records = await self.client.execute_query(query, [hours, limit])
            return [dict(record) for record in records]

        except Exception as e:
            self.logger.error(f"Failed to get recent analysis records: {e}")
            return []

    async def get_trade_summary_stats(self, days: int = 7) -> Dict[str, Any]:
        """
        Get summary statistics for trades.

        Args:
            days: Number of days to look back

        Returns:
            Dictionary with trade statistics
        """
        try:
            query = """
                SELECT 
                    COUNT(*) as total_trades,
                    COUNT(CASE WHEN status = 'filled' THEN 1 END) as filled_trades,
                    COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_trades,
                    SUM(executed_quantity * COALESCE(average_fill_price, 0)) as total_volume,
                    AVG(COALESCE(average_fill_price, 0)) as avg_fill_price,
                    SUM(COALESCE(expected_profit, 0)) as expected_profit_total,
                    AVG(COALESCE(expected_profit, 0)) as avg_expected_profit
                FROM trade_tickets 
                WHERE created_at >= NOW() - INTERVAL '%s days'
            """

            result = await self.client.execute_query(query, [days])

            if result:
                stats = dict(result[0])
                # Convert Decimal types to float for JSON serialization
                for key, value in stats.items():
                    if hasattr(value, '__float__'):
                        stats[key] = float(value)
                return stats
            else:
                return {}

        except Exception as e:
            self.logger.error(f"Failed to get trade summary stats: {e}")
            return {}

    def _track_write_latency(self, latency_ms: float):
        """Track write latency for performance monitoring"""
        self.write_latencies.append(latency_ms)
        if len(self.write_latencies) > self.max_latency_samples:
            self.write_latencies = self.write_latencies[-self.max_latency_samples:]

        # Calculate average latency
        if self.write_latencies:
            self.stats['avg_write_latency_ms'] = sum(self.write_latencies) / len(self.write_latencies)

    def get_status(self) -> Dict[str, Any]:
        """Get writer status and statistics"""
        current_time = time.time()

        # Calculate rates
        uptime = current_time - (self.stats.get('start_time', current_time))
        write_rate = sum([
            self.stats['trade_tickets_written'],
            self.stats['analysis_records_written'],
            self.stats['arbitrage_pairs_written']
        ]) / max(uptime, 1)

        return {
            'is_running': self.is_running,
            'client_status': self.client.get_connection_status(),
            'statistics': self.stats.copy(),
            'performance': {
                'write_rate_per_second': write_rate,
                'avg_write_latency_ms': self.stats['avg_write_latency_ms'],
                'recent_latencies_ms': self.write_latencies[-5:] if self.write_latencies else []
            }
        }

    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for monitoring"""
        current_time = time.time()

        # Determine health status
        is_healthy = (
                self.is_running and
                self.client.is_connected and
                (current_time - self.stats.get('last_successful_write', current_time)) < 300  # 5 minutes
        )

        error_rate = self.stats['write_errors'] / max(sum([
            self.stats['trade_tickets_written'],
            self.stats['analysis_records_written'],
            self.stats['arbitrage_pairs_written']
        ]), 1)

        return {
            'is_healthy': is_healthy,
            'is_running': self.is_running,
            'client_connected': self.client.is_connected,
            'error_rate_percent': error_rate * 100,
            'last_successful_write': self.stats.get('last_successful_write', 0),
            'last_error_time': self.stats.get('last_error_time', 0),
            'total_writes': sum([
                self.stats['trade_tickets_written'],
                self.stats['analysis_records_written'],
                self.stats['arbitrage_pairs_written']
            ])
        }


# Demo and testing
async def test_postgres_writer():
    """Test PostgreSQL writer functionality"""
    print("TESTING POSTGRESQL WRITER")
    print("=" * 50)

    try:
        # Initialize writer
        writer = PostgreSQLWriter()
        await writer.start()
        print("✅ PostgreSQL writer started")

        # Test trade ticket write
        from models.postgres import TradeTicket, TradeVenue, TradeSide, ArbitrageType

        trade_ticket = TradeTicket(
            kalshi_ticker="TEST-KALSHI",
            polymarket_condition_id="0x1234567890abcdef",
            pair_id="test_pair_1",
            market_title="Test Market",
            venue=TradeVenue.KALSHI,
            side=TradeSide.BUY,
            outcome="YES",
            quantity=100.0,
            arbitrage_type=ArbitrageType.KALSHI_YES_POLYMARKET_NO,
            expected_profit=15.50
        )

        # Add analyzed snapshots
        trade_ticket.add_analyzed_snapshot("snapshot_123")
        trade_ticket.add_analyzed_snapshot("snapshot_124")
        trade_ticket.set_executed_snapshot("snapshot_124")

        print("Testing trade ticket write...")
        success = await writer.write_trade_ticket(trade_ticket)
        print(f"✅ Trade ticket write: {success}")

        # Test trade fill
        print("Testing trade fill...")
        success = await writer.add_trade_fill(trade_ticket.trade_id, 100.0, 0.65)
        print(f"✅ Trade fill: {success}")

        # Test analysis record write
        from models.postgres import AnalysisRecord

        analysis_record = AnalysisRecord(
            kalshi_ticker="TEST-KALSHI",
            polymarket_condition_id="0x1234567890abcdef",
            pair_id="test_pair_1",
            arbitrage_type=ArbitrageType.KALSHI_YES_POLYMARKET_NO,
            potential_profit=8.25,
            rejection_reason="spread_too_small"
        )

        analysis_record.add_analyzed_snapshot("snapshot_125")

        print("Testing analysis record write...")
        success = await writer.write_analysis_record(analysis_record)
        print(f"✅ Analysis record write: {success}")

        # Test arbitrage pair write
        from models.postgres import ArbitragePair

        arbitrage_pair = ArbitragePair(
            kalshi_ticker="TEST-KALSHI",
            polymarket_condition_id="0x1234567890abcdef",
            market_title="Test Market",
            min_spread_bps=100.0
        )

        print("Testing arbitrage pair write...")
        success = await writer.write_arbitrage_pair(arbitrage_pair)
        print(f"✅ Arbitrage pair write: {success}")

        # Test performance update
        print("Testing performance update...")
        success = await writer.update_arbitrage_pair_performance(
            arbitrage_pair.pair_id, 15.50, 100.0
        )
        print(f"✅ Performance update: {success}")

        # Test queries
        print("Testing snapshot lookup...")
        trades = await writer.get_trade_tickets_by_snapshot("snapshot_124")
        print(f"✅ Snapshot lookup: {len(trades)} trades found")

        print("Testing trade stats...")
        stats = await writer.get_trade_summary_stats(7)
        print(f"✅ Trade stats: {stats.get('total_trades', 0)} trades")

        # Show status
        status = writer.get_status()
        print(f"\nWriter Status:")
        print(f"  Running: {status['is_running']}")
        print(f"  Trade Tickets Written: {status['statistics']['trade_tickets_written']}")
        print(f"  Analysis Records Written: {status['statistics']['analysis_records_written']}")
        print(f"  Arbitrage Pairs Written: {status['statistics']['arbitrage_pairs_written']}")
        print(f"  Avg Latency: {status['performance']['avg_write_latency_ms']:.2f}ms")

        # Show health
        health = writer.get_health_summary()
        print(f"\nHealth Summary:")
        print(f"  Healthy: {health['is_healthy']}")
        print(f"  Total Writes: {health['total_writes']}")
        print(f"  Error Rate: {health['error_rate_percent']:.2f}%")

        # Stop writer
        await writer.stop()
        print("✅ PostgreSQL writer stopped")

        print("\n✅ PostgreSQL writer test completed!")
        return True

    except Exception as e:
        print(f"❌ PostgreSQL writer test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(test_postgres_writer())
        if not success:
            exit(1)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test execution failed: {e}")
        exit(1)