#!/usr/bin/env python3
"""
broker/database_publisher.py

ZeroMQ publisher for Virginia server to Database server.
Sends market data, analysis records, and trade data for storage.
Three separate streams for different data types.

FIXED: Uses IntervalLogger for high-frequency data instead of flooding with DEBUG logs.
"""

import sys
import asyncio
import time
import zmq
import zmq.asyncio
import msgpack
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import uuid

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_logger, get_interval_logger


@dataclass
class MarketDataMessage:
    """Market data message to send to Database server"""
    type: str  # "market_data_batch"
    source: str  # "kalshi" or "polymarket"
    snapshots: List[Dict[str, Any]]
    virginia_sent_timestamp: float
    virginia_server_id: str
    message_id: str


@dataclass
class AnalysisDataMessage:
    """Analysis data message to send to Database server"""
    type: str  # "analysis_update"
    snapshot_id: str
    analyzed: bool
    executed: bool
    analysis_timestamp: float
    virginia_server_id: str
    message_id: str


@dataclass
class TradeDataMessage:
    """Trade data message to send to Database server"""
    type: str  # "trade_data"
    trade_ticket: Dict[str, Any]
    analysis_record: Optional[Dict[str, Any]]
    arbitrage_pair: Optional[Dict[str, Any]]
    virginia_sent_timestamp: float
    virginia_server_id: str
    message_id: str


class DatabasePublisher:
    """
    ZeroMQ publisher for Virginia → Database server communication.
    Handles three separate data streams for efficient database storage.

    FIXED: Uses IntervalLogger for high-frequency data messages.
    """

    def __init__(self):
        """Initialize Database ZeroMQ publisher"""
        # Regular logger for system events (startup, shutdown, errors)
        self.logger = get_logger('database_publisher')

        # Interval logger for high-frequency data operations
        self.interval_logger = get_interval_logger('database_publisher')

        # ZeroMQ context and sockets
        self.context = zmq.asyncio.Context()
        self.market_data_socket: Optional[zmq.asyncio.Socket] = None
        self.analysis_socket: Optional[zmq.asyncio.Socket] = None
        self.trade_socket: Optional[zmq.asyncio.Socket] = None

        # Connection endpoints - Virginia pushes to Database server
        # USE CONFIG INSTEAD OF HARDCODED VALUES
        database_host = CONFIG.zeromq.database.host
        market_data_port = CONFIG.zeromq.database.market_data_port
        analysis_data_port = CONFIG.zeromq.database.analysis_data_port
        trade_data_port = CONFIG.zeromq.database.trade_data_port

        self.market_data_endpoint = f"tcp://{database_host}:{market_data_port}"
        self.analysis_endpoint = f"tcp://{database_host}:{analysis_data_port}"
        self.trade_endpoint = f"tcp://{database_host}:{trade_data_port}"

        # State tracking
        self.virginia_server_id = f"virginia-{uuid.uuid4().hex[:8]}"
        self.is_running = False
        self.shutdown_requested = False

        # Statistics
        self.stats = {
            'market_data_sent': 0,
            'analysis_data_sent': 0,
            'trade_data_sent': 0,
            'total_messages_sent': 0,
            'last_send_time': 0,
            'send_errors': 0,
            'kalshi_snapshots_sent': 0,
            'polymarket_snapshots_sent': 0
        }

        # System startup logs (always show)
        self.logger.info(f"Database ZeroMQ publisher initialized - Server ID: {self.virginia_server_id}")
        self.logger.info(
            f"Database endpoints: {self.market_data_endpoint}, {self.analysis_endpoint}, {self.trade_endpoint}")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def start(self):
        """Start Database ZeroMQ publisher connections"""
        try:
            self.logger.info("Starting Database ZeroMQ publisher...")

            # Create market data push socket (PUSH)
            self.market_data_socket = self.context.socket(zmq.PUSH)
            self.market_data_socket.setsockopt(zmq.SNDHWM, 1000)
            self.market_data_socket.setsockopt(zmq.LINGER, 0)

            # Create analysis push socket (PUSH)
            self.analysis_socket = self.context.socket(zmq.PUSH)
            self.analysis_socket.setsockopt(zmq.SNDHWM, 1000)
            self.analysis_socket.setsockopt(zmq.LINGER, 0)

            # Create trade push socket (PUSH)
            self.trade_socket = self.context.socket(zmq.PUSH)
            self.trade_socket.setsockopt(zmq.SNDHWM, 1000)
            self.trade_socket.setsockopt(zmq.LINGER, 0)

            # Connect to Database server endpoints
            try:
                self.market_data_socket.connect(self.market_data_endpoint)
                self.logger.info(f"Market data publisher connected: {self.market_data_endpoint}")
            except Exception as e:
                self.logger.warning(f"Could not connect to Database market data: {e}")

            try:
                self.analysis_socket.connect(self.analysis_endpoint)
                self.logger.info(f"Analysis publisher connected: {self.analysis_endpoint}")
            except Exception as e:
                self.logger.warning(f"Could not connect to Database analysis: {e}")

            try:
                self.trade_socket.connect(self.trade_endpoint)
                self.logger.info(f"Trade publisher connected: {self.trade_endpoint}")
            except Exception as e:
                self.logger.warning(f"Could not connect to Database trade: {e}")

            self.is_running = True
            self.logger.info("Database ZeroMQ publisher started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start Database publisher: {e}")
            self.is_running = True  # Allow testing to continue

    async def stop(self):
        """Stop Database ZeroMQ publisher"""
        self.logger.info("Stopping Database ZeroMQ publisher...")
        self.shutdown_requested = True

        if self.market_data_socket:
            self.market_data_socket.close()
        if self.analysis_socket:
            self.analysis_socket.close()
        if self.trade_socket:
            self.trade_socket.close()

        self.context.term()

        self.is_running = False
        self.logger.info("Database ZeroMQ publisher stopped")

    def _serialize_message(self, message: Any) -> bytes:
        """Serialize message to MessagePack"""
        try:
            if hasattr(message, '__dict__'):
                data = asdict(message)
            else:
                data = message

            # Use interval logger for serialization details
            self.interval_logger.debug(
                'serialization',
                f'Serializing message type: {type(message).__name__}',
                {
                    'message_type': type(message).__name__,
                    'data_keys': list(data.keys()) if isinstance(data, dict) else 'not_dict',
                    'snapshots_count': len(data.get('snapshots', [])) if isinstance(data, dict) else 0,
                    'message_size_estimate': len(str(data))
                }
            )

            packed_data = msgpack.packb(data, use_bin_type=True)

            # Use interval logger for serialization success
            self.interval_logger.debug(
                'serialization_success',
                f'MessagePack serialization successful: {len(packed_data)} bytes',
                {
                    'packed_size_bytes': len(packed_data),
                    'compression_ratio': len(str(data)) / len(packed_data) if len(packed_data) > 0 else 0
                }
            )

            return packed_data

        except Exception as e:
            # Errors always show
            self.logger.error(f"Failed to serialize message: {e}")
            raise

    async def _send_with_retry(self, socket: zmq.asyncio.Socket, message_bytes: bytes,
                               max_retries: int = 3) -> bool:
        """Send message with retry logic"""
        # Use interval logger for send attempts
        self.interval_logger.debug(
            'socket_send',
            f'Attempting to send {len(message_bytes)} bytes',
            {
                'message_size_bytes': len(message_bytes),
                'max_retries': max_retries,
                'socket_type': str(socket.socket_type)
            }
        )

        for attempt in range(max_retries):
            try:
                await socket.send(message_bytes, zmq.NOBLOCK)

                # Use interval logger for send success
                self.interval_logger.debug(
                    'socket_send_success',
                    f'Message sent successfully on attempt {attempt + 1}',
                    {
                        'attempt_number': attempt + 1,
                        'message_size_bytes': len(message_bytes),
                        'total_messages_sent': self.stats['total_messages_sent'] + 1
                    }
                )

                self.stats['total_messages_sent'] += 1
                self.stats['last_send_time'] = time.time()
                return True

            except zmq.Again:
                # Socket blocking - use interval logger
                self.interval_logger.debug(
                    'socket_block',
                    f'Socket would block on attempt {attempt + 1}',
                    {
                        'attempt_number': attempt + 1,
                        'max_retries': max_retries,
                        'will_retry': attempt < max_retries - 1
                    }
                )

                if attempt < max_retries - 1:
                    sleep_time = 0.001 * (2 ** attempt)
                    await asyncio.sleep(sleep_time)
                    continue
                else:
                    self.logger.warning("Socket send would block after retries")

            except Exception as e:
                # Errors always show
                self.logger.error(f"Failed to send message (attempt {attempt + 1}): {e}")
                self.stats['send_errors'] += 1
                if attempt < max_retries - 1:
                    sleep_time = 0.01 * (2 ** attempt)
                    await asyncio.sleep(sleep_time)

        return False

    async def send_market_data(self, source: str, snapshots: List[Dict[str, Any]]) -> bool:
        """
        Send market data batch to Database server.

        Args:
            source: "kalshi" or "polymarket"
            snapshots: List of market data snapshots

        Returns:
            True if sent successfully
        """
        if not self.is_running or not self.market_data_socket or not snapshots:
            return False

        try:
            # Use interval logger for market data processing
            first_snapshot = snapshots[0] if snapshots else {}

            self.interval_logger.debug(
                'market_data_processing',
                f'Processing {source} market data batch: {len(snapshots)} snapshots',
                {
                    'source': source,
                    'snapshots_count': len(snapshots),
                    'first_snapshot': {
                        'snapshot_id': first_snapshot.get('snapshot_id', 'missing'),
                        'ticker': first_snapshot.get('ticker', 'missing'),
                        'yes_bid': first_snapshot.get('yes_bid'),
                        'yes_ask': first_snapshot.get('yes_ask'),
                        'no_bid': first_snapshot.get('no_bid'),
                        'no_ask': first_snapshot.get('no_ask'),
                        'full_orderbook_keys': list(first_snapshot.get('full_orderbook', {}).keys()) if isinstance(
                            first_snapshot.get('full_orderbook'), dict) else 'not_dict',
                        'full_orderbook_size': len(str(first_snapshot.get('full_orderbook', {}))),
                        'virginia_received_ns': first_snapshot.get('virginia_received_ns'),
                        'processing_timestamp': first_snapshot.get('processing_timestamp')
                    }
                }
            )

            # Add Virginia send timestamp (UTC)
            virginia_sent_timestamp_ns = time.time_ns()

            # Add Virginia timestamps to each snapshot
            for i, snapshot in enumerate(snapshots):
                snapshot['virginia_sent_to_data_server_ns'] = virginia_sent_timestamp_ns

            message = MarketDataMessage(
                type="market_data_batch",
                source=source,
                snapshots=snapshots,
                virginia_sent_timestamp=time.time(),
                virginia_server_id=self.virginia_server_id,
                message_id=f"market-{uuid.uuid4().hex[:12]}"
            )

            message_bytes = self._serialize_message(message)
            success = await self._send_with_retry(self.market_data_socket, message_bytes)

            if success:
                self.stats['market_data_sent'] += 1
                if source == "kalshi":
                    self.stats['kalshi_snapshots_sent'] += len(snapshots)
                elif source == "polymarket":
                    self.stats['polymarket_snapshots_sent'] += len(snapshots)

                # Use interval logger for success summary
                self.interval_logger.debug(
                    'market_data_success',
                    f'Successfully sent {source} market data batch',
                    {
                        'source': source,
                        'snapshots_sent': len(snapshots),
                        'total_market_data_sent': self.stats['market_data_sent'],
                        'total_snapshots_sent': self.stats.get(f'{source}_snapshots_sent', 0),
                        'message_id': message.message_id
                    }
                )
            else:
                # Errors always show
                self.logger.error(f"Failed to send {source} market data batch: {len(snapshots)} snapshots")

            return success

        except Exception as e:
            # Errors always show
            self.logger.error(f"Error sending {source} market data: {e}")
            self.stats['send_errors'] += 1
            return False

    async def send_analysis_data(self, snapshot_id: str, analyzed: bool = False,
                                 executed: bool = False) -> bool:
        """
        Send analysis update to Database server.

        Args:
            snapshot_id: Snapshot ID that was analyzed
            analyzed: Whether snapshot was analyzed for arbitrage
            executed: Whether snapshot was used for trade execution

        Returns:
            True if sent successfully
        """
        if not self.is_running or not self.analysis_socket:
            return False

        try:
            message = AnalysisDataMessage(
                type="analysis_update",
                snapshot_id=snapshot_id,
                analyzed=analyzed,
                executed=executed,
                analysis_timestamp=time.time(),
                virginia_server_id=self.virginia_server_id,
                message_id=f"analysis-{uuid.uuid4().hex[:12]}"
            )

            # Use interval logger for analysis data
            self.interval_logger.debug(
                'analysis_data',
                f'Sending analysis update for snapshot {snapshot_id[:8]}...',
                {
                    'snapshot_id_short': snapshot_id[:16] + "..." if len(snapshot_id) > 16 else snapshot_id,
                    'analyzed': analyzed,
                    'executed': executed,
                    'message_id': message.message_id
                }
            )

            message_bytes = self._serialize_message(message)
            success = await self._send_with_retry(self.analysis_socket, message_bytes)

            if success:
                self.stats['analysis_data_sent'] += 1
                # Use interval logger for success
                self.interval_logger.debug(
                    'analysis_success',
                    f'Analysis update sent successfully',
                    {
                        'snapshot_id_short': snapshot_id[:16] + "..." if len(snapshot_id) > 16 else snapshot_id,
                        'total_analysis_sent': self.stats['analysis_data_sent']
                    }
                )
            else:
                # Errors always show
                self.logger.error(f"Failed to send analysis update: {snapshot_id[:8]}...")

            return success

        except Exception as e:
            # Errors always show
            self.logger.error(f"Error sending analysis data: {e}")
            self.stats['send_errors'] += 1
            return False

    async def send_trade_data(self, trade_ticket: Dict[str, Any],
                              analysis_record: Optional[Dict[str, Any]] = None,
                              arbitrage_pair: Optional[Dict[str, Any]] = None) -> bool:
        """
        Send trade data to Database server.

        Args:
            trade_ticket: Trade execution data
            analysis_record: Associated analysis record
            arbitrage_pair: Arbitrage pair configuration

        Returns:
            True if sent successfully
        """
        if not self.is_running or not self.trade_socket:
            return False

        try:
            message = TradeDataMessage(
                type="trade_data",
                trade_ticket=trade_ticket,
                analysis_record=analysis_record,
                arbitrage_pair=arbitrage_pair,
                virginia_sent_timestamp=time.time(),
                virginia_server_id=self.virginia_server_id,
                message_id=f"trade-{uuid.uuid4().hex[:12]}"
            )

            trade_id = trade_ticket.get('trade_id', 'unknown')

            # Trade data is important - always log (not interval)
            self.logger.info(f"Sending trade data: {trade_id}")

            message_bytes = self._serialize_message(message)
            success = await self._send_with_retry(self.trade_socket, message_bytes)

            if success:
                self.stats['trade_data_sent'] += 1
                # Trade success is important - always log
                self.logger.info(f"Sent trade data: {trade_id}")
            else:
                # Errors always show
                self.logger.error(f"Failed to send trade data: {trade_id}")

            return success

        except Exception as e:
            # Errors always show
            self.logger.error(f"Error sending trade data: {e}")
            self.stats['send_errors'] += 1
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get publisher status"""
        current_time = time.time()

        # Calculate message rate
        elapsed = current_time - (self.stats['last_send_time'] or current_time)
        if elapsed > 0:
            messages_per_minute = self.stats['total_messages_sent'] / max(elapsed / 60, 1)
        else:
            messages_per_minute = 0.0

        return {
            "is_running": self.is_running,
            "virginia_server_id": self.virginia_server_id,
            "statistics": self.stats.copy(),
            "performance": {
                "messages_per_minute": messages_per_minute,
                "error_rate_percent": (self.stats['send_errors'] / max(self.stats['total_messages_sent'], 1)) * 100
            },
            "endpoints": {
                "market_data": self.market_data_endpoint,
                "analysis": self.analysis_endpoint,
                "trade": self.trade_endpoint
            },
            # NEW: Interval logger stats
            "interval_logging": self.interval_logger.get_stats() if hasattr(self, 'interval_logger') else None
        }

    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for monitoring"""
        current_time = time.time()

        is_healthy = (
                self.is_running and
                (current_time - self.stats.get('last_send_time', current_time)) < 300  # 5 minutes
        )

        return {
            "is_healthy": is_healthy,
            "is_running": self.is_running,
            "total_messages_sent": self.stats['total_messages_sent'],
            "kalshi_snapshots_sent": self.stats['kalshi_snapshots_sent'],
            "polymarket_snapshots_sent": self.stats['polymarket_snapshots_sent'],
            "send_errors": self.stats['send_errors'],
            "last_send_time": self.stats.get('last_send_time', 0),
            # NEW: Interval logger stats
            "interval_stats": self.interval_logger.get_stats() if hasattr(self, 'interval_logger') else None
        }


# Demo and testing
async def test_database_publisher():
    """Test Database ZeroMQ publisher with interval logging"""
    print("DATABASE ZEROMQ PUBLISHER TEST WITH INTERVAL LOGGING")
    print("=" * 60)

    try:
        async with DatabasePublisher() as publisher:
            print("Database ZeroMQ publisher started")

            # Test high-frequency market data sending to trigger interval logging
            print("Testing high-frequency market data to trigger interval logging...")

            for i in range(1200):  # Send more than LOG_SAMPLE_INTERVAL to see samples
                kalshi_snapshots = [
                    {
                        "snapshot_id": f"kalshi-{uuid.uuid4().hex[:8]}-{i}",
                        "source": "kalshi",
                        "ticker": f"TEST-KALSHI-{i}",
                        "yes_bid": 0.45 + (i * 0.001),
                        "yes_ask": 0.47 + (i * 0.001),
                        "no_bid": 0.53 - (i * 0.001),
                        "no_ask": 0.55 - (i * 0.001),
                        "full_orderbook": {
                            "yes": [[45 + i, 1000], [46 + i, 500]],
                            "no": [[55 - i, 800], [56 - i, 400]],
                            "ticker": f"TEST-KALSHI-{i}",
                            "volume": i * 10
                        },
                        "api_call_start_ns": time.time_ns() - 1000000,
                        "api_response_ns": time.time_ns() - 500000,
                        "processing_complete_ns": time.time_ns(),
                        "virginia_received_ns": time.time_ns(),
                        "virginia_enriched_ns": time.time_ns()
                    }
                ]

                success = await publisher.send_market_data("kalshi", kalshi_snapshots)

                if i % 100 == 0:
                    print(f"Sent {i} market data batches...")

            # Show final status
            status = publisher.get_status()
            print(f"\nFinal Publisher Status:")
            print(f"  Running: {status['is_running']}")
            print(f"  Total messages sent: {status['statistics']['total_messages_sent']}")
            print(f"  Market data sent: {status['statistics']['market_data_sent']}")
            print(f"  Send errors: {status['statistics']['send_errors']}")

            if status.get('interval_logging'):
                print(f"  Interval logging stats: {status['interval_logging']}")

            print("Database publisher interval logging test completed!")

    except Exception as e:
        print(f"Database publisher test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(test_database_publisher())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed: {e}")