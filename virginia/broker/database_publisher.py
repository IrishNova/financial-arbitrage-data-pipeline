#!/usr/bin/env python3
"""
broker/database_publisher.py

ZeroMQ publisher for Virginia server to Database server.
Sends market data, analysis records, and trade data for storage.
Three separate streams for different data types.
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
from utils.logger import get_logger


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
    """

    def __init__(self, verbose: bool = False):
        """Initialize Database ZeroMQ publisher"""
        self.logger = get_logger('database_publisher')
        self.verbose = verbose

        # ZeroMQ context and sockets
        self.context = zmq.asyncio.Context()
        self.market_data_socket: Optional[zmq.asyncio.Socket] = None
        self.analysis_socket: Optional[zmq.asyncio.Socket] = None
        self.trade_socket: Optional[zmq.asyncio.Socket] = None

        # Connection endpoints - Virginia pushes to Database server
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

        if self.verbose:
            self.logger.info(
                f"Database ZeroMQ publisher initialized with VERBOSE logging - Server ID: {self.virginia_server_id}")
            self.logger.info(
                f"Database endpoints: {self.market_data_endpoint}, {self.analysis_endpoint}, {self.trade_endpoint}")
        else:
            self.logger.info(f"Database ZeroMQ publisher initialized - Server ID: {self.virginia_server_id}")

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
            if self.verbose:
                self.logger.debug(f"Serializing message type: {type(message)}")

            if hasattr(message, '__dict__'):
                data = asdict(message)
                if self.verbose:
                    self.logger.debug(f"Converted dataclass to dict, keys: {list(data.keys())}")
            else:
                data = message

            # Verbose logging for market data
            if self.verbose and isinstance(data, dict) and 'snapshots' in data:
                snapshots = data['snapshots']
                self.logger.debug(f"Message contains {len(snapshots)} snapshots")
                if snapshots:
                    first_snapshot = snapshots[0]

                    # Check full_orderbook specifically
                    if 'full_orderbook' in first_snapshot:
                        full_orderbook = first_snapshot['full_orderbook']
                        if isinstance(full_orderbook, dict):
                            yes_levels = len(full_orderbook.get('yes', []))
                            no_levels = len(full_orderbook.get('no', []))
                            self.logger.debug(f"Full orderbook: {yes_levels} YES, {no_levels} NO levels")

            packed_data = msgpack.packb(data, use_bin_type=True)

            if self.verbose:
                self.logger.debug(f"MessagePack serialization successful: {len(packed_data)} bytes")

            return packed_data

        except Exception as e:
            self.logger.error(f"Failed to serialize message: {e}")
            if self.verbose:
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def _send_with_retry(self, socket: zmq.asyncio.Socket, message_bytes: bytes,
                               max_retries: int = 3) -> bool:
        """Send message with retry logic"""
        if self.verbose:
            self.logger.debug(f"Sending {len(message_bytes)} bytes with retry")

        for attempt in range(max_retries):
            try:
                await socket.send(message_bytes, zmq.NOBLOCK)

                if self.verbose:
                    self.logger.debug(f"Message sent successfully on attempt {attempt + 1}")

                self.stats['total_messages_sent'] += 1
                self.stats['last_send_time'] = time.time()
                return True

            except zmq.Again:
                if self.verbose:
                    self.logger.warning(f"Socket would block on attempt {attempt + 1}")

                if attempt < max_retries - 1:
                    sleep_time = 0.001 * (2 ** attempt)
                    await asyncio.sleep(sleep_time)
                    continue
                else:
                    self.logger.warning("Socket send would block after retries")

            except Exception as e:
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
            if self.verbose:
                self.logger.debug(f"Market data send precondition failed: "
                                  f"running={self.is_running}, socket={self.market_data_socket is not None}, "
                                  f"snapshots={bool(snapshots)}")
            return False

        try:
            # Verbose logging for first snapshot analysis
            if self.verbose and snapshots:
                first_snapshot = snapshots[0]
                self.logger.debug(f"Sending {source} batch: {len(snapshots)} snapshots")
                self.logger.debug(f"First snapshot: {first_snapshot.get('ticker', 'unknown')} "
                                  f"ID: {first_snapshot.get('snapshot_id', 'unknown')[:12]}...")

                # Check orderbook structure
                full_orderbook = first_snapshot.get('full_orderbook')
                if isinstance(full_orderbook, dict):
                    yes_levels = len(full_orderbook.get('yes', []))
                    no_levels = len(full_orderbook.get('no', []))
                    self.logger.debug(f"Orderbook depth: {yes_levels} YES, {no_levels} NO levels")

            # Add Virginia send timestamp
            virginia_sent_timestamp_ns = time.time_ns()

            # Add Virginia timestamps to each snapshot
            for snapshot in snapshots:
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

                # Log at appropriate level
                if self.verbose:
                    self.logger.debug(f"Sent {source} market data batch: {len(snapshots)} snapshots")
                elif len(snapshots) > 10:  # Only log large batches in production
                    self.logger.debug(f"Sent large {source} batch: {len(snapshots)} snapshots")
            else:
                self.logger.error(f"Failed to send {source} market data batch: {len(snapshots)} snapshots")

            return success

        except Exception as e:
            self.logger.error(f"Error sending {source} market data: {e}")
            if self.verbose:
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
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

            message_bytes = self._serialize_message(message)
            success = await self._send_with_retry(self.analysis_socket, message_bytes)

            if success:
                self.stats['analysis_data_sent'] += 1
                if self.verbose:
                    self.logger.debug(f"Sent analysis update: {snapshot_id[:8]}... "
                                      f"analyzed={analyzed}, executed={executed}")
            else:
                self.logger.error(f"Failed to send analysis update: {snapshot_id[:8]}...")

            return success

        except Exception as e:
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

            message_bytes = self._serialize_message(message)
            success = await self._send_with_retry(self.trade_socket, message_bytes)

            if success:
                self.stats['trade_data_sent'] += 1
                trade_id = trade_ticket.get('trade_id', 'unknown')
                self.logger.info(f"Sent trade data: {trade_id}")  # Always log trades
            else:
                self.logger.error(f"Failed to send trade data: {trade_ticket.get('trade_id', 'unknown')}")

            return success

        except Exception as e:
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
            "verbose_mode": self.verbose,
            "statistics": self.stats.copy(),
            "performance": {
                "messages_per_minute": messages_per_minute,
                "error_rate_percent": (self.stats['send_errors'] / max(self.stats['total_messages_sent'], 1)) * 100
            },
            "endpoints": {
                "market_data": self.market_data_endpoint,
                "analysis": self.analysis_endpoint,
                "trade": self.trade_endpoint
            }
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
            "last_send_time": self.stats.get('last_send_time', 0)
        }


# Demo and testing
async def test_database_publisher():
    """Test Database ZeroMQ publisher"""
    print("DATABASE ZEROMQ PUBLISHER TEST")
    print("=" * 50)

    try:
        async with DatabasePublisher(verbose=True) as publisher:
            print("Database ZeroMQ publisher started")

            # Test Kalshi market data sending
            kalshi_snapshots = [
                {
                    "snapshot_id": f"kalshi-{uuid.uuid4().hex[:8]}",
                    "source": "kalshi",
                    "ticker": "TEST-KALSHI",
                    "yes_bid": 0.45,
                    "yes_ask": 0.47,
                    "no_bid": 0.53,
                    "no_ask": 0.55,
                    "full_orderbook": {
                        "yes": [[45, 1000], [46, 500]],
                        "no": [[55, 800], [56, 400]]
                    },
                    "api_call_start_ns": time.time_ns() - 1000000,
                    "api_response_ns": time.time_ns() - 500000,
                    "processing_complete_ns": time.time_ns(),
                    "virginia_received_ns": time.time_ns(),
                    "virginia_enriched_ns": time.time_ns()
                }
            ]

            print("Testing Kalshi market data send...")
            success = await publisher.send_market_data("kalshi", kalshi_snapshots)
            print(f"Kalshi market data send result: {success}")

            # Show status
            status = publisher.get_status()
            print(f"\nPublisher Status:")
            print(f"  Running: {status['is_running']}")
            print(f"  Total messages sent: {status['statistics']['total_messages_sent']}")
            print(f"  Market data sent: {status['statistics']['market_data_sent']}")
            print(f"  Send errors: {status['statistics']['send_errors']}")

            print("Database publisher test completed!")

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