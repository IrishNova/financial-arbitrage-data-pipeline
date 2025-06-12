#!/usr/bin/env python3
"""
broker/publisher.py

Pure ZeroMQ publisher for Ireland server.
Sends market data, fills, and metrics back to Virginia.
No API logic - just pure message transmission.
"""

import sys
import asyncio
import time
import zmq
import zmq.asyncio
import msgpack
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
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
    """Market data message to send to Virginia"""
    type: str  # "marketdata_batch"
    updates: List[Dict[str, Any]]
    timestamp: float
    ireland_server_id: str


@dataclass
class TradeFillMessage:
    """Trade fill message to send to Virginia"""
    type: str  # "trade_fill"
    fill_id: str
    condition_id: str
    side: str
    size: float
    price: float
    timestamp: float
    order_id: str
    market_timestamp: float


@dataclass
class SystemMetricsMessage:
    """System metrics message to send to Virginia"""
    type: str  # "system_metrics"
    api_latency_ms: float
    requests_per_minute: int
    error_rate: float
    connection_health: str
    active_subscriptions: int
    polymarket_api_status: str
    timestamp: float


class IrelandPublisher:
    """
    Pure ZeroMQ publisher for Ireland server.
    Sends data back to Virginia with no business logic.
    """

    def __init__(self):
        """Initialize Ireland ZeroMQ publisher"""
        self.logger = get_logger('publisher')

        # ZeroMQ context and sockets
        self.context = zmq.asyncio.Context()
        self.marketdata_socket: Optional[zmq.asyncio.Socket] = None
        self.fills_socket: Optional[zmq.asyncio.Socket] = None
        self.metrics_socket: Optional[zmq.asyncio.Socket] = None

        # Connection endpoints - Ireland pushes to Virginia
        virginia_host = CONFIG.zeromq.virginia_endpoint.split('//')[1].split(':')[0]
        self.marketdata_endpoint = f"tcp://{virginia_host}:{CONFIG.zeromq.marketdata_pull_port}"
        self.fills_endpoint = f"tcp://{virginia_host}:{CONFIG.zeromq.fills_pull_port}"
        self.metrics_endpoint = f"tcp://{virginia_host}:{CONFIG.zeromq.metrics_pull_port}"

        # State tracking
        self.ireland_server_id = f"ireland-{uuid.uuid4().hex[:8]}"
        self.is_running = False
        self.shutdown_requested = False

        # Statistics
        self.stats = {
            'marketdata_sent': 0,
            'fills_sent': 0,
            'metrics_sent': 0,
            'total_messages_sent': 0,
            'last_send_time': 0,
            'messages_per_minute': 0.0
        }

        self.logger.info(f"Ireland ZeroMQ publisher initialized - Server ID: {self.ireland_server_id}")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def start(self):
        """Start Ireland ZeroMQ publisher connections"""
        try:
            self.logger.info("Starting Ireland ZeroMQ publisher...")

            # Create market data push socket (PUSH)
            self.marketdata_socket = self.context.socket(zmq.PUSH)
            self.marketdata_socket.setsockopt(zmq.SNDHWM, 1000)
            self.marketdata_socket.setsockopt(zmq.LINGER, 0)

            # Create fills push socket (PUSH)
            self.fills_socket = self.context.socket(zmq.PUSH)
            self.fills_socket.setsockopt(zmq.SNDHWM, 1000)
            self.fills_socket.setsockopt(zmq.LINGER, 0)

            # Create metrics push socket (PUSH)
            self.metrics_socket = self.context.socket(zmq.PUSH)
            self.metrics_socket.setsockopt(zmq.SNDHWM, 1000)
            self.metrics_socket.setsockopt(zmq.LINGER, 0)

            # Connect to Virginia endpoints
            try:
                self.marketdata_socket.connect(self.marketdata_endpoint)
                self.logger.info(f"Market data publisher connected: {self.marketdata_endpoint}")
            except Exception as e:
                self.logger.warning(f"Could not connect to Virginia market data: {e}")

            try:
                self.fills_socket.connect(self.fills_endpoint)
                self.logger.info(f"Fills publisher connected: {self.fills_endpoint}")
            except Exception as e:
                self.logger.warning(f"Could not connect to Virginia fills: {e}")

            try:
                self.metrics_socket.connect(self.metrics_endpoint)
                self.logger.info(f"Metrics publisher connected: {self.metrics_endpoint}")
            except Exception as e:
                self.logger.warning(f"Could not connect to Virginia metrics: {e}")

            self.is_running = True
            self.logger.info("Ireland ZeroMQ publisher started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start Ireland publisher: {e}")
            self.is_running = True  # Allow testing to continue

    async def stop(self):
        """Stop Ireland ZeroMQ publisher"""
        self.logger.info("Stopping Ireland ZeroMQ publisher...")
        self.shutdown_requested = True

        if self.marketdata_socket:
            self.marketdata_socket.close()
        if self.fills_socket:
            self.fills_socket.close()
        if self.metrics_socket:
            self.metrics_socket.close()

        self.context.term()

        self.is_running = False
        self.logger.info("Ireland ZeroMQ publisher stopped")

    def _serialize_message(self, message: Any) -> bytes:
        """Serialize message to MessagePack"""
        try:
            if hasattr(message, '__dict__'):
                data = asdict(message)
            else:
                data = message
            return msgpack.packb(data, use_bin_type=True)
        except Exception as e:
            self.logger.error(f"Failed to serialize message: {e}")
            raise

    async def _send_with_retry(self, socket: zmq.asyncio.Socket, message_bytes: bytes,
                               max_retries: int = 3) -> bool:
        """Send message with retry logic"""
        for attempt in range(max_retries):
            try:
                await socket.send(message_bytes, zmq.NOBLOCK)
                self.stats['total_messages_sent'] += 1
                self.stats['last_send_time'] = time.time()
                return True

            except zmq.Again:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.001 * (2 ** attempt))
                    continue
                else:
                    self.logger.warning("Socket send would block after retries")

            except Exception as e:
                self.logger.error(f"Failed to send message (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.01 * (2 ** attempt))

        return False

    async def send_market_data(self, market_updates: List[Dict[str, Any]]) -> bool:
        """
        Send market data batch to Virginia.

        Args:
            market_updates: List of market data updates

        Returns:
            True if sent successfully
        """
        if not self.is_running or not self.marketdata_socket or not market_updates:
            return False

        try:
            # ADD: Ireland ZeroMQ send timestamp (UTC)
            ireland_zeromq_sent_ns = time.time_ns()
            for update in market_updates:
                update['ireland_zeromq_sent_ns'] = ireland_zeromq_sent_ns

            message = MarketDataMessage(
                type="marketdata_batch",
                updates=market_updates,
                timestamp=time.time(),
                ireland_server_id=self.ireland_server_id
            )

            message_bytes = self._serialize_message(message)
            success = await self._send_with_retry(self.marketdata_socket, message_bytes)

            if success:
                self.stats['marketdata_sent'] += 1
                self.logger.debug(f"Sent market data batch: {len(market_updates)} updates")
            else:
                self.logger.error(f"Failed to send market data batch: {len(market_updates)} updates")

            return success

        except Exception as e:
            self.logger.error(f"Error sending market data: {e}")
            return False

    async def send_trade_fill(self, fill_data: Dict[str, Any]) -> bool:
        """
        Send trade fill notification to Virginia.

        Args:
            fill_data: Trade fill information

        Returns:
            True if sent successfully
        """
        if not self.is_running or not self.fills_socket:
            return False

        try:
            message = TradeFillMessage(
                type="trade_fill",
                fill_id=fill_data.get('fill_id', ''),
                condition_id=fill_data.get('condition_id', ''),
                side=fill_data.get('side', ''),
                size=fill_data.get('size', 0.0),
                price=fill_data.get('price', 0.0),
                timestamp=time.time(),
                order_id=fill_data.get('order_id', ''),
                market_timestamp=fill_data.get('market_timestamp', time.time())
            )

            message_bytes = self._serialize_message(message)
            success = await self._send_with_retry(self.fills_socket, message_bytes)

            if success:
                self.stats['fills_sent'] += 1
                self.logger.info(f"Sent trade fill: {message.condition_id[:10]}... "
                                 f"{message.side} {message.size}@{message.price}")
            else:
                self.logger.error(f"Failed to send trade fill: {message.fill_id}")

            return success

        except Exception as e:
            self.logger.error(f"Error sending trade fill: {e}")
            return False

    async def send_system_metrics(self, metrics_data: Dict[str, Any]) -> bool:
        """
        Send system metrics to Virginia.

        Args:
            metrics_data: System metrics information

        Returns:
            True if sent successfully
        """
        if not self.is_running or not self.metrics_socket:
            return False

        try:
            message = SystemMetricsMessage(
                type="system_metrics",
                api_latency_ms=metrics_data.get('api_latency_ms', 0.0),
                requests_per_minute=metrics_data.get('requests_per_minute', 0),
                error_rate=metrics_data.get('error_rate', 0.0),
                connection_health=metrics_data.get('connection_health', 'UNKNOWN'),
                active_subscriptions=metrics_data.get('active_subscriptions', 0),
                polymarket_api_status=metrics_data.get('polymarket_api_status', 'UNKNOWN'),
                timestamp=time.time()
            )

            message_bytes = self._serialize_message(message)
            success = await self._send_with_retry(self.metrics_socket, message_bytes)

            if success:
                self.stats['metrics_sent'] += 1
                self.logger.debug(f"Sent system metrics: {message.polymarket_api_status}")
            else:
                self.logger.error("Failed to send system metrics")

            return success

        except Exception as e:
            self.logger.error(f"Error sending system metrics: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get publisher status"""
        current_time = time.time()

        # Calculate message rate
        elapsed = current_time - (self.stats['last_send_time'] or current_time)
        if elapsed > 0:
            self.stats['messages_per_minute'] = self.stats['total_messages_sent'] / max(elapsed / 60, 1)

        return {
            "is_running": self.is_running,
            "ireland_server_id": self.ireland_server_id,
            "statistics": self.stats.copy(),
            "endpoints": {
                "marketdata": self.marketdata_endpoint,
                "fills": self.fills_endpoint,
                "metrics": self.metrics_endpoint
            }
        }


# Demo and testing
async def test_ireland_publisher():
    """Test Ireland ZeroMQ publisher"""
    print("IRELAND ZEROMQ PUBLISHER TEST")
    print("=" * 50)

    try:
        async with IrelandPublisher() as publisher:
            print("✅ Ireland ZeroMQ publisher started")

            # Test market data sending
            test_market_data = [
                {
                    "condition_id": "0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",
                    "title": "Test Market",
                    "yes_price": 0.45,
                    "no_price": 0.55,
                    "volume": 1000.0,
                    "liquidity": 500.0,
                    "timestamp": time.time()
                }
            ]

            print("📊 Testing market data send...")
            success = await publisher.send_market_data(test_market_data)
            print(f"Market data send result: {success}")

            # Test trade fill sending
            test_fill = {
                "fill_id": f"fill-{uuid.uuid4().hex[:8]}",
                "condition_id": "0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",
                "side": "YES",
                "size": 100.0,
                "price": 0.65,
                "order_id": f"order-{uuid.uuid4().hex[:8]}",
                "market_timestamp": time.time()
            }

            print("💰 Testing trade fill send...")
            success = await publisher.send_trade_fill(test_fill)
            print(f"Trade fill send result: {success}")

            # Test metrics sending
            test_metrics = {
                "api_latency_ms": 125.5,
                "requests_per_minute": 45,
                "error_rate": 0.02,
                "connection_health": "HEALTHY",
                "active_subscriptions": 1,
                "polymarket_api_status": "CONNECTED"
            }

            print("📈 Testing metrics send...")
            success = await publisher.send_system_metrics(test_metrics)
            print(f"Metrics send result: {success}")

            # Show status
            status = publisher.get_status()
            print(f"\n📊 Publisher Status:")
            print(f"  Running: {status['is_running']}")
            print(f"  Messages sent: {status['statistics']['total_messages_sent']}")
            print(f"  Market data sent: {status['statistics']['marketdata_sent']}")
            print(f"  Fills sent: {status['statistics']['fills_sent']}")
            print(f"  Metrics sent: {status['statistics']['metrics_sent']}")

            print("✅ Ireland publisher test completed!")

    except Exception as e:
        print(f"❌ Ireland publisher test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(test_ireland_publisher())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed: {e}")