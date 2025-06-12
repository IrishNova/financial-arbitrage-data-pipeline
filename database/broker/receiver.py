#!/usr/bin/env python3
"""
broker/receiver.py

ZeroMQ receiver for Data Server.
Receives market data, analysis records, and trade data from Virginia server.
"""

import asyncio
import time
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from datetime import datetime
import json

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_broker_logger

# ZeroMQ imports
try:
    import zmq
    import zmq.asyncio
    import msgpack

    ZMQ_AVAILABLE = True
except ImportError:
    ZMQ_AVAILABLE = False
    print("WARNING: pyzmq not installed. Install with: pip install pyzmq")

# MessagePack imports
try:
    import msgpack

    MSGPACK_AVAILABLE = True
except ImportError:
    MSGPACK_AVAILABLE = False
    print("WARNING: msgpack not installed. Install with: pip install msgpack")


@dataclass
class MarketDataMessage:
    """Market data message from Virginia"""
    message_type: str  # "market_data"
    snapshots: List[Dict[str, Any]]
    virginia_sent_timestamp: float
    message_id: str


@dataclass
class AnalysisMessage:
    """Analysis message from Virginia"""
    message_type: str  # "analysis_update"
    snapshot_id: str
    analyzed: bool
    executed: bool
    analysis_timestamp: float
    message_id: str


@dataclass
class TradeMessage:
    """Trade data message from Virginia"""
    message_type: str  # "trade_data"
    trade_ticket: Dict[str, Any]
    analysis_record: Optional[Dict[str, Any]]
    arbitrage_pair: Optional[Dict[str, Any]]
    virginia_sent_timestamp: float
    message_id: str


class ZeroMQReceiver:
    """
    ZeroMQ receiver for Data Server.

    Receives three types of messages from Virginia:
    1. Market Data - Raw market snapshots for storage
    2. Analysis Updates - Usage flag updates for snapshot correlation
    3. Trade Data - Trade tickets, analysis records, arbitrage pairs
    """

    def __init__(self, verbose: bool = False):
        """Initialize ZeroMQ receiver"""
        self.logger = get_broker_logger()
        self.verbose = verbose

        # Check dependencies
        if not ZMQ_AVAILABLE:
            self.logger.error("ZeroMQ not available")
            return
        if not MSGPACK_AVAILABLE:
            self.logger.error("MessagePack not available")
            return

        # ZeroMQ context and sockets
        self.context = zmq.asyncio.Context()
        self.market_data_socket: Optional[zmq.asyncio.Socket] = None
        self.analysis_socket: Optional[zmq.asyncio.Socket] = None
        self.trade_socket: Optional[zmq.asyncio.Socket] = None

        # Receiver settings
        self.recv_timeout = CONFIG.zeromq.recv_timeout
        self.recv_hwm = CONFIG.zeromq.recv_hwm

        # State tracking
        self.is_running = False
        self.shutdown_requested = False

        # Message callbacks
        self.market_data_callbacks: List[Callable[[MarketDataMessage], None]] = []
        self.analysis_callbacks: List[Callable[[AnalysisMessage], None]] = []
        self.trade_callbacks: List[Callable[[TradeMessage], None]] = []

        # Performance metrics
        self.stats = {
            'total_messages_received': 0,
            'market_data_messages': 0,
            'analysis_messages': 0,
            'trade_messages': 0,
            'message_errors': 0,
            'last_message_time': 0,
            'connection_attempts': 0,
            'successful_connections': 0,
            'avg_message_latency_ms': 0.0,
            'kalshi_snapshots_received': 0,
            'polymarket_snapshots_received': 0,
            'raw_message_bytes_received': 0
        }

        # Message latency tracking
        self.message_latencies = []
        self.max_latency_samples = 100

        # Initialize with appropriate logging level
        if self.verbose:
            self.logger.info("ZeroMQ receiver initialized with VERBOSE LOGGING")
        else:
            self.logger.info("ZeroMQ receiver initialized")

    async def start(self):
        """Start ZeroMQ receiver"""
        if not ZMQ_AVAILABLE or not MSGPACK_AVAILABLE:
            raise RuntimeError("ZeroMQ or MessagePack not available")

        try:
            self.logger.info("Starting ZeroMQ receiver...")
            self.stats['connection_attempts'] += 1

            # Create sockets
            await self._create_sockets()

            # Bind sockets to listen for Virginia connections
            await self._bind_for_virginia()

            self.is_running = True
            self.stats['successful_connections'] += 1
            self.logger.info("ZeroMQ receiver started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start ZeroMQ receiver: {e}")
            raise

    async def stop(self):
        """Stop ZeroMQ receiver gracefully"""
        self.logger.info("Stopping ZeroMQ receiver...")
        self.shutdown_requested = True

        # Close sockets
        if self.market_data_socket:
            self.market_data_socket.close()
        if self.analysis_socket:
            self.analysis_socket.close()
        if self.trade_socket:
            self.trade_socket.close()

        # Terminate context
        if hasattr(self, 'context'):
            self.context.term()

        self.is_running = False
        self.logger.info("ZeroMQ receiver stopped")

    async def _create_sockets(self):
        """Create ZeroMQ sockets"""
        # Market data socket (PULL)
        self.market_data_socket = self.context.socket(zmq.PULL)
        self.market_data_socket.setsockopt(zmq.RCVHWM, self.recv_hwm)
        self.market_data_socket.setsockopt(zmq.LINGER, 0)
        self.market_data_socket.setsockopt(zmq.RCVTIMEO, self.recv_timeout)

        # Analysis socket (PULL)
        self.analysis_socket = self.context.socket(zmq.PULL)
        self.analysis_socket.setsockopt(zmq.RCVHWM, self.recv_hwm)
        self.analysis_socket.setsockopt(zmq.LINGER, 0)
        self.analysis_socket.setsockopt(zmq.RCVTIMEO, self.recv_timeout)

        # Trade socket (PULL)
        self.trade_socket = self.context.socket(zmq.PULL)
        self.trade_socket.setsockopt(zmq.RCVHWM, self.recv_hwm)
        self.trade_socket.setsockopt(zmq.LINGER, 0)
        self.trade_socket.setsockopt(zmq.RCVTIMEO, self.recv_timeout)

        self.logger.debug("ZeroMQ sockets created")

    async def _bind_for_virginia(self):
        """Bind sockets to listen for Virginia connections"""
        try:
            # Bind market data socket to listen
            self.market_data_socket.bind(f"tcp://*:{CONFIG.zeromq.market_data_port}")
            self.logger.info(f"Market data receiver bound to port {CONFIG.zeromq.market_data_port}")

            # Bind analysis socket to listen
            self.analysis_socket.bind(f"tcp://*:{CONFIG.zeromq.analysis_data_port}")
            self.logger.info(f"Analysis receiver bound to port {CONFIG.zeromq.analysis_data_port}")

            # Bind trade socket to listen
            self.trade_socket.bind(f"tcp://*:{CONFIG.zeromq.trade_data_port}")
            self.logger.info(f"Trade receiver bound to port {CONFIG.zeromq.trade_data_port}")

        except Exception as e:
            self.logger.error(f"Failed to bind sockets: {e}")
            raise

    def add_market_data_callback(self, callback: Callable[[MarketDataMessage], None]):
        """Add callback for market data messages"""
        self.market_data_callbacks.append(callback)
        self.logger.info(f"Added market data callback: {callback.__name__}")

    def add_analysis_callback(self, callback: Callable[[AnalysisMessage], None]):
        """Add callback for analysis messages"""
        self.analysis_callbacks.append(callback)
        self.logger.info(f"Added analysis callback: {callback.__name__}")

    def add_trade_callback(self, callback: Callable[[TradeMessage], None]):
        """Add callback for trade messages"""
        self.trade_callbacks.append(callback)
        self.logger.info(f"Added trade callback: {callback.__name__}")

    async def _receive_with_timeout(self, socket: zmq.asyncio.Socket,
                                    timeout_ms: int = None) -> Optional[bytes]:
        """
        Receive message with timeout.

        Args:
            socket: ZeroMQ socket
            timeout_ms: Timeout in milliseconds

        Returns:
            Message bytes or None if timeout
        """
        try:
            if timeout_ms:
                socket.setsockopt(zmq.RCVTIMEO, timeout_ms)

            message_bytes = await socket.recv()

            # Verbose logging only if enabled
            if self.verbose and message_bytes:
                self.stats['raw_message_bytes_received'] += len(message_bytes)
                self.logger.debug(f"Raw message received: {len(message_bytes)} bytes")

                try:
                    # Try to decode as msgpack to see structure
                    decoded = msgpack.unpackb(message_bytes, raw=False)
                    # Handle both 'type' and 'message_type' fields
                    msg_type = decoded.get('message_type') or decoded.get('type', 'unknown')

                    if msg_type in ['market_data', 'market_data_batch']:
                        snapshots = decoded.get('snapshots', [])
                        self.logger.debug(f"Market data: {len(snapshots)} snapshots")

                        for i, snapshot in enumerate(snapshots[:3]):  # Log first 3 snapshots
                            source = snapshot.get('source', 'unknown')
                            ticker = snapshot.get('ticker', 'unknown')

                            if source.lower() == 'kalshi':
                                self.stats['kalshi_snapshots_received'] += 1
                                orderbook = snapshot.get('full_orderbook', {})
                                yes_data = orderbook.get('yes', [])
                                no_data = orderbook.get('no', [])
                                self.logger.debug(f"Kalshi {ticker}: {len(yes_data)} YES, {len(no_data)} NO levels")

                            elif source.lower() == 'polymarket':
                                self.stats['polymarket_snapshots_received'] += 1
                                self.logger.debug(f"Polymarket: {snapshot.get('condition_id', 'no_id')}")

                except Exception as decode_error:
                    if self.verbose:
                        self.logger.warning(f"Could not decode message for logging: {decode_error}")

            return message_bytes

        except zmq.Again:
            # Timeout - no message available
            return None
        except Exception as e:
            self.logger.error(f"Error receiving message: {e}")
            return None

    def _deserialize_message(self, message_bytes: bytes) -> Dict[str, Any]:
        """
        Deserialize message using MessagePack.

        Args:
            message_bytes: Serialized message bytes

        Returns:
            Deserialized message dictionary
        """
        try:
            decoded = msgpack.unpackb(message_bytes, raw=False)

            if self.verbose:
                self.logger.debug(f"Message deserialized successfully: {list(decoded.keys())}")

            return decoded

        except Exception as e:
            self.logger.error(f"Failed to deserialize message: {e}")
            if self.verbose:
                self.logger.error(f"Message bytes length: {len(message_bytes)}")
            raise

    async def _handle_market_data_message(self, message_data: Dict[str, Any]):
        """
        Handle incoming market data message.

        Args:
            message_data: Deserialized market data message
        """
        try:
            message = MarketDataMessage(
                message_type=message_data.get('message_type') or message_data.get('type', 'market_data'),
                snapshots=message_data.get('snapshots', []),
                virginia_sent_timestamp=message_data.get('virginia_sent_timestamp', time.time()),
                message_id=message_data.get('message_id', '')
            )

            # Calculate message latency
            current_time = time.time()
            latency = (current_time - message.virginia_sent_timestamp) * 1000  # Convert to ms
            self._track_message_latency(latency)

            self.stats['market_data_messages'] += 1
            self.stats['last_message_time'] = current_time

            if self.verbose:
                # Detailed snapshot analysis
                kalshi_count = 0
                polymarket_count = 0

                for snapshot in message.snapshots:
                    source = snapshot.get('source', '').lower()
                    if source == 'kalshi':
                        kalshi_count += 1
                    elif source == 'polymarket':
                        polymarket_count += 1

                self.logger.debug(f"Processing message {message.message_id}: "
                                  f"{kalshi_count} Kalshi, {polymarket_count} Polymarket snapshots, "
                                  f"latency: {latency:.2f}ms")
            else:
                # Production: only log significant batches or errors
                if len(message.snapshots) > 10:
                    self.logger.debug(f"Large batch received: {len(message.snapshots)} snapshots")

            # Notify callbacks
            await self._notify_market_data_callbacks(message)

        except Exception as e:
            self.stats['message_errors'] += 1
            self.logger.error(f"Error handling market data message: {e}")

    async def _handle_analysis_message(self, message_data: Dict[str, Any]):
        """
        Handle incoming analysis update message.

        Args:
            message_data: Deserialized analysis message
        """
        try:
            message = AnalysisMessage(
                message_type=message_data.get('message_type') or message_data.get('type', 'analysis_update'),
                snapshot_id=message_data.get('snapshot_id', ''),
                analyzed=message_data.get('analyzed', False),
                executed=message_data.get('executed', False),
                analysis_timestamp=message_data.get('analysis_timestamp', time.time()),
                message_id=message_data.get('message_id', '')
            )

            self.stats['analysis_messages'] += 1
            self.stats['last_message_time'] = time.time()

            if self.verbose:
                self.logger.debug(f"Analysis update: {message.snapshot_id[:8]}... "
                                  f"analyzed={message.analyzed}, executed={message.executed}")

            # Notify callbacks
            await self._notify_analysis_callbacks(message)

        except Exception as e:
            self.stats['message_errors'] += 1
            self.logger.error(f"Error handling analysis message: {e}")

    async def _handle_trade_message(self, message_data: Dict[str, Any]):
        """
        Handle incoming trade data message.

        Args:
            message_data: Deserialized trade message
        """
        try:
            message = TradeMessage(
                message_type=message_data.get('message_type') or message_data.get('type', 'trade_data'),
                trade_ticket=message_data.get('trade_ticket', {}),
                analysis_record=message_data.get('analysis_record'),
                arbitrage_pair=message_data.get('arbitrage_pair'),
                virginia_sent_timestamp=message_data.get('virginia_sent_timestamp', time.time()),
                message_id=message_data.get('message_id', '')
            )

            self.stats['trade_messages'] += 1
            self.stats['last_message_time'] = time.time()

            # Trade messages are important - always log
            self.logger.info(f"Trade data received: trade={bool(message.trade_ticket)}, "
                             f"analysis={bool(message.analysis_record)}, "
                             f"pair={bool(message.arbitrage_pair)}")

            # Notify callbacks
            await self._notify_trade_callbacks(message)

        except Exception as e:
            self.stats['message_errors'] += 1
            self.logger.error(f"Error handling trade message: {e}")

    async def _notify_market_data_callbacks(self, message: MarketDataMessage):
        """Notify all market data callbacks"""
        for callback in self.market_data_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(message)
                else:
                    callback(message)
            except Exception as e:
                self.logger.error(f"Error in market data callback {callback.__name__}: {e}")

    async def _notify_analysis_callbacks(self, message: AnalysisMessage):
        """Notify all analysis callbacks"""
        for callback in self.analysis_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(message)
                else:
                    callback(message)
            except Exception as e:
                self.logger.error(f"Error in analysis callback {callback.__name__}: {e}")

    async def _notify_trade_callbacks(self, message: TradeMessage):
        """Notify all trade callbacks"""
        for callback in self.trade_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(message)
                else:
                    callback(message)
            except Exception as e:
                self.logger.error(f"Error in trade callback {callback.__name__}: {e}")

    def _track_message_latency(self, latency_ms: float):
        """Track message latency for performance monitoring"""
        self.message_latencies.append(latency_ms)
        if len(self.message_latencies) > self.max_latency_samples:
            self.message_latencies = self.message_latencies[-self.max_latency_samples:]

        # Calculate average latency
        if self.message_latencies:
            self.stats['avg_message_latency_ms'] = sum(self.message_latencies) / len(self.message_latencies)

    async def run_receiver_loop(self):
        """
        Main receiver loop - continuously listens for messages from Virginia.
        """
        if self.verbose:
            self.logger.info("Starting ZeroMQ receiver loop with verbose logging")
        else:
            self.logger.info("Starting ZeroMQ receiver loop")

        loop_count = 0
        last_status_report = time.time()

        while not self.shutdown_requested:
            try:
                loop_count += 1
                current_time = time.time()

                # Periodic status update (every 5 minutes in production, 1000 iterations in verbose)
                if self.verbose and loop_count % 1000 == 0:
                    self.logger.debug(f"Receiver loop status (iteration {loop_count}): "
                                      f"{self.stats['total_messages_received']} total messages, "
                                      f"{self.stats['kalshi_snapshots_received']} Kalshi snapshots")
                elif not self.verbose and (current_time - last_status_report) > 300:  # 5 minutes
                    if self.stats['total_messages_received'] > 0:
                        self.logger.debug(
                            f"Receiver active: {self.stats['total_messages_received']} messages processed, "
                            f"avg latency: {self.stats['avg_message_latency_ms']:.1f}ms")
                    last_status_report = current_time

                # Check for market data messages
                if self.market_data_socket:
                    message_bytes = await self._receive_with_timeout(self.market_data_socket, 10)
                    if message_bytes:
                        self.stats['total_messages_received'] += 1
                        message_data = self._deserialize_message(message_bytes)
                        await self._handle_market_data_message(message_data)

                # Check for analysis messages
                if self.analysis_socket:
                    message_bytes = await self._receive_with_timeout(self.analysis_socket, 10)
                    if message_bytes:
                        self.stats['total_messages_received'] += 1
                        message_data = self._deserialize_message(message_bytes)
                        await self._handle_analysis_message(message_data)

                # Check for trade messages
                if self.trade_socket:
                    message_bytes = await self._receive_with_timeout(self.trade_socket, 10)
                    if message_bytes:
                        self.stats['total_messages_received'] += 1
                        message_data = self._deserialize_message(message_bytes)
                        await self._handle_trade_message(message_data)

                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.001)  # 1ms

            except Exception as e:
                self.logger.error(f"Error in receiver loop: {e}")
                await asyncio.sleep(1)  # Wait longer on errors

        self.logger.info("ZeroMQ receiver loop stopped")

    def get_status(self) -> Dict[str, Any]:
        """Get receiver status and statistics"""
        current_time = time.time()

        # Calculate message rate
        uptime = current_time - (self.stats.get('start_time', current_time))
        message_rate = self.stats['total_messages_received'] / max(uptime, 1)

        return {
            'is_running': self.is_running,
            'verbose_mode': self.verbose,
            'endpoints': {
                'market_data': f"tcp://*:{CONFIG.zeromq.market_data_port}",
                'analysis': f"tcp://*:{CONFIG.zeromq.analysis_data_port}",
                'trade': f"tcp://*:{CONFIG.zeromq.trade_data_port}"
            },
            'statistics': self.stats.copy(),
            'performance': {
                'message_rate_per_second': message_rate,
                'avg_message_latency_ms': self.stats['avg_message_latency_ms'],
                'recent_latencies_ms': self.message_latencies[-5:] if self.message_latencies else []
            },
            'callbacks': {
                'market_data': len(self.market_data_callbacks),
                'analysis': len(self.analysis_callbacks),
                'trade': len(self.trade_callbacks)
            }
        }

    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for monitoring"""
        current_time = time.time()

        # Determine health status
        is_healthy = (
                self.is_running and
                (current_time - self.stats.get('last_message_time', current_time)) < 300  # 5 minutes
        )

        error_rate = self.stats['message_errors'] / max(self.stats['total_messages_received'], 1)

        return {
            'is_healthy': is_healthy,
            'is_running': self.is_running,
            'total_messages': self.stats['total_messages_received'],
            'kalshi_snapshots': self.stats['kalshi_snapshots_received'],
            'polymarket_snapshots': self.stats['polymarket_snapshots_received'],
            'error_rate_percent': error_rate * 100,
            'last_message_time': self.stats.get('last_message_time', 0),
            'avg_latency_ms': self.stats['avg_message_latency_ms'],
            'successful_connections': self.stats['successful_connections']
        }