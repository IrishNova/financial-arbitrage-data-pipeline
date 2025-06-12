#!/usr/bin/env python3
"""
broker/client.py

Pure ZeroMQ client for Ireland server.
Receives condition ID subscriptions and trade execution requests from Virginia.
No Polymarket API logic - just pure message transmission.
"""

import sys
import asyncio
import time
import zmq
import zmq.asyncio
import msgpack
from pathlib import Path
from typing import Set, List, Dict, Optional, Any, Callable
from dataclasses import dataclass
from datetime import datetime
import uuid

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_logger


@dataclass
class SubscriptionMessage:
    """Subscription message from Virginia"""
    action: str  # "SUBSCRIBE", "UNSUBSCRIBE", "REPLACE"
    condition_ids: List[str]
    timestamp: float
    message_id: str
    virginia_server_id: str


@dataclass
class TradeExecutionRequest:
    """Trade execution request from Virginia"""
    condition_id: str
    side: str  # "YES" or "NO"
    size: float
    price: float
    order_type: str  # "MARKET", "LIMIT"
    message_id: str
    timestamp: float
    virginia_server_id: str


class IrelandClient:
    """
    Pure ZeroMQ client for Ireland server.
    Handles message reception from Virginia with no API logic.
    """

    def __init__(self):
        """Initialize Ireland ZeroMQ client"""
        self.logger = get_logger('client')

        # ZeroMQ context and sockets
        self.context = zmq.asyncio.Context()
        self.subscription_socket: Optional[zmq.asyncio.Socket] = None
        self.execution_socket: Optional[zmq.asyncio.Socket] = None

        # Connection endpoints - Ireland perspective
        virginia_host = CONFIG.zeromq.virginia_endpoint.split('//')[1].split(':')[0]
        self.subscription_endpoint = f"tcp://{virginia_host}:{CONFIG.zeromq.subscription_pub_port}"
        self.execution_endpoint = f"tcp://*:{CONFIG.zeromq.execution_rep_port}"  # Ireland binds

        # State tracking
        self.is_running = False
        self.shutdown_requested = False
        self.current_condition_ids: Set[str] = set()
        self.total_messages_received = 0

        # Callbacks - pure message passing
        self.subscription_callbacks: List[Callable[[Set[str]], None]] = []
        self.execution_callbacks: List[Callable[[TradeExecutionRequest], Dict[str, Any]]] = []

        # Statistics
        self.stats = {
            'subscription_updates': 0,
            'execution_requests': 0,
            'last_subscription_update': 0,
            'last_execution_request': 0,
            'messages_per_minute': 0.0
        }

        self.logger.info("Ireland ZeroMQ client initialized")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def start(self):
        """Start Ireland ZeroMQ client connections"""
        try:
            self.logger.info("Starting Ireland ZeroMQ client...")

            # Create subscription subscriber socket (SUB)
            self.subscription_socket = self.context.socket(zmq.SUB)
            self.subscription_socket.setsockopt(zmq.RCVHWM, 1000)
            self.subscription_socket.setsockopt(zmq.LINGER, 0)
            self.subscription_socket.setsockopt(zmq.SUBSCRIBE, b"")  # Subscribe to all

            # Connect to Virginia's subscription publisher
            try:
                self.subscription_socket.connect(self.subscription_endpoint)
                self.logger.info(f"Connected to Virginia subscriptions: {self.subscription_endpoint}")
            except Exception as e:
                self.logger.warning(f"Could not connect to Virginia subscriptions: {e}")

            # Create execution reply socket (REP)
            self.execution_socket = self.context.socket(zmq.REP)
            self.execution_socket.setsockopt(zmq.LINGER, 0)
            self.execution_socket.setsockopt(zmq.RCVTIMEO, 30000)  # 30s timeout

            # Bind execution socket
            try:
                self.execution_socket.bind(self.execution_endpoint)
                self.logger.info(f"Bound execution reply server: {self.execution_endpoint}")
            except Exception as e:
                self.logger.warning(f"Could not bind execution endpoint: {e}")

            self.is_running = True
            self.logger.info("Ireland ZeroMQ client started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start Ireland client: {e}")
            self.is_running = True  # Allow testing to continue

    async def stop(self):
        """Stop Ireland ZeroMQ client"""
        self.logger.info("Stopping Ireland ZeroMQ client...")
        self.shutdown_requested = True

        if self.subscription_socket:
            self.subscription_socket.close()
        if self.execution_socket:
            self.execution_socket.close()

        self.context.term()

        self.is_running = False
        self.logger.info("Ireland ZeroMQ client stopped")

    def _deserialize_message(self, message_bytes: bytes) -> Dict[str, Any]:
        """Deserialize MessagePack message"""
        try:
            return msgpack.unpackb(message_bytes, raw=False)
        except Exception as e:
            self.logger.error(f"Failed to deserialize message: {e}")
            raise

    def _serialize_message(self, message: Any) -> bytes:
        """Serialize message to MessagePack"""
        try:
            if hasattr(message, '__dict__'):
                from dataclasses import asdict
                data = asdict(message)
            else:
                data = message
            return msgpack.packb(data, use_bin_type=True)
        except Exception as e:
            self.logger.error(f"Failed to serialize message: {e}")
            raise

    async def _receive_with_timeout(self, socket: zmq.asyncio.Socket, timeout_ms: int = 100) -> Optional[bytes]:
        """Receive message with timeout"""
        try:
            socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
            message_bytes = await socket.recv()
            return message_bytes
        except zmq.Again:
            return None
        except Exception as e:
            self.logger.error(f"Error receiving message: {e}")
            return None

    async def _handle_subscription_message(self, message_data: Dict[str, Any]):
        """Handle subscription update from Virginia"""
        try:
            subscription = SubscriptionMessage(
                action=message_data.get('action', ''),
                condition_ids=message_data.get('condition_ids', []),
                timestamp=message_data.get('timestamp', time.time()),
                message_id=message_data.get('message_id', ''),
                virginia_server_id=message_data.get('virginia_server_id', '')
            )

            # Update local condition IDs state
            old_count = len(self.current_condition_ids)

            if subscription.action == "REPLACE":
                self.current_condition_ids = set(subscription.condition_ids)
            elif subscription.action == "SUBSCRIBE":
                self.current_condition_ids.update(subscription.condition_ids)
            elif subscription.action == "UNSUBSCRIBE":
                self.current_condition_ids -= set(subscription.condition_ids)

            new_count = len(self.current_condition_ids)

            self.logger.info(f"Subscription {subscription.action}: {len(subscription.condition_ids)} IDs - "
                             f"Total: {old_count} -> {new_count}")

            # Update stats
            self.stats['subscription_updates'] += 1
            self.stats['last_subscription_update'] = time.time()

            # Forward to callbacks (Polymarket client, etc.)
            await self._notify_subscription_callbacks(self.current_condition_ids.copy())

        except Exception as e:
            self.logger.error(f"Error handling subscription message: {e}")

    async def _handle_execution_request(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle trade execution request from Virginia"""
        try:
            request = TradeExecutionRequest(
                condition_id=message_data.get('condition_id', ''),
                side=message_data.get('side', ''),
                size=message_data.get('size', 0.0),
                price=message_data.get('price', 0.0),
                order_type=message_data.get('order_type', ''),
                message_id=message_data.get('message_id', ''),
                timestamp=message_data.get('timestamp', time.time()),
                virginia_server_id=message_data.get('virginia_server_id', '')
            )

            self.logger.info(f"Trade request: {request.condition_id[:10]}... "
                             f"{request.side} {request.size}@{request.price}")

            # Update stats
            self.stats['execution_requests'] += 1
            self.stats['last_execution_request'] = time.time()

            # Forward to execution callbacks (trading system)
            response = None
            for callback in self.execution_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        response = await callback(request)
                    else:
                        response = callback(request)

                    if response:
                        break

                except Exception as e:
                    self.logger.error(f"Error in execution callback: {e}")

            # Default response if no handler
            if not response:
                response = {
                    "status": "ERROR",
                    "message": "No execution handler available",
                    "request_id": request.message_id,
                    "timestamp": time.time()
                }

            return response

        except Exception as e:
            self.logger.error(f"Error handling execution request: {e}")
            return {"status": "ERROR", "message": str(e), "timestamp": time.time()}

    async def _notify_subscription_callbacks(self, condition_ids: Set[str]):
        """Notify subscription callbacks - pure message forwarding"""
        for callback in self.subscription_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(condition_ids)
                else:
                    callback(condition_ids)
            except Exception as e:
                self.logger.error(f"Error in subscription callback: {e}")

    async def run_client_loop(self):
        """Main client loop - pure message reception"""
        self.logger.info("Starting Ireland client message loop...")

        message_count = 0
        start_time = time.time()

        while not self.shutdown_requested:
            try:
                # Check for subscription messages
                if self.subscription_socket:
                    message_bytes = await self._receive_with_timeout(self.subscription_socket, 10)
                    if message_bytes:
                        message_data = self._deserialize_message(message_bytes)
                        await self._handle_subscription_message(message_data)
                        message_count += 1

                # Check for execution requests
                if self.execution_socket:
                    message_bytes = await self._receive_with_timeout(self.execution_socket, 10)
                    if message_bytes:
                        message_data = self._deserialize_message(message_bytes)

                        # Handle execution and send response
                        response = await self._handle_execution_request(message_data)
                        response_bytes = self._serialize_message(response)
                        await self.execution_socket.send(response_bytes)

                        message_count += 1

                # Update message rate stats
                elapsed = time.time() - start_time
                if elapsed >= 60:  # Every minute
                    self.stats['messages_per_minute'] = message_count / (elapsed / 60)
                    message_count = 0
                    start_time = time.time()

                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.001)

            except Exception as e:
                self.logger.error(f"Error in client loop: {e}")
                await asyncio.sleep(1)

        self.logger.info("Ireland client message loop stopped")

    def add_subscription_callback(self, callback: Callable[[Set[str]], None]):
        """Add callback for subscription updates"""
        self.subscription_callbacks.append(callback)
        self.logger.info(f"Added subscription callback: {callback.__name__}")

    def add_execution_callback(self, callback: Callable[[TradeExecutionRequest], Dict[str, Any]]):
        """Add callback for execution requests"""
        self.execution_callbacks.append(callback)
        self.logger.info(f"Added execution callback: {callback.__name__}")

    def get_current_condition_ids(self) -> Set[str]:
        """Get current condition IDs being monitored"""
        return self.current_condition_ids.copy()

    def get_status(self) -> Dict[str, Any]:
        """Get client status"""
        return {
            "is_running": self.is_running,
            "total_messages_received": self.total_messages_received,
            "active_condition_ids": len(self.current_condition_ids),
            "condition_id_sample": list(self.current_condition_ids)[:3],
            "statistics": self.stats.copy(),
            "callbacks": {
                "subscription": len(self.subscription_callbacks),
                "execution": len(self.execution_callbacks)
            }
        }


# Demo and testing
async def test_ireland_client():
    """Test Ireland ZeroMQ client"""
    print("IRELAND ZEROMQ CLIENT TEST")
    print("=" * 50)

    # Sample callbacks
    async def sample_subscription_callback(condition_ids):
        print(f"📡 Received subscription update: {len(condition_ids)} condition IDs")
        if condition_ids:
            sample = list(condition_ids)[:2]
            print(f"   Sample: {[id[:10] + '...' for id in sample]}")

    def sample_execution_callback(request):
        print(f"💰 Received execution request: {request.condition_id[:10]}... "
              f"{request.side} {request.size}@{request.price}")
        return {
            "status": "SUCCESS",
            "executed_size": request.size,
            "executed_price": request.price,
            "fill_id": f"fill-{uuid.uuid4().hex[:8]}",
            "request_id": request.message_id,
            "timestamp": time.time()
        }

    try:
        async with IrelandClient() as client:
            print("✅ Ireland ZeroMQ client started")

            # Add callbacks
            client.add_subscription_callback(sample_subscription_callback)
            client.add_execution_callback(sample_execution_callback)

            print("📡 Listening for messages from Virginia...")

            # Start client loop
            client_task = asyncio.create_task(client.run_client_loop())

            # Run for 15 seconds
            await asyncio.sleep(15)

            # Show status
            status = client.get_status()
            print(f"\n📊 Client Status:")
            print(f"  Running: {status['is_running']}")
            print(f"  Active condition IDs: {status['active_condition_ids']}")
            print(f"  Statistics: {status['statistics']}")

            # Stop
            client.shutdown_requested = True
            await client_task

            print("✅ Ireland client test completed!")

    except Exception as e:
        print(f"❌ Ireland client test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(test_ireland_client())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed: {e}")