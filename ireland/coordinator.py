#!/usr/bin/env python3
"""
coordinator.py

Pure coordinator that routes requests between components.
NO API CALLS - just intelligent routing, caching, and coordination.
"""

import time
import asyncio
from typing import Dict, List, Optional, Set, Any, Union
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Official Polymarket client (sync)
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.constants import POLYGON

from utils.config import CONFIG
from utils.logger import get_logger
from broker.publisher import IrelandPublisher


@dataclass
class OrderBookLevel:
    """Single level in order book"""
    price: float
    size: float

    def to_dict(self) -> Dict[str, float]:
        return {"price": self.price, "size": self.size}


@dataclass
class FullOrderBook:
    """Complete order book with all levels"""
    token_id: str
    condition_id: str
    outcome: str  # "YES" or "NO"

    # Full order book arrays
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]

    # Quick access
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None

    # Derived metrics
    midpoint: Optional[float] = None
    spread: Optional[float] = None

    # Metadata
    timestamp: float = 0
    market_active: bool = True

    def __post_init__(self):
        if self.bids is None:
            self.bids = []
        if self.asks is None:
            self.asks = []

        # Calculate quick access values
        if self.bids:
            self.best_bid = self.bids[0].price
            self.bid_size = self.bids[0].size

        if self.asks:
            self.best_ask = self.asks[0].price
            self.ask_size = self.asks[0].size

        # Calculate derived metrics
        if self.best_bid and self.best_ask:
            self.midpoint = (self.best_bid + self.best_ask) / 2
            self.spread = self.best_ask - self.best_bid


@dataclass
class TradingResult:
    """Result from trade execution"""
    success: bool
    order_id: Optional[str] = None
    executed_price: Optional[float] = None
    executed_size: Optional[float] = None
    message: str = ""
    timestamp: float = 0


class PolymarketCoordinator:
    """
    Pure coordinator that routes requests to appropriate clients.
    NO API CALLS - just intelligent routing and caching.
    """

    def __init__(self):
        """Initialize the coordinator (no API clients started here)"""
        self.logger = get_logger('polymarket_coordinator')

        # Client references (will be injected)
        self.trading_client: Optional[ClobClient] = None
        self.publisher: Optional[IrelandPublisher] = None

        # State tracking
        self.is_running = False
        self.active_condition_ids: Set[str] = set()
        self.condition_to_tokens: Dict[str, List[str]] = {}
        self.order_book_cache: Dict[str, FullOrderBook] = {}
        self.condition_questions: Dict[str, str] = {}

        # Performance metrics
        self.stats = {
            'trading_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'condition_id_updates': 0,
            'total_trades_routed': 0,
            'publisher_sends_attempted': 0,
            'publisher_sends_successful': 0,
            'data_callbacks_received': 0
        }

        self.logger.info("Polymarket coordinator initialized (pure routing layer)")

    async def start(self):
        """Start coordinator and all integrated components"""
        try:
            # Initialize official trading client (sync)
            host = CONFIG.polymarket.clob_host
            private_key = CONFIG.polymarket.private_key
            chain_id = POLYGON

            # Determine signature type and funder based on config
            signature_type = getattr(CONFIG.polymarket, 'signature_type', 0)
            funder_address = getattr(CONFIG.polymarket, 'funder_address', None)

            if funder_address:
                # Using proxy wallet
                self.trading_client = ClobClient(
                    host,
                    key=private_key,
                    chain_id=chain_id,
                    signature_type=signature_type,
                    funder=funder_address
                )
            else:
                # Direct EOA trading
                self.trading_client = ClobClient(
                    host,
                    key=private_key,
                    chain_id=chain_id
                )

            # Set up API credentials for trading
            try:
                api_creds = self.trading_client.create_or_derive_api_creds()
                self.trading_client.set_api_creds(api_creds)
                self.logger.info("Trading client initialized")
            except Exception as e:
                self.logger.warning(f"Trading client setup failed: {e}")
                self.logger.info("Continuing without trading capabilities...")
                self.trading_client = None

            # Initialize and start ZeroMQ publisher
            self.publisher = IrelandPublisher()
            await self.publisher.start()
            self.logger.info("ZeroMQ publisher initialized")

            self.is_running = True
            self.logger.info("Coordinator started (pure routing mode)")

        except Exception as e:
            self.logger.error(f"Failed to start coordinator: {e}")
            raise

    async def stop(self):
        """Stop coordinator and all integrated components"""
        self.is_running = False

        if self.publisher:
            await self.publisher.stop()

        self.logger.info("Coordinator stopped")

    # =============================================================================
    # CONDITION ID MANAGEMENT
    # =============================================================================

    def update_condition_ids(self, condition_ids: List[str]):
        """
        Update active condition IDs (called when Virginia sends updates).
        Pure state management - no API calls.
        """
        old_count = len(self.active_condition_ids)
        self.active_condition_ids = set(condition_ids)
        new_count = len(self.active_condition_ids)

        self.stats['condition_id_updates'] += 1
        self.logger.info(f"Condition IDs updated: {old_count} -> {new_count}")

        # Clear cache for removed condition IDs
        removed_conditions = set(self.condition_to_tokens.keys()) - self.active_condition_ids
        for condition_id in removed_conditions:
            self.condition_to_tokens.pop(condition_id, None)
            # Also clear related token cache
            if condition_id in self.condition_to_tokens:
                for token_id in self.condition_to_tokens[condition_id]:
                    self.order_book_cache.pop(token_id, None)

    def map_condition_to_tokens(self, condition_id: str, token_ids: List[str]):
        """
        Store the mapping from condition ID to token IDs.
        Called after successful market discovery via data_feed.
        """
        self.condition_to_tokens[condition_id] = token_ids
        self.logger.debug(f"Mapped condition {condition_id[:10]}... to {len(token_ids)} tokens")

    def get_all_active_token_ids(self) -> List[str]:
        """Get all token IDs for active condition IDs"""
        all_tokens = []
        for condition_id in self.active_condition_ids:
            tokens = self.condition_to_tokens.get(condition_id, [])
            all_tokens.extend(tokens)
        return all_tokens

    # =============================================================================
    # ORDER BOOK CACHING AND MANAGEMENT
    # =============================================================================

    def cache_full_order_book(self, token_id: str, order_book_data: Dict):
        """
        Cache processed order book with ALL levels.
        Pure data processing - no API calls.
        """
        current_time = time.time()

        # Parse full order book
        raw_bids = order_book_data.get('bids', [])
        raw_asks = order_book_data.get('asks', [])

        # Convert to OrderBookLevel objects (ALL levels, not just top)
        bids = []
        for bid in raw_bids:
            if isinstance(bid, dict) and 'price' in bid and 'size' in bid:
                bids.append(OrderBookLevel(
                    price=float(bid['price']),
                    size=float(bid['size'])
                ))

        asks = []
        for ask in raw_asks:
            if isinstance(ask, dict) and 'price' in ask and 'size' in ask:
                asks.append(OrderBookLevel(
                    price=float(ask['price']),
                    size=float(ask['size'])
                ))

        # Determine outcome (YES/NO) - heuristic based on token_id
        outcome = self._determine_token_outcome(token_id, order_book_data)

        # Create full order book
        full_book = FullOrderBook(
            token_id=token_id,
            condition_id=order_book_data.get('market', ''),
            outcome=outcome,
            bids=bids,
            asks=asks,
            timestamp=current_time
        )

        # Store in cache
        self.order_book_cache[token_id] = full_book
        self.logger.debug(
            f"Cached full order book for token {token_id[:10]}... ({outcome}) - {len(bids)} bids, {len(asks)} asks")

    def _determine_token_outcome(self, token_id: str, order_book_data: Dict) -> str:
        """Determine if token represents YES or NO outcome"""
        # Method 1: Check if explicitly provided
        outcome = order_book_data.get('outcome', '').upper()
        if outcome in ['YES', 'NO']:
            return outcome

        # Method 2: Simple heuristic based on token_id hash
        return "YES" if hash(token_id) % 2 == 0 else "NO"

    def get_cached_order_books(self, token_ids: List[str], max_age_seconds: int = 30) -> Dict[str, FullOrderBook]:
        """
        Get cached order books (if fresh enough).
        """
        current_time = time.time()
        cached_books = {}

        for token_id in token_ids:
            cached = self.order_book_cache.get(token_id)
            if cached and (current_time - cached.timestamp) < max_age_seconds:
                cached_books[token_id] = cached
                self.stats['cache_hits'] += 1
            else:
                self.stats['cache_misses'] += 1

        return cached_books

    def get_tokens_needing_refresh(self, token_ids: List[str], max_age_seconds: int = 30) -> List[str]:
        """Get list of token IDs that need fresh data"""
        current_time = time.time()
        needs_refresh = []

        for token_id in token_ids:
            cached = self.order_book_cache.get(token_id)
            if not cached or (current_time - cached.timestamp) >= max_age_seconds:
                needs_refresh.append(token_id)

        return needs_refresh

    # =============================================================================
    # DATA CALLBACK HANDLING (FROM DATA_FEED) - FIXED
    # =============================================================================

    async def handle_market_data_callback(self, market_data: Dict[str, Any]):
        """
        Handle market data callback from data_feed.
        Receives: Dict[token_id, PolymarketMarketData]
        Sends raw data to Virginia without processing.
        """
        if not market_data:
            return

        self.stats['data_callbacks_received'] += 1
        self.logger.debug(f"Received market data callback with {len(market_data)} tokens")

        try:
            # Convert PolymarketMarketData objects to dicts for serialization
            serialized_data = {}
            for token_id, market_data_obj in market_data.items():
                # market_data_obj is a PolymarketMarketData object
                serialized_data[token_id] = market_data_obj.to_dict()

            # ADD: Ireland processing timestamp (UTC)
            ireland_processing_complete_ns = time.time_ns()
            for token_id, token_data in serialized_data.items():
                token_data['ireland_processing_complete_ns'] = ireland_processing_complete_ns

            # Send raw data to Virginia via publisher
            if serialized_data:
                await self._send_to_virginia_with_retry(serialized_data)
            else:
                self.logger.warning("No serialized data to send to Virginia")

        except Exception as e:
            self.logger.error(f"Error in market data callback: {e}")

    async def _send_to_virginia_with_retry(self, market_data: Dict[str, Dict], max_retries: int = 3):
        """Send raw market data to Virginia with retry logic"""
        if not self.publisher or not market_data:
            return

        self.stats['publisher_sends_attempted'] += 1

        # Prepare market data in the format Virginia expects
        market_updates = []
        for token_id, token_data in market_data.items():
            market_updates.append(token_data)

        for attempt in range(max_retries):
            try:
                success = await self.publisher.send_market_data(market_updates)

                if success:
                    self.stats['publisher_sends_successful'] += 1
                    self.logger.debug(f"Successfully sent {len(market_updates)} token updates to Virginia")
                    return
                else:
                    self.logger.warning(f"Publisher send failed (attempt {attempt + 1}/{max_retries})")

            except Exception as e:
                self.logger.error(f"Publisher send error (attempt {attempt + 1}/{max_retries}): {e}")

            # Wait before retry
            if attempt < max_retries - 1:
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff

        self.logger.error(f"Failed to send market data after {max_retries} attempts")

    # =============================================================================
    # TRADING COORDINATION (sync methods) - FIXED
    # =============================================================================

    def place_order(self, token_id: str, side: str, price: float, size: float,
                    order_type: str = "GTC") -> TradingResult:
        """
        Route order placement to trading client.
        Pure routing - no async needed since trading client is sync.
        """
        self.stats['trading_requests'] += 1

        if not self.trading_client:
            return TradingResult(
                success=False,
                message="Trading client not available",
                timestamp=time.time()
            )

        try:
            # Convert to official client format
            side_constant = BUY if side.upper() == "BUY" else SELL
            order_type_enum = OrderType.GTC if order_type == "GTC" else OrderType.GTD

            # Create order arguments
            order_args = OrderArgs(
                price=price,
                size=size,
                side=side_constant,
                token_id=token_id
            )

            # Execute order via trading client
            response = self.trading_client.create_and_post_order(order_args, order_type_enum)

            if response:
                self.stats['total_trades_routed'] += 1
                return TradingResult(
                    success=True,
                    order_id=response.get('orderID', ''),
                    executed_price=price,
                    executed_size=size,
                    message="Order placed successfully",
                    timestamp=time.time()
                )
            else:
                return TradingResult(
                    success=False,
                    message="Order placement failed - no response",
                    timestamp=time.time()
                )

        except Exception as e:
            self.logger.error(f"Error routing order placement: {e}")
            return TradingResult(
                success=False,
                message=f"Order routing error: {str(e)}",
                timestamp=time.time()
            )

    def cancel_order(self, order_id: str) -> TradingResult:
        """Route order cancellation to trading client"""
        if not self.trading_client:
            return TradingResult(
                success=False,
                message="Trading client not available",
                timestamp=time.time()
            )

        try:
            response = self.trading_client.cancel_order(order_id)
            return TradingResult(
                success=bool(response),
                message="Order cancelled" if response else "Cancel failed",
                timestamp=time.time()
            )
        except Exception as e:
            return TradingResult(
                success=False,
                message=f"Cancel routing error: {str(e)}",
                timestamp=time.time()
            )

    # REMOVED: get_account_balance, get_positions, get_open_orders
    # These methods don't exist on ClobClient - trading client handles this differently

    # =============================================================================
    # STATUS AND HEALTH
    # =============================================================================

    def get_status(self) -> Dict[str, Any]:
        """Get coordinator status (pure state - no API calls)"""
        return {
            "is_running": self.is_running,
            "trading_client_connected": self.trading_client is not None,
            "publisher_connected": self.publisher is not None,
            "active_conditions": len(self.active_condition_ids),
            "mapped_tokens": sum(len(tokens) for tokens in self.condition_to_tokens.values()),
            "cached_snapshots": len(self.order_book_cache),
            "statistics": self.stats.copy()
        }

    def health_check(self) -> Dict[str, bool]:
        """Check coordinator health (state only - no API calls)"""
        return {
            "coordinator_running": self.is_running,
            "trading_client_available": self.trading_client is not None,
            "publisher_available": self.publisher is not None,
            "has_active_conditions": len(self.active_condition_ids) > 0,
            "has_cached_data": len(self.order_book_cache) > 0
        }


# =============================================================================
# DEMO AND TESTING
# =============================================================================

async def test_coordinator():
    """Test the coordinator with proper separation of concerns"""
    print("POLYMARKET COORDINATOR TEST (PURE ROUTING)")
    print("=" * 50)

    try:
        coordinator = PolymarketCoordinator()
        await coordinator.start()
        print("Coordinator started")

        # Test condition ID management
        test_condition_ids = [
            "0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",
            "0x17cd926896aa499bea870e218dc2e32c99f102237e6ae90f49f4b03d69cac4da",
        ]

        coordinator.update_condition_ids(test_condition_ids)
        print(f"Updated condition IDs: {len(coordinator.active_condition_ids)}")

        # Test market data callback with mock PROCESSED data (what data_feed should send)
        mock_processed_data = {
            "token1": {
                "condition_id": test_condition_ids[0],
                "market": test_condition_ids[0],
                "bids": [
                    {"price": "0.45", "size": "1000"},
                    {"price": "0.44", "size": "500"},
                    {"price": "0.43", "size": "200"}
                ],
                "asks": [
                    {"price": "0.55", "size": "800"},
                    {"price": "0.56", "size": "300"},
                    {"price": "0.57", "size": "150"}
                ]
            },
            "token2": {
                "condition_id": test_condition_ids[0],
                "market": test_condition_ids[0],
                "bids": [
                    {"price": "0.55", "size": "1200"},
                    {"price": "0.54", "size": "600"}
                ],
                "asks": [
                    {"price": "0.45", "size": "900"},
                    {"price": "0.46", "size": "400"}
                ]
            }
        }

        print("Testing market data callback...")
        await coordinator.handle_market_data_callback(mock_processed_data)

        # Show status
        status = coordinator.get_status()
        print(f"\nStatus:")
        print(f"  Active conditions: {status['active_conditions']}")
        print(f"  Cached snapshots: {status['cached_snapshots']}")
        print(f"  Publisher connected: {status['publisher_connected']}")
        print(f"  Statistics: {status['statistics']}")

        await coordinator.stop()
        print("Coordinator test completed!")

    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        import asyncio

        asyncio.run(test_coordinator())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed: {e}")