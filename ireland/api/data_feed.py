#!/usr/bin/env python3
"""
api/polymarket_data_feed.py

Production Polymarket market data feed for Ireland server.
Mirrors the Kalshi data feed architecture but for Polymarket APIs.
Uses the coordinator for state management and api_client for HTTP calls.
"""

import sys
import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Callable, Any
from dataclasses import dataclass
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from api.api_client import PolymarketAPIClient

logger = logging.getLogger(__name__)


@dataclass
class OrderBookLevel:
    """Single level in order book"""
    price: float
    size: float

    def to_dict(self) -> Dict[str, float]:
        return {"price": self.price, "size": self.size}


@dataclass
class PolymarketMarketData:
    """Polymarket market data structure with FULL order book depth"""
    condition_id: str
    token_id: str
    question: str = ""
    outcome: str = ""  # "YES" or "NO"

    # FULL ORDER BOOK DATA (all levels)
    bids: List[OrderBookLevel] = None
    asks: List[OrderBookLevel] = None

    # Top of book for quick access
    best_bid: float = 0.0
    best_ask: float = 0.0
    bid_size: float = 0.0
    ask_size: float = 0.0
    bid_levels: int = 0
    ask_levels: int = 0

    # Derived metrics
    midpoint: float = 0.0
    spread: float = 0.0

    # Raw data for debugging
    raw_orderbook: dict = None

    # Precise timing (matching Kalshi structure)
    api_call_start_ns: int = 0
    api_response_ns: int = 0
    processing_complete_ns: int = 0

    def __post_init__(self):
        """Initialize empty lists if None"""
        if self.bids is None:
            self.bids = []
        if self.asks is None:
            self.asks = []
        if self.raw_orderbook is None:
            self.raw_orderbook = {}

        # Calculate quick access values from full book
        if self.bids:
            self.best_bid = self.bids[0].price
            self.bid_size = self.bids[0].size
            self.bid_levels = len(self.bids)

        if self.asks:
            self.best_ask = self.asks[0].price
            self.ask_size = self.asks[0].size
            self.ask_levels = len(self.asks)

        # Calculate derived metrics
        if self.best_bid and self.best_ask:
            self.midpoint = (self.best_bid + self.best_ask) / 2
            self.spread = self.best_ask - self.best_bid

    @property
    def api_latency_us(self) -> float:
        """API call latency in microseconds"""
        return (self.api_response_ns - self.api_call_start_ns) / 1000

    @property
    def processing_latency_us(self) -> float:
        """Processing latency in microseconds"""
        return (self.processing_complete_ns - self.api_response_ns) / 1000

    @property
    def total_latency_us(self) -> float:
        """Total latency in microseconds"""
        return (self.processing_complete_ns - self.api_call_start_ns) / 1000

    @property
    def age_ms(self) -> float:
        """How old is this data in milliseconds"""
        now_ns = time.time_ns()
        return (now_ns - self.processing_complete_ns) / 1_000_000

    def get_full_bids(self) -> List[Dict[str, float]]:
        """Get full bid book as list of dicts"""
        return [bid.to_dict() for bid in self.bids]

    def get_full_asks(self) -> List[Dict[str, float]]:
        """Get full ask book as list of dicts"""
        return [ask.to_dict() for ask in self.asks]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for serialization to Virginia"""
        return {
            'condition_id': self.condition_id,
            'token_id': self.token_id,
            'question': self.question,
            'outcome': self.outcome,
            'bids': self.get_full_bids(),  # Full order book
            'asks': self.get_full_asks(),  # Full order book
            'best_bid': self.best_bid,
            'best_ask': self.best_ask,
            'bid_size': self.bid_size,
            'ask_size': self.ask_size,
            'bid_levels': self.bid_levels,
            'ask_levels': self.ask_levels,
            'midpoint': self.midpoint,
            'spread': self.spread,
            'timestamp': self.processing_complete_ns / 1_000_000_000,  # Convert to seconds
            'api_latency_us': self.api_latency_us,
            'processing_latency_us': self.processing_latency_us,
            'total_latency_us': self.total_latency_us,
            'age_ms': self.age_ms
        }


class PolymarketDataFeed:
    """
    Production Polymarket data feed for Ireland server.
    Mirrors KalshiDataFeed architecture but uses coordinator + api_client.
    """

    def __init__(self, market_data_interval: float = 1.0):
        """
        Initialize Polymarket data feed.

        Args:
            market_data_interval: How often to fetch market data (seconds)
        """
        self.market_data_interval = market_data_interval

        # Core components
        self.api_client: Optional[PolymarketAPIClient] = None
        self.coordinator: Optional[PolymarketCoordinator] = None

        # State - managed by Virginia via ZeroMQ
        self.active_condition_ids: Set[str] = set()
        self.market_data: Dict[str, PolymarketMarketData] = {}
        self.last_data_fetch = 0

        # Callbacks for sending data back to Virginia
        self.data_callbacks: List[Callable] = []

        # Control flags
        self.is_running = False
        self.stop_requested = False

        # Performance tracking
        self.stats = {
            'total_cycles': 0,
            'successful_cycles': 0,
            'failed_cycles': 0,
            'total_api_calls': 0,
            'total_markets_processed': 0,
            'avg_cycle_time': 0.0,
            'last_successful_fetch': 0
        }

        logger.info(f"Polymarket data feed initialized - update interval: {market_data_interval}s")

    async def __aenter__(self):
        """Async context manager entry"""
        # Initialize API client
        self.api_client = PolymarketAPIClient()
        await self.api_client.start()

        logger.info("Polymarket data feed connection established")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        self.stop_requested = True

        if self.api_client:
            await self.api_client.stop()

        logger.info("Polymarket data feed connection closed")

    def set_active_condition_ids(self, condition_ids: Set[str]):
        """
        Set active condition IDs from Virginia.

        Args:
            condition_ids: Set of Polymarket condition IDs to monitor
        """
        if condition_ids != self.active_condition_ids:
            added = condition_ids - self.active_condition_ids
            removed = self.active_condition_ids - condition_ids

            if added:
                logger.info(f"Adding Polymarket condition IDs: {[id[:10] + '...' for id in added]}")
            if removed:
                logger.info(f"Removing Polymarket condition IDs: {[id[:10] + '...' for id in removed]}")
                # Clean up removed condition IDs
                for condition_id in removed:
                    # Remove related market data
                    to_remove = [k for k in self.market_data.keys() if k.startswith(condition_id)]
                    for key in to_remove:
                        self.market_data.pop(key, None)

            self.active_condition_ids = condition_ids

            # Update coordinator
            if self.coordinator:
                self.coordinator.update_condition_ids(list(condition_ids))

            logger.info(f"Active Polymarket condition IDs updated: {len(self.active_condition_ids)} total")

    def add_data_callback(self, callback: Callable):
        """
        Add callback function to receive market data updates.

        Args:
            callback: Function that will receive Dict[str, PolymarketMarketData]
        """
        self.data_callbacks.append(callback)
        logger.info(f"Added data callback: {callback.__name__}")

    async def _discover_token_ids(self) -> Dict[str, List[str]]:
        """
        Convert condition IDs to token IDs using API client.
        Returns mapping of condition_id -> [token_ids]
        """
        if not self.active_condition_ids or not self.api_client:
            return {}

        mapping = {}

        for condition_id in self.active_condition_ids:
            try:
                # Get market info to extract token IDs
                market_info = await self.api_client.get_market_by_condition_id(condition_id)

                if market_info and 'tokens' in market_info:
                    token_ids = []
                    for token in market_info['tokens']:
                        if 'token_id' in token:
                            token_ids.append(token['token_id'])

                    if token_ids:
                        mapping[condition_id] = token_ids
                        # Update coordinator mapping
                        if self.coordinator:
                            self.coordinator.map_condition_to_tokens(condition_id, token_ids)
                        logger.debug(f"Mapped {condition_id[:10]}... to {len(token_ids)} tokens")

            except Exception as e:
                logger.error(f"Error discovering tokens for {condition_id[:10]}...: {e}")

        return mapping

    async def fetch_complete_market_data(self) -> Dict[str, PolymarketMarketData]:
        """
        Fetch complete market data with FULL order book depth.
        Returns structured PolymarketMarketData objects with full bid/ask arrays.
        """
        if not self.active_condition_ids or not self.coordinator:
            return {}

        cycle_start_ns = time.time_ns()
        current_data = {}

        try:
            # Step 1: Discover token IDs for all condition IDs
            logger.debug(f"Discovering token IDs for {len(self.active_condition_ids)} condition IDs...")
            token_mapping = await self._discover_token_ids()

            if not token_mapping:
                logger.warning("No token mappings discovered")
                return {}

            # Step 2: Get all token IDs that need fresh data
            all_token_ids = []
            for token_list in token_mapping.values():
                all_token_ids.extend(token_list)

            if not all_token_ids:
                logger.warning("No token IDs to fetch")
                return {}

            logger.debug(f"Fetching market data for {len(all_token_ids)} tokens...")

            # Step 3: Fetch fresh data for all tokens (simplified - no caching for now)
            logger.debug(f"Fetching fresh data for {len(all_token_ids)} tokens")

            # Step 4: Fetch fresh data via API client
            api_call_start = time.time_ns()
            order_books = await self.api_client.get_order_books_batch(all_token_ids)
            api_call_end = time.time_ns()

            if order_books:
                # Step 5: Process each order book with FULL DEPTH
                for i, order_book in enumerate(order_books):
                    if i < len(all_token_ids):
                        token_id = all_token_ids[i]

                        # Create full market data with complete order book
                        market_data = self._convert_orderbook_to_full_market_data(
                            token_id, order_book, api_call_start, api_call_end
                        )

                        if market_data:
                            current_data[token_id] = market_data

            self.stats['total_api_calls'] += 1

            # Update stats
            self.stats['total_markets_processed'] += len(current_data)
            self.stats['last_successful_fetch'] = time.time()

            logger.debug(f"Successfully processed {len(current_data)} markets with full order books")

        except Exception as e:
            logger.error(f"Error fetching market data: {e}")
            self.stats['failed_cycles'] += 1
            return {}

        # Send to callbacks
        if current_data and self.data_callbacks:
            logger.debug(f"Sending {len(current_data)} markets to {len(self.data_callbacks)} callbacks")

            for callback in self.data_callbacks:
                try:
                    await callback(current_data)
                except Exception as e:
                    logger.error(f"Callback error: {e}")

        return current_data

    def _convert_orderbook_to_full_market_data(self, token_id: str, order_book: dict,
                                               api_start: int, api_end: int) -> Optional[PolymarketMarketData]:
        """Convert raw order book to PolymarketMarketData with FULL order book depth"""
        try:
            processing_complete = time.time_ns()

            # Extract basic info
            condition_id = order_book.get('market', '')

            # Parse FULL order book (all levels)
            raw_bids = order_book.get('bids', [])
            raw_asks = order_book.get('asks', [])

            # Convert ALL bid levels to OrderBookLevel objects
            bids = []
            for bid in raw_bids:
                if isinstance(bid, dict) and 'price' in bid and 'size' in bid:
                    bids.append(OrderBookLevel(
                        price=float(bid['price']),
                        size=float(bid['size'])
                    ))

            # Convert ALL ask levels to OrderBookLevel objects
            asks = []
            for ask in raw_asks:
                if isinstance(ask, dict) and 'price' in ask and 'size' in ask:
                    asks.append(OrderBookLevel(
                        price=float(ask['price']),
                        size=float(ask['size'])
                    ))

            return PolymarketMarketData(
                condition_id=condition_id,
                token_id=token_id,
                question="Market",  # Would need market info for full question
                outcome="YES",  # Would need token info for actual outcome
                bids=bids,  # FULL ORDER BOOK
                asks=asks,  # FULL ORDER BOOK
                raw_orderbook=order_book,
                api_call_start_ns=api_start,
                api_response_ns=api_end,
                processing_complete_ns=processing_complete
            )

        except Exception as e:
            logger.error(f"Error converting order book for {token_id}: {e}")
            return None

    def get_current_data(self) -> Dict[str, PolymarketMarketData]:
        """Get current market data without fetching new data"""
        return self.market_data.copy()

    def get_condition_data(self, condition_id: str) -> Dict[str, PolymarketMarketData]:
        """Get market data for specific condition ID"""
        return {k: v for k, v in self.market_data.items() if v.condition_id == condition_id}

    async def run_data_feed(self):
        """
        High-frequency data feed loop with full order book processing.
        """
        logger.info("Starting high-frequency Polymarket data feed...")
        self.is_running = True

        logger.info(f"   Data feed configuration:")
        logger.info(f"   Update interval: {self.market_data_interval}s")
        logger.info(f"   Active condition IDs: {len(self.active_condition_ids)}")
        logger.info(f"   Data callbacks: {len(self.data_callbacks)}")

        loop_count = 0
        while not self.stop_requested:
            try:
                loop_count += 1
                cycle_start = time.time()
                self.stats['total_cycles'] += 1

                logger.debug(f"Starting data cycle #{loop_count}")

                # Only fetch if we have active condition IDs
                if self.active_condition_ids:
                    logger.debug(
                        f"Data cycle #{loop_count} starting with {len(self.active_condition_ids)} conditions...")

                    # Fetch all market data with full order books
                    fresh_data = await self.fetch_complete_market_data()

                    if fresh_data:
                        # Update internal cache
                        self.market_data.update(fresh_data)
                        self.stats['successful_cycles'] += 1

                        cycle_duration = time.time() - cycle_start
                        self.stats['avg_cycle_time'] = (
                                (self.stats['avg_cycle_time'] * (self.stats['successful_cycles'] - 1) + cycle_duration)
                                / self.stats['successful_cycles']
                        )

                        logger.debug(
                            f"Cycle #{loop_count} completed successfully in {cycle_duration:.2f}s with {len(fresh_data)} tokens")
                    else:
                        logger.warning(f"Cycle #{loop_count} returned no data")
                        self.stats['failed_cycles'] += 1

                    # Calculate sleep time to maintain target interval
                    cycle_duration = time.time() - cycle_start
                    sleep_time = max(0, self.market_data_interval - cycle_duration)

                    if sleep_time > 0:
                        logger.debug(f"Sleeping {sleep_time:.2f}s until next cycle")
                        await asyncio.sleep(sleep_time)
                    else:
                        logger.warning(f"Cycle overran by {-sleep_time:.2f}s!")

                else:
                    logger.debug("No active condition IDs, sleeping...")
                    await asyncio.sleep(self.market_data_interval)

            except Exception as e:
                logger.error(f"Error in data feed loop #{loop_count}: {e}")
                import traceback
                logger.error(
                    f"Data feed loop exception traceback: {''.join(traceback.format_exception(type(e), e, e.__traceback__))}")
                self.stats['failed_cycles'] += 1
                await asyncio.sleep(1)  # Brief pause on error

        logger.info("High-frequency Polymarket data feed stopped")
        self.is_running = False

    def stop(self):
        """Stop the data feed"""
        logger.info("Stopping Polymarket data feed...")
        self.stop_requested = True

    def get_status(self) -> Dict[str, any]:
        """Get data feed status"""
        return {
            "is_running": self.is_running,
            "active_condition_ids": len(self.active_condition_ids),
            "cached_markets": len(self.market_data),
            "data_callbacks": len(self.data_callbacks),
            "statistics": self.stats.copy(),
            "coordinator_status": self.coordinator.get_status() if self.coordinator else {},
            "health": self.coordinator.health_check() if self.coordinator else {}
        }


# Callback functions for data distribution
async def print_market_data_callback(market_data: Dict[str, PolymarketMarketData]):
    """Print market data with full order book display"""
    if not market_data:
        return

    print(f"\nPOLYMARKET FULL BOOK UPDATE {datetime.now().strftime('%H:%M:%S')}")

    for token_id, data in market_data.items():
        print(f"{data.condition_id[:10]}... | {data.outcome}")
        print(f"  Full Bid Book ({len(data.bids)} levels):")
        for i, bid in enumerate(data.bids[:5]):  # Show top 5
            print(f"    [{i + 1}] ${bid.price:.3f} x {bid.size}")

        print(f"  Full Ask Book ({len(data.asks)} levels):")
        for i, ask in enumerate(data.asks[:5]):  # Show top 5
            print(f"    [{i + 1}] ${ask.price:.3f} x {ask.size}")

        print(f"  Mid: ${data.midpoint:.3f} | Spread: ${data.spread:.3f}")
        print(f"  Latency: {data.api_latency_us:.1f}μs | Age: {data.age_ms:.1f}ms")


# Standalone demo
async def run_standalone_demo():
    """Demo the Polymarket data feed with full order book display"""
    print("POLYMARKET DATA FEED DEMO (FULL ORDER BOOKS)")
    print("=" * 50)

    try:
        async with PolymarketDataFeed(market_data_interval=2.0) as feed:
            # Add callback
            feed.add_data_callback(print_market_data_callback)

            # Set test condition IDs (Irish Presidential Election)
            test_condition_ids = {
                "0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",  # Mairead McGuinness
                "0x17cd926896aa499bea870e218dc2e32c99f102237e6ae90f49f4b03d69cac4da",  # Conor McGregor
            }
            feed.set_active_condition_ids(test_condition_ids)

            print(f"Set active condition IDs: {[id[:10] + '...' for id in test_condition_ids]}")

            # Start data feed
            feed_task = asyncio.create_task(feed.run_data_feed())

            # Let it run for 30 seconds
            await asyncio.sleep(30)

            # Show status
            status = feed.get_status()
            print(f"\nFinal Status:")
            print(f"  Cycles: {status['statistics']['total_cycles']} "
                  f"({status['statistics']['successful_cycles']} successful)")
            print(f"  Markets processed: {status['statistics']['total_markets_processed']}")
            print(f"  Avg cycle time: {status['statistics']['avg_cycle_time']:.2f}s")
            print(f"  API calls: {status['statistics']['total_api_calls']}")

            feed.stop()
            await feed_task

            print("Polymarket data feed demo completed!")
            return True

    except Exception as e:
        print(f"Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        success = asyncio.run(run_standalone_demo())
        if not success:
            exit(1)
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Demo execution failed: {e}")
        exit(1)