#!/usr/bin/env python3
"""
api/data_feed.py

Production Kalshi market data feed for arbitrage trading system.
Uses lean market data structures optimized for high-frequency arbitrage.
Enhanced with debug logging to trace data flow.
"""

import sys
import os
import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Callable
from dataclasses import dataclass
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from api.connection import KalshiConnection

logger = logging.getLogger(__name__)


@dataclass
class OrderLevel:
    """Single price level in orderbook"""
    price: int  # Price in cents
    size: int  # Shares available


@dataclass
class KalshiMarketData:
    """Raw market data - minimal processing for maximum speed"""
    ticker: str
    title: str = ""
    status: str = ""
    yes_bid: int = 0
    yes_ask: int = 0
    no_bid: int = 0
    no_ask: int = 0
    volume: float = 0.0

    # Raw orderbook data - exactly as received from API
    raw_orderbook: dict = None

    # Precise timing
    api_call_start_ns: int = 0
    api_response_ns: int = 0
    processing_complete_ns: int = 0

    def __post_init__(self):
        """Initialize empty dict if None"""
        if self.raw_orderbook is None:
            self.raw_orderbook = {}

    # Quick access to best prices (derived from depth)
    @property
    def best_yes_bid(self) -> int:
        return self.yes_bids[0].price if self.yes_bids else 0

    @property
    def best_yes_ask(self) -> int:
        return self.yes_asks[0].price if self.yes_asks else 0

    @property
    def best_no_bid(self) -> int:
        return self.no_bids[0].price if self.no_bids else 0

    @property
    def best_no_ask(self) -> int:
        return self.no_asks[0].price if self.no_asks else 0

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

    def calculate_fill_price(self, side: str, direction: str, shares: int) -> float:
        """Calculate average fill price for a given order size"""
        if side == 'yes' and direction == 'buy':
            levels = self.yes_asks
        elif side == 'yes' and direction == 'sell':
            levels = self.yes_bids
        elif side == 'no' and direction == 'buy':
            levels = self.no_asks
        elif side == 'no' and direction == 'sell':
            levels = self.no_bids
        else:
            return 0.0

        remaining = shares
        total_cost = 0

        for level in levels:
            if remaining <= 0:
                break

            fill_size = min(remaining, level.size)
            total_cost += fill_size * level.price
            remaining -= fill_size

        if remaining > 0:
            return 0.0  # Not enough liquidity

        return total_cost / shares  # Average price


class KalshiDataFeed:
    """
    Production Kalshi data feed for arbitrage trading.
    Optimized for high-frequency updates with lean market data.
    """

    def __init__(self, market_data_interval: float = 1.0):
        """
        Initialize production data feed.

        Args:
            market_data_interval: How often to fetch market data (seconds)
        """
        self.market_data_interval = market_data_interval

        # Connection
        self.kalshi_connection: Optional[KalshiConnection] = None

        # State - managed by runtime broker
        self.active_tickers: Set[str] = set()
        self.market_data: Dict[str, dict] = {}
        self.last_data_fetch = 0

        # Callbacks for sending data to other components
        self.data_callbacks: List[Callable] = []

        # Control flags
        self.is_running = False
        self.stop_requested = False

        logger.info(f"Production Kalshi data feed initialized - update interval: {market_data_interval}s")

    async def __aenter__(self):
        """Async context manager entry"""
        self.kalshi_connection = KalshiConnection()
        await self.kalshi_connection.__aenter__()

        logger.info("Kalshi data feed connection established")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        self.stop_requested = True

        if self.kalshi_connection:
            await self.kalshi_connection.__aexit__(exc_type, exc_val, exc_tb)

        logger.info("Kalshi data feed connection closed")

    def set_active_tickers(self, tickers: Set[str]):
        """
        Set active tickers from runtime broker.

        Args:
            tickers: Set of Kalshi tickers to monitor
        """
        if tickers != self.active_tickers:
            added = tickers - self.active_tickers
            removed = self.active_tickers - tickers

            if added:
                logger.info(f"Adding Kalshi tickers: {list(added)}")
            if removed:
                logger.info(f"Removing Kalshi tickers: {list(removed)}")
                # Clean up removed tickers
                for ticker in removed:
                    self.market_data.pop(ticker, None)

            self.active_tickers = tickers
            logger.info(f"Active Kalshi tickers updated: {len(self.active_tickers)} total")

    def add_data_callback(self, callback: Callable):
        """
        Add callback function to receive market data updates.

        Args:
            callback: Function that will receive Dict[str, LeanMarketData]
        """
        self.data_callbacks.append(callback)
        logger.info(f"Added data callback: {callback.__name__}")

    async def fetch_complete_market_data(self) -> Dict[str, dict]:
        """
        Fetch raw orderbook data with timing info added.
        Returns dict exactly as received from API plus timing fields.
        """
        if not self.active_tickers:
            return {}

        current_data = {}
        semaphore = asyncio.Semaphore(8)

        async def fetch_ticker(ticker: str) -> Optional[dict]:
            """Get raw orderbook for one ticker"""
            async with semaphore:
                try:
                    api_start_ns = time.time_ns()

                    orderbook_response = await self.kalshi_connection._make_request(
                        'GET', f'/trade-api/v2/markets/{ticker}/orderbook'
                    )

                    api_response_ns = time.time_ns()

                    # DEBUG LOGGING - Log raw API response
                    logger.info(f"🔍 RAW KALSHI API RESPONSE for {ticker}:")
                    logger.info(f"   Response type: {type(orderbook_response)}")
                    logger.info(
                        f"   Response keys: {list(orderbook_response.keys()) if orderbook_response else 'None'}")

                    if orderbook_response and 'orderbook' in orderbook_response:
                        raw_orderbook = orderbook_response['orderbook']
                        logger.info(f"   Orderbook type: {type(raw_orderbook)}")
                        logger.info(f"   Orderbook keys: {list(raw_orderbook.keys()) if raw_orderbook else 'None'}")

                        # Log YES side structure
                        if 'yes' in raw_orderbook:
                            yes_data = raw_orderbook['yes']
                            logger.info(f"   YES side type: {type(yes_data)}")
                            logger.info(
                                f"   YES side length: {len(yes_data) if isinstance(yes_data, list) else 'Not a list'}")
                            if isinstance(yes_data, list) and len(yes_data) > 0:
                                logger.info(f"   YES first item: {yes_data[0]}")
                                logger.info(f"   YES first item type: {type(yes_data[0])}")

                        # Log NO side structure
                        if 'no' in raw_orderbook:
                            no_data = raw_orderbook['no']
                            logger.info(f"   NO side type: {type(no_data)}")
                            logger.info(
                                f"   NO side length: {len(no_data) if isinstance(no_data, list) else 'Not a list'}")
                            if isinstance(no_data, list) and len(no_data) > 0:
                                logger.info(f"   NO first item: {no_data[0]}")
                                logger.info(f"   NO first item type: {type(no_data[0])}")

                        # Log complete raw orderbook (first few levels only)
                        logger.info(f"   Raw orderbook structure: {raw_orderbook}")
                    else:
                        logger.warning(f"   ❌ No orderbook in response for {ticker}")

                    if not orderbook_response or 'orderbook' not in orderbook_response:
                        return None

                    # Take the raw orderbook dict and just add timing + ticker
                    raw_data = orderbook_response['orderbook'].copy()
                    raw_data['ticker'] = ticker
                    raw_data['title'] = 'Market'  # Add title field for coordinator compatibility
                    raw_data['status'] = 'active'  # Add status field for coordinator compatibility
                    raw_data['volume'] = 0.0  # Add volume field for coordinator compatibility
                    raw_data['api_call_start_ns'] = api_start_ns
                    raw_data['api_response_ns'] = api_response_ns
                    raw_data['processing_complete_ns'] = time.time_ns()

                    return raw_data

                except Exception as e:
                    logger.error(f"Error fetching {ticker}: {e}")
                    return None

        # Fetch all tickers
        tasks = [fetch_ticker(ticker) for ticker in self.active_tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Build results dict
        for i, result in enumerate(results):
            ticker = list(self.active_tickers)[i]
            if result and not isinstance(result, Exception):
                current_data[ticker] = result

        # DEBUG LOGGING - Log processed data before sending to callbacks
        if current_data:
            logger.info(f"🔄 PROCESSED DATA BEFORE CALLBACKS:")
            logger.info(f"   Number of tickers processed: {len(current_data)}")

            for ticker, data in current_data.items():
                logger.info(f"   📊 TICKER {ticker}:")
                logger.info(f"      Data type: {type(data)}")
                logger.info(f"      Data keys: {list(data.keys())}")

                # Check for timing fields
                timing_fields = ['api_call_start_ns', 'api_response_ns', 'processing_complete_ns']
                for field in timing_fields:
                    if field in data:
                        logger.info(f"      {field}: {data[field]}")

                # Check for orderbook structure
                if 'yes' in data:
                    logger.info(
                        f"      YES levels: {len(data['yes']) if isinstance(data['yes'], list) else 'Not a list'}")
                if 'no' in data:
                    logger.info(f"      NO levels: {len(data['no']) if isinstance(data['no'], list) else 'Not a list'}")

                # Log a subset of the data to avoid spam
                data_subset = {k: v for k, v in data.items() if k in ['ticker', 'title', 'status', 'volume']}
                logger.info(f"      Basic fields: {data_subset}")

            logger.info(f"   Sending to {len(self.data_callbacks)} callbacks")

        # Send to callbacks
        if current_data:
            logger.info(f"Sending {len(current_data)} markets to callbacks")

            # Log all the data we're sending
            for ticker, data in current_data.items():
                logger.info(f"DATA FOR {ticker}:")
                logger.info(f"  Raw orderbook: {data}")

            for callback in self.data_callbacks:
                try:
                    await callback(current_data)
                except Exception as e:
                    logger.error(f"Callback error: {e}")

        return current_data

    def get_current_data(self) -> Dict[str, dict]:
        """Get current market data without fetching new data"""
        return self.market_data.copy()

    def get_ticker_data(self, ticker: str) -> Optional[dict]:
        """Get market data for specific ticker"""
        return self.market_data.get(ticker)

    async def run_data_feed(self):
        """
        High-frequency data feed loop optimized for 1-second updates.
        """
        logger.info("🚀 Starting high-frequency Kalshi data feed (1-second updates)...")
        self.is_running = True

        logger.info(f"📊 Data feed configuration:")
        logger.info(f"   Update interval: {self.market_data_interval}s")
        logger.info(f"   Active tickers: {len(self.active_tickers)}")
        logger.info(f"   API calls per cycle: {len(self.active_tickers)}")

        loop_count = 0
        while not self.stop_requested:
            try:
                loop_count += 1
                cycle_start = time.time()

                # Only fetch if we have active tickers
                if self.active_tickers:
                    logger.debug(f"🔄 Data cycle #{loop_count} starting...")

                    # Fetch all market data in parallel
                    await self.fetch_complete_market_data()

                    cycle_duration = time.time() - cycle_start
                    logger.debug(f"✅ Cycle #{loop_count} completed in {cycle_duration:.2f}s")

                    # Calculate sleep time to maintain target intervals
                    sleep_time = max(0, self.market_data_interval - cycle_duration)

                    if sleep_time > 0:
                        logger.debug(f"⏳ Sleeping {sleep_time:.2f}s until next cycle")
                        await asyncio.sleep(sleep_time)
                    else:
                        logger.warning(f"⚠️  Cycle overran by {-sleep_time:.2f}s!")

                else:
                    logger.debug("⏸️  No active tickers, sleeping...")
                    await asyncio.sleep(self.market_data_interval)

            except Exception as e:
                logger.error(f"💥 Error in data feed loop #{loop_count}: {e}")
                await asyncio.sleep(1)  # Brief pause on error

        logger.info("🛑 High-frequency Kalshi data feed stopped")
        self.is_running = False

    def stop(self):
        """Stop the data feed"""
        logger.info("Stopping high-frequency data feed...")
        self.stop_requested = True


# Callback functions for data distribution
async def print_market_data_callback(market_data: Dict[str, dict]):
    """Print raw market data"""
    if not market_data:
        return

    print(f"\nKALSHI MARKET UPDATE {datetime.now().strftime('%H:%M:%S')}")

    for ticker, data in market_data.items():
        yes_levels = len(data.get('yes', []))
        no_levels = len(data.get('no', []))
        api_latency = (data.get('api_response_ns', 0) - data.get('api_call_start_ns', 0)) / 1000
        print(f"{ticker}: YES {yes_levels} levels, NO {no_levels} levels, Latency: {api_latency:.1f}us")


# Standalone demo
async def run_standalone_demo():
    """Demo the lean data feed"""
    print("LEAN KALSHI DATA FEED DEMO")
    print("=" * 50)

    try:
        async with KalshiDataFeed(market_data_interval=2.0) as feed:
            # Add callback
            feed.add_data_callback(print_market_data_callback)

            # Set test ticker
            test_tickers = {"KXPRESIRELAND-25-MM"}
            feed.set_active_tickers(test_tickers)

            print(f"Set active tickers: {test_tickers}")

            # Start data feed
            feed_task = asyncio.create_task(feed.run_data_feed())

            # Let it run for 30 seconds
            await asyncio.sleep(30)

            feed.stop()
            await feed_task

            print("Lean data feed demo completed!")
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