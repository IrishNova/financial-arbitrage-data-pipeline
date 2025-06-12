#!/usr/bin/env python3
"""
broker/runtime_broker.py

Central runtime broker for arbitrage trading system.
Sole purpose: Direct traffic at runtime by parsing arbitrage pairs
and routing them to appropriate local and remote services.
"""

import sys
import os
import asyncio
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set
from dataclasses import dataclass

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from database.read_from import DatabaseReader, ArbPair
from api.data_feed import KalshiDataFeed
from broker.publisher import PublisherManager

logger = logging.getLogger(__name__)


@dataclass
class TrafficRouting:
    """Data structure for traffic routing decisions"""
    kalshi_tickers: Set[str]
    polymarket_condition_ids: Set[str]
    pair_count: int
    timestamp: float


class RuntimeBroker:
    """
    Central runtime traffic director for arbitrage trading system.
    Manages data flow between database and both local and remote services.

    Routes traffic to:
    - Local Kalshi data feed (Virginia server)
    - Remote Ireland server via ZeroMQ (for Polymarket data)
    """

    def __init__(self, refresh_interval: int = 180):
        """
        Initialize runtime broker.

        Args:
            refresh_interval: How often to refresh arbitrage pairs (seconds)
        """
        self.refresh_interval = refresh_interval

        # Components
        self.db_reader: Optional[DatabaseReader] = None
        self.kalshi_feed: Optional[KalshiDataFeed] = None
        self.publisher_manager: Optional[PublisherManager] = None

        # State tracking
        self.current_pairs: Dict[int, ArbPair] = {}
        self.current_routing: Optional[TrafficRouting] = None
        self.last_refresh = 0

        # Control flags
        self.is_running = False
        self.stop_requested = False

        logger.info(f"Runtime broker initialized with {refresh_interval}s refresh interval")

    async def __aenter__(self):
        """Async context manager entry"""
        # Initialize database connection
        self.db_reader = DatabaseReader()
        self.db_reader.connect()

        # Initialize Kalshi data feed
        self.kalshi_feed = KalshiDataFeed(
            market_data_interval=CONFIG.kalshi.min_delay
        )
        await self.kalshi_feed.__aenter__()

        # Initialize ZeroMQ publisher manager
        self.publisher_manager = PublisherManager()
        await self.publisher_manager.start()

        logger.info("Runtime broker connections established")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        self.stop_requested = True

        if self.kalshi_feed:
            await self.kalshi_feed.__aexit__(exc_type, exc_val, exc_tb)

        if self.publisher_manager:
            await self.publisher_manager.stop()

        if self.db_reader:
            self.db_reader.disconnect()

        logger.info("Runtime broker connections closed")

    def analyze_traffic_routing(self, pairs: List[ArbPair]) -> TrafficRouting:
        """
        Analyze arbitrage pairs and determine traffic routing.

        Args:
            pairs: List of arbitrage pairs from database

        Returns:
            TrafficRouting object with routing decisions
        """
        kalshi_tickers = set()
        polymarket_condition_ids = set()

        for pair in pairs:
            # Every pair has both Kalshi and Polymarket components
            kalshi_tickers.add(pair.kalshi_ticker)
            polymarket_condition_ids.add(pair.polymarket_condition_id)

        routing = TrafficRouting(
            kalshi_tickers=kalshi_tickers,
            polymarket_condition_ids=polymarket_condition_ids,
            pair_count=len(pairs),
            timestamp=time.time()
        )

        logger.info(f"Traffic analysis: {len(pairs)} pairs -> "
                    f"{len(kalshi_tickers)} Kalshi tickers, "
                    f"{len(polymarket_condition_ids)} Polymarket condition IDs")

        return routing

    def route_kalshi_traffic(self, kalshi_tickers: Set[str]):
        """
        Route Kalshi traffic to local data feed.

        Args:
            kalshi_tickers: Set of Kalshi tickers to subscribe to
        """
        if not self.kalshi_feed:
            logger.warning("Kalshi data feed not available for routing")
            return

        # Direct traffic to Kalshi data feed
        current_tickers = self.kalshi_feed.active_tickers

        if kalshi_tickers != current_tickers:
            added = kalshi_tickers - current_tickers
            removed = current_tickers - kalshi_tickers

            if added:
                logger.info(f"Routing new Kalshi traffic: {list(added)}")
            if removed:
                logger.info(f"Removing Kalshi traffic: {list(removed)}")

            # Update data feed with new ticker routing
            self.kalshi_feed.set_active_tickers(kalshi_tickers)

            logger.info(f"Kalshi traffic routed: {len(kalshi_tickers)} active subscriptions")

    async def route_polymarket_traffic(self, condition_ids: Set[str]) -> bool:
        """
        Route Polymarket traffic to Ireland server via ZeroMQ.

        Args:
            condition_ids: Set of Polymarket condition IDs

        Returns:
            True if routing was successful
        """
        if not self.publisher_manager:
            logger.error("Publisher manager not available for Polymarket routing")
            return False

        try:
            # Send condition IDs to Ireland server
            success = await self.publisher_manager.update_polymarket_subscriptions(condition_ids)

            if success:
                logger.info(f"Successfully routed Polymarket traffic to Ireland: {len(condition_ids)} condition IDs")

                # Log sample IDs for debugging
                if condition_ids:
                    sample_ids = list(condition_ids)[:3]
                    logger.debug(f"Sample condition IDs routed: {[id[:10] + '...' for id in sample_ids]}")
            else:
                logger.error(f"Failed to route Polymarket traffic to Ireland: {len(condition_ids)} condition IDs")

            return success

        except Exception as e:
            logger.error(f"Error routing Polymarket traffic: {e}")
            return False

    async def refresh_traffic_routing(self) -> bool:
        """
        Refresh arbitrage pairs and update traffic routing.

        Returns:
            True if routing changed, False otherwise
        """
        try:
            # Get current active pairs
            pairs = self.db_reader.get_active_pairs()

            # Create new pairs dictionary
            new_pairs = {pair.id: pair for pair in pairs}

            # Check if pairs changed
            pairs_changed = new_pairs.keys() != self.current_pairs.keys()

            if pairs_changed or not self.current_routing:
                if pairs_changed:
                    added = set(new_pairs.keys()) - set(self.current_pairs.keys())
                    removed = set(self.current_pairs.keys()) - set(new_pairs.keys())

                    if added:
                        logger.info(f"New arbitrage pairs detected: {len(added)}")
                    if removed:
                        logger.info(f"Removed arbitrage pairs: {len(removed)}")

                self.current_pairs = new_pairs

                # Analyze new routing requirements
                new_routing = self.analyze_traffic_routing(pairs)

                # Route Kalshi traffic (local)
                self.route_kalshi_traffic(new_routing.kalshi_tickers)

                # Route Polymarket traffic (to Ireland via ZeroMQ)
                polymarket_success = await self.route_polymarket_traffic(new_routing.polymarket_condition_ids)

                # Store current routing state
                self.current_routing = new_routing
                self.last_refresh = time.time()

                if polymarket_success:
                    logger.info(f"Traffic routing updated successfully: {new_routing.pair_count} pairs processed")
                else:
                    logger.warning(f"Traffic routing partially updated: Kalshi OK, Polymarket failed")

                return True

            self.last_refresh = time.time()
            return False

        except Exception as e:
            logger.error(f"Failed to refresh traffic routing: {e}")
            return False

    def get_current_routing_status(self) -> Dict[str, any]:
        """
        Get current traffic routing status.

        Returns:
            Dictionary with current routing information
        """
        if not self.current_routing:
            return {'status': 'no_routing_active'}

        # Get component health status
        kalshi_feed_status = self.kalshi_feed.is_running if self.kalshi_feed else False
        publisher_status = self.publisher_manager.get_status() if self.publisher_manager else {'is_active': False}

        return {
            'status': 'active',
            'pair_count': self.current_routing.pair_count,
            'kalshi_subscriptions': len(self.current_routing.kalshi_tickers),
            'polymarket_subscriptions': len(self.current_routing.polymarket_condition_ids),
            'last_refresh': self.last_refresh,
            'kalshi_feed_active': kalshi_feed_status,
            'publisher_active': publisher_status.get('is_active', False),
            'kalshi_tickers': list(self.current_routing.kalshi_tickers),
            'polymarket_condition_ids': list(self.current_routing.polymarket_condition_ids)
        }

    def get_polymarket_routing_data(self) -> Optional[Dict[str, any]]:
        """
        Get Polymarket routing data for external systems.

        Returns:
            Dictionary with Polymarket condition IDs and routing info
        """
        if not self.current_routing:
            return None

        return {
            'condition_ids': list(self.current_routing.polymarket_condition_ids),
            'timestamp': self.current_routing.timestamp,
            'pair_count': self.current_routing.pair_count,
            'routing_method': 'zeromq_publisher'
        }

    async def execute_trade_via_ireland(self, condition_id: str, side: str, size: float, price: float) -> Dict[
        str, any]:
        """
        Execute trade on Polymarket via Ireland server.

        Args:
            condition_id: Polymarket condition ID
            side: "YES" or "NO"
            size: Trade size
            price: Trade price

        Returns:
            Trade execution response
        """
        if not self.publisher_manager:
            logger.error("Cannot execute trade - publisher manager not available")
            return {"status": "ERROR", "message": "Publisher not available"}

        logger.info(f"Executing trade via Ireland: {condition_id[:10]}... {side} {size}@{price}")

        try:
            response = await self.publisher_manager.execute_trade(condition_id, side, size, price)

            if response.get('status') == 'SUCCESS':
                logger.info(f"Trade executed successfully via Ireland: {condition_id[:10]}...")
            else:
                logger.error(f"Trade execution failed: {response.get('message', 'Unknown error')}")

            return response

        except Exception as e:
            logger.error(f"Error executing trade via Ireland: {e}")
            return {"status": "ERROR", "message": str(e)}

    async def run_traffic_director(self):
        """
        Main traffic direction loop. Monitors pairs and directs traffic.
        """
        logger.info("Starting runtime traffic director...")
        self.is_running = True

        # Initial routing setup
        await self.refresh_traffic_routing()

        # Start Kalshi data feed
        kalshi_task = None
        if self.kalshi_feed:
            kalshi_task = asyncio.create_task(self.kalshi_feed.run_data_feed())

        # Main traffic direction loop
        try:
            while not self.stop_requested:
                try:
                    current_time = time.time()

                    # Check if we need to refresh routing
                    if current_time - self.last_refresh >= self.refresh_interval:
                        logger.info("Refreshing traffic routing...")
                        await self.refresh_traffic_routing()

                    # Status logging every 5 minutes
                    if int(current_time) % 300 == 0:
                        status = self.get_current_routing_status()
                        logger.info(f"Traffic status: {status['pair_count']} pairs, "
                                    f"{status['kalshi_subscriptions']} Kalshi routes, "
                                    f"{status['polymarket_subscriptions']} Polymarket routes, "
                                    f"Kalshi feed: {status['kalshi_feed_active']}, "
                                    f"Publisher: {status['publisher_active']}")

                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"Error in traffic director loop: {e}")
                    await asyncio.sleep(5)

        finally:
            # Cleanup
            if kalshi_task and not kalshi_task.done():
                self.kalshi_feed.stop()
                await kalshi_task

        logger.info("Runtime traffic director stopped")
        self.is_running = False

    def stop(self):
        """Stop the runtime broker"""
        logger.info("Stopping runtime traffic director...")
        self.stop_requested = True

    def get_health_status(self) -> Dict[str, any]:
        """Get comprehensive health status of runtime broker"""
        current_time = time.time()

        # Component health checks
        db_healthy = self.db_reader.health_check() if self.db_reader else False
        kalshi_healthy = self.kalshi_feed.is_running if self.kalshi_feed else False

        publisher_status = self.publisher_manager.get_status() if self.publisher_manager else {'is_active': False}
        publisher_healthy = publisher_status.get('is_active', False)

        return {
            'is_running': self.is_running,
            'components': {
                'database': db_healthy,
                'kalshi_feed': kalshi_healthy,
                'publisher': publisher_healthy
            },
            'routing': self.get_current_routing_status(),
            'last_refresh': self.last_refresh,
            'refresh_interval': self.refresh_interval,
            'uptime_seconds': current_time - (self.last_refresh or current_time)
        }


async def run_broker_demo():
    """Demo the runtime broker with ZeroMQ integration"""
    print("RUNTIME TRAFFIC DIRECTOR WITH ZEROMQ DEMO")
    print("=" * 50)

    try:
        async with RuntimeBroker(refresh_interval=60) as broker:
            print("Runtime broker initialized")

            # Test initial routing
            print("Analyzing traffic routing...")
            changed = await broker.refresh_traffic_routing()

            status = broker.get_current_routing_status()
            print(f"Routing status: {status}")
            print()

            if broker.current_pairs:
                print("Current arbitrage pairs:")
                for i, (pair_id, pair) in enumerate(list(broker.current_pairs.items())[:3]):
                    print(f"  {i + 1}. {pair.kalshi_ticker} <-> {pair.polymarket_condition_id[:10]}...")
                    print(f"     Description: {pair.pair_description}")
                print()

                # Show routing decisions
                print("Traffic routing decisions:")
                if broker.current_routing:
                    print(f"  Kalshi tickers: {list(broker.current_routing.kalshi_tickers)}")
                    print(f"  Polymarket condition IDs: {len(broker.current_routing.polymarket_condition_ids)}")

                    # Show Polymarket routing data
                    polymarket_data = broker.get_polymarket_routing_data()
                    if polymarket_data:
                        print(f"  Routed to Ireland via ZeroMQ: {polymarket_data['pair_count']} pairs")
                print()

            # Show health status
            health = broker.get_health_status()
            print("Health Status:")
            print(f"  Database: {health['components']['database']}")
            print(f"  Kalshi Feed: {health['components']['kalshi_feed']}")
            print(f"  ZeroMQ Publisher: {health['components']['publisher']}")
            print()

            # Test trade execution (will timeout without Ireland server)
            if broker.current_pairs:
                sample_pair = next(iter(broker.current_pairs.values()))
                print(f"Testing trade execution via Ireland...")
                response = await broker.execute_trade_via_ireland(
                    condition_id=sample_pair.polymarket_condition_id,
                    side="YES",
                    size=100.0,
                    price=0.65
                )
                print(f"Trade response: {response}")
                print()

            # Run traffic director for 30 seconds
            print("Running traffic director for 30 seconds...")
            director_task = asyncio.create_task(broker.run_traffic_director())

            await asyncio.sleep(30)

            broker.stop()
            await director_task

            print("Runtime traffic director demo completed!")

    except Exception as e:
        print(f"Runtime broker demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        asyncio.run(run_broker_demo())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Error: {e}")