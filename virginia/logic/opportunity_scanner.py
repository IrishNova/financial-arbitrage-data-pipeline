#!/usr/bin/env python3
"""
logic/opportunity_scanner.py

Placeholder opportunity scanner for arbitrage detection.
Receives data from both Kalshi and Polymarket sources and processes for arbitrage opportunities.

TODO: Implement actual arbitrage detection algorithms, risk calculations, and trade signals.
"""

import sys
import asyncio
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, NamedTuple
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.logger import get_scanner_logger
from utils.config import CONFIG
from api.data_feed import KalshiMarketData
from broker.message_broker import PolymarketData


@dataclass
class ArbitrageOpportunity:
    """Detected arbitrage opportunity"""
    kalshi_ticker: str
    polymarket_condition_id: str
    kalshi_side: str  # 'yes' or 'no'
    polymarket_side: str  # 'yes' or 'no'
    kalshi_price: float
    polymarket_price: float
    profit_margin: float
    confidence_score: float
    timestamp: float
    expiration_date: str


@dataclass
class DataFreshness:
    """Track data freshness for arbitrage pairs"""
    kalshi_timestamp: float
    polymarket_timestamp: float
    pair_complete: bool  # True if we have both Kalshi and Polymarket data


class OpportunityScanner:
    """
    Placeholder opportunity scanner for arbitrage detection.

    This receives market data from both Kalshi and Polymarket sources,
    maintains synchronized data structures, and scans for arbitrage opportunities.

    Current placeholder functionality:
    - Receives and logs data from both sources
    - Maintains LIFO queues for fresh data processing
    - Tracks data freshness and completeness
    - Logs detected price discrepancies

    TODO for production:
    - Implement sophisticated arbitrage detection algorithms
    - Add risk calculation and position sizing
    - Create trade signal generation
    - Add profit/loss tracking
    - Implement market impact analysis
    """

    def __init__(self):
        """Initialize the opportunity scanner"""
        self.logger = get_scanner_logger()

        # Data storage - LIFO queues for freshest data processing
        self.kalshi_data_queue: asyncio.LifoQueue = asyncio.LifoQueue(maxsize=1000)
        self.polymarket_data_queue: asyncio.LifoQueue = asyncio.LifoQueue(maxsize=1000)

        # Current market data snapshots
        self.current_kalshi_data: Dict[str, KalshiMarketData] = {}
        self.current_polymarket_data: Dict[str, PolymarketData] = {}

        # Data freshness tracking
        self.data_freshness: Dict[str, DataFreshness] = {}  # Key: kalshi_ticker or condition_id

        # Arbitrage pair mapping (loaded from database via coordinator)
        self.ticker_to_condition_id: Dict[str, str] = {}
        self.condition_id_to_ticker: Dict[str, str] = {}

        # Opportunity tracking
        self.detected_opportunities: List[ArbitrageOpportunity] = []
        self.last_scan_time = 0

        # Statistics
        self.stats = {
            'kalshi_updates_received': 0,
            'polymarket_updates_received': 0,
            'scans_performed': 0,
            'opportunities_detected': 0,
            'pairs_with_complete_data': 0
        }

        # Control flags
        self.is_running = False
        self.shutdown_requested = False

        self.logger.info("Opportunity scanner placeholder initialized")

    def set_arbitrage_pairs(self, pairs_mapping: Dict[str, str]):
        """
        Set the mapping between Kalshi tickers and Polymarket condition IDs.

        Args:
            pairs_mapping: Dictionary mapping kalshi_ticker -> polymarket_condition_id
        """
        self.ticker_to_condition_id = pairs_mapping.copy()
        self.condition_id_to_ticker = {v: k for k, v in pairs_mapping.items()}

        self.logger.info(f"Updated arbitrage pairs mapping: {len(pairs_mapping)} pairs")

        # Initialize data freshness tracking
        for ticker, condition_id in pairs_mapping.items():
            self.data_freshness[ticker] = DataFreshness(
                kalshi_timestamp=0,
                polymarket_timestamp=0,
                pair_complete=False
            )

    async def receive_kalshi_data(self, market_data: Dict[str, dict]):
        """
        Receive Kalshi market data from the coordinator.
        FIXED: Now handles raw orderbook data format from KalshiDataFeed

        Args:
            market_data: Dictionary of ticker -> raw orderbook dict
        """
        if not market_data:
            return

        self.stats['kalshi_updates_received'] += 1

        try:
            # Convert raw orderbook data to simplified format for processing
            processed_data = {}

            for ticker, raw_data in market_data.items():
                try:
                    # Extract orderbook data
                    yes_levels = raw_data.get('yes', [])
                    no_levels = raw_data.get('no', [])

                    if not yes_levels or not no_levels:
                        self.logger.warning(f"No orderbook data for {ticker}")
                        continue

                    # Calculate best bid/ask from orderbook levels
                    # Each level is [price_cents, quantity]
                    yes_best_bid = max(level[0] for level in yes_levels) if yes_levels else 0
                    yes_best_ask = min(level[0] for level in yes_levels) if yes_levels else 0
                    no_best_bid = max(level[0] for level in no_levels) if no_levels else 0
                    no_best_ask = min(level[0] for level in no_levels) if no_levels else 0

                    # Create simplified data structure for opportunity detection
                    processed_data[ticker] = {
                        'ticker': ticker,
                        'yes_bid': yes_best_bid,
                        'yes_ask': yes_best_ask,
                        'no_bid': no_best_bid,
                        'no_ask': no_best_ask,
                        'volume': raw_data.get('volume', 0),
                        'status': raw_data.get('status', 'unknown'),
                        'timestamp': time.time(),
                        'raw_data': raw_data  # Keep raw data for reference
                    }

                except Exception as e:
                    self.logger.error(f"Error processing {ticker} data: {e}")
                    continue

            if not processed_data:
                self.logger.warning("No valid Kalshi data to process")
                return

            # Add to LIFO queue for processing
            await self.kalshi_data_queue.put(('kalshi', processed_data, time.time()))

            # Update current data snapshot
            self.current_kalshi_data.update(processed_data)

            # Update freshness tracking
            current_time = time.time()
            for ticker in processed_data.keys():
                if ticker in self.data_freshness:
                    self.data_freshness[ticker].kalshi_timestamp = current_time
                    self._update_pair_completeness(ticker)

            # Log sample data
            sample_ticker = next(iter(processed_data.keys()))
            sample_data = processed_data[sample_ticker]

            self.logger.debug(f"KALSHI DATA PROCESSED: {sample_ticker} | "
                              f"YES {sample_data['yes_bid']}¢/{sample_data['yes_ask']}¢ | "
                              f"NO {sample_data['no_bid']}¢/{sample_data['no_ask']}¢ | "
                              f"Vol: {sample_data['volume']:,.0f} | "
                              f"[+{len(processed_data) - 1} others]")

        except Exception as e:
            self.logger.error(f"Error receiving Kalshi data: {e}")

    def _detect_simple_arbitrage(self, ticker: str, condition_id: str,
                                 kalshi_data: dict,  # Now expects dict instead of KalshiMarketData
                                 polymarket_data) -> Optional[ArbitrageOpportunity]:
        """
        Simple arbitrage detection algorithm (placeholder).
        FIXED: Now handles raw data format
        """
        try:
            # Convert Kalshi prices from cents to dollars
            kalshi_yes_bid = kalshi_data['yes_bid'] / 100
            kalshi_yes_ask = kalshi_data['yes_ask'] / 100
            kalshi_no_bid = kalshi_data['no_bid'] / 100
            kalshi_no_ask = kalshi_data['no_ask'] / 100

            # Polymarket prices are already in dollars
            poly_yes_price = polymarket_data.yes_price
            poly_no_price = polymarket_data.no_price

            # Simple arbitrage detection: look for price discrepancies > min threshold
            min_profit_threshold = CONFIG.trading.min_profit_threshold

            opportunities = []

            # Check YES side arbitrage: Buy low, sell high
            if poly_yes_price < kalshi_yes_bid:
                profit_margin = (kalshi_yes_bid - poly_yes_price) / poly_yes_price
                if profit_margin > min_profit_threshold:
                    opportunities.append(ArbitrageOpportunity(
                        kalshi_ticker=ticker,
                        polymarket_condition_id=condition_id,
                        kalshi_side='sell_yes',
                        polymarket_side='buy_yes',
                        kalshi_price=kalshi_yes_bid,
                        polymarket_price=poly_yes_price,
                        profit_margin=profit_margin,
                        confidence_score=0.8,
                        timestamp=time.time(),
                        expiration_date='TBD'
                    ))

            # Check NO side arbitrage
            if poly_no_price < kalshi_no_bid:
                profit_margin = (kalshi_no_bid - poly_no_price) / poly_no_price
                if profit_margin > min_profit_threshold:
                    opportunities.append(ArbitrageOpportunity(
                        kalshi_ticker=ticker,
                        polymarket_condition_id=condition_id,
                        kalshi_side='sell_no',
                        polymarket_side='buy_no',
                        kalshi_price=kalshi_no_bid,
                        polymarket_price=poly_no_price,
                        profit_margin=profit_margin,
                        confidence_score=0.8,
                        timestamp=time.time(),
                        expiration_date='TBD'
                    ))

            # Return the best opportunity if any found
            if opportunities:
                return max(opportunities, key=lambda x: x.profit_margin)

            return None

        except Exception as e:
            self.logger.error(f"Error in arbitrage detection for {ticker}: {e}")
            return None

    async def receive_polymarket_data(self, market_data: Dict[str, PolymarketData]):
        """
        Receive Polymarket market data from the message broker.

        Args:
            market_data: Dictionary of condition_id -> PolymarketData
        """
        if not market_data:
            return

        self.stats['polymarket_updates_received'] += 1

        try:
            # Add to LIFO queue for processing
            await self.polymarket_data_queue.put(('polymarket', market_data, time.time()))

            # Update current data snapshot
            self.current_polymarket_data.update(market_data)

            # Update freshness tracking
            current_time = time.time()
            for condition_id, data in market_data.items():
                # Find corresponding Kalshi ticker
                ticker = self.condition_id_to_ticker.get(condition_id)
                if ticker and ticker in self.data_freshness:
                    self.data_freshness[ticker].polymarket_timestamp = current_time
                    self._update_pair_completeness(ticker)

            # Log sample data
            sample_condition_id = next(iter(market_data.keys()))
            sample_data = market_data[sample_condition_id]

            self.logger.info(f"POLYMARKET DATA RECEIVED: {sample_condition_id[:8]}... | "
                             f"YES ${sample_data.yes_price:.3f} | NO ${sample_data.no_price:.3f} | "
                             f"Vol: {sample_data.volume:,.0f} | "
                             f"[+{len(market_data) - 1} others]")

        except Exception as e:
            self.logger.error(f"Error receiving Polymarket data: {e}")

    def _update_pair_completeness(self, ticker: str):
        """
        Update whether a trading pair has complete data from both sources.

        Args:
            ticker: Kalshi ticker to check
        """
        if ticker not in self.data_freshness:
            return

        freshness = self.data_freshness[ticker]
        current_time = time.time()

        # Consider data fresh if received within last 60 seconds
        kalshi_fresh = (current_time - freshness.kalshi_timestamp) < 60
        polymarket_fresh = (current_time - freshness.polymarket_timestamp) < 60

        was_complete = freshness.pair_complete
        freshness.pair_complete = kalshi_fresh and polymarket_fresh

        # Log when pairs become complete or incomplete
        if freshness.pair_complete and not was_complete:
            self.logger.debug(f"Pair {ticker} now has complete data")
        elif not freshness.pair_complete and was_complete:
            self.logger.debug(f"Pair {ticker} lost complete data")

    async def _process_data_queue(self):
        """
        Process data from LIFO queues - always gets the freshest data first.
        """
        while not self.shutdown_requested:
            try:
                # Process Kalshi data queue
                try:
                    source, data, timestamp = await asyncio.wait_for(
                        self.kalshi_data_queue.get(), timeout=0.1
                    )
                    self.logger.debug(f"Processed {source} data from queue: {len(data)} items")
                except asyncio.TimeoutError:
                    pass

                # Process Polymarket data queue
                try:
                    source, data, timestamp = await asyncio.wait_for(
                        self.polymarket_data_queue.get(), timeout=0.1
                    )
                    self.logger.debug(f"Processed {source} data from queue: {len(data)} items")
                except asyncio.TimeoutError:
                    pass

                await asyncio.sleep(0.1)  # Small delay to prevent busy waiting

            except Exception as e:
                self.logger.error(f"Error processing data queue: {e}")
                await asyncio.sleep(1)

    async def _scan_for_opportunities(self):
        """
        Scan current market data for arbitrage opportunities.
        TODO: Implement sophisticated arbitrage detection algorithms.
        """
        if not self.ticker_to_condition_id:
            return

        self.stats['scans_performed'] += 1
        current_time = time.time()

        complete_pairs = 0
        opportunities_found = 0

        for ticker, condition_id in self.ticker_to_condition_id.items():
            # Check if we have fresh data for both sides
            if ticker not in self.data_freshness or not self.data_freshness[ticker].pair_complete:
                continue

            kalshi_data = self.current_kalshi_data.get(ticker)
            polymarket_data = self.current_polymarket_data.get(condition_id)

            if not kalshi_data or not polymarket_data:
                continue

            complete_pairs += 1

            # Simple arbitrage detection (placeholder algorithm)
            # TODO: Implement sophisticated detection with proper risk calculations
            opportunity = self._detect_simple_arbitrage(ticker, condition_id, kalshi_data, polymarket_data)

            if opportunity:
                opportunities_found += 1
                self.detected_opportunities.append(opportunity)

                # Log the opportunity
                self.logger.warning(f"ARBITRAGE OPPORTUNITY DETECTED!")
                self.logger.warning(f"  Pair: {ticker} <-> {condition_id[:8]}...")
                self.logger.warning(
                    f"  Strategy: {opportunity.kalshi_side} Kalshi, {opportunity.polymarket_side} Polymarket")
                self.logger.warning(
                    f"  Prices: Kalshi ${opportunity.kalshi_price:.3f}, Polymarket ${opportunity.polymarket_price:.3f}")
                self.logger.warning(f"  Profit Margin: {opportunity.profit_margin:.2%}")
                self.logger.warning(f"  Confidence: {opportunity.confidence_score:.2f}")

        # Update statistics
        self.stats['pairs_with_complete_data'] = complete_pairs
        self.stats['opportunities_detected'] += opportunities_found
        self.last_scan_time = current_time

        # Log scan summary
        if complete_pairs > 0:
            self.logger.debug(f"Opportunity scan completed: {complete_pairs} pairs analyzed, "
                              f"{opportunities_found} opportunities found")

    def _detect_simple_arbitrage(self, ticker: str, condition_id: str,
                                 kalshi_data: KalshiMarketData,
                                 polymarket_data: PolymarketData) -> Optional[ArbitrageOpportunity]:
        """
        Simple arbitrage detection algorithm (placeholder).
        TODO: Replace with sophisticated detection logic.

        Args:
            ticker: Kalshi ticker
            condition_id: Polymarket condition ID
            kalshi_data: Kalshi market data
            polymarket_data: Polymarket market data

        Returns:
            ArbitrageOpportunity if found, None otherwise
        """
        # Convert Kalshi prices from cents to dollars
        kalshi_yes_bid = kalshi_data.yes_bid / 100
        kalshi_yes_ask = kalshi_data.yes_ask / 100
        kalshi_no_bid = kalshi_data.no_bid / 100
        kalshi_no_ask = kalshi_data.no_ask / 100

        # Polymarket prices are already in dollars
        poly_yes_price = polymarket_data.yes_price
        poly_no_price = polymarket_data.no_price

        # Simple arbitrage detection: look for price discrepancies > 5%
        min_profit_threshold = CONFIG.trading.min_profit_threshold

        opportunities = []

        # Check YES side arbitrage: Buy low, sell high
        if poly_yes_price < kalshi_yes_bid:
            profit_margin = (kalshi_yes_bid - poly_yes_price) / poly_yes_price
            if profit_margin > min_profit_threshold:
                opportunities.append(ArbitrageOpportunity(
                    kalshi_ticker=ticker,
                    polymarket_condition_id=condition_id,
                    kalshi_side='sell_yes',
                    polymarket_side='buy_yes',
                    kalshi_price=kalshi_yes_bid,
                    polymarket_price=poly_yes_price,
                    profit_margin=profit_margin,
                    confidence_score=0.8,  # Placeholder confidence
                    timestamp=time.time(),
                    expiration_date='TBD'
                ))

        # Check NO side arbitrage
        if poly_no_price < kalshi_no_bid:
            profit_margin = (kalshi_no_bid - poly_no_price) / poly_no_price
            if profit_margin > min_profit_threshold:
                opportunities.append(ArbitrageOpportunity(
                    kalshi_ticker=ticker,
                    polymarket_condition_id=condition_id,
                    kalshi_side='sell_no',
                    polymarket_side='buy_no',
                    kalshi_price=kalshi_no_bid,
                    polymarket_price=poly_no_price,
                    profit_margin=profit_margin,
                    confidence_score=0.8,  # Placeholder confidence
                    timestamp=time.time(),
                    expiration_date='TBD'
                ))

        # Return the best opportunity if any found
        if opportunities:
            return max(opportunities, key=lambda x: x.profit_margin)

        return None

    async def _opportunity_scan_loop(self):
        """
        Main opportunity scanning loop.
        Continuously scans for arbitrage opportunities.
        """
        self.logger.info("Starting opportunity scanning loop...")

        scan_interval = 1.0  # Scan every second for opportunities

        while not self.shutdown_requested:
            try:
                current_time = time.time()

                # Perform opportunity scan
                if current_time - self.last_scan_time >= scan_interval:
                    await self._scan_for_opportunities()

                # Clean up old opportunities (keep last 100)
                if len(self.detected_opportunities) > 100:
                    self.detected_opportunities = self.detected_opportunities[-100:]

                await asyncio.sleep(0.5)  # Check twice per second

            except Exception as e:
                self.logger.error(f"Error in opportunity scan loop: {e}")
                await asyncio.sleep(5)

        self.logger.info("Opportunity scanning loop stopped")

    async def _statistics_report_loop(self):
        """
        Periodic statistics reporting loop.
        """
        self.logger.info("Starting statistics reporting loop...")

        while not self.shutdown_requested:
            try:
                await asyncio.sleep(60)  # Report every minute

                # Log statistics summary
                self.logger.info(f"SCANNER STATS - Kalshi updates: {self.stats['kalshi_updates_received']}, "
                                 f"Polymarket updates: {self.stats['polymarket_updates_received']}, "
                                 f"Scans: {self.stats['scans_performed']}, "
                                 f"Opportunities: {self.stats['opportunities_detected']}, "
                                 f"Complete pairs: {self.stats['pairs_with_complete_data']}")

                # Log recent opportunities
                recent_opportunities = [
                    opp for opp in self.detected_opportunities
                    if time.time() - opp.timestamp < 300  # Last 5 minutes
                ]

                if recent_opportunities:
                    self.logger.info(f"Recent opportunities (last 5 min): {len(recent_opportunities)}")
                    for opp in recent_opportunities[-3:]:  # Show last 3
                        self.logger.info(f"  {opp.kalshi_ticker}: {opp.profit_margin:.2%} profit")

            except Exception as e:
                self.logger.error(f"Error in statistics report loop: {e}")
                await asyncio.sleep(60)

        self.logger.info("Statistics reporting loop stopped")

    async def start_scanner(self):
        """Start the opportunity scanner"""
        self.logger.info("Starting opportunity scanner...")
        self.is_running = True

        # Start all async loops
        tasks = [
            asyncio.create_task(self._process_data_queue()),
            asyncio.create_task(self._opportunity_scan_loop()),
            asyncio.create_task(self._statistics_report_loop())
        ]

        try:
            # Wait for all tasks
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Error in scanner tasks: {e}")
            # Cancel remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()

        self.logger.info("Opportunity scanner stopped")
        self.is_running = False

    def stop_scanner(self):
        """Stop the opportunity scanner"""
        self.logger.info("Stopping opportunity scanner...")
        self.shutdown_requested = True

    def get_status(self) -> Dict[str, Any]:
        """Get opportunity scanner status"""
        complete_pairs = sum(1 for freshness in self.data_freshness.values() if freshness.pair_complete)

        return {
            'is_running': self.is_running,
            'tracked_pairs': len(self.ticker_to_condition_id),
            'complete_pairs': complete_pairs,
            'kalshi_markets': len(self.current_kalshi_data),
            'polymarket_markets': len(self.current_polymarket_data),
            'queue_sizes': {
                'kalshi': self.kalshi_data_queue.qsize(),
                'polymarket': self.polymarket_data_queue.qsize()
            },
            'statistics': self.stats.copy(),
            'recent_opportunities': len([
                opp for opp in self.detected_opportunities
                if time.time() - opp.timestamp < 300
            ]),
            'last_scan_time': self.last_scan_time
        }

    def get_recent_opportunities(self, minutes: int = 5) -> List[ArbitrageOpportunity]:
        """
        Get opportunities detected in the last N minutes.

        Args:
            minutes: Number of minutes to look back

        Returns:
            List of recent arbitrage opportunities
        """
        cutoff_time = time.time() - (minutes * 60)
        return [
            opp for opp in self.detected_opportunities
            if opp.timestamp >= cutoff_time
        ]


# Demo and testing
async def run_scanner_demo():
    """Demo the opportunity scanner placeholder"""
    print("OPPORTUNITY SCANNER PLACEHOLDER DEMO")
    print("=" * 50)

    try:
        scanner = OpportunityScanner()

        # Set up some test arbitrage pairs
        test_pairs = {
            "KXPRESIRELAND-25-MM": "0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",
            "KXPRESIRELAND-25-FF": "0x12345678901234567890123456789012345678901234567890123456789012345"
        }
        scanner.set_arbitrage_pairs(test_pairs)

        # Start scanner
        scanner_task = asyncio.create_task(scanner.start_scanner())

        # Simulate some data feeds
        await asyncio.sleep(2)

        # Simulate Kalshi data
        from api.data_feed import KalshiMarketData, OrderLevel

        kalshi_data = {
            "KXPRESIRELAND-25-MM": KalshiMarketData(
                ticker="KXPRESIRELAND-25-MM",
                event_ticker="KXPRESIRELAND-25",
                market_type="binary",
                title="Mairead McGuinness to win Ireland Presidential Election",
                subtitle="Will Mairead McGuinness win?",
                yes_sub_title="Yes",
                no_sub_title="No",
                open_time="2025-01-01T00:00:00Z",
                close_time="2025-11-11T23:59:59Z",
                expected_expiration_time="2025-11-11T23:59:59Z",
                expiration_time="2025-11-11T23:59:59Z",
                latest_expiration_time="2025-11-11T23:59:59Z",
                settlement_timer_seconds=0,
                status="active",
                response_price_units="usd_cent",
                notional_value=100,
                tick_size=1,
                yes_bid=45,  # 45 cents
                yes_ask=47,  # 47 cents
                no_bid=53,  # 53 cents
                no_ask=55,  # 55 cents
                last_price=46.0,
                previous_yes_bid=44,
                previous_yes_ask=46,
                previous_price=45.0,
                volume=1000.0,
                volume_24h=5000.0,
                liquidity=500,
                open_interest=2000,
                can_close_early=False,
                result="",
                expiration_value="",
                category="Politics",
                risk_limit_cents=10000,
                strike_type="",
                custom_strike={},
                rules_primary="Test rules",
                rules_secondary="",
                yes_levels=[OrderLevel(45, 100), OrderLevel(44, 50)],
                no_levels=[OrderLevel(53, 100), OrderLevel(54, 50)],
                yes_bid_size=100,
                no_bid_size=100,
                timestamp=time.time()
            )
        }

        await scanner.receive_kalshi_data(kalshi_data)

        # Simulate Polymarket data
        from broker.message_broker import PolymarketData

        polymarket_data = {
            "0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d": PolymarketData(
                condition_id="0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",
                title="Mairead McGuinness Presidential Election",
                yes_price=0.40,  # 40 cents - potential arbitrage!
                no_price=0.60,  # 60 cents
                volume=2000.0,
                liquidity=800.0,
                timestamp=time.time()
            )
        }

        await scanner.receive_polymarket_data(polymarket_data)

        # Let scanner run for 30 seconds
        await asyncio.sleep(30)

        # Show final status
        status = scanner.get_status()
        print(f"\nScanner Status: {status}")

        # Show any opportunities found
        opportunities = scanner.get_recent_opportunities()
        if opportunities:
            print(f"\nOpportunities Found: {len(opportunities)}")
            for opp in opportunities:
                print(f"  {opp.kalshi_ticker}: {opp.profit_margin:.2%} profit")

        scanner.stop_scanner()
        await scanner_task

        print("Opportunity scanner demo completed!")

    except Exception as e:
        print(f"Opportunity scanner demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(run_scanner_demo())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Demo execution failed: {e}")