#!/usr/bin/env python3
"""
read_from.py

Centralized database read operations hub for arbitrage trading system.
Handles all database queries and data retrieval operations.
"""

import sys
import os
import asyncio
import logging
import psycopg2
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

# Add project root to Python path
project_root = Path(__file__).parent
if 'database' in str(project_root):
    project_root = project_root.parent
sys.path.insert(0, str(project_root))

from api.connection import KalshiConnection
from utils.config import CONFIG

logger = logging.getLogger(__name__)


@dataclass
class ArbPair:
    """Data class for arbitrage trading pairs"""
    id: int
    kalshi_ticker: str
    polymarket_condition_id: str
    expiration_date: date
    pair_description: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass
class MarketData:
    """Data class for market data"""
    ticker: str
    title: str
    yes_bid: float
    yes_ask: float
    no_bid: float
    no_ask: float
    last_price: float
    volume: float
    status: str
    timestamp: datetime


class DatabaseReader:
    """
    Centralized database reader for all arbitrage trading operations.
    Handles PostgreSQL connections and all read queries.
    """

    def __init__(self, connection_string: str = None):
        """Initialize database connection"""
        try:
            self.connection_string = connection_string or self._build_connection_string()
        except ValueError as e:
            logger.error(f"Database configuration error: {e}")
            self.connection_string = None
        self.connection = None

    def _build_connection_string(self) -> str:
        """Build connection string from configuration"""
        # Get database config from environment (config.py loads these)
        host = os.getenv('DB_HOST', 'localhost')
        database = os.getenv('DB_NAME', 'arb_trading')
        user = os.getenv('DB_USER', 'ryanmoloney')
        password = os.getenv('DB_PASSWORD', '')
        port = os.getenv('DB_PORT', '5432')

        if password:
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
        else:
            return f"postgresql://{user}@{host}:{port}/{database}"

    def connect(self):
        """Establish database connection"""
        if not self.connection_string:
            logger.error("Cannot connect: no valid connection string")
            return False

        try:
            self.connection = psycopg2.connect(self.connection_string)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry"""
        if not self.connect():
            raise RuntimeError("Failed to establish database connection")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

    def health_check(self) -> bool:
        """Check if database connection is healthy"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    def get_active_pairs(self) -> List[ArbPair]:
        """Get all active arbitrage pairs from database"""
        if not self.connection:
            raise RuntimeError("Database connection not established")

        query = """
        SELECT id, kalshi_ticker, polymarket_condition_id, expiration_date,
               pair_description, is_active, created_at, updated_at
        FROM arb_pairs 
        WHERE is_active = TRUE 
        AND expiration_date > CURRENT_DATE
        ORDER BY created_at DESC
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

                pairs = []
                for row in rows:
                    pair = ArbPair(
                        id=row[0],
                        kalshi_ticker=row[1],
                        polymarket_condition_id=row[2],
                        expiration_date=row[3],
                        pair_description=row[4],
                        is_active=row[5],
                        created_at=row[6],
                        updated_at=row[7]
                    )
                    pairs.append(pair)

                logger.info(f"Retrieved {len(pairs)} active arbitrage pairs")
                return pairs

        except Exception as e:
            logger.error(f"Failed to retrieve arbitrage pairs: {e}")
            raise

    def get_kalshi_tickers(self) -> List[str]:
        """Get list of unique Kalshi tickers for active pairs"""
        pairs = self.get_active_pairs()
        tickers = [pair.kalshi_ticker for pair in pairs]
        unique_tickers = list(dict.fromkeys(tickers))
        logger.info(f"Found {len(unique_tickers)} unique Kalshi tickers")
        return unique_tickers

    def get_polymarket_condition_ids(self) -> List[str]:
        """Get list of unique Polymarket condition IDs for active pairs"""
        pairs = self.get_active_pairs()
        condition_ids = [pair.polymarket_condition_id for pair in pairs]
        unique_ids = list(dict.fromkeys(condition_ids))
        logger.info(f"Found {len(unique_ids)} unique Polymarket condition IDs")
        return unique_ids

    def get_pair_by_kalshi_ticker(self, ticker: str) -> Optional[ArbPair]:
        """Get specific arbitrage pair by Kalshi ticker"""
        if not self.connection:
            raise RuntimeError("Database connection not established")

        query = """
        SELECT id, kalshi_ticker, polymarket_condition_id, expiration_date,
               pair_description, is_active, created_at, updated_at
        FROM arb_pairs 
        WHERE kalshi_ticker = %s AND is_active = TRUE
        LIMIT 1
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (ticker,))
                row = cursor.fetchone()

                if row:
                    return ArbPair(
                        id=row[0],
                        kalshi_ticker=row[1],
                        polymarket_condition_id=row[2],
                        expiration_date=row[3],
                        pair_description=row[4],
                        is_active=row[5],
                        created_at=row[6],
                        updated_at=row[7]
                    )
                return None

        except Exception as e:
            logger.error(f"Failed to retrieve pair for ticker {ticker}: {e}")
            return None

    def get_pair_by_polymarket_id(self, condition_id: str) -> Optional[ArbPair]:
        """Get specific arbitrage pair by Polymarket condition ID"""
        if not self.connection:
            raise RuntimeError("Database connection not established")

        query = """
        SELECT id, kalshi_ticker, polymarket_condition_id, expiration_date,
               pair_description, is_active, created_at, updated_at
        FROM arb_pairs 
        WHERE polymarket_condition_id = %s AND is_active = TRUE
        LIMIT 1
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (condition_id,))
                row = cursor.fetchone()

                if row:
                    return ArbPair(
                        id=row[0],
                        kalshi_ticker=row[1],
                        polymarket_condition_id=row[2],
                        expiration_date=row[3],
                        pair_description=row[4],
                        is_active=row[5],
                        created_at=row[6],
                        updated_at=row[7]
                    )
                return None

        except Exception as e:
            logger.error(f"Failed to retrieve pair for condition ID {condition_id}: {e}")
            return None

    def get_pairs_expiring_soon(self, days: int = 7) -> List[ArbPair]:
        """Get arbitrage pairs expiring within specified days"""
        if not self.connection:
            raise RuntimeError("Database connection not established")

        query = """
        SELECT id, kalshi_ticker, polymarket_condition_id, expiration_date,
               pair_description, is_active, created_at, updated_at
        FROM arb_pairs 
        WHERE is_active = TRUE 
        AND expiration_date > CURRENT_DATE
        AND expiration_date <= CURRENT_DATE + INTERVAL '%s days'
        ORDER BY expiration_date ASC
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (days,))
                rows = cursor.fetchall()

                pairs = []
                for row in rows:
                    pair = ArbPair(
                        id=row[0],
                        kalshi_ticker=row[1],
                        polymarket_condition_id=row[2],
                        expiration_date=row[3],
                        pair_description=row[4],
                        is_active=row[5],
                        created_at=row[6],
                        updated_at=row[7]
                    )
                    pairs.append(pair)

                logger.info(f"Found {len(pairs)} pairs expiring within {days} days")
                return pairs

        except Exception as e:
            logger.error(f"Failed to retrieve expiring pairs: {e}")
            raise


async def get_kalshi_market_data(tickers: List[str]) -> Dict[str, MarketData]:
    """
    Fetch Kalshi market data for specified tickers.

    Args:
        tickers: List of Kalshi ticker symbols

    Returns:
        Dictionary mapping ticker to MarketData
    """
    market_data = {}

    try:
        async with KalshiConnection() as kalshi:
            # Get bulk market data
            all_markets = await kalshi.get_markets_bulk(limit=1000)

            if not all_markets:
                logger.warning("No market data received from Kalshi")
                return market_data

            # Filter to requested tickers
            for market in all_markets:
                ticker = market.get('ticker', '')

                if ticker in tickers:
                    data = MarketData(
                        ticker=ticker,
                        title=market.get('title', ''),
                        yes_bid=market.get('yes_bid', 0.0) or 0.0,
                        yes_ask=market.get('yes_ask', 0.0) or 0.0,
                        no_bid=market.get('no_bid', 0.0) or 0.0,
                        no_ask=market.get('no_ask', 0.0) or 0.0,
                        last_price=market.get('last_price', 0.0) or 0.0,
                        volume=market.get('volume', 0.0) or 0.0,
                        status=market.get('status', ''),
                        timestamp=datetime.now()
                    )
                    market_data[ticker] = data

            logger.info(f"Retrieved market data for {len(market_data)} tickers")
            return market_data

    except Exception as e:
        logger.error(f"Failed to fetch Kalshi market data: {e}")
        return market_data


def print_arbitrage_data():
    """Print all arbitrage pairs with current Kalshi market data"""

    async def _print_data():
        print("ARBITRAGE PAIRS WITH KALSHI MARKET DATA")
        print("=" * 70)

        # Load pairs from database
        try:
            with DatabaseReader() as db:
                pairs = db.get_active_pairs()

                if not pairs:
                    print("No active arbitrage pairs found!")
                    return

                kalshi_tickers = db.get_kalshi_tickers()
                polymarket_ids = db.get_polymarket_condition_ids()

                print(f"Found {len(pairs)} active arbitrage pairs")
                print(f"Kalshi tickers: {kalshi_tickers}")
                print(f"Polymarket condition IDs: {len(polymarket_ids)}")
                print()

        except Exception as e:
            print(f"Database error: {e}")
            return

        # Get Kalshi market data
        try:
            market_data = await get_kalshi_market_data(kalshi_tickers)

            if not market_data:
                print("No Kalshi market data available")
                return

            # Print data for each pair
            for pair in pairs:
                print(f"PAIR: {pair.kalshi_ticker}")
                print(f"Description: {pair.pair_description}")
                print(f"Polymarket ID: {pair.polymarket_condition_id}")
                print(f"Expires: {pair.expiration_date}")

                if pair.kalshi_ticker in market_data:
                    data = market_data[pair.kalshi_ticker]
                    print(f"Title: {data.title}")
                    print(f"Status: {data.status}")
                    print(
                        f"YES - Bid: ${data.yes_bid:.3f} | Ask: ${data.yes_ask:.3f} | Spread: ${data.yes_ask - data.yes_bid:.3f}")
                    print(
                        f"NO  - Bid: ${data.no_bid:.3f} | Ask: ${data.no_ask:.3f} | Spread: ${data.no_ask - data.no_bid:.3f}")
                    print(f"Last Price: ${data.last_price:.3f}")
                    print(f"Volume: {data.volume:,.0f}")
                else:
                    print("No Kalshi market data available")

                print("-" * 50)

        except Exception as e:
            print(f"Market data error: {e}")

    return asyncio.run(_print_data())


def run_database_demo():
    """Demo the database operations"""
    print("DATABASE READ OPERATIONS DEMO")
    print("=" * 50)

    # Show config loading status
    try:
        print(f"Configuration loaded successfully")
        print(f"Environment: {CONFIG.environment}")
        print(f"Debug mode: {CONFIG.debug_mode}")
        print()
    except Exception as e:
        print(f"Config error: {e}")
        return False

    # Check database environment variables
    required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_PORT']
    print("Database configuration:")
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"  {var}=***" if 'PASSWORD' in var else f"  {var}={value}")
        else:
            print(f"  {var}=NOT SET")
    print()

    try:
        with DatabaseReader() as db:
            print("Database connection established")

            if not db.health_check():
                print("Database health check failed")
                return False

            # Show basic stats
            pairs = db.get_active_pairs()
            kalshi_tickers = db.get_kalshi_tickers()
            polymarket_ids = db.get_polymarket_condition_ids()

            print(f"Active pairs: {len(pairs)}")
            print(f"Kalshi tickers: {kalshi_tickers}")
            print(f"Polymarket IDs: {len(polymarket_ids)}")

            # Show expiring soon
            expiring = db.get_pairs_expiring_soon(30)
            print(f"Pairs expiring within 30 days: {len(expiring)}")

            print("Database demo completed successfully!")
            return True

    except Exception as e:
        print(f"Database demo failed: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    import argparse

    parser = argparse.ArgumentParser(description='Database read operations')
    parser.add_argument('--demo', action='store_true', help='Run database demo')
    parser.add_argument('--print-data', action='store_true', help='Print arbitrage data with market prices')

    args = parser.parse_args()

    if args.print_data:
        print_arbitrage_data()
    else:
        success = run_database_demo()
        if not success:
            exit(1)