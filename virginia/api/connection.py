"""
api/connection.py

Centralized Kalshi API connection handler for Virginia server.
Uses modern API key authentication with RSA-PSS signatures.
Ready for standalone use and import by other modules.
"""

import sys
import os
from pathlib import Path

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import aiohttp
import asyncio
import time
import logging
import base64
from typing import Dict, List, Optional, Any
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature

logger = logging.getLogger(__name__)

class KalshiAuth:
    """Authentication class using API keys with RSA-PSS signatures"""

    def __init__(self, private_key_path: str, key_id: str):
        self.key_id = key_id
        self.private_key = self._load_private_key(private_key_path)

    def _load_private_key(self, file_path: str) -> rsa.RSAPrivateKey:
        """Load RSA private key from PEM file"""
        try:
            with open(file_path, "rb") as key_file:
                return serialization.load_pem_private_key(
                    key_file.read(), password=None, backend=default_backend()
                )
        except FileNotFoundError:
            raise FileNotFoundError(f"Private key file not found: {file_path}")
        except Exception as e:
            raise ValueError(f"Failed to load private key: {e}")

    def _sign_message(self, message: str) -> str:
        """Sign a message using RSA-PSS"""
        try:
            signature = self.private_key.sign(
                message.encode("utf-8"),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            return base64.b64encode(signature).decode("utf-8")
        except InvalidSignature as e:
            raise ValueError("Failed to sign message") from e

    def get_auth_headers(self, method: str, path: str) -> Dict[str, str]:
        """Generate authentication headers - match your working code exactly"""
        timestamp = str(int(time.time() * 1000))  # Milliseconds
        # Message format: timestamp + method + path (your working pattern)
        message = timestamp + method.upper() + path

        # Debug logging
        logger.debug(f"Auth message: '{message}'")
        logger.debug(f"Timestamp: {timestamp}")
        logger.debug(f"Method: {method.upper()}")
        logger.debug(f"Path: {path}")

        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": self._sign_message(message),
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
        }

class KalshiConnection:
    """
    Centralized Kalshi API connection handler.
    Handles authentication, rate limiting, and all API calls.
    """

    def __init__(self, api_url: str = None, private_key_path: str = None,
                 key_id: str = None, min_delay: float = 0.11):
        """
        Initialize Kalshi connection.

        Args:
            api_url: Kalshi API URL
            private_key_path: Path to RSA private key file
            key_id: API key ID
            min_delay: Minimum delay between requests (seconds)
        """
        # Use provided values or try to load from config
        self.api_url = api_url or self._get_default_api_url()
        self.min_delay = min_delay

        # Initialize authentication
        key_path = private_key_path or self._get_default_key_path()
        key_id_val = key_id or self._get_default_key_id()

        if not key_path or not key_id_val:
            raise ValueError("Must provide private_key_path and key_id either as parameters or via config")

        self.auth = KalshiAuth(key_path, key_id_val)
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_request_time = 0

        logger.info(f"Kalshi connection initialized - API: {self.api_url}")

    def _get_default_api_url(self) -> str:
        """Get default API URL - use base domain like the docs"""
        return "https://api.elections.kalshi.com"

    def _get_default_key_path(self) -> Optional[str]:
        """Try to get default key path from environment or config"""
        # Try environment variable first
        env_path = os.getenv('KALSHI_PRIVATE_KEY_PATH')
        if env_path:
            return env_path

        # Try to load from config if available
        try:
            from utils.config import get_kalshi_config
            return get_kalshi_config().private_key_path
        except ImportError:
            logger.warning("Config module not available, using environment variables only")
            return None

    def _get_default_key_id(self) -> Optional[str]:
        """Try to get default key ID from environment or config"""
        # Try environment variable first
        env_key_id = os.getenv('KALSHI_KEY_ID')
        if env_key_id:
            return env_key_id

        # Try to load from config if available
        try:
            from utils.config import get_kalshi_config
            return get_kalshi_config().key_id
        except ImportError:
            logger.warning("Config module not available, using environment variables only")
            return None

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def _throttle(self):
        """Rate limit to stay under API limits"""
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_delay:
            await asyncio.sleep(self.min_delay - time_since_last)
        self.last_request_time = time.time()

    async def _make_request(self, method: str, endpoint: str, params: Dict = None, json_data: Dict = None):
        """Make authenticated API request"""
        if not self.session:
            raise RuntimeError("Session not initialized - use async context manager")

        await self._throttle()

        url = f"{self.api_url}{endpoint}"
        headers = self.auth.get_auth_headers(method.upper(), endpoint)

        # Debug logging
        logger.debug(f"Making {method.upper()} request to {url}")
        logger.debug(f"Headers: {headers}")
        logger.debug(f"Params: {params}")

        try:
            async with self.session.request(
                method, url, headers=headers, params=params, json=json_data,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:

                response_text = await response.text()
                logger.debug(f"Response status: {response.status}")
                logger.debug(f"Response text: {response_text}")

                if response.status == 200:
                    try:
                        return await response.json()
                    except:
                        # Try to parse the text we already read
                        import json
                        return json.loads(response_text)
                elif response.status == 429:
                    logger.warning("Rate limited by Kalshi API")
                    return None
                else:
                    logger.error(f"API request failed: {response.status} - {response_text}")
                    return None

        except asyncio.TimeoutError:
            logger.error(f"Request timeout for {method} {endpoint}")
            return None
        except asyncio.CancelledError:
            logger.warning(f"Request cancelled for {method} {endpoint}")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"Client error for {method} {endpoint}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {method} {endpoint}: {e}")
            return None

    # ===== MARKET DATA METHODS =====

    async def get_markets_bulk(self, limit: int = 1000, status: str = None, series_ticker: str = None):
        """
        Get markets in bulk - THE KEY METHOD for arbitrage scanning.
        Returns up to 1000 markets in a single API call.

        Args:
            limit: Maximum number of markets to return (default 1000)
            status: Market status filter (None = all markets, "open", "active", "closed", "settled")
            series_ticker: Filter by series ticker (optional)
        """
        # Build params - only include status if specified
        params = {'limit': limit}
        if status:
            params['status'] = status
        if series_ticker:
            params['series_ticker'] = series_ticker

        logger.debug(f"Fetching markets with params: {params}")

        response = await self._make_request('GET', '/trade-api/v2/markets', params=params)

        if response and 'markets' in response:
            markets = []
            for market in response['markets']:
                markets.append({
                    'ticker': market.get('ticker', ''),
                    'title': market.get('title', ''),
                    'status': market.get('status', ''),  # Include status in response
                    'yes_bid': market.get('yes_bid', 0.0) or 0.0,
                    'yes_ask': market.get('yes_ask', 0.0) or 0.0,
                    'no_bid': market.get('no_bid', 0.0) or 0.0,
                    'no_ask': market.get('no_ask', 0.0) or 0.0,
                    'last_price': market.get('last_price', 0.0) or 0.0,
                    'volume': market.get('volume', 0.0) or 0.0,
                    'close_time': market.get('close_time', ''),
                    'can_close_early': market.get('can_close_early', False),
                    'expiration_time': market.get('expiration_time', ''),

                    # Add all the fields your data feed expects
                    'event_ticker': market.get('event_ticker', ''),
                    'market_type': market.get('market_type', ''),
                    'subtitle': market.get('subtitle', ''),
                    'yes_sub_title': market.get('yes_sub_title', ''),
                    'no_sub_title': market.get('no_sub_title', ''),
                    'open_time': market.get('open_time', ''),
                    'expected_expiration_time': market.get('expected_expiration_time', ''),
                    'latest_expiration_time': market.get('latest_expiration_time', ''),
                    'settlement_timer_seconds': market.get('settlement_timer_seconds', 0),
                    'response_price_units': market.get('response_price_units', ''),
                    'notional_value': market.get('notional_value', 100),
                    'tick_size': market.get('tick_size', 1),
                    'previous_yes_bid': market.get('previous_yes_bid', 0) or 0,
                    'previous_yes_ask': market.get('previous_yes_ask', 0) or 0,
                    'previous_price': market.get('previous_price', 0.0) or 0.0,
                    'volume_24h': market.get('volume_24h', 0.0) or 0.0,
                    'liquidity': market.get('liquidity', 0) or 0,
                    'open_interest': market.get('open_interest', 0) or 0,
                    'can_close_early': market.get('can_close_early', False),
                    'result': market.get('result', ''),
                    'expiration_value': market.get('expiration_value', ''),
                    'category': market.get('category', ''),
                    'risk_limit_cents': market.get('risk_limit_cents', 0),
                    'strike_type': market.get('strike_type', ''),
                    'custom_strike': market.get('custom_strike', {}),
                    'rules_primary': market.get('rules_primary', ''),
                    'rules_secondary': market.get('rules_secondary', '')
                })

            logger.info(f"Retrieved {len(markets)} markets from Kalshi (status filter: {status or 'ALL'})")

            # Debug: Show status breakdown
            status_counts = {}
            for market in markets:
                status = market.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1

            logger.debug(f"Market status breakdown: {status_counts}")

            return markets

        logger.warning("No markets data received from API")
        return []

    async def get_market_orderbook(self, ticker: str):
        """Get orderbook for specific market"""
        response = await self._make_request('GET', f'/trade-api/v2/markets/{ticker}/orderbook')

        if response:
            return {
                'ticker': ticker,
                'yes_bids': response.get('yes', []),
                'no_bids': response.get('no', []),
                'timestamp': time.time()
            }
        return None

    async def get_market_trades(self, ticker: str, limit: int = 100):
        """Get recent trades for a specific market"""
        params = {'limit': limit}
        response = await self._make_request('GET', f'/trade-api/v2/markets/{ticker}/trades', params=params)

        if response and 'trades' in response:
            return response['trades']
        return []

    # ===== ACCOUNT METHODS =====

    async def get_account_balance(self):
        """Get account balance - match official docs exactly"""
        response = await self._make_request('GET', '/trade-api/v2/portfolio/balance')
        return response.get('balance') if response else None

    async def get_positions(self):
        """Get open positions - match official docs"""
        params = {
            'settlement_status': 'unsettled',
            'limit': 1000
        }
        response = await self._make_request('GET', '/trade-api/v2/portfolio/positions', params=params)

        if response and 'positions' in response:
            positions = []
            for pos in response['positions']:
                if pos.get('position', 0) != 0:
                    positions.append({
                        'ticker': pos.get('ticker', ''),
                        'direction': 'yes' if pos.get('position', 0) > 0 else 'no',
                        'quantity': abs(pos.get('position', 0)),
                        'avg_price': pos.get('avg_price', 0.0),
                        'market_exposure': pos.get('market_exposure', 0.0),
                        'realized_pnl': pos.get('realized_pnl', 0.0),
                        'unrealized_pnl': pos.get('unrealized_pnl', 0.0)
                    })
            return positions
        return []

    async def get_orders(self, status: str = None):
        """Get orders - match official docs"""
        params = {'limit': 1000}
        if status:
            params['status'] = status

        response = await self._make_request('GET', '/trade-api/v2/portfolio/orders', params=params)

        if response and 'orders' in response:
            return response['orders']
        return []

    # ===== UTILITY METHODS =====

    async def get_exchange_status(self):
        """Get exchange status and trading hours"""
        response = await self._make_request('GET', '/trade-api/v2/exchange/status')
        return response

    async def health_check(self):
        """Simple health check - get exchange status"""
        try:
            status = await self.get_exchange_status()
            return status is not None
        except Exception:
            return False


# Standalone verification function
async def verify_connection():
    """Test the Kalshi connection - for standalone verification"""
    print("🚀 Testing Kalshi Connection...")
    print("=" * 50)

    try:
        # Initialize connection with environment variables
        async with KalshiConnection() as connection:
            print(f"✅ Connection initialized")
            print(f"   API URL: {connection.api_url}")
            print(f"   Key ID: {connection.auth.key_id}")
            print(f"   Min delay: {connection.min_delay}s")
            print()

            # Test 1: Health check
            print("🔍 Testing health check...")
            is_healthy = await connection.health_check()
            print(f"   Health status: {'✅ Healthy' if is_healthy else '❌ Unhealthy'}")
            print()

            # Test 2: Get account balance
            print("💰 Testing account balance...")
            balance = await connection.get_account_balance()
            if balance is not None:
                print(f"   Balance: ${balance:.2f}")
            else:
                print("   ❌ Could not retrieve balance")
            print()

            # Test 3: Get positions
            print("📊 Testing positions...")
            positions = await connection.get_positions()
            print(f"   Open positions: {len(positions)}")
            for i, pos in enumerate(positions[:3]):  # Show first 3
                print(f"   {i+1}. {pos['ticker']}: {pos['direction']} x{pos['quantity']}")
            print()

            # Test 4: Get markets (bulk call)
            print("📈 Testing bulk market data...")
            markets = await connection.get_markets_bulk(limit=20)
            print(f"   Retrieved {len(markets)} markets in one API call")

            # Show sample markets
            for i, market in enumerate(markets[:3]):
                print(f"   {i+1}. {market['ticker']}")
                print(f"      Title: {market['title'][:50]}...")
                print(f"      Yes: ${market['yes_bid']:.2f} bid / ${market['yes_ask']:.2f} ask")
                print(f"      No:  ${market['no_bid']:.2f} bid / ${market['no_ask']:.2f} ask")
                print(f"      Volume: {market['volume']}")
                print()

            # Test 5: Get orderbook for first market
            if markets:
                print("📋 Testing orderbook...")
                ticker = markets[0]['ticker']
                orderbook = await connection.get_market_orderbook(ticker)
                if orderbook:
                    print(f"   Orderbook for {ticker}:")
                    print(f"   Yes levels: {len(orderbook.get('yes_bids', []))}")
                    print(f"   No levels: {len(orderbook.get('no_bids', []))}")
                else:
                    print(f"   ❌ Could not get orderbook for {ticker}")
                print()

            print("🎉 All tests completed successfully!")

    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        print("\nMake sure you have set these environment variables:")
        print("export KALSHI_PRIVATE_KEY_PATH=/path/to/your/Bot_2.pem")
        print("export KALSHI_KEY_ID=your_key_id")
        raise


if __name__ == "__main__":
    # Setup detailed logging for debugging
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG for auth troubleshooting
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run the test
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        print("uvloop not available, using default event loop")

    asyncio.run(verify_connection())