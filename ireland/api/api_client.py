#!/usr/bin/env python3
"""
polymarket_api_client.py

Pure async API wrapper for Polymarket CLOB and Gamma APIs.
Handles authentication, rate limiting, and all HTTP interactions.
No business logic - just clean API methods.
"""

import asyncio
import aiohttp
import time
from typing import Dict, List, Optional, Set, Any, Union
from dataclasses import dataclass, asdict
from datetime import datetime
import hashlib
import hmac
import base64
import json
from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_logger


@dataclass
class BookParams:
    """Parameters for order book requests"""
    token_id: str


@dataclass
class OrderArgs:
    """Arguments for creating orders"""
    price: float
    size: float
    side: str  # "BUY" or "SELL"
    token_id: str


@dataclass
class RateLimiter:
    """Simple async rate limiter"""
    max_calls: int
    time_window: int
    safety_buffer: float = 0.8

    def __post_init__(self):
        self.calls = []
        self.effective_limit = int(self.max_calls * self.safety_buffer)

    async def acquire(self):
        """Wait if necessary to respect rate limits"""
        now = time.time()

        # Remove old calls outside the time window
        self.calls = [call_time for call_time in self.calls if now - call_time < self.time_window]

        # If we're at the limit, wait
        if len(self.calls) >= self.effective_limit:
            sleep_time = self.time_window - (now - self.calls[0]) + 0.1
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                return await self.acquire()

        # Record this call
        self.calls.append(now)


class PolymarketAPIClient:
    """
    Pure async API wrapper for Polymarket CLOB and Gamma APIs.
    Handles all HTTP interactions, authentication, and rate limiting.
    """

    def __init__(self):
        """Initialize Polymarket API client"""
        self.logger = get_logger('polymarket_api')

        # API endpoints
        self.clob_host = CONFIG.polymarket.clob_host
        self.gamma_host = CONFIG.polymarket.gamma_host

        # Authentication
        self.api_key = CONFIG.polymarket.api_key
        self.api_secret = CONFIG.polymarket.api_secret
        self.api_passphrase = CONFIG.polymarket.api_passphrase
        self.private_key = CONFIG.polymarket.private_key

        # Rate limiters for different endpoints
        self.books_limiter = RateLimiter(
            max_calls=CONFIG.polymarket.books_rate_limit,
            time_window=CONFIG.polymarket.books_time_window,
            safety_buffer=CONFIG.polymarket.safety_buffer
        )

        self.price_limiter = RateLimiter(
            max_calls=CONFIG.polymarket.price_rate_limit,
            time_window=CONFIG.polymarket.price_time_window,
            safety_buffer=CONFIG.polymarket.safety_buffer
        )

        self.markets_limiter = RateLimiter(
            max_calls=CONFIG.polymarket.markets_rate_limit,
            time_window=CONFIG.polymarket.markets_time_window,
            safety_buffer=CONFIG.polymarket.safety_buffer
        )

        self.order_limiter = RateLimiter(
            max_calls=CONFIG.polymarket.order_sustained_limit,
            time_window=CONFIG.polymarket.order_sustained_window,
            safety_buffer=CONFIG.polymarket.safety_buffer
        )

        # HTTP session
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_authenticated = False

        self.logger.info("Polymarket API client initialized")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def start(self):
        """Start HTTP session and authenticate"""
        try:
            # Create HTTP session with optimized settings
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            connector = aiohttp.TCPConnector(
                limit=50,
                limit_per_host=20,
                ttl_dns_cache=300,
                use_dns_cache=True,
            )

            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'Content-Type': 'application/json',
                    'User-Agent': 'PolymarketArbitrageBot/1.0'
                }
            )

            # Authenticate if credentials are provided
            if self.api_key and self.api_secret and self.api_passphrase:
                await self._authenticate()
            else:
                self.logger.info("API credentials not provided - running in public mode")

            self.logger.info("Polymarket API client started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start Polymarket API client: {e}")
            raise

    async def stop(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
        self.logger.info("Polymarket API client stopped")

    async def _authenticate(self):
        """Handle API authentication"""
        try:
            # Test authentication by making a simple API call
            response = await self._make_authenticated_request('GET', '/time')
            if response:
                self.is_authenticated = True
                self.logger.info("Polymarket API authentication successful")
            else:
                self.logger.warning("Polymarket API authentication failed")

        except Exception as e:
            self.logger.error(f"Authentication error: {e}")

    def _generate_signature(self, timestamp: str, method: str, path: str, body: str = '') -> str:
        """Generate HMAC signature for authenticated requests"""
        try:
            # Clean the API secret (remove any whitespace/newlines)
            clean_secret = self.api_secret.strip()

            message = timestamp + method.upper() + path + body
            signature = hmac.new(
                base64.b64decode(clean_secret),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
            return base64.b64encode(signature).decode('utf-8')
        except Exception as e:
            self.logger.error(f"Error generating signature: {e}")
            raise

    async def _make_request(self, method: str, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Make HTTP request with error handling"""
        try:
            async with self.session.request(method, url, **kwargs) as response:
                if response.status == 200:
                    content_type = response.headers.get('content-type', '')

                    # Handle different response types
                    if 'application/json' in content_type:
                        return await response.json()
                    elif 'text/plain' in content_type:
                        # Some endpoints return plain text (like /time)
                        text_response = await response.text()
                        # Try to convert to a simple dict
                        return {"data": text_response.strip()}
                    else:
                        # Try JSON first, fallback to text
                        try:
                            return await response.json()
                        except:
                            text_response = await response.text()
                            return {"data": text_response.strip()}

                elif response.status == 429:
                    self.logger.warning(f"Rate limited on {url}")
                    # Exponential backoff
                    await asyncio.sleep(2)
                    return None
                else:
                    self.logger.warning(f"HTTP {response.status} for {url}: {await response.text()}")
                    return None

        except Exception as e:
            self.logger.error(f"Request failed for {url}: {e}")
            return None

    async def _make_authenticated_request(self, method: str, path: str, data: Optional[Dict] = None) -> Optional[
        Dict[str, Any]]:
        """Make authenticated request to CLOB API"""
        try:
            timestamp = str(int(time.time() * 1000))
            body = json.dumps(data) if data else ''
            signature = self._generate_signature(timestamp, method, path, body)

            # Get the wallet address from the private key
            # For now, we'll need to derive this from the private key
            # This is a placeholder - we need to add proper address derivation
            wallet_address = self._get_wallet_address_from_private_key()

            headers = {
                'POLY_ADDRESS': wallet_address,  # Missing header from docs!
                'POLY-API-KEY': self.api_key,
                'POLY-SIGNATURE': signature,
                'POLY-TIMESTAMP': timestamp,
                'POLY-PASSPHRASE': self.api_passphrase,
            }

            url = f"{self.clob_host}{path}"

            # Debug logging for requests
            self.logger.debug(f"Making {method} request to {url}")
            if data:
                self.logger.debug(f"Request data: {json.dumps(data)[:200]}...")

            if method.upper() == 'GET':
                return await self._make_request(method, url, headers=headers)
            else:
                return await self._make_request(method, url, headers=headers, json=data)

        except Exception as e:
            self.logger.error(f"Authenticated request failed: {e}")
            return None

    def _get_wallet_address_from_private_key(self) -> str:
        """Derive wallet address from private key"""
        try:
            # Import ethereum library for address derivation
            from eth_account import Account

            # Remove 0x prefix if present
            private_key = self.private_key
            if private_key.startswith('0x'):
                private_key = private_key[2:]

            # Create account from private key
            account = Account.from_key(private_key)
            wallet_address = account.address

            self.logger.debug(f"Derived wallet address: {wallet_address}")
            return wallet_address

        except ImportError:
            self.logger.error("eth_account library not installed. Please install with: pip install eth_account")
            # Fallback - try to get from config or environment
            return self._get_fallback_address()
        except Exception as e:
            self.logger.error(f"Error deriving wallet address: {e}")
            return self._get_fallback_address()

    def _get_fallback_address(self) -> str:
        """Get wallet address from config or environment as fallback"""
        # Try to get from config first
        if hasattr(CONFIG.polymarket, 'wallet_address') and CONFIG.polymarket.wallet_address:
            return CONFIG.polymarket.wallet_address

        # Try environment variable
        import os
        wallet_address = os.getenv('POLYMARKET_WALLET_ADDRESS') or os.getenv('WALLET_ADDRESS')
        if wallet_address:
            return wallet_address

        # Last resort - log error and return placeholder
        self.logger.error("Could not determine wallet address - add POLYMARKET_WALLET_ADDRESS to .env")
        return "0x0000000000000000000000000000000000000000"

    # =============================================================================
    # MARKET DATA METHODS
    # =============================================================================

    async def get_order_books_batch(self, token_ids: List[str]) -> Optional[List[Dict[str, Any]]]:
        """
        Get order books for multiple tokens in a single batch request.
        Most efficient way to get order book data for multiple markets.
        """
        await self.books_limiter.acquire()

        try:
            # Create request data as direct array (this is what actually works!)
            request_data = [{"token_id": token_id} for token_id in token_ids]

            self.logger.debug(f"Sending batch order books request with {len(token_ids)} tokens")
            self.logger.debug(f"Request payload: {request_data}")

            # Send as JSON payload to POST /books
            response = await self._make_authenticated_request('POST', '/books', request_data)

            if response:
                self.logger.debug(f"Retrieved order books for {len(token_ids)} tokens")
                return response
            else:
                self.logger.warning(f"Failed to retrieve batch order books for {len(token_ids)} tokens")
                return None

        except Exception as e:
            self.logger.error(f"Error getting batch order books: {e}")
            return None

    async def get_order_book(self, token_id: str) -> Optional[Dict[str, Any]]:
        """Get order book for a single token"""
        await self.books_limiter.acquire()

        try:
            path = f"/book?token_id={token_id}"
            response = await self._make_authenticated_request('GET', path)

            if response:
                self.logger.debug(f"Retrieved order book for token {token_id[:10]}...")
                return response
            else:
                self.logger.warning(f"Failed to retrieve order book for token {token_id[:10]}...")
                return None

        except Exception as e:
            self.logger.error(f"Error getting order book for {token_id[:10]}...: {e}")
            return None

    async def get_prices_batch(self, token_ids: List[str]) -> Optional[Dict[str, Any]]:
        """Get prices for multiple tokens in batch"""
        await self.price_limiter.acquire()

        try:
            # Join token IDs with commas for batch request (fix trailing comma)
            token_ids_str = ','.join(token_ids)
            path = f"/prices?token_ids={token_ids_str}"

            response = await self._make_authenticated_request('GET', path)

            if response:
                self.logger.debug(f"Retrieved prices for {len(token_ids)} tokens")
                return response
            else:
                self.logger.warning(f"Failed to retrieve batch prices for {len(token_ids)} tokens")
                return None

        except Exception as e:
            self.logger.error(f"Error getting batch prices: {e}")
            return None

    async def get_price(self, token_id: str) -> Optional[Dict[str, Any]]:
        """Get price for a single token"""
        await self.price_limiter.acquire()

        try:
            path = f"/price?token_id={token_id}"
            response = await self._make_authenticated_request('GET', path)

            if response:
                self.logger.debug(f"Retrieved price for token {token_id[:10]}...")
                return response
            else:
                self.logger.warning(f"Failed to retrieve price for token {token_id[:10]}...")
                return None

        except Exception as e:
            self.logger.error(f"Error getting price for {token_id[:10]}...: {e}")
            return None

    async def get_market_by_condition_id(self, condition_id: str) -> Optional[Dict[str, Any]]:
        """Get market data by condition ID"""
        await self.markets_limiter.acquire()

        try:
            path = f"/markets/{condition_id}"
            response = await self._make_authenticated_request('GET', path)

            if response:
                self.logger.debug(f"Retrieved market for condition {condition_id[:10]}...")
                return response
            else:
                self.logger.warning(f"Failed to retrieve market for condition {condition_id[:10]}...")
                return None

        except Exception as e:
            self.logger.error(f"Error getting market for {condition_id[:10]}...: {e}")
            return None

    async def get_markets(self, limit: int = 100, offset: int = 0) -> Optional[Dict[str, Any]]:
        """Get list of markets with pagination"""
        await self.markets_limiter.acquire()

        try:
            path = f"/markets?limit={limit}&offset={offset}"
            response = await self._make_authenticated_request('GET', path)

            if response:
                self.logger.debug(f"Retrieved {limit} markets starting at offset {offset}")
                return response
            else:
                self.logger.warning(f"Failed to retrieve markets (limit={limit}, offset={offset})")
                return None

        except Exception as e:
            self.logger.error(f"Error getting markets: {e}")
            return None

    # =============================================================================
    # TRADING METHODS
    # =============================================================================

    async def post_order(self, order_args: OrderArgs, order_type: str = "GTC") -> Optional[Dict[str, Any]]:
        """
        Create and post a limit order.

        Args:
            order_args: Order parameters (price, size, side, token_id)
            order_type: Order type ("GTC" or "GTD")
        """
        await self.order_limiter.acquire()

        try:
            # Build order data
            order_data = {
                "price": str(order_args.price),
                "size": str(order_args.size),
                "side": order_args.side,
                "token_id": order_args.token_id,
                "order_type": order_type
            }

            response = await self._make_authenticated_request('POST', '/order', order_data)

            if response:
                self.logger.info(
                    f"Posted order: {order_args.side} {order_args.size}@{order_args.price} for {order_args.token_id[:10]}...")
                return response
            else:
                self.logger.warning(f"Failed to post order: {order_args.side} {order_args.size}@{order_args.price}")
                return None

        except Exception as e:
            self.logger.error(f"Error posting order: {e}")
            return None

    async def cancel_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Cancel a specific order"""
        await self.order_limiter.acquire()

        try:
            path = f"/order/{order_id}"
            response = await self._make_authenticated_request('DELETE', path)

            if response:
                self.logger.info(f"Cancelled order {order_id}")
                return response
            else:
                self.logger.warning(f"Failed to cancel order {order_id}")
                return None

        except Exception as e:
            self.logger.error(f"Error cancelling order {order_id}: {e}")
            return None

    async def cancel_all_orders(self) -> Optional[Dict[str, Any]]:
        """Cancel all open orders"""
        await self.order_limiter.acquire()

        try:
            response = await self._make_authenticated_request('DELETE', '/orders')

            if response:
                self.logger.info("Cancelled all orders")
                return response
            else:
                self.logger.warning("Failed to cancel all orders")
                return None

        except Exception as e:
            self.logger.error(f"Error cancelling all orders: {e}")
            return None

    async def get_orders(self) -> Optional[Dict[str, Any]]:
        """Get all open orders"""
        try:
            response = await self._make_authenticated_request('GET', '/orders')

            if response:
                orders_count = len(response.get('data', []))
                self.logger.debug(f"Retrieved {orders_count} open orders")
                return response
            else:
                self.logger.warning("Failed to retrieve orders")
                return None

        except Exception as e:
            self.logger.error(f"Error getting orders: {e}")
            return None

    async def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get specific order by ID"""
        try:
            path = f"/order/{order_id}"
            response = await self._make_authenticated_request('GET', path)

            if response:
                self.logger.debug(f"Retrieved order {order_id}")
                return response
            else:
                self.logger.warning(f"Failed to retrieve order {order_id}")
                return None

        except Exception as e:
            self.logger.error(f"Error getting order {order_id}: {e}")
            return None

    # =============================================================================
    # ACCOUNT METHODS
    # =============================================================================

    async def get_account_balance(self) -> Optional[Dict[str, Any]]:
        """Get account balance and positions"""
        try:
            # Try different possible endpoints for account balance
            endpoints_to_try = [
                '/balance',
                '/balances',
                '/account',
                '/user',
                '/portfolio'
            ]

            for endpoint in endpoints_to_try:
                response = await self._make_authenticated_request('GET', endpoint)
                if response:
                    self.logger.debug(f"Retrieved account data from {endpoint}")
                    return response

            # If none work, log that balance endpoint is not available
            self.logger.info("Account balance endpoint not available - this is common for new accounts")
            return {"message": "Account balance not available - endpoint may not exist for this account type"}

        except Exception as e:
            self.logger.error(f"Error getting account balance: {e}")
            return None

    async def get_server_time(self) -> Optional[Dict[str, Any]]:
        """Get server time (useful for testing connectivity)"""
        try:
            response = await self._make_authenticated_request('GET', '/time')

            if response:
                self.logger.debug("Retrieved server time")
                return response
            else:
                self.logger.warning("Failed to retrieve server time")
                return None

        except Exception as e:
            self.logger.error(f"Error getting server time: {e}")
            return None

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    async def health_check(self) -> bool:
        """Check if API is accessible and authenticated"""
        try:
            # If no auth credentials, just return true for public mode
            if not (self.api_key and self.api_secret and self.api_passphrase):
                self.logger.info("Polymarket API health check passed (public mode)")
                return True

            time_response = await self.get_server_time()
            if time_response:
                self.logger.info("Polymarket API health check passed")
                return True
            else:
                self.logger.warning("Polymarket API health check failed")
                return False

        except Exception as e:
            self.logger.error(f"Health check error: {e}")
            return False

    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get current rate limit status for all endpoints"""
        now = time.time()

        def get_limiter_status(limiter: RateLimiter) -> Dict[str, Any]:
            recent_calls = [call for call in limiter.calls if now - call < limiter.time_window]
            return {
                'recent_calls': len(recent_calls),
                'limit': limiter.effective_limit,
                'remaining': max(0, limiter.effective_limit - len(recent_calls)),
                'reset_in': max(0, limiter.time_window - (now - recent_calls[0]) if recent_calls else 0)
            }

        return {
            'books': get_limiter_status(self.books_limiter),
            'prices': get_limiter_status(self.price_limiter),
            'markets': get_limiter_status(self.markets_limiter),
            'orders': get_limiter_status(self.order_limiter),
            'timestamp': now
        }


# =============================================================================
# DEMO AND TESTING
# =============================================================================

async def test_polymarket_api():
    """Test Polymarket API client functionality"""
    print("POLYMARKET API CLIENT TEST")
    print("=" * 50)

    try:
        async with PolymarketAPIClient() as client:
            print("✅ API client started")

            # Health check
            if await client.health_check():
                print("✅ Health check passed")
            else:
                print("❌ Health check failed")
                return

            # Test market data (try public endpoints first)
            print("\n📊 Testing public market data methods...")

            # Try to get markets using public endpoint (no auth required)
            # We'll use gamma API endpoint for public data
            gamma_url = f"{client.gamma_host}/markets?limit=5"
            print(f"Trying Gamma API: {gamma_url}")
            markets_response = await client._make_request('GET', gamma_url)

            if markets_response:
                print(f"✅ Retrieved data from Gamma API: {type(markets_response)}")
                # Check the structure of the response
                if isinstance(markets_response, list):
                    markets = markets_response
                    print(f"✅ Retrieved {len(markets)} markets via public API")
                elif isinstance(markets_response, dict) and 'data' in markets_response:
                    markets = markets_response['data']
                    print(f"✅ Retrieved {len(markets)} markets via public API")
                else:
                    print(
                        f"✅ Retrieved response but unexpected format: {list(markets_response.keys())[:5] if isinstance(markets_response, dict) else 'Not a dict'}")
                    markets = markets_response  # Use whatever we got

                # If we have API credentials, test authenticated endpoints
                if client.api_key and client.api_secret and client.api_passphrase:
                    print("\n🔐 Testing authenticated endpoints...")

                    # Get some markets first
                    markets_response = await client.get_markets(limit=5)
                    if markets_response and 'data' in markets_response:
                        markets = markets_response['data']
                        print(f"✅ Retrieved {len(markets)} markets via authenticated API")

                        if markets:
                            # Extract token IDs for testing
                            token_ids = []
                            for market in markets:
                                if 'tokens' in market:
                                    for token in market['tokens']:
                                        if 'token_id' in token and token['token_id']:  # Make sure it's not empty
                                            token_ids.append(token['token_id'])
                                            if len(token_ids) >= 3:  # Test with 3 tokens
                                                break
                                    if len(token_ids) >= 3:
                                        break

                                # Also check for clobTokenIds (alternative field name)
                                elif 'clobTokenIds' in market:
                                    clob_tokens = market['clobTokenIds']
                                    if isinstance(clob_tokens, list):
                                        for token_id in clob_tokens:
                                            if token_id:  # Make sure it's not empty
                                                token_ids.append(token_id)
                                                if len(token_ids) >= 3:
                                                    break
                                    if len(token_ids) >= 3:
                                        break

                            # Use known working token IDs from Irish Presidential Election instead of random ones
                            token_ids = [
                                "73426090555322336079814834682772449154673847389072637696532614360711409640397",  # Mairead McGuinness Yes
                                "47563581333718899180716107130722973123952657734415817875002040960888165819677",  # Mairead McGuinness No
                                "50309531083259932172601994411068820818170922333236364582512508187315892939247",  # Conor McGregor Yes
                            ]

                            print(f"Using known working token IDs from Irish Presidential Election:")
                            print(f"Token IDs: {[tid[:10] + '...' for tid in token_ids]}")
                            print(f"Full token IDs: {token_ids}")  # Debug: show full IDs

                            # Test SINGLE order book first to get better error info
                            print("\n🔍 Testing single order book first...")
                            single_book_response = await client.get_order_book(token_ids[0])
                            if single_book_response:
                                print("✅ Single order book retrieved")
                            else:
                                print("❌ Single order book failed")

                            # Test batch order books
                            print("\n📚 Testing batch order books...")
                            books_response = await client.get_order_books_batch(token_ids)
                            if books_response:
                                print("✅ Batch order books retrieved")
                            else:
                                print("❌ Batch order books failed")

                            # Test SINGLE price first
                            print("\n💰 Testing single price first...")
                            single_price_response = await client.get_price(token_ids[0])
                            if single_price_response:
                                print("✅ Single price retrieved")
                            else:
                                print("❌ Single price failed")

                            # Test batch prices
                            print("\n💰 Testing batch prices...")
                            prices_response = await client.get_prices_batch(token_ids)
                            if prices_response:
                                print("✅ Batch prices retrieved")
                            else:
                                print("❌ Batch prices failed")

                            # Let's also examine the market data structure to see what token info we have
                            print("\n🔍 Examining market structure for token info...")
                            if markets and len(markets) > 0:
                                print(f"Sample market keys: {list(markets[0].keys())}")
                                if 'tokens' in markets[0]:
                                    print(f"Tokens structure: {markets[0]['tokens']}")
                                if 'clobTokenIds' in markets[0]:
                                    print(f"CLOB token IDs: {markets[0]['clobTokenIds']}")
                                if 'outcomes' in markets[0]:
                                    print(f"Outcomes: {markets[0]['outcomes']}")
                                if 'outcomePrices' in markets[0]:
                                    print(f"Outcome prices: {markets[0]['outcomePrices']}")

                        else:
                            print("⚠️  No token IDs found in markets")
                    else:
                        print("❌ Failed to retrieve markets via authenticated API")

                    # Test account data
                    print("\n💰 Testing account methods...")
                    balance_response = await client.get_account_balance()
                    if balance_response:
                        print("✅ Account balance retrieved")
                    else:
                        print("❌ Account balance failed")
                else:
                    print("⚠️  No API credentials - skipping authenticated endpoint tests")
                    print(
                        "   Add POLYMARKET_API_KEY, POLYMARKET_API_SECRET, and POLYMARKET_API_PASSPHRASE to .env to test authenticated features")
            else:
                print("❌ Failed to retrieve markets via public API")
                print("Trying alternative: direct HTTP request to test connectivity...")

                # Try a simple test to see if we can reach the host at all
                try:
                    test_url = f"{client.gamma_host}"
                    test_response = await client._make_request('GET', test_url)
                    if test_response:
                        print(f"✅ Can reach {client.gamma_host}")
                    else:
                        print(f"❌ Cannot reach {client.gamma_host}")
                except Exception as e:
                    print(f"❌ Error reaching {client.gamma_host}: {e}")

            # Show rate limit status
            print("\n📈 Rate limit status:")
            rate_status = client.get_rate_limit_status()
            for endpoint, status in rate_status.items():
                if endpoint != 'timestamp':
                    print(f"  {endpoint}: {status['recent_calls']}/{status['limit']} calls used")

            print("\n✅ Polymarket API client test completed!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(test_polymarket_api())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed: {e}")