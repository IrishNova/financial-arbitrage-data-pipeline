"""
utils/config.py

Configuration management for Ireland arbitrage server.
Loads environment variables and provides typed access to config values.
"""

import os
from typing import Optional, Union
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Load .env file from project root
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)


@dataclass
class DatabaseConfig:
    """Database configuration for PostgreSQL connection"""
    host: str
    port: int
    name: str
    user: str
    password: str
    connection_timeout: int
    max_connections: int
    retry_attempts: int
    retry_delay: float


@dataclass
class PolymarketConfig:
    """Polymarket API configuration"""
    # API endpoints
    clob_host: str
    gamma_host: str

    # Authentication
    api_key: str
    api_secret: str
    api_passphrase: str
    private_key: str

    # Rate limits
    books_rate_limit: int
    books_time_window: int
    price_rate_limit: int
    price_time_window: int
    markets_rate_limit: int
    markets_time_window: int
    order_sustained_limit: int
    order_sustained_window: int
    safety_buffer: float

    # Batch settings
    batch_size: int
    max_concurrent_batches: int

    # Legacy fields (for backwards compatibility)
    api_url: str
    wallet_address: str
    min_delay: float
    max_retries: int
    timeout: int


@dataclass
class ZeroMQConfig:
    """ZeroMQ configuration for Ireland <-> Virginia communication"""
    # Basic endpoints
    virginia_endpoint: str
    ireland_endpoint: str

    # Channel-specific ports
    subscription_pub_port: int      # Virginia publishes condition IDs
    subscription_sub_port: int      # Ireland subscribes
    marketdata_push_port: int       # Ireland pushes market data
    marketdata_pull_port: int       # Virginia pulls market data
    execution_req_port: int         # Virginia requests trades
    execution_rep_port: int         # Ireland replies
    fills_push_port: int           # Ireland pushes fill data
    fills_pull_port: int           # Virginia pulls fills
    metrics_push_port: int         # Ireland pushes metrics
    metrics_pull_port: int         # Virginia pulls metrics

    # Timing and connection settings
    heartbeat_interval: int         # Seconds between heartbeats
    connection_timeout: int         # Milliseconds
    send_timeout: int              # Milliseconds

    # Security settings (optional)
    curve_public_key: Optional[str]
    curve_private_key: Optional[str]
    curve_server_key: Optional[str]


@dataclass
class TradingConfig:
    """Trading configuration for Ireland server"""
    max_position_size: float    # Maximum position size per trade
    risk_limit: float          # Daily risk limit
    enable_trading: bool       # Enable actual trading (vs simulation)


@dataclass
class IrelandConfig:
    """Main Ireland server configuration"""
    database: DatabaseConfig
    polymarket: PolymarketConfig
    zeromq: ZeroMQConfig
    trading: TradingConfig
    environment: str
    log_level: str
    debug_mode: bool


def get_env_var(key: str, default: Optional[Union[str, int, float]] = None, required: bool = True) -> str:
    """
    Get environment variable with validation.

    Args:
        key: Environment variable name
        default: Default value if not found
        required: Whether the variable is required

    Returns:
        Environment variable value

    Raises:
        ValueError: If required variable is missing
    """
    value = os.getenv(key, default)

    if required and value is None:
        raise ValueError(f"Required environment variable '{key}' is not set")

    return value


def get_env_bool(key: str, default: bool = False) -> bool:
    """Get boolean environment variable"""
    value = os.getenv(key, str(default)).lower()
    return value in ('true', '1', 'yes', 'on')


def get_env_int(key: str, default: Optional[int] = None, required: bool = True) -> int:
    """Get integer environment variable"""
    value = get_env_var(key, default, required)
    try:
        return int(value) if value is not None else default
    except ValueError:
        raise ValueError(f"Environment variable '{key}' must be an integer, got: {value}")


def get_env_float(key: str, default: Optional[float] = None, required: bool = True) -> float:
    """Get float environment variable"""
    value = get_env_var(key, default, required)
    try:
        return float(value) if value is not None else default
    except ValueError:
        raise ValueError(f"Environment variable '{key}' must be a float, got: {value}")


def load_config() -> IrelandConfig:
    """
    Load complete Ireland server configuration from environment variables.

    Returns:
        IrelandConfig: Complete server configuration

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    try:
        # Database configuration
        database = DatabaseConfig(
            host=get_env_var('DB_HOST', 'localhost', required=False),
            port=get_env_int('DB_PORT', 5432, required=False),
            name=get_env_var('DB_NAME', 'arb_trading', required=False),
            user=get_env_var('DB_USER', 'postgres', required=False),
            password=get_env_var('DB_PASSWORD', required=True),
            connection_timeout=get_env_int('DB_CONNECTION_TIMEOUT', 30, required=False),
            max_connections=get_env_int('DB_MAX_CONNECTIONS', 10, required=False),
            retry_attempts=get_env_int('DB_RETRY_ATTEMPTS', 3, required=False),
            retry_delay=get_env_float('DB_RETRY_DELAY', 1.0, required=False)
        )

        # Polymarket configuration
        polymarket = PolymarketConfig(
            # API endpoints
            clob_host=get_env_var('POLYMARKET_CLOB_HOST', 'https://clob.polymarket.com'),
            gamma_host=get_env_var('POLYMARKET_GAMMA_HOST', 'https://gamma-api.polymarket.com'),

            # Authentication
            api_key=get_env_var('POLYMARKET_API_KEY', required=False),
            api_secret=get_env_var('POLYMARKET_API_SECRET', required=False),
            api_passphrase=get_env_var('POLYMARKET_API_PASSPHRASE', required=False),
            private_key=get_env_var('POLY_KEY', required=False),  # Use your existing POLY_KEY

            # Rate limits
            books_rate_limit=get_env_int('POLYMARKET_BOOKS_RATE_LIMIT', 50, required=False),
            books_time_window=get_env_int('POLYMARKET_BOOKS_TIME_WINDOW', 10, required=False),
            price_rate_limit=get_env_int('POLYMARKET_PRICE_RATE_LIMIT', 100, required=False),
            price_time_window=get_env_int('POLYMARKET_PRICE_TIME_WINDOW', 10, required=False),
            markets_rate_limit=get_env_int('POLYMARKET_MARKETS_RATE_LIMIT', 50, required=False),
            markets_time_window=get_env_int('POLYMARKET_MARKETS_TIME_WINDOW', 10, required=False),
            order_sustained_limit=get_env_int('POLYMARKET_ORDER_SUSTAINED_LIMIT', 3000, required=False),
            order_sustained_window=get_env_int('POLYMARKET_ORDER_SUSTAINED_WINDOW', 600, required=False),
            safety_buffer=get_env_float('POLYMARKET_SAFETY_BUFFER', 0.8, required=False),

            # Batch settings
            batch_size=get_env_int('POLYMARKET_BATCH_SIZE', 10, required=False),
            max_concurrent_batches=get_env_int('POLYMARKET_MAX_CONCURRENT_BATCHES', 3, required=False),

            # Legacy fields (for backwards compatibility)
            api_url=get_env_var('POLYMARKET_GAMMA_HOST', 'https://gamma-api.polymarket.com'),
            wallet_address=get_env_var('POLYMARKET_WALLET_ADDRESS', 'placeholder', required=False),
            min_delay=get_env_float('POLYMARKET_MIN_DELAY', 0.75, required=False),
            max_retries=get_env_int('POLYMARKET_MAX_RETRIES', 3, required=False),
            timeout=get_env_int('POLYMARKET_TIMEOUT', 30, required=False)
        )

        # ZeroMQ configuration
        zeromq = ZeroMQConfig(
            virginia_endpoint=get_env_var('ZEROMQ_VIRGINIA_ENDPOINT', 'tcp://virginia-server:5555'),
            ireland_endpoint=get_env_var('ZEROMQ_IRELAND_ENDPOINT', 'tcp://0.0.0.0:5556'),

            # Channel ports
            subscription_pub_port=get_env_int('ZEROMQ_SUBSCRIPTION_PUB_PORT', 5555, required=False),
            subscription_sub_port=get_env_int('ZEROMQ_SUBSCRIPTION_SUB_PORT', 5555, required=False),
            marketdata_push_port=get_env_int('ZEROMQ_MARKETDATA_PUSH_PORT', 5556, required=False),
            marketdata_pull_port=get_env_int('ZEROMQ_MARKETDATA_PULL_PORT', 5556, required=False),
            execution_req_port=get_env_int('ZEROMQ_EXECUTION_REQ_PORT', 5557, required=False),
            execution_rep_port=get_env_int('ZEROMQ_EXECUTION_REP_PORT', 5557, required=False),
            fills_push_port=get_env_int('ZEROMQ_FILLS_PUSH_PORT', 5558, required=False),
            fills_pull_port=get_env_int('ZEROMQ_FILLS_PULL_PORT', 5558, required=False),
            metrics_push_port=get_env_int('ZEROMQ_METRICS_PUSH_PORT', 5559, required=False),
            metrics_pull_port=get_env_int('ZEROMQ_METRICS_PULL_PORT', 5559, required=False),

            # Timing settings
            heartbeat_interval=get_env_int('ZEROMQ_HEARTBEAT_INTERVAL', 10, required=False),
            connection_timeout=get_env_int('ZEROMQ_CONNECTION_TIMEOUT', 5000, required=False),
            send_timeout=get_env_int('ZEROMQ_SEND_TIMEOUT', 1000, required=False),

            # Security (optional)
            curve_public_key=get_env_var('ZEROMQ_CURVE_PUBLIC_KEY', None, required=False),
            curve_private_key=get_env_var('ZEROMQ_CURVE_PRIVATE_KEY', None, required=False),
            curve_server_key=get_env_var('ZEROMQ_CURVE_SERVER_KEY', None, required=False)
        )

        # Trading configuration
        trading = TradingConfig(
            max_position_size=get_env_float('MAX_POSITION_SIZE', 1000.0, required=False),  # $1000 max
            risk_limit=get_env_float('DAILY_RISK_LIMIT', 5000.0, required=False),  # $5000 daily limit
            enable_trading=get_env_bool('ENABLE_TRADING', False)  # Disabled by default for safety
        )

        # Main configuration
        config = IrelandConfig(
            database=database,
            polymarket=polymarket,
            zeromq=zeromq,
            trading=trading,
            environment=get_env_var('ENVIRONMENT', 'development', required=False),
            log_level=get_env_var('LOG_LEVEL', 'INFO', required=False),
            debug_mode=get_env_bool('DEBUG_MODE', False)
        )

        logger.info(f"Ireland server configuration loaded - Environment: {config.environment}")
        return config

    except Exception as e:
        logger.error(f"Failed to load Ireland server configuration: {e}")
        raise


# Global config instance
try:
    CONFIG = load_config()
except Exception as e:
    logger.error(f"Failed to initialize Ireland server configuration: {e}")
    raise


# Convenience functions
def get_database_config() -> DatabaseConfig:
    """Get database configuration"""
    return CONFIG.database


def get_polymarket_config() -> PolymarketConfig:
    """Get Polymarket configuration"""
    return CONFIG.polymarket


def get_zeromq_config() -> ZeroMQConfig:
    """Get ZeroMQ configuration"""
    return CONFIG.zeromq


def get_trading_config() -> TradingConfig:
    """Get trading configuration"""
    return CONFIG.trading


def is_production() -> bool:
    """Check if running in production environment"""
    return CONFIG.environment.lower() == 'production'


def is_debug_mode() -> bool:
    """Check if debug mode is enabled"""
    return CONFIG.debug_mode


def is_trading_enabled() -> bool:
    """Check if actual trading is enabled"""
    return CONFIG.trading.enable_trading


if __name__ == "__main__":
    # Test configuration loading
    print("Testing Ireland server configuration loading...")

    try:
        config = load_config()
        print(f"Configuration loaded successfully")
        print(f"Environment: {config.environment}")
        print(f"Database: {config.database.host}:{config.database.port}/{config.database.name}")
        print(f"Database user: {config.database.user}")
        print(f"Database password: {'SET' if config.database.password else 'NOT SET'}")
        print(f"Polymarket CLOB: {config.polymarket.clob_host}")
        print(f"Polymarket Gamma: {config.polymarket.gamma_host}")
        print(f"Polymarket API key: {'SET' if config.polymarket.api_key else 'NOT SET'}")
        print(f"Polymarket private key: {'SET' if config.polymarket.private_key else 'NOT SET'}")
        print(f"Virginia endpoint: {config.zeromq.virginia_endpoint}")
        print(f"Market data push port: {config.zeromq.marketdata_push_port}")
        print(f"Execution reply port: {config.zeromq.execution_rep_port}")
        print(f"Trading enabled: {config.trading.enable_trading}")
        print(f"Debug mode: {config.debug_mode}")
        print(f"Books rate limit: {config.polymarket.books_rate_limit}/{config.polymarket.books_time_window}s")
        print(f"Batch size: {config.polymarket.batch_size}")

    except Exception as e:
        print(f"Configuration loading failed: {e}")
        print("\nMake sure you have a .env file with required variables:")
        print("# Database configuration")
        print("DB_HOST=localhost")
        print("DB_PORT=5432")
        print("DB_NAME=arb_trading")
        print("DB_USER=your_username")
        print("DB_PASSWORD=your_password")
        print("")
        print("# Polymarket configuration")
        print("POLYMARKET_CLOB_HOST=https://clob.polymarket.com")
        print("POLYMARKET_GAMMA_HOST=https://gamma-api.polymarket.com")
        print("POLYMARKET_API_KEY=your_api_key")
        print("POLYMARKET_API_SECRET=your_api_secret")
        print("POLYMARKET_API_PASSPHRASE=your_passphrase")
        print("POLY_KEY=your_private_key")
        print("")
        print("# ZeroMQ configuration")
        print("ZEROMQ_VIRGINIA_ENDPOINT=tcp://virginia-ip:5555")
        print("# ... and other configuration variables")