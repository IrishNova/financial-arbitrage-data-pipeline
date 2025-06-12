"""
utils/config.py

Centralized configuration management for the arbitrage bot.
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
class KalshiConfig:
    """Kalshi API configuration"""
    api_url: str
    private_key_path: str
    key_id: str
    min_delay: float  # Rate limiting delay in seconds
    max_retries: int
    timeout: int


@dataclass
class PolymarketConfig:
    """Polymarket API configuration"""
    api_url: str
    private_key: Optional[str]
    wallet_address: Optional[str]
    min_delay: float
    max_retries: int
    timeout: int


@dataclass
class DatabaseConfig:
    """Database server ZeroMQ configuration for bifurcation"""
    host: str
    market_data_port: int
    analysis_data_port: int
    trade_data_port: int


@dataclass
class ZeroMQConfig:
    """ZeroMQ configuration for Ireland <-> Virginia communication + Database bifurcation"""
    # Basic endpoints
    ireland_endpoint: str
    virginia_endpoint: str

    # Channel-specific ports (Ireland communication)
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

    # Database server configuration (for bifurcation)
    database: DatabaseConfig

    # Timing and connection settings
    heartbeat_interval: int         # Seconds between heartbeats
    connection_timeout: int         # Milliseconds
    send_timeout: int              # Milliseconds

    # Security settings (optional)
    curve_public_key: Optional[str]
    curve_private_key: Optional[str]
    curve_server_key: Optional[str]


@dataclass
class RedisConfig:
    """Redis configuration"""
    host: str
    port: int
    db: int
    password: Optional[str]


@dataclass
class MongoConfig:
    """MongoDB configuration"""
    uri: str
    database: str
    collections: str


@dataclass
class TradingConfig:
    """Trading and arbitrage configuration"""
    min_profit_threshold: float  # Minimum profit to execute trade
    max_position_size: float  # Maximum position size per trade
    risk_limit: float  # Daily risk limit
    min_volume_threshold: float  # Minimum market volume to consider


@dataclass
class CoordinatorConfig:
    """Coordinator and system timing configuration"""
    ticker_refresh_interval: int  # How often to refresh tickers from database (seconds)
    health_check_interval: int   # How often to perform health checks (seconds)
    max_startup_time: int        # Maximum time to wait for startup (seconds)
    shutdown_timeout: int        # Maximum time to wait for graceful shutdown (seconds)


@dataclass
class AppConfig:
    """Main application configuration"""
    kalshi: KalshiConfig
    polymarket: PolymarketConfig
    zeromq: ZeroMQConfig
    redis: RedisConfig
    mongo: MongoConfig
    trading: TradingConfig
    coordinator: CoordinatorConfig
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


def load_config() -> AppConfig:
    """
    Load complete application configuration from environment variables.

    Returns:
        AppConfig: Complete application configuration

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    try:
        # Kalshi configuration
        kalshi = KalshiConfig(
            api_url=get_env_var('KALSHI_API_URL', 'https://api.elections.kalshi.com/trade-api/v2'),
            private_key_path=get_env_var('KALSHI_PRIVATE_KEY_PATH'),
            key_id=get_env_var('KALSHI_KEY_ID'),
            min_delay=get_env_float('KALSHI_MIN_DELAY', 0.11),  # 110ms default for Basic tier
            max_retries=get_env_int('KALSHI_MAX_RETRIES', 3),
            timeout=get_env_int('KALSHI_TIMEOUT', 30)
        )

        # Polymarket configuration
        polymarket = PolymarketConfig(
            api_url=get_env_var('POLYMARKET_API_URL', 'https://gamma-api.polymarket.com'),
            private_key=get_env_var('POLYMARKET_PRIVATE_KEY', None, required=False),
            wallet_address=get_env_var('POLYMARKET_WALLET_ADDRESS', None, required=False),
            min_delay=get_env_float('POLYMARKET_MIN_DELAY', 0.75),  # 750ms conservative
            max_retries=get_env_int('POLYMARKET_MAX_RETRIES', 3),
            timeout=get_env_int('POLYMARKET_TIMEOUT', 30)
        )

        # Database configuration (for bifurcation)
        database = DatabaseConfig(
            host=get_env_var('DATABASE_HOST', 'localhost'),
            market_data_port=get_env_int('ZEROMQ_DATABASE_MARKET_DATA_PORT', 5560),
            analysis_data_port=get_env_int('ZEROMQ_DATABASE_ANALYSIS_DATA_PORT', 5561),
            trade_data_port=get_env_int('ZEROMQ_DATABASE_TRADE_DATA_PORT', 5562)
        )

        # ZeroMQ configuration - UPDATED WITH DATABASE CONFIG
        zeromq = ZeroMQConfig(
            ireland_endpoint=get_env_var('ZEROMQ_IRELAND_ENDPOINT', 'tcp://ireland-server:5555'),
            virginia_endpoint=get_env_var('ZEROMQ_VIRGINIA_ENDPOINT', 'tcp://virginia-server:5556'),

            # Channel ports (Ireland communication)
            subscription_pub_port=get_env_int('ZEROMQ_SUBSCRIPTION_PUB_PORT', 5555),
            subscription_sub_port=get_env_int('ZEROMQ_SUBSCRIPTION_SUB_PORT', 5555),
            marketdata_push_port=get_env_int('ZEROMQ_MARKETDATA_PUSH_PORT', 5556),
            marketdata_pull_port=get_env_int('ZEROMQ_MARKETDATA_PULL_PORT', 5556),
            execution_req_port=get_env_int('ZEROMQ_EXECUTION_REQ_PORT', 5557),
            execution_rep_port=get_env_int('ZEROMQ_EXECUTION_REP_PORT', 5557),
            fills_push_port=get_env_int('ZEROMQ_FILLS_PUSH_PORT', 5558),
            fills_pull_port=get_env_int('ZEROMQ_FILLS_PULL_PORT', 5558),
            metrics_push_port=get_env_int('ZEROMQ_METRICS_PUSH_PORT', 5559),
            metrics_pull_port=get_env_int('ZEROMQ_METRICS_PULL_PORT', 5559),

            # Database server configuration (NEW)
            database=database,

            # Timing settings
            heartbeat_interval=get_env_int('ZEROMQ_HEARTBEAT_INTERVAL', 10),
            connection_timeout=get_env_int('ZEROMQ_CONNECTION_TIMEOUT', 5000),
            send_timeout=get_env_int('ZEROMQ_SEND_TIMEOUT', 1000),

            # Security (optional)
            curve_public_key=get_env_var('ZEROMQ_CURVE_PUBLIC_KEY', None, required=False),
            curve_private_key=get_env_var('ZEROMQ_CURVE_PRIVATE_KEY', None, required=False),
            curve_server_key=get_env_var('ZEROMQ_CURVE_SERVER_KEY', None, required=False)
        )

        # Redis configuration
        redis = RedisConfig(
            host=get_env_var('REDIS_HOST', 'localhost'),
            port=get_env_int('REDIS_PORT', 6379),
            db=get_env_int('REDIS_DB', 0),
            password=get_env_var('REDIS_PASSWORD', None, required=False)
        )

        # MongoDB configuration
        mongo = MongoConfig(
            uri=get_env_var('MONGODB_URI', 'mongodb://localhost:27017'),
            database=get_env_var('MONGODB_DATABASE', 'arbitrage_bot'),
            collections=get_env_var('MONGODB_COLLECTIONS', 'trades,orders,market_data')
        )

        # Trading configuration
        trading = TradingConfig(
            min_profit_threshold=get_env_float('MIN_PROFIT_THRESHOLD', 0.02),  # 2% minimum profit
            max_position_size=get_env_float('MAX_POSITION_SIZE', 1000.0),  # $1000 max position
            risk_limit=get_env_float('DAILY_RISK_LIMIT', 5000.0),  # $5000 daily risk limit
            min_volume_threshold=get_env_float('MIN_VOLUME_THRESHOLD', 100.0)  # $100 minimum volume
        )

        # Coordinator configuration
        coordinator = CoordinatorConfig(
            ticker_refresh_interval=get_env_int('TICKER_REFRESH_INTERVAL', 30),  # 30 seconds default
            health_check_interval=get_env_int('HEALTH_CHECK_INTERVAL', 60),     # 60 seconds default
            max_startup_time=get_env_int('MAX_STARTUP_TIME', 120),              # 2 minutes max startup
            shutdown_timeout=get_env_int('SHUTDOWN_TIMEOUT', 30)                # 30 seconds max shutdown
        )

        # Main app configuration
        config = AppConfig(
            kalshi=kalshi,
            polymarket=polymarket,
            zeromq=zeromq,
            redis=redis,
            mongo=mongo,
            trading=trading,
            coordinator=coordinator,
            environment=get_env_var('ENVIRONMENT', 'development'),
            log_level=get_env_var('LOG_LEVEL', 'INFO'),
            debug_mode=get_env_bool('DEBUG_MODE', False)
        )

        logger.info(f"Configuration loaded successfully for environment: {config.environment}")
        return config

    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise


# Global config instance - load once on import
try:
    CONFIG = load_config()
except Exception as e:
    logger.error(f"Failed to initialize global configuration: {e}")
    raise


# Convenience functions for common config access
def get_kalshi_config() -> KalshiConfig:
    """Get Kalshi configuration"""
    return CONFIG.kalshi


def get_polymarket_config() -> PolymarketConfig:
    """Get Polymarket configuration"""
    return CONFIG.polymarket


def get_trading_config() -> TradingConfig:
    """Get trading configuration"""
    return CONFIG.trading


def get_coordinator_config() -> CoordinatorConfig:
    """Get coordinator configuration"""
    return CONFIG.coordinator


def get_database_config() -> DatabaseConfig:
    """Get database server configuration"""
    return CONFIG.zeromq.database


def is_production() -> bool:
    """Check if running in production environment"""
    return CONFIG.environment.lower() == 'production'


def is_debug_mode() -> bool:
    """Check if debug mode is enabled"""
    return CONFIG.debug_mode


if __name__ == "__main__":
    # Test configuration loading
    print("Testing configuration loading...")

    try:
        config = load_config()
        print(f"✅ Configuration loaded successfully")
        print(f"Environment: {config.environment}")
        print(f"Kalshi key_id: {config.kalshi.key_id}")
        print(f"Kalshi private key path: {config.kalshi.private_key_path}")
        print(f"Kalshi min_delay: {config.kalshi.min_delay}s")
        print(f"Polymarket min_delay: {config.polymarket.min_delay}s")
        print(f"Trading profit threshold: {config.trading.min_profit_threshold}")
        print(f"Ticker refresh interval: {config.coordinator.ticker_refresh_interval}s")
        print(f"Health check interval: {config.coordinator.health_check_interval}s")
        print(f"Debug mode: {config.debug_mode}")
        print(f"ZeroMQ subscription port: {config.zeromq.subscription_pub_port}")
        print(f"ZeroMQ market data port: {config.zeromq.marketdata_push_port}")
        print(f"Database host: {config.zeromq.database.host}")
        print(f"Database market data port: {config.zeromq.database.market_data_port}")
        print(f"Database analysis port: {config.zeromq.database.analysis_data_port}")
        print(f"Database trade port: {config.zeromq.database.trade_data_port}")

    except Exception as e:
        print(f"❌ Configuration loading failed: {e}")
        print("\nMake sure you have a .env file with required variables:")
        print("KALSHI_PRIVATE_KEY_PATH=/path/to/key.pem")
        print("KALSHI_KEY_ID=your_key_id")
        print("TICKER_REFRESH_INTERVAL=30")
        print("DATABASE_HOST=localhost")
        print("ZEROMQ_DATABASE_MARKET_DATA_PORT=5560")
        print("ZEROMQ_DATABASE_ANALYSIS_DATA_PORT=5561")
        print("ZEROMQ_DATABASE_TRADE_DATA_PORT=5562")
        print("# ... and other required variables")