#!/usr/bin/env python3
"""
utils/config.py

Configuration management for Data Server.
Loads all settings from .env file - no hardcoded values in Python.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class InfluxDBConfig:
    """InfluxDB configuration"""
    url: str
    token: str
    org: str
    bucket: str
    timeout: int
    batch_size: int
    flush_interval: int


@dataclass
class PostgresConfig:
    """PostgreSQL configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    max_connections: int
    connection_timeout: int


@dataclass
class ZeroMQConfig:
    """ZeroMQ configuration for receiving data from Virginia"""
    virginia_host: str
    market_data_port: int
    analysis_data_port: int
    trade_data_port: int
    recv_timeout: int
    recv_hwm: int


@dataclass
class BufferConfig:
    """Buffering configuration"""
    max_memory_buffer_size: int
    memory_buffer_timeout: int
    disk_buffer_enabled: bool
    disk_buffer_path: str
    redis_buffer_enabled: bool
    redis_host: str
    redis_port: int
    redis_db: int


@dataclass
class DataServerConfig:
    """Main data server configuration"""
    server_id: str
    environment: str
    debug_mode: bool
    log_level: str
    shutdown_timeout: int
    health_check_interval: int

    # Component configs
    influxdb: InfluxDBConfig
    postgres: PostgresConfig
    zeromq: ZeroMQConfig
    buffer: BufferConfig


def load_env_var(var_name: str, default: Optional[str] = None, required: bool = True) -> str:
    """
    Load environment variable with proper error handling.

    Args:
        var_name: Environment variable name
        default: Default value if not found
        required: Whether this variable is required

    Returns:
        Environment variable value

    Raises:
        ValueError: If required variable is missing
    """
    value = os.getenv(var_name, default)

    if required and value is None:
        raise ValueError(f"Required environment variable {var_name} is not set")

    return value


def load_env_int(var_name: str, default: Optional[int] = None, required: bool = True) -> int:
    """Load environment variable as integer"""
    value_str = load_env_var(var_name, str(default) if default is not None else None, required)

    if value_str is None:
        return None

    try:
        return int(value_str)
    except ValueError:
        raise ValueError(f"Environment variable {var_name} must be an integer, got: {value_str}")


def load_env_bool(var_name: str, default: bool = False) -> bool:
    """Load environment variable as boolean"""
    value_str = load_env_var(var_name, str(default), required=False)
    return value_str.lower() in ('true', '1', 'yes', 'on')


def load_influxdb_config() -> InfluxDBConfig:
    """Load InfluxDB configuration from environment"""
    return InfluxDBConfig(
        url=load_env_var('INFLUXDB_URL'),
        token=load_env_var('INFLUXDB_TOKEN'),
        org=load_env_var('INFLUXDB_ORG'),
        bucket=load_env_var('INFLUXDB_BUCKET'),
        timeout=load_env_int('INFLUXDB_TIMEOUT', 10000),  # milliseconds
        batch_size=load_env_int('INFLUXDB_BATCH_SIZE', 1000),
        flush_interval=load_env_int('INFLUXDB_FLUSH_INTERVAL', 5)  # seconds
    )


def load_postgres_config() -> PostgresConfig:
    """Load PostgreSQL configuration from environment"""
    return PostgresConfig(
        host=load_env_var('POSTGRES_HOST'),
        port=load_env_int('POSTGRES_PORT', 5432),
        database=load_env_var('POSTGRES_DATABASE'),
        username=load_env_var('POSTGRES_USERNAME'),
        password=load_env_var('POSTGRES_PASSWORD'),
        max_connections=load_env_int('POSTGRES_MAX_CONNECTIONS', 20),
        connection_timeout=load_env_int('POSTGRES_CONNECTION_TIMEOUT', 30)
    )


def load_zeromq_config() -> ZeroMQConfig:
    """Load ZeroMQ configuration from environment"""
    return ZeroMQConfig(
        virginia_host=load_env_var('VIRGINIA_HOST'),
        market_data_port=load_env_int('ZEROMQ_MARKET_DATA_PORT'),
        analysis_data_port=load_env_int('ZEROMQ_ANALYSIS_DATA_PORT'),
        trade_data_port=load_env_int('ZEROMQ_TRADE_DATA_PORT'),
        recv_timeout=load_env_int('ZEROMQ_RECV_TIMEOUT', 1000),  # milliseconds
        recv_hwm=load_env_int('ZEROMQ_RECV_HWM', 1000)  # high water mark
    )


def load_buffer_config() -> BufferConfig:
    """Load buffering configuration from environment"""
    return BufferConfig(
        max_memory_buffer_size=load_env_int('BUFFER_MAX_MEMORY_SIZE', 10000),
        memory_buffer_timeout=load_env_int('BUFFER_MEMORY_TIMEOUT', 5),  # seconds
        disk_buffer_enabled=load_env_bool('BUFFER_DISK_ENABLED', False),
        disk_buffer_path=load_env_var('BUFFER_DISK_PATH', '/tmp/data_server_buffer', required=False),
        redis_buffer_enabled=load_env_bool('BUFFER_REDIS_ENABLED', False),
        redis_host=load_env_var('REDIS_HOST', 'localhost', required=False),
        redis_port=load_env_int('REDIS_PORT', 6379, required=False),
        redis_db=load_env_int('REDIS_DB', 0, required=False)
    )


def load_config() -> DataServerConfig:
    """
    Load complete data server configuration from environment variables.

    Returns:
        DataServerConfig: Complete configuration object

    Raises:
        ValueError: If required environment variables are missing
    """
    try:
        config = DataServerConfig(
            server_id=load_env_var('DATA_SERVER_ID', f'data-server-{os.getpid()}'),
            environment=load_env_var('ENVIRONMENT', 'development'),
            debug_mode=load_env_bool('DEBUG_MODE', False),
            log_level=load_env_var('LOG_LEVEL', 'INFO'),
            shutdown_timeout=load_env_int('SHUTDOWN_TIMEOUT', 30),
            health_check_interval=load_env_int('HEALTH_CHECK_INTERVAL', 60),

            # Load component configurations
            influxdb=load_influxdb_config(),
            postgres=load_postgres_config(),
            zeromq=load_zeromq_config(),
            buffer=load_buffer_config()
        )

        return config

    except Exception as e:
        raise ValueError(f"Failed to load data server configuration: {e}")


def validate_config(config: DataServerConfig) -> bool:
    """
    Validate configuration for common issues.

    Args:
        config: Configuration to validate

    Returns:
        True if configuration is valid

    Raises:
        ValueError: If configuration is invalid
    """
    # Validate InfluxDB URL format
    if not config.influxdb.url.startswith(('http://', 'https://')):
        raise ValueError(f"Invalid InfluxDB URL format: {config.influxdb.url}")

    # Validate PostgreSQL port range
    if not (1 <= config.postgres.port <= 65535):
        raise ValueError(f"Invalid PostgreSQL port: {config.postgres.port}")

    # Validate ZeroMQ ports
    zeromq_ports = [
        config.zeromq.market_data_port,
        config.zeromq.analysis_data_port,
        config.zeromq.trade_data_port
    ]

    for port in zeromq_ports:
        if not (1024 <= port <= 65535):
            raise ValueError(f"Invalid ZeroMQ port: {port}")

    # Check for port conflicts
    if len(set(zeromq_ports)) != len(zeromq_ports):
        raise ValueError("ZeroMQ ports must be unique")

    # Validate buffer settings
    if config.buffer.max_memory_buffer_size <= 0:
        raise ValueError("Memory buffer size must be positive")

    # Validate disk buffer path if enabled
    if config.buffer.disk_buffer_enabled:
        buffer_path = Path(config.buffer.disk_buffer_path)
        try:
            buffer_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError(f"Cannot create disk buffer directory: {e}")

    return True


def get_config_summary(config: DataServerConfig) -> str:
    """
    Get human-readable configuration summary (excluding sensitive data).

    Args:
        config: Configuration object

    Returns:
        Configuration summary string
    """
    summary = f"""
Data Server Configuration Summary:
  Server ID: {config.server_id}
  Environment: {config.environment}
  Debug Mode: {config.debug_mode}
  Log Level: {config.log_level}

InfluxDB:
  URL: {config.influxdb.url}
  Org: {config.influxdb.org}
  Bucket: {config.influxdb.bucket}
  Batch Size: {config.influxdb.batch_size}
  Flush Interval: {config.influxdb.flush_interval}s

PostgreSQL:
  Host: {config.postgres.host}
  Port: {config.postgres.port}
  Database: {config.postgres.database}
  Username: {config.postgres.username}
  Max Connections: {config.postgres.max_connections}

ZeroMQ:
  Virginia Host: {config.zeromq.virginia_host}
  Market Data Port: {config.zeromq.market_data_port}
  Analysis Data Port: {config.zeromq.analysis_data_port}
  Trade Data Port: {config.zeromq.trade_data_port}

Buffering:
  Memory Buffer Size: {config.buffer.max_memory_buffer_size}
  Memory Buffer Timeout: {config.buffer.memory_buffer_timeout}s
  Disk Buffer: {'Enabled' if config.buffer.disk_buffer_enabled else 'Disabled'}
  Redis Buffer: {'Enabled' if config.buffer.redis_buffer_enabled else 'Disabled'}
"""
    return summary.strip()


# Load global configuration
try:
    CONFIG = load_config()
    validate_config(CONFIG)
except Exception as e:
    print(f"FATAL: Failed to load data server configuration: {e}")
    print("\nRequired environment variables:")
    print("  INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET")
    print("  POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD")
    print("  VIRGINIA_HOST, ZEROMQ_MARKET_DATA_PORT, ZEROMQ_ANALYSIS_DATA_PORT, ZEROMQ_TRADE_DATA_PORT")
    raise

# Demo and testing
if __name__ == "__main__":
    print("DATA SERVER CONFIGURATION TEST")
    print("=" * 50)

    try:
        # Test configuration loading
        test_config = load_config()
        print("✅ Configuration loaded successfully")

        # Test validation
        validate_config(test_config)
        print("✅ Configuration validation passed")

        # Show configuration summary
        print("\n" + get_config_summary(test_config))

        # Test individual component access
        print(f"\nComponent Access Test:")
        print(f"  InfluxDB URL: {test_config.influxdb.url}")
        print(f"  Postgres Host: {test_config.postgres.host}")
        print(f"  ZeroMQ Virginia Host: {test_config.zeromq.virginia_host}")
        print(f"  Buffer Memory Size: {test_config.buffer.max_memory_buffer_size}")

        print("\n✅ Data server configuration test completed!")

    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        print("\nMake sure your .env file contains all required variables")