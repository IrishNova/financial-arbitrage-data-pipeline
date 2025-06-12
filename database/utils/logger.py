#!/usr/bin/env python3
"""
utils/logger.py

Centralized logging system for Data Server.
Provides component-specific loggers with hourly rotation.
Follows the same architecture as Ireland/Virginia servers.
"""

import os
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import sys

# Add project root to path for config access
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG


class ComponentLogger:
    """
    Component-specific logger with hourly rotation.
    Creates separate log files for each Data Server component.
    """

    def __init__(self, component_name: str, log_level: str = None):
        """
        Initialize component logger.

        Args:
            component_name: Name of the component (coordinator, influx, postgres, broker, etc.)
            log_level: Override log level for this component
        """
        self.component_name = component_name
        self.log_level = log_level or CONFIG.log_level

        # Create logs directory in project root
        self.log_dir = project_root / 'logs'
        self.log_dir.mkdir(exist_ok=True)

        # Setup logger
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger with file and console handlers"""
        logger = logging.getLogger(f'data_server.{self.component_name}')
        logger.setLevel(getattr(logging, self.log_level.upper()))

        # Clear any existing handlers
        logger.handlers.clear()

        # Create formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # File handler with hourly rotation
        log_file = self.log_dir / f'data_server_{self.component_name}.log'
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=log_file,
            when='H',  # Rotate every hour
            interval=1,  # Every 1 hour
            backupCount=24,  # Keep 24 files (24 hours)
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, self.log_level.upper()))

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        # Console shows INFO and above, regardless of component log level
        console_handler.setLevel(logging.INFO)

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        # Prevent propagation to root logger
        logger.propagate = False

        return logger

    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance"""
        return self.logger

    def set_level(self, level: str):
        """Change log level dynamically"""
        self.log_level = level
        self.logger.setLevel(getattr(logging, level.upper()))

        # Update file handler level
        for handler in self.logger.handlers:
            if isinstance(handler, logging.handlers.TimedRotatingFileHandler):
                handler.setLevel(getattr(logging, level.upper()))


class LoggerManager:
    """
    Central manager for all Data Server component loggers.
    """

    def __init__(self):
        """Initialize logger manager"""
        self.loggers: Dict[str, ComponentLogger] = {}

    def get_logger(self, component_name: str, log_level: str = None) -> logging.Logger:
        """
        Get or create logger for a component.

        Args:
            component_name: Component name (coordinator, influx, postgres, broker, etc.)
            log_level: Optional log level override

        Returns:
            Configured logger instance
        """
        if component_name not in self.loggers:
            # Get component-specific log level from environment
            env_var = f'DATA_SERVER_{component_name.upper()}_LOG_LEVEL'
            component_level = os.getenv(env_var, log_level or CONFIG.log_level)

            self.loggers[component_name] = ComponentLogger(
                component_name=component_name,
                log_level=component_level
            )

            # Log the logger creation
            logger = self.loggers[component_name].get_logger()
            logger.info(f"Data Server {component_name} logger initialized - Level: {component_level}")

        return self.loggers[component_name].get_logger()

    def set_global_level(self, level: str):
        """Set log level for all existing loggers"""
        for component_logger in self.loggers.values():
            component_logger.set_level(level)

    def get_log_status(self) -> Dict[str, any]:
        """Get status of all loggers"""
        status = {
            'initialized_loggers': list(self.loggers.keys()),
            'log_directory': str(Path(project_root) / 'logs'),
            'global_log_level': CONFIG.log_level,
            'server_type': 'data_server'
        }

        # Add component-specific levels
        for name, component_logger in self.loggers.items():
            status[f'{name}_level'] = component_logger.log_level

        return status

    def cleanup_old_logs(self, hours_to_keep: int = 24):
        """
        Clean up log files older than specified hours.
        """
        log_dir = Path(project_root) / 'logs'
        if not log_dir.exists():
            return

        cutoff_time = datetime.now().timestamp() - (hours_to_keep * 3600)

        for log_file in log_dir.glob('data_server_*.log*'):
            if log_file.stat().st_mtime < cutoff_time:
                try:
                    log_file.unlink()
                    print(f"Cleaned up old Data Server log file: {log_file}")
                except Exception as e:
                    print(f"Failed to clean up {log_file}: {e}")


# Global logger manager instance
_logger_manager = LoggerManager()


def get_logger(component_name: str, log_level: str = None) -> logging.Logger:
    """
    Convenience function to get a Data Server component logger.

    Args:
        component_name: Component name (coordinator, influx, postgres, broker, etc.)
        log_level: Optional log level override

    Returns:
        Configured logger instance

    Example:
        logger = get_logger('influx')
        logger.info("InfluxDB writer starting...")
    """
    return _logger_manager.get_logger(component_name, log_level)


def set_global_log_level(level: str):
    """Set log level for all Data Server components"""
    _logger_manager.set_global_level(level)


def get_logging_status() -> Dict[str, any]:
    """Get status of all Data Server loggers"""
    return _logger_manager.get_log_status()


def cleanup_logs(hours_to_keep: int = 24):
    """Clean up old Data Server log files"""
    _logger_manager.cleanup_old_logs(hours_to_keep)


# Common Data Server logger instances for easy import
def get_coordinator_logger() -> logging.Logger:
    """Get Data Server coordinator logger"""
    return get_logger('coordinator')


def get_influx_logger() -> logging.Logger:
    """Get InfluxDB component logger"""
    return get_logger('influx')


def get_postgres_logger() -> logging.Logger:
    """Get PostgreSQL component logger"""
    return get_logger('postgres')


def get_broker_logger() -> logging.Logger:
    """Get ZeroMQ broker logger"""
    return get_logger('broker')


def get_buffer_logger() -> logging.Logger:
    """Get buffering component logger"""
    return get_logger('buffer')


def get_writer_logger() -> logging.Logger:
    """Get generic writer logger"""
    return get_logger('writer')


def get_client_logger() -> logging.Logger:
    """Get generic client logger"""
    return get_logger('client')


# Demo and testing
if __name__ == "__main__":
    print("Testing Data Server logging system...")
    print("=" * 50)

    # Test different component loggers
    components = ['coordinator', 'influx', 'postgres', 'broker', 'buffer']

    for component in components:
        logger = get_logger(component)
        logger.info(f"Testing Data Server {component} logger")
        logger.debug(f"Debug message from Data Server {component}")
        logger.warning(f"Warning message from Data Server {component}")
        logger.error(f"Error message from Data Server {component}")

    # Show logging status
    status = get_logging_status()
    print("\nData Server Logging Status:")
    for key, value in status.items():
        print(f"  {key}: {value}")

    # Test log level changes
    print("\nTesting log level changes...")
    set_global_log_level('DEBUG')

    influx_logger = get_influx_logger()
    influx_logger.debug("This debug message should now appear")

    print("\nData Server logging system test completed!")
    print(f"Check log files in: {Path.cwd() / 'logs'}")
    print("Data Server log files will be prefixed with 'data_server_'")