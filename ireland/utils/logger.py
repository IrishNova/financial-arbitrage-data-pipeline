#!/usr/bin/env python3
"""
utils/logger.py

Centralized logging system for Ireland arbitrage server.
Provides component-specific loggers with hourly rotation.
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
    Creates separate log files for each Ireland server component.
    """

    def __init__(self, component_name: str, log_level: str = None):
        """
        Initialize component logger.

        Args:
            component_name: Name of the component (receiver, sender, polymarket, etc.)
            log_level: Override log level for this component
        """
        self.component_name = component_name
        self.log_level = log_level or getattr(CONFIG, 'log_level', 'INFO')

        # Create logs directory in project root
        self.log_dir = project_root / 'logs'
        self.log_dir.mkdir(exist_ok=True)

        # Setup logger
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger with file and console handlers"""
        logger = logging.getLogger(f'ireland.{self.component_name}')
        logger.setLevel(getattr(logging, self.log_level.upper()))

        # Clear any existing handlers
        logger.handlers.clear()

        # Create formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # File handler with hourly rotation
        log_file = self.log_dir / f'ireland_{self.component_name}.log'
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
    Central manager for all Ireland server component loggers.
    """

    def __init__(self):
        """Initialize logger manager"""
        self.loggers: Dict[str, ComponentLogger] = {}

    def get_logger(self, component_name: str, log_level: str = None) -> logging.Logger:
        """
        Get or create logger for a component.

        Args:
            component_name: Component name (receiver, sender, polymarket, etc.)
            log_level: Optional log level override

        Returns:
            Configured logger instance
        """
        if component_name not in self.loggers:
            # Get component-specific log level from environment
            env_var = f'IRELAND_{component_name.upper()}_LOG_LEVEL'
            component_level = os.getenv(env_var, log_level or CONFIG.log_level)

            self.loggers[component_name] = ComponentLogger(
                component_name=component_name,
                log_level=component_level
            )

            # Log the logger creation
            logger = self.loggers[component_name].get_logger()
            logger.info(f"Ireland {component_name} logger initialized - Level: {component_level}")

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
            'server_type': 'ireland'
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

        for log_file in log_dir.glob('ireland_*.log*'):
            if log_file.stat().st_mtime < cutoff_time:
                try:
                    log_file.unlink()
                    print(f"Cleaned up old Ireland log file: {log_file}")
                except Exception as e:
                    print(f"Failed to clean up {log_file}: {e}")


# Global logger manager instance
_logger_manager = LoggerManager()


def get_logger(component_name: str, log_level: str = None) -> logging.Logger:
    """
    Convenience function to get an Ireland component logger.

    Args:
        component_name: Component name (receiver, sender, polymarket, coordinator, etc.)
        log_level: Optional log level override

    Returns:
        Configured logger instance

    Example:
        logger = get_logger('polymarket')
        logger.info("Polymarket client starting...")
    """
    return _logger_manager.get_logger(component_name, log_level)


def set_global_log_level(level: str):
    """Set log level for all Ireland components"""
    _logger_manager.set_global_level(level)


def get_logging_status() -> Dict[str, any]:
    """Get status of all Ireland loggers"""
    return _logger_manager.get_log_status()


def cleanup_logs(hours_to_keep: int = 24):
    """Clean up old Ireland log files"""
    _logger_manager.cleanup_old_logs(hours_to_keep)


# Common Ireland logger instances for easy import
def get_receiver_logger() -> logging.Logger:
    """Get Ireland receiver logger"""
    return get_logger('receiver')


def get_sender_logger() -> logging.Logger:
    """Get Ireland sender logger"""
    return get_logger('sender')


def get_polymarket_logger() -> logging.Logger:
    """Get Polymarket client logger"""
    return get_logger('polymarket')


def get_coordinator_logger() -> logging.Logger:
    """Get Ireland coordinator logger"""
    return get_logger('coordinator')


def get_trading_logger() -> logging.Logger:
    """Get trading execution logger"""
    return get_logger('trading')


# Demo and testing
if __name__ == "__main__":
    print("Testing Ireland server logging system...")
    print("=" * 50)

    # Test different component loggers
    components = ['receiver', 'sender', 'polymarket', 'coordinator', 'trading']

    for component in components:
        logger = get_logger(component)
        logger.info(f"Testing Ireland {component} logger")
        logger.debug(f"Debug message from Ireland {component}")
        logger.warning(f"Warning message from Ireland {component}")
        logger.error(f"Error message from Ireland {component}")

    # Show logging status
    status = get_logging_status()
    print("\nIreland Logging Status:")
    for key, value in status.items():
        print(f"  {key}: {value}")

    # Test log level changes
    print("\nTesting log level changes...")
    set_global_log_level('DEBUG')

    polymarket_logger = get_polymarket_logger()
    polymarket_logger.debug("This debug message should now appear")

    print("\nIreland logging system test completed!")
    print(f"Check log files in: {Path.cwd() / 'logs'}")
    print("Ireland log files will be prefixed with 'ireland_'")