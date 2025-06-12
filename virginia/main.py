#!/usr/bin/env python3
"""
main.py

Main entry point for the arbitrage trading system.
Initializes logging, loads configuration, and launches the coordinator.
"""

import sys
import asyncio
import signal
from pathlib import Path
from typing import Optional

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import after path setup
from utils.logger import get_main_logger
from utils.config import CONFIG
from coordinator import ArbitrageCoordinator


class ArbitrageBot:
    """
    Main arbitrage bot application.
    Manages the lifecycle of the entire system.
    """

    def __init__(self):
        """Initialize the arbitrage bot"""
        self.logger = get_main_logger()
        self.coordinator: Optional[ArbitrageCoordinator] = None
        self.shutdown_requested = False

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.logger.info("Arbitrage bot initialized")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        self.logger.info(f"Received {signal_name}, initiating graceful shutdown...")
        self.shutdown_requested = True

    def _validate_configuration(self) -> bool:
        """
        Validate critical configuration before starting.

        Returns:
            True if configuration is valid, False otherwise
        """
        try:
            # Check Kalshi configuration
            if not CONFIG.kalshi.key_id:
                self.logger.error("Kalshi key_id not configured")
                return False

            if not CONFIG.kalshi.private_key_path:
                self.logger.error("Kalshi private_key_path not configured")
                return False

            # Verify private key file exists
            key_path = Path(CONFIG.kalshi.private_key_path)
            if not key_path.exists():
                self.logger.error(f"Kalshi private key file not found: {key_path}")
                return False

            # Check rate limiting configuration
            if CONFIG.kalshi.min_delay <= 0:
                self.logger.error(f"Invalid Kalshi min_delay: {CONFIG.kalshi.min_delay}")
                return False

            # Log configuration summary
            self.logger.info("Configuration validation successful:")
            self.logger.info(f"  Environment: {CONFIG.environment}")
            self.logger.info(f"  Kalshi min_delay: {CONFIG.kalshi.min_delay}s")
            self.logger.info(f"  Debug mode: {CONFIG.debug_mode}")
            self.logger.info(f"  Ticker refresh interval: {getattr(CONFIG, 'ticker_refresh_interval', 'Not set')}")

            return True

        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False

    def _log_startup_banner(self):
        """Log system startup information"""
        self.logger.info("=" * 60)
        self.logger.info("ARBITRAGE TRADING SYSTEM - STARTING UP")
        self.logger.info("=" * 60)
        self.logger.info(f"Environment: {CONFIG.environment}")
        self.logger.info(f"Debug Mode: {CONFIG.debug_mode}")
        self.logger.info(f"Kalshi API: {CONFIG.kalshi.api_url}")
        self.logger.info(f"Rate Limit: {CONFIG.kalshi.min_delay}s between calls")
        self.logger.info("System Architecture:")
        self.logger.info("  Virginia (Main): Kalshi API + Coordination")
        self.logger.info("  Ireland (Remote): Polymarket API + Data Relay")
        self.logger.info("=" * 60)

    async def start_system(self):
        """
        Start the complete arbitrage trading system.

        Returns:
            True if system started successfully, False otherwise
        """
        try:
            self._log_startup_banner()

            # Validate configuration
            if not self._validate_configuration():
                self.logger.error("System startup aborted due to configuration errors")
                return False

            # Initialize coordinator
            self.logger.info("Initializing arbitrage coordinator...")
            self.coordinator = ArbitrageCoordinator()

            # Start coordinator
            self.logger.info("Starting arbitrage coordinator...")
            await self.coordinator.start()

            self.logger.info("Arbitrage trading system started successfully!")
            self.logger.info("Press Ctrl+C to stop the system gracefully")

            # Main system loop
            while not self.shutdown_requested:
                # Check coordinator health
                if not self.coordinator.is_healthy():
                    self.logger.error("Coordinator health check failed!")
                    break

                # Small sleep to prevent busy waiting
                await asyncio.sleep(1)

            return True

        except Exception as e:
            self.logger.error(f"Failed to start arbitrage system: {e}")
            return False

    async def shutdown_system(self):
        """Shutdown the arbitrage trading system gracefully"""
        self.logger.info("Shutting down arbitrage trading system...")

        if self.coordinator:
            try:
                await self.coordinator.stop()
                self.logger.info("Coordinator stopped successfully")
            except Exception as e:
                self.logger.error(f"Error stopping coordinator: {e}")

        self.logger.info("Arbitrage trading system shutdown complete")
        self.logger.info("=" * 60)

    async def run(self):
        """
        Main application run method.
        Handles startup, operation, and shutdown.
        """
        try:
            # Start the system
            success = await self.start_system()

            if not success:
                self.logger.error("System failed to start properly")
                return False

            return True

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
        finally:
            # Always attempt graceful shutdown
            await self.shutdown_system()


async def main():
    """
    Main entry point function.
    Creates and runs the arbitrage bot.
    """
    bot = ArbitrageBot()

    try:
        await bot.run()
    except Exception as e:
        bot.logger.error(f"Critical error in main(): {e}")
        return 1

    return 0


if __name__ == "__main__":
    # Run the arbitrage bot
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nProgram interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)