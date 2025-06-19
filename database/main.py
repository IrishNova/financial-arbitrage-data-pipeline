#!/usr/bin/env python3
"""
main.py

Production Data Server for Arbitrage Trading System.
Simple entry point that imports and runs the coordinator.
"""

import asyncio
import signal
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from coordinator import DataServerCoordinator, setup_signal_handlers
from utils.config import CONFIG


def main():
    """Main production entry point"""
    print("ARBITRAGE DATA SERVER - PRODUCTION")
    print("=" * 40)

    # Initialize coordinator
    coordinator = DataServerCoordinator()

    # Setup signal handlers for graceful shutdown
    setup_signal_handlers(coordinator)

    try:
        # Run forever
        asyncio.run(coordinator.run_forever())

    except KeyboardInterrupt:
        print("\nShutdown initiated by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

    print("Data Server shutdown complete")


if __name__ == "__main__":
    main()