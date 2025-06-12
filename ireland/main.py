#!/usr/bin/env python3
"""
main.py

Complete Ireland server integration.
Orchestrates: API client -> data_feed -> coordinator -> ZeroMQ publisher -> Virginia

Entry point for the Ireland Polymarket arbitrage server.
"""

import sys
import asyncio
import logging
import time
import signal
from pathlib import Path
from typing import Set

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from coordinator import PolymarketCoordinator
from api.data_feed import PolymarketDataFeed
from utils.logger import get_logger

logger = get_logger('main')


class IrelandServer:
    """
    Main Ireland server that coordinates all components.
    Runs the complete Polymarket arbitrage pipeline.
    """

    def __init__(self, market_data_interval: float = 2.0):
        """Initialize Ireland server"""
        self.market_data_interval = market_data_interval

        # Core components
        self.coordinator: PolymarketCoordinator = None
        self.data_feed: PolymarketDataFeed = None

        # Control
        self.is_running = False
        self.shutdown_requested = False

        # Mock data for development (will be replaced by Virginia subscriptions)
        self.mock_condition_ids = {
            "0x26d06d9c6303c11bf7388cff707e4dac836e03628630720bca3d8cbf4234713d",  # Mairead McGuinness
            "0x17cd926896aa499bea870e218dc2e32c99f102237e6ae90f49f4b03d69cac4da",  # Conor McGregor
        }

        # Performance tracking
        self.stats = {
            'start_time': 0,
            'uptime_seconds': 0,
            'total_data_cycles': 0,
            'successful_publishes': 0,
            'last_activity': 0
        }

        logger.info("Ireland server initialized")
        logger.info(f"Market data interval: {market_data_interval}s")
        logger.info(f"Mock condition IDs: {[id[:10] + '...' for id in self.mock_condition_ids]}")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def start(self):
        """Start all Ireland server components"""
        try:
            logger.info("Starting Ireland Polymarket server...")
            self.stats['start_time'] = time.time()

            # Step 1: Start coordinator (trading client + publisher)
            logger.info("Starting coordinator...")
            self.coordinator = PolymarketCoordinator()
            await self.coordinator.start()
            logger.info("Coordinator started")

            # Step 2: Start data feed (API client only)
            logger.info("Starting data feed...")
            self.data_feed = PolymarketDataFeed(self.market_data_interval)
            await self.data_feed.__aenter__()
            logger.info("Data feed started")

            # Step 3: Wire up the callback chain (data_feed -> coordinator)
            logger.info("Wiring up callback chain...")
            self.data_feed.add_data_callback(self.coordinator.handle_market_data_callback)

            # Inject coordinator reference into data_feed for token mapping
            self.data_feed.coordinator = self.coordinator

            logger.info("Callback chain established: data_feed -> coordinator -> publisher")

            # Step 4: Set active condition IDs (mock data for now)
            logger.info("Injecting mock condition IDs...")
            self.set_active_condition_ids(self.mock_condition_ids)
            logger.info("Mock condition IDs injected")

            # Step 5: Publish initial account status to Virginia
            logger.info("Publishing initial account status...")
            await self.publish_account_status()

            self.is_running = True
            logger.info("Ireland server started successfully!")
            logger.info("Pipeline: Polymarket APIs -> data_feed -> coordinator -> ZeroMQ -> Virginia")

        except Exception as e:
            logger.error(f"Failed to start Ireland server: {e}")
            await self.stop()
            raise

    async def stop(self):
        """Stop all Ireland server components"""
        logger.info("Stopping Ireland server...")
        self.shutdown_requested = True

        if self.data_feed:
            self.data_feed.stop()
            await self.data_feed.__aexit__(None, None, None)
            logger.info("Data feed stopped")

        if self.coordinator:
            await self.coordinator.stop()
            logger.info("Coordinator stopped")

        self.is_running = False

        # Calculate final stats
        if self.stats['start_time'] > 0:
            self.stats['uptime_seconds'] = time.time() - self.stats['start_time']

        logger.info("Ireland server stopped")
        logger.info(f"Final uptime: {self.stats['uptime_seconds']:.1f} seconds")

    def set_active_condition_ids(self, condition_ids: Set[str]):
        """
        Set active condition IDs (from Virginia or mock).
        This will eventually be called by ZeroMQ receiver.
        """
        logger.info(f"Setting active condition IDs: {len(condition_ids)} conditions")

        # Update data feed (handles all API discovery)
        if self.data_feed:
            self.data_feed.set_active_condition_ids(condition_ids)

        # Update coordinator (handles routing/caching)
        if self.coordinator:
            self.coordinator.update_condition_ids(list(condition_ids))

        logger.info("Condition IDs updated across all components")

    async def publish_account_status(self):
        """Publish current account status to Virginia"""
        if not self.coordinator or not self.coordinator.publisher:
            return

        try:
            # Prepare account data (no API calls, just status)
            account_data = {
                'balance': {'available': 0.0, 'total': 0.0},  # Mock data
                'positions': [],  # Mock data
                'server_id': 'ireland',
                'timestamp': time.time(),
                'status': 'READY'
            }

            # Send to Virginia
            success = await self.coordinator.publisher.send_system_metrics(account_data)

            if success:
                logger.info("Account status published to Virginia")
            else:
                logger.warning("Failed to publish account status")

        except Exception as e:
            logger.error(f"Error publishing account status: {e}")

    async def run_server(self):
        """
        Main server loop.
        Monitors the data pipeline and handles graceful shutdown.
        """
        logger.info("Starting Ireland server main loop...")

        # Start data feed task with proper exception handling
        try:
            logger.info("Creating data feed task...")
            data_feed_task = asyncio.create_task(self.data_feed.run_data_feed())
            logger.info("Data feed task created successfully")
        except Exception as e:
            logger.error(f"Failed to create data feed task: {e}")
            import traceback
            traceback.print_exc()
            return

        # Monitor loop
        last_status_log = 0

        try:
            while not self.shutdown_requested and self.is_running:
                current_time = time.time()

                # Update stats
                self.stats['uptime_seconds'] = current_time - self.stats['start_time']

                # Log status every 30 seconds
                if current_time - last_status_log >= 30:
                    await self.log_status()
                    last_status_log = current_time

                # Check if data feed is still running
                if data_feed_task.done():
                    exception = data_feed_task.exception()
                    if exception:
                        logger.error(f"Data feed crashed with exception: {exception}")
                        import traceback
                        logger.error(
                            f"Exception traceback: {''.join(traceback.format_exception(type(exception), exception, exception.__traceback__))}")
                        break
                    else:
                        logger.warning("Data feed completed unexpectedly (no exception)")
                        try:
                            result = data_feed_task.result()
                            logger.info(f"Data feed task result: {result}")
                        except Exception as e:
                            logger.error(f"Error getting task result: {e}")
                        break
                else:
                    # Log that task is still running (every 10 seconds)
                    if current_time % 10 < 1:  # Rough 10-second interval
                        logger.debug("Data feed task is still running...")

                # Sleep briefly
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in server main loop: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # Clean shutdown
            logger.info("Initiating graceful shutdown...")

            if not data_feed_task.done():
                logger.info("Stopping data feed task...")
                self.data_feed.stop()
                try:
                    await asyncio.wait_for(data_feed_task, timeout=5.0)
                    logger.info("Data feed task completed gracefully")
                except asyncio.TimeoutError:
                    logger.warning("Data feed shutdown timeout")
                    data_feed_task.cancel()
                    logger.info("Data feed task cancelled")
            else:
                logger.info("Data feed task was already done")

            logger.info("Ireland server main loop completed")

    async def log_status(self):
        """Log current server status"""
        try:
            # Get coordinator status
            coord_status = self.coordinator.get_status() if self.coordinator else {}

            # Get data feed status
            feed_status = self.data_feed.get_status() if self.data_feed else {}

            logger.info("Ireland Server Status:")
            logger.info(f"   Uptime: {self.stats['uptime_seconds']:.1f}s")
            logger.info(f"   Active conditions: {coord_status.get('active_conditions', 0)}")
            logger.info(f"   Cached snapshots: {coord_status.get('cached_snapshots', 0)}")
            logger.info(f"   Data callbacks: {coord_status.get('statistics', {}).get('data_callbacks_received', 0)}")
            logger.info(
                f"   Publisher sends: {coord_status.get('statistics', {}).get('publisher_sends_successful', 0)}")
            logger.info(f"   Feed cycles: {feed_status.get('statistics', {}).get('total_cycles', 0)}")

        except Exception as e:
            logger.error(f"Error logging status: {e}")

    def get_status(self) -> dict:
        """Get complete server status"""
        return {
            'is_running': self.is_running,
            'uptime_seconds': self.stats['uptime_seconds'],
            'coordinator_status': self.coordinator.get_status() if self.coordinator else {},
            'data_feed_status': self.data_feed.get_status() if self.data_feed else {},
            'statistics': self.stats.copy()
        }


# Signal handling for graceful shutdown
shutdown_event = None


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    if shutdown_event:
        shutdown_event.set()


async def main():
    """Main entry point for Ireland server"""
    global shutdown_event

    print("IRELAND POLYMARKET ARBITRAGE SERVER")
    print("=" * 50)

    # Create shutdown event in the correct event loop
    shutdown_event = asyncio.Event()

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        async with IrelandServer(market_data_interval=2.0) as server:
            logger.info("Ireland server ready for Virginia connections")

            # Create tasks
            server_task = asyncio.create_task(server.run_server())
            shutdown_task = asyncio.create_task(shutdown_event.wait())

            logger.info("Created server_task and shutdown_task, starting wait...")

            # Wait for either server completion or shutdown signal
            done, pending = await asyncio.wait(
                [server_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            logger.info(
                f"One task completed. Done tasks: {[task.get_name() if hasattr(task, 'get_name') else 'unnamed' for task in done]}")

            # Check which task completed
            if shutdown_task in done:
                logger.info("Shutdown event was triggered")
            if server_task in done:
                logger.info("Server task completed")

            # Cancel pending tasks
            for task in pending:
                logger.info(f"Cancelling pending task: {task.get_name() if hasattr(task, 'get_name') else 'unnamed'}")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Check if server task completed with exception
            if server_task in done:
                exception = server_task.exception()
                if exception:
                    logger.error(f"Server task failed: {exception}")
                    raise exception

            logger.info("Ireland server shutdown complete")

    except KeyboardInterrupt:
        logger.info("Shutdown initiated by user")
    except Exception as e:
        logger.error(f"Ireland server failed: {e}")
        raise


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        # Run the server
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer interrupted by user")
    except Exception as e:
        print(f"Server failed: {e}")
        exit(1)