#!/usr/bin/env python3
"""
influx/writer.py

High-level InfluxDB writer for Data Server.
Handles buffering, batch writing, and robust error recovery.
"""

import asyncio
import time
import json
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_influx_logger
from models.influx import MarketSnapshot, MarketDataBatch
from influx.client import DataServerInfluxClient, InfluxDBWriteError, InfluxDBConnectionError


class InfluxWriteBuffer:
    """
    High-performance write buffer with automatic flushing.
    """

    def __init__(self, max_size: int = None, max_age_seconds: float = None):
        """
        Initialize write buffer.

        Args:
            max_size: Maximum number of snapshots before flush
            max_age_seconds: Maximum age before flush
        """
        self.max_size = max_size or CONFIG.influxdb.batch_size
        self.max_age_seconds = max_age_seconds or CONFIG.influxdb.flush_interval

        # Buffer storage
        self.batch = MarketDataBatch()
        self.snapshots_by_id: Dict[str, MarketSnapshot] = {}

        # Timing
        self.last_flush_time = time.time()

        # Statistics
        self.stats = {
            'snapshots_added': 0,
            'flushes_triggered': 0,
            'size_triggered_flushes': 0,
            'time_triggered_flushes': 0,
            'manual_flushes': 0
        }

    def add_snapshot(self, snapshot: MarketSnapshot):
        """Add snapshot to buffer"""
        self.batch.add_snapshot(snapshot)
        self.snapshots_by_id[snapshot.snapshot_id] = snapshot
        self.stats['snapshots_added'] += 1

    def should_flush(self) -> tuple[bool, str]:
        """
        Check if buffer should be flushed.

        Returns:
            (should_flush, reason)
        """
        # Check size limit
        if self.batch.size >= self.max_size:
            return True, "size_limit"

        # Check age limit
        current_time = time.time()
        if (current_time - self.last_flush_time) >= self.max_age_seconds:
            return True, "age_limit"

        return False, "no_flush_needed"

    def get_snapshots_for_flush(self) -> List[MarketSnapshot]:
        """Get all snapshots and clear buffer"""
        snapshots = self.batch.snapshots.copy()
        self.clear()
        return snapshots

    def clear(self):
        """Clear buffer"""
        self.batch.clear()
        self.snapshots_by_id.clear()
        self.last_flush_time = time.time()

    def update_snapshot_usage(self, snapshot_id: str, analyzed: bool = None, executed: bool = None):
        """Update usage flags for buffered snapshot"""
        if snapshot_id in self.snapshots_by_id:
            snapshot = self.snapshots_by_id[snapshot_id]
            if analyzed is not None:
                if analyzed:
                    snapshot.mark_as_analyzed()
            if executed is not None:
                if executed:
                    snapshot.mark_as_executed()

    @property
    def size(self) -> int:
        """Current buffer size"""
        return self.batch.size

    @property
    def age_seconds(self) -> float:
        """Buffer age in seconds"""
        return time.time() - self.last_flush_time


class InfluxDBWriter:
    """
    High-level InfluxDB writer with buffering, error recovery, and performance optimization.

    Features:
    - Automatic batching and flushing
    - Robust error handling with retry logic
    - Performance monitoring
    - Usage flag updates for trade correlation
    """

    def __init__(self):
        """Initialize InfluxDB writer"""
        self.logger = get_influx_logger()

        # InfluxDB client
        self.client = DataServerInfluxClient()

        # Write buffer
        self.buffer = InfluxWriteBuffer(
            max_size=CONFIG.influxdb.batch_size,
            max_age_seconds=CONFIG.influxdb.flush_interval
        )

        # Background tasks
        self.flush_task: Optional[asyncio.Task] = None
        self.is_running = False
        self.shutdown_requested = False

        # Error handling
        self.max_retries = 3
        self.retry_delay_base = 1.0  # seconds
        self.failed_writes: List[Dict[str, Any]] = []
        self.max_failed_writes = 1000

        # Performance tracking
        self.stats = {
            'snapshots_written': 0,
            'batch_writes': 0,
            'write_errors': 0,
            'retry_attempts': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'usage_updates': 0,
            'usage_update_errors': 0,
            'avg_batch_size': 0.0,
            'avg_write_latency_ms': 0.0,
            'last_successful_write': 0,
            'last_error_time': 0
        }

        # Write latency tracking
        self.write_latencies = []
        self.max_latency_samples = 50

        self.logger.info("InfluxDB writer initialized")

    async def start(self):
        """Start the InfluxDB writer"""
        try:
            self.logger.info("Starting InfluxDB writer...")

            # Connect to InfluxDB
            await self.client.connect()

            # Start background flush task
            self.is_running = True
            self.flush_task = asyncio.create_task(self._background_flush_loop())

            self.logger.info("InfluxDB writer started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start InfluxDB writer: {e}")
            raise

    async def stop(self):
        """Stop the InfluxDB writer gracefully"""
        self.logger.info("Stopping InfluxDB writer...")
        self.shutdown_requested = True

        # Cancel background task
        if self.flush_task and not self.flush_task.done():
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass

        # Flush any remaining data
        await self.flush_buffer(force=True)

        # Disconnect from InfluxDB
        await self.client.disconnect()

        self.is_running = False
        self.logger.info("InfluxDB writer stopped")

    async def write_snapshot(self, snapshot: MarketSnapshot) -> bool:
        """
        Write market snapshot to InfluxDB (buffered).

        Args:
            snapshot: MarketSnapshot to write

        Returns:
            True if successfully added to buffer
        """
        try:
            # Add timestamps
            snapshot.data_server_received_ns = time.time_ns()

            # Add to buffer
            self.buffer.add_snapshot(snapshot)

            self.logger.debug(f"Added snapshot {snapshot.snapshot_id[:8]}... to buffer "
                              f"({self.buffer.size}/{self.buffer.max_size})")

            # Check if we should flush immediately
            should_flush, reason = self.buffer.should_flush()
            if should_flush:
                await self.flush_buffer(reason=reason)

            return True

        except Exception as e:
            self.logger.error(f"Error adding snapshot to buffer: {e}")
            return False

    async def write_snapshots_batch(self, snapshots: List[MarketSnapshot]) -> bool:
        """
        Write multiple snapshots in a single operation.

        Args:
            snapshots: List of MarketSnapshots to write

        Returns:
            True if all snapshots written successfully
        """
        try:
            if not snapshots:
                return True

            # Add timestamps to all snapshots
            current_time_ns = time.time_ns()
            for snapshot in snapshots:
                snapshot.data_server_received_ns = current_time_ns

            # Write directly (bypass buffer for batch operations)
            success = await self._write_snapshots_to_influx(snapshots)

            if success:
                self.logger.info(f"Successfully wrote batch of {len(snapshots)} snapshots")
            else:
                self.logger.error(f"Failed to write batch of {len(snapshots)} snapshots")

            return success

        except Exception as e:
            self.logger.error(f"Error writing snapshot batch: {e}")
            return False

    async def flush_buffer(self, force: bool = False, reason: str = "manual") -> bool:
        """
        Flush write buffer to InfluxDB.

        Args:
            force: Force flush even if buffer is empty
            reason: Reason for flush (for logging)

        Returns:
            True if flush successful
        """
        if self.buffer.size == 0 and not force:
            return True

        try:
            # Get snapshots from buffer
            snapshots = self.buffer.get_snapshots_for_flush()

            if not snapshots:
                self.logger.debug("Buffer flush requested but no snapshots to write")
                return True

            # Update flush statistics
            self.buffer.stats['flushes_triggered'] += 1
            if reason == "size_limit":
                self.buffer.stats['size_triggered_flushes'] += 1
            elif reason == "age_limit":
                self.buffer.stats['time_triggered_flushes'] += 1
            elif reason == "manual":
                self.buffer.stats['manual_flushes'] += 1

            self.logger.debug(f"Flushing buffer: {len(snapshots)} snapshots, reason: {reason}")

            # Write to InfluxDB
            success = await self._write_snapshots_to_influx(snapshots)

            if success:
                self.logger.debug(f"Successfully flushed {len(snapshots)} snapshots")
            else:
                # On failure, we've already lost the data from buffer
                # Could implement a failed write queue here
                self.logger.error(f"Failed to flush {len(snapshots)} snapshots")
                self._store_failed_write(snapshots)

            return success

        except Exception as e:
            self.logger.error(f"Error during buffer flush: {e}")
            return False

    async def _write_snapshots_to_influx(self, snapshots: List[MarketSnapshot]) -> bool:
        """
        Write snapshots directly to InfluxDB with retry logic.

        Args:
            snapshots: List of snapshots to write

        Returns:
            True if write successful
        """
        write_start = time.time()

        try:
            # Add storage timestamps
            storage_time_ns = time.time_ns()
            for snapshot in snapshots:
                snapshot.data_server_stored_ns = storage_time_ns

            # Convert to line protocol
            line_protocols = []
            for snapshot in snapshots:
                try:
                    line_protocol = snapshot.to_influx_line_protocol("market_snapshot")
                    line_protocols.append(line_protocol)
                except Exception as e:
                    self.logger.error(f"Error converting snapshot {snapshot.snapshot_id} to line protocol: {e}")
                    continue

            if not line_protocols:
                self.logger.warning("No valid line protocols generated from snapshots")
                return False

            # Write with retry logic
            for attempt in range(self.max_retries + 1):
                try:
                    success = await self.client.write_line_protocol(line_protocols)

                    if success:
                        # Update statistics
                        write_duration = (time.time() - write_start) * 1000
                        self.stats['snapshots_written'] += len(snapshots)
                        self.stats['batch_writes'] += 1
                        self.stats['last_successful_write'] = time.time()

                        # Track batch size
                        total_batches = self.stats['batch_writes']
                        self.stats['avg_batch_size'] = (
                                (self.stats['avg_batch_size'] * (total_batches - 1) + len(snapshots)) / total_batches
                        )

                        # Track write latency
                        self.write_latencies.append(write_duration)
                        if len(self.write_latencies) > self.max_latency_samples:
                            self.write_latencies = self.write_latencies[-self.max_latency_samples:]

                        if self.write_latencies:
                            self.stats['avg_write_latency_ms'] = sum(self.write_latencies) / len(self.write_latencies)

                        self.logger.debug(f"Wrote {len(snapshots)} snapshots in {write_duration:.2f}ms")
                        return True
                    else:
                        raise InfluxDBWriteError("Write returned False")

                except Exception as e:
                    self.stats['retry_attempts'] += 1

                    if attempt < self.max_retries:
                        delay = self.retry_delay_base * (2 ** attempt)  # Exponential backoff
                        self.logger.warning(f"Write attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                    else:
                        self.stats['write_errors'] += 1
                        self.stats['failed_retries'] += 1
                        self.stats['last_error_time'] = time.time()
                        self.logger.error(
                            f"Failed to write {len(snapshots)} snapshots after {self.max_retries} retries: {e}")
                        return False

            return False

        except Exception as e:
            self.stats['write_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Unexpected error writing snapshots: {e}")
            return False

    def _store_failed_write(self, snapshots: List[MarketSnapshot]):
        """Store failed write for potential retry"""
        failed_write = {
            'timestamp': time.time(),
            'snapshot_count': len(snapshots),
            'snapshot_ids': [s.snapshot_id for s in snapshots],
            'line_protocols': [s.to_influx_line_protocol() for s in snapshots]
        }

        self.failed_writes.append(failed_write)

        # Limit failed writes queue
        if len(self.failed_writes) > self.max_failed_writes:
            self.failed_writes = self.failed_writes[-self.max_failed_writes:]

        self.logger.warning(f"Stored {len(snapshots)} failed snapshots for potential retry")

    async def retry_failed_writes(self) -> int:
        """
        Retry all failed writes.

        Returns:
            Number of successful retries
        """
        if not self.failed_writes:
            return 0

        successful_retries = 0
        failed_writes_copy = self.failed_writes.copy()
        self.failed_writes.clear()

        for failed_write in failed_writes_copy:
            try:
                line_protocols = failed_write['line_protocols']
                success = await self.client.write_line_protocol(line_protocols)

                if success:
                    successful_retries += 1
                    self.stats['successful_retries'] += 1
                    self.logger.info(f"Successfully retried failed write: {failed_write['snapshot_count']} snapshots")
                else:
                    # Put it back in failed queue
                    self.failed_writes.append(failed_write)

            except Exception as e:
                self.logger.error(f"Error retrying failed write: {e}")
                self.failed_writes.append(failed_write)

        return successful_retries

    async def update_snapshot_usage(self, snapshot_id: str, analyzed: bool = None, executed: bool = None) -> bool:
        """
        Update usage flags for a snapshot.

        Args:
            snapshot_id: Snapshot ID to update
            analyzed: Set analyzed flag
            executed: Set executed flag

        Returns:
            True if update successful
        """
        try:
            self.stats['usage_updates'] += 1

            # Check if snapshot is in buffer first
            if snapshot_id in self.buffer.snapshots_by_id:
                self.buffer.update_snapshot_usage(snapshot_id, analyzed, executed)
                self.logger.debug(f"Updated buffered snapshot {snapshot_id[:8]}... usage flags")
                return True

            # Otherwise update in InfluxDB
            success = await self.client.update_snapshot_usage(snapshot_id, analyzed, executed)

            if success:
                self.logger.debug(f"Updated InfluxDB snapshot {snapshot_id[:8]}... usage flags")
            else:
                self.stats['usage_update_errors'] += 1
                self.logger.error(f"Failed to update snapshot {snapshot_id[:8]}... usage flags")

            return success

        except Exception as e:
            self.stats['usage_update_errors'] += 1
            self.logger.error(f"Error updating snapshot usage for {snapshot_id}: {e}")
            return False

    async def _background_flush_loop(self):
        """Background task to flush buffer based on time"""
        self.logger.info("Starting background flush loop")

        while not self.shutdown_requested:
            try:
                # Check if buffer should be flushed
                should_flush, reason = self.buffer.should_flush()

                if should_flush and reason == "age_limit":
                    await self.flush_buffer(reason=reason)

                # Sleep for a short interval
                await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in background flush loop: {e}")
                await asyncio.sleep(5.0)  # Wait longer on error

        self.logger.info("Background flush loop stopped")

    def get_status(self) -> Dict[str, Any]:
        """Get writer status and statistics"""
        current_time = time.time()

        # Calculate rates
        uptime = current_time - (self.stats.get('start_time', current_time))
        write_rate = self.stats['snapshots_written'] / max(uptime, 1)

        return {
            'is_running': self.is_running,
            'buffer_size': self.buffer.size,
            'buffer_max_size': self.buffer.max_size,
            'buffer_age_seconds': self.buffer.age_seconds,
            'buffer_max_age_seconds': self.buffer.max_age_seconds,
            'failed_writes_queued': len(self.failed_writes),
            'client_status': self.client.get_connection_status(),
            'statistics': self.stats.copy(),
            'buffer_statistics': self.buffer.stats.copy(),
            'performance': {
                'write_rate_per_second': write_rate,
                'avg_batch_size': self.stats['avg_batch_size'],
                'avg_write_latency_ms': self.stats['avg_write_latency_ms'],
                'recent_latencies_ms': self.write_latencies[-5:] if self.write_latencies else []
            }
        }

    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for monitoring"""
        current_time = time.time()

        # Determine health status
        is_healthy = (
                self.is_running and
                self.client.is_connected and
                (current_time - self.stats.get('last_successful_write', current_time)) < 300  # 5 minutes
        )

        error_rate = self.stats['write_errors'] / max(self.stats['batch_writes'], 1)

        return {
            'is_healthy': is_healthy,
            'is_running': self.is_running,
            'client_connected': self.client.is_connected,
            'buffer_utilization_percent': (self.buffer.size / self.buffer.max_size) * 100,
            'error_rate_percent': error_rate * 100,
            'failed_writes_count': len(self.failed_writes),
            'last_successful_write': self.stats.get('last_successful_write', 0),
            'last_error_time': self.stats.get('last_error_time', 0)
        }


# Demo and testing
async def test_influx_writer():
    """Test InfluxDB writer functionality"""
    print("TESTING INFLUXDB WRITER")
    print("=" * 50)

    try:
        # Initialize writer
        writer = InfluxDBWriter()
        await writer.start()
        print("✅ InfluxDB writer started")

        # Create test snapshots
        from models.influx import create_kalshi_snapshot, create_polymarket_snapshot

        # Test single snapshot write
        kalshi_orderbook = {
            "yes": [{"price": 45, "quantity": 1000}],
            "no": [{"price": 55, "quantity": 800}]
        }

        kalshi_snapshot = create_kalshi_snapshot(
            ticker="TEST-KALSHI",
            orderbook_dict=kalshi_orderbook,
            api_timestamps={
                'api_call_start_ns': time.time_ns() - 1000000,
                'api_response_ns': time.time_ns() - 500000,
                'processing_complete_ns': time.time_ns()
            }
        )

        print("Testing single snapshot write...")
        success = await writer.write_snapshot(kalshi_snapshot)
        print(f"✅ Single snapshot write: {success}")

        # Test batch write
        snapshots = []
        for i in range(5):
            snapshot = create_kalshi_snapshot(
                ticker=f"TEST-BATCH-{i}",
                orderbook_dict=kalshi_orderbook
            )
            snapshots.append(snapshot)

        print("Testing batch write...")
        success = await writer.write_snapshots_batch(snapshots)
        print(f"✅ Batch write: {success}")

        # Test usage update
        print("Testing usage update...")
        success = await writer.update_snapshot_usage(kalshi_snapshot.snapshot_id, analyzed=True)
        print(f"✅ Usage update: {success}")

        # Test buffer flush
        print("Testing manual buffer flush...")
        success = await writer.flush_buffer(force=True)
        print(f"✅ Buffer flush: {success}")

        # Show status
        status = writer.get_status()
        print(f"\nWriter Status:")
        print(f"  Running: {status['is_running']}")
        print(f"  Buffer: {status['buffer_size']}/{status['buffer_max_size']}")
        print(f"  Snapshots Written: {status['statistics']['snapshots_written']}")
        print(f"  Batch Writes: {status['statistics']['batch_writes']}")
        print(f"  Write Rate: {status['performance']['write_rate_per_second']:.2f}/sec")
        print(f"  Avg Latency: {status['performance']['avg_write_latency_ms']:.2f}ms")

        # Show health
        health = writer.get_health_summary()
        print(f"\nHealth Summary:")
        print(f"  Healthy: {health['is_healthy']}")
        print(f"  Buffer Utilization: {health['buffer_utilization_percent']:.1f}%")
        print(f"  Error Rate: {health['error_rate_percent']:.2f}%")

        # Stop writer
        await writer.stop()
        print("✅ InfluxDB writer stopped")

        print("\n✅ InfluxDB writer test completed!")
        return True

    except Exception as e:
        print(f"❌ InfluxDB writer test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(test_influx_writer())
        if not success:
            exit(1)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test execution failed: {e}")
        exit(1)