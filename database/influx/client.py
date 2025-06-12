#!/usr/bin/env python3
"""
influx/client.py

InfluxDB client for Data Server.
Handles connection management, health checks, and low-level database operations.
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_influx_logger

# InfluxDB imports
try:
    from influxdb_client import InfluxDBClient as InfluxDBClientLibrary, Point
    from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
    from influxdb_client.rest import ApiException

    INFLUXDB_AVAILABLE = True
except ImportError:
    INFLUXDB_AVAILABLE = False
    print("WARNING: influxdb-client not installed. Install with: pip install influxdb-client")


class InfluxDBConnectionError(Exception):
    """InfluxDB connection error"""
    pass


class InfluxDBWriteError(Exception):
    """InfluxDB write error"""
    pass


class DataServerInfluxClient:
    """
    High-performance InfluxDB client for market data storage.

    Handles connection management, health monitoring, and provides
    both synchronous and asynchronous write capabilities.
    """

    def __init__(self):
        """Initialize InfluxDB client"""
        self.logger = get_influx_logger()

        # Configuration
        self.url = CONFIG.influxdb.url
        self.token = CONFIG.influxdb.token
        self.org = CONFIG.influxdb.org
        self.bucket = CONFIG.influxdb.bucket
        self.timeout = CONFIG.influxdb.timeout

        # Client instances
        self.client: Optional[InfluxDBClientLibrary] = None
        self.write_api = None
        self.query_api = None
        self.health_api = None

        # Connection state
        self.is_connected = False
        self.last_health_check = 0
        self.health_check_interval = 30  # seconds

        # Performance metrics
        self.stats = {
            'connection_attempts': 0,
            'successful_connections': 0,
            'write_operations': 0,
            'write_errors': 0,
            'query_operations': 0,
            'query_errors': 0,
            'total_points_written': 0,
            'last_write_time': 0,
            'last_error_time': 0,
            'avg_write_latency_ms': 0.0
        }

        # Write latency tracking
        self.write_latencies = []
        self.max_latency_samples = 100

        if not INFLUXDB_AVAILABLE:
            self.logger.error("InfluxDB client library not available")

    async def connect(self) -> bool:
        """
        Establish connection to InfluxDB.

        Returns:
            True if connection successful

        Raises:
            InfluxDBConnectionError: If connection fails
        """
        if not INFLUXDB_AVAILABLE:
            raise InfluxDBConnectionError("InfluxDB client library not installed")

        self.stats['connection_attempts'] += 1

        try:
            self.logger.info(f"Connecting to InfluxDB at {self.url}")

            # Create client - now using the correctly imported class
            self.client = InfluxDBClientLibrary(
                url=self.url,
                token=self.token,
                org=self.org,
                timeout=self.timeout
            )

            # Create API instances
            self.write_api = self.client.write_api(write_options=ASYNCHRONOUS)
            self.query_api = self.client.query_api()
            self.health_api = self.client.health()

            # Test connection
            health_check = await self._perform_health_check()

            if health_check:
                self.is_connected = True
                self.stats['successful_connections'] += 1
                self.logger.info(f"Successfully connected to InfluxDB - Org: {self.org}, Bucket: {self.bucket}")
                return True
            else:
                raise InfluxDBConnectionError("Health check failed after connection")

        except Exception as e:
            self.logger.error(f"Failed to connect to InfluxDB: {e}")
            self.is_connected = False
            raise InfluxDBConnectionError(f"Connection failed: {e}")

    async def disconnect(self):
        """Disconnect from InfluxDB gracefully"""
        try:
            if self.write_api:
                # Flush any pending writes
                self.write_api.close()

            if self.client:
                self.client.close()

            self.is_connected = False
            self.client = None
            self.write_api = None
            self.query_api = None
            self.health_api = None

            self.logger.info("Disconnected from InfluxDB")

        except Exception as e:
            self.logger.error(f"Error during InfluxDB disconnect: {e}")

    async def _perform_health_check(self) -> bool:
        """
        Perform health check on InfluxDB connection.

        Returns:
            True if healthy
        """
        try:
            if not self.client:
                return False

            # Use synchronous health check
            health = self.client.health()

            if health and health.status == "pass":
                self.last_health_check = time.time()
                self.logger.debug("InfluxDB health check passed")
                return True
            else:
                self.logger.warning(f"InfluxDB health check failed: {health}")
                return False

        except Exception as e:
            self.logger.error(f"InfluxDB health check error: {e}")
            return False

    async def ensure_connected(self) -> bool:
        """
        Ensure connection is healthy, reconnect if needed.

        Returns:
            True if connection is healthy
        """
        current_time = time.time()

        # Check if we need to perform health check
        if (current_time - self.last_health_check) > self.health_check_interval:
            if not await self._perform_health_check():
                self.logger.warning("Health check failed, attempting reconnection...")
                self.is_connected = False

                try:
                    await self.disconnect()
                    return await self.connect()
                except Exception as e:
                    self.logger.error(f"Reconnection failed: {e}")
                    return False

        return self.is_connected

    async def write_line_protocol(self, line_protocol: Union[str, List[str]],
                                  bucket: str = None) -> bool:
        """
        Write line protocol data to InfluxDB.

        Args:
            line_protocol: Single line protocol string or list of strings
            bucket: Optional bucket override

        Returns:
            True if write successful

        Raises:
            InfluxDBWriteError: If write fails
        """
        if not await self.ensure_connected():
            raise InfluxDBWriteError("Not connected to InfluxDB")

        write_start = time.time()
        target_bucket = bucket or self.bucket

        try:
            # Convert single string to list
            if isinstance(line_protocol, str):
                lines = [line_protocol]
                point_count = 1
            else:
                lines = line_protocol
                point_count = len(lines)

            self.logger.debug(f"Writing {point_count} points to InfluxDB bucket: {target_bucket}")

            # Write to InfluxDB
            self.write_api.write(
                bucket=target_bucket,
                org=self.org,
                record=lines
            )

            # Update statistics
            write_duration = (time.time() - write_start) * 1000  # Convert to ms
            self.stats['write_operations'] += 1
            self.stats['total_points_written'] += point_count
            self.stats['last_write_time'] = time.time()

            # Track latency
            self.write_latencies.append(write_duration)
            if len(self.write_latencies) > self.max_latency_samples:
                self.write_latencies = self.write_latencies[-self.max_latency_samples:]

            # Calculate average latency
            if self.write_latencies:
                self.stats['avg_write_latency_ms'] = sum(self.write_latencies) / len(self.write_latencies)

            self.logger.debug(f"Successfully wrote {point_count} points in {write_duration:.2f}ms")
            return True

        except Exception as e:
            self.stats['write_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Failed to write {point_count} points to InfluxDB: {e}")
            raise InfluxDBWriteError(f"Write failed: {e}")

    async def write_points(self, points: Union[Point, List[Point]],
                           bucket: str = None) -> bool:
        """
        Write Point objects to InfluxDB.

        Args:
            points: Single Point or list of Points
            bucket: Optional bucket override

        Returns:
            True if write successful
        """
        if not await self.ensure_connected():
            raise InfluxDBWriteError("Not connected to InfluxDB")

        write_start = time.time()
        target_bucket = bucket or self.bucket

        try:
            # Convert single point to list
            if isinstance(points, Point):
                point_list = [points]
                point_count = 1
            else:
                point_list = points
                point_count = len(points)

            self.logger.debug(f"Writing {point_count} Point objects to bucket: {target_bucket}")

            # Write to InfluxDB
            self.write_api.write(
                bucket=target_bucket,
                org=self.org,
                record=point_list
            )

            # Update statistics
            write_duration = (time.time() - write_start) * 1000
            self.stats['write_operations'] += 1
            self.stats['total_points_written'] += point_count
            self.stats['last_write_time'] = time.time()

            self.logger.debug(f"Successfully wrote {point_count} Point objects in {write_duration:.2f}ms")
            return True

        except Exception as e:
            self.stats['write_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Failed to write {point_count} Point objects: {e}")
            raise InfluxDBWriteError(f"Point write failed: {e}")

    async def query(self, flux_query: str, bucket: str = None) -> List[Dict[str, Any]]:
        """
        Execute Flux query against InfluxDB.

        Args:
            flux_query: Flux query string
            bucket: Optional bucket override

        Returns:
            List of query results as dictionaries

        Raises:
            InfluxDBWriteError: If query fails
        """
        if not await self.ensure_connected():
            raise InfluxDBWriteError("Not connected to InfluxDB")

        query_start = time.time()
        target_bucket = bucket or self.bucket

        try:
            self.stats['query_operations'] += 1

            # Execute query
            result = self.query_api.query(flux_query, org=self.org)

            # Convert result to list of dictionaries
            records = []
            for table in result:
                for record in table.records:
                    records.append(record.values)

            query_duration = (time.time() - query_start) * 1000
            self.logger.debug(f"Query executed in {query_duration:.2f}ms, returned {len(records)} records")

            return records

        except Exception as e:
            self.stats['query_errors'] += 1
            self.logger.error(f"Query failed: {e}")
            raise InfluxDBWriteError(f"Query failed: {e}")

    async def get_snapshot_by_id(self, snapshot_id: str,
                                 time_range: str = "1h") -> Optional[Dict[str, Any]]:
        """
        Retrieve market snapshot by snapshot_id.

        Args:
            snapshot_id: Snapshot ID to find
            time_range: Time range to search (e.g., "1h", "1d")

        Returns:
            Snapshot data as dictionary, or None if not found
        """
        flux_query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: -{time_range})
          |> filter(fn: (r) => r._measurement == "market_snapshot")
          |> filter(fn: (r) => r.snapshot_id == "{snapshot_id}")
          |> last()
        '''

        try:
            results = await self.query(flux_query)
            return results[0] if results else None
        except Exception as e:
            self.logger.error(f"Failed to retrieve snapshot {snapshot_id}: {e}")
            return None

    async def update_snapshot_usage(self, snapshot_id: str,
                                    analyzed: bool = None,
                                    executed: bool = None) -> bool:
        """
        Update usage flags for a market snapshot.

        Args:
            snapshot_id: Snapshot ID to update
            analyzed: Set analyzed flag
            executed: Set executed flag

        Returns:
            True if update successful
        """
        try:
            # Get current snapshot
            current_snapshot = await self.get_snapshot_by_id(snapshot_id)

            if not current_snapshot:
                self.logger.warning(f"Snapshot {snapshot_id} not found for usage update")
                return False

            # Create updated line protocol
            # Note: InfluxDB doesn't support true updates, so we write a new point
            # with the same timestamp but updated fields

            tags = []
            fields = []

            # Preserve existing tags
            for key, value in current_snapshot.items():
                if key.startswith('_') or key in ['table', 'result']:
                    continue
                if key in ['source', 'ticker', 'snapshot_id', 'condition_id', 'pair_id']:
                    if value:
                        tags.append(f'{key}={value}')
                else:
                    # Preserve existing fields
                    if isinstance(value, str):
                        fields.append(f'{key}="{value}"')
                    elif isinstance(value, bool):
                        fields.append(f'{key}={str(value).lower()}')
                    elif isinstance(value, int):
                        fields.append(f'{key}={value}i')
                    else:
                        fields.append(f'{key}={value}')

            # Update usage flags
            if analyzed is not None:
                fields.append(f'analyzed={str(analyzed).lower()}')
                if analyzed:
                    fields.append(f'analysis_timestamp_ns={time.time_ns()}i')

            if executed is not None:
                fields.append(f'executed={str(executed).lower()}')
                if executed:
                    fields.append(f'execution_timestamp_ns={time.time_ns()}i')

            # Build line protocol
            tags_str = ','.join(tags)
            fields_str = ','.join(fields)
            timestamp_ns = current_snapshot.get('_time', time.time_ns())

            line_protocol = f'market_snapshot,{tags_str} {fields_str} {timestamp_ns}'

            # Write updated snapshot
            return await self.write_line_protocol(line_protocol)

        except Exception as e:
            self.logger.error(f"Failed to update snapshot usage for {snapshot_id}: {e}")
            return False

    def get_connection_status(self) -> Dict[str, Any]:
        """Get detailed connection status"""
        return {
            'is_connected': self.is_connected,
            'url': self.url,
            'org': self.org,
            'bucket': self.bucket,
            'last_health_check': self.last_health_check,
            'health_check_interval': self.health_check_interval,
            'statistics': self.stats.copy(),
            'avg_write_latency_ms': self.stats['avg_write_latency_ms']
        }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        current_time = time.time()

        # Calculate rates
        uptime = current_time - (self.stats.get('first_connection_time', current_time))
        write_rate = self.stats['write_operations'] / max(uptime, 1)
        error_rate = self.stats['write_errors'] / max(self.stats['write_operations'], 1)

        return {
            'write_operations_total': self.stats['write_operations'],
            'write_errors_total': self.stats['write_errors'],
            'points_written_total': self.stats['total_points_written'],
            'write_rate_per_second': write_rate,
            'error_rate_percent': error_rate * 100,
            'avg_write_latency_ms': self.stats['avg_write_latency_ms'],
            'recent_latencies_ms': self.write_latencies[-10:] if self.write_latencies else [],
            'last_write_time': self.stats['last_write_time'],
            'last_error_time': self.stats['last_error_time']
        }


# Demo and testing
async def test_influx_client():
    """Test InfluxDB client functionality"""
    print("TESTING INFLUXDB CLIENT")
    print("=" * 50)

    if not INFLUXDB_AVAILABLE:
        print("❌ InfluxDB client library not available")
        print("Install with: pip install influxdb-client")
        return False

    try:
        # Initialize client
        client = DataServerInfluxClient()

        # Test connection
        print("Testing connection...")
        await client.connect()
        print("✅ Connected to InfluxDB")

        # Test line protocol write
        test_line_protocol = f'''
        test_measurement,source=test,ticker=TEST field1=42.5,field2="test_value",field3=true {time.time_ns()}
        '''

        print("Testing line protocol write...")
        success = await client.write_line_protocol(test_line_protocol.strip())
        print(f"✅ Line protocol write: {success}")

        # Test Point write
        from influxdb_client import Point

        test_point = Point("test_measurement") \
            .tag("source", "test") \
            .tag("ticker", "TEST") \
            .field("field1", 42.5) \
            .field("field2", "test_value") \
            .field("field3", True) \
            .time(time.time_ns())

        print("Testing Point object write...")
        success = await client.write_points(test_point)
        print(f"✅ Point write: {success}")

        # Test health check
        print("Testing health check...")
        healthy = await client._perform_health_check()
        print(f"✅ Health check: {healthy}")

        # Show status
        status = client.get_connection_status()
        print(f"\nConnection Status:")
        print(f"  Connected: {status['is_connected']}")
        print(f"  Bucket: {status['bucket']}")
        print(f"  Write Operations: {status['statistics']['write_operations']}")
        print(f"  Points Written: {status['statistics']['total_points_written']}")
        print(f"  Avg Latency: {status['avg_write_latency_ms']:.2f}ms")

        # Show performance metrics
        metrics = client.get_performance_metrics()
        print(f"\nPerformance Metrics:")
        print(f"  Write Rate: {metrics['write_rate_per_second']:.2f}/sec")
        print(f"  Error Rate: {metrics['error_rate_percent']:.2f}%")
        print(f"  Recent Latencies: {metrics['recent_latencies_ms']}")

        # Test disconnect
        await client.disconnect()
        print("✅ Disconnected from InfluxDB")

        print("\n✅ InfluxDB client test completed!")
        return True

    except Exception as e:
        print(f"❌ InfluxDB client test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(test_influx_client())
        if not success:
            exit(1)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test execution failed: {e}")
        exit(1)