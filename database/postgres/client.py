#!/usr/bin/env python3
"""
postgres/client.py

PostgreSQL client for Data Server.
Handles connection management, health checks, and low-level database operations.
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.config import CONFIG
from utils.logger import get_postgres_logger

# PostgreSQL imports
try:
    import asyncpg
    from asyncpg import Connection, Pool
    from asyncpg.exceptions import PostgresError, ConnectionDoesNotExistError

    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    print("WARNING: asyncpg not installed. Install with: pip install asyncpg")


class PostgreSQLConnectionError(Exception):
    """PostgreSQL connection error"""
    pass


class PostgreSQLQueryError(Exception):
    """PostgreSQL query error"""
    pass


class PostgreSQLClient:
    """
    High-performance PostgreSQL client for trade data storage.

    Handles connection pooling, health monitoring, and provides
    async database operations with transaction support.
    """

    def __init__(self):
        """Initialize PostgreSQL client"""
        self.logger = get_postgres_logger()

        # Configuration
        self.host = CONFIG.postgres.host
        self.port = CONFIG.postgres.port
        self.database = CONFIG.postgres.database
        self.username = CONFIG.postgres.username
        self.password = CONFIG.postgres.password
        self.max_connections = CONFIG.postgres.max_connections
        self.connection_timeout = CONFIG.postgres.connection_timeout

        # Connection pool
        self.pool: Optional[Pool] = None

        # Connection state
        self.is_connected = False
        self.last_health_check = 0
        self.health_check_interval = 30  # seconds

        # Performance metrics
        self.stats = {
            'connection_attempts': 0,
            'successful_connections': 0,
            'query_operations': 0,
            'query_errors': 0,
            'transaction_operations': 0,
            'transaction_errors': 0,
            'total_queries_executed': 0,
            'last_query_time': 0,
            'last_error_time': 0,
            'avg_query_latency_ms': 0.0
        }

        # Query latency tracking
        self.query_latencies = []
        self.max_latency_samples = 100

        if not ASYNCPG_AVAILABLE:
            self.logger.error("AsyncPG library not available")

    async def connect(self) -> bool:
        """
        Establish connection pool to PostgreSQL.

        Returns:
            True if connection successful

        Raises:
            PostgreSQLConnectionError: If connection fails
        """
        if not ASYNCPG_AVAILABLE:
            raise PostgreSQLConnectionError("AsyncPG library not installed")

        self.stats['connection_attempts'] += 1

        try:
            self.logger.info(f"Connecting to PostgreSQL at {self.host}:{self.port}/{self.database}")

            # Build connection string
            dsn = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

            # Create connection pool
            self.pool = await asyncpg.create_pool(
                dsn,
                min_size=1,
                max_size=self.max_connections,
                command_timeout=self.connection_timeout,
                server_settings={
                    'jit': 'off',  # Disable JIT for consistent performance
                    'application_name': 'data_server'
                }
            )

            # Test connection
            health_check = await self._perform_health_check()

            if health_check:
                self.is_connected = True
                self.stats['successful_connections'] += 1
                self.logger.info(f"Successfully connected to PostgreSQL - Pool size: {self.max_connections}")
                return True
            else:
                raise PostgreSQLConnectionError("Health check failed after connection")

        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.is_connected = False
            raise PostgreSQLConnectionError(f"Connection failed: {e}")

    async def disconnect(self):
        """Disconnect from PostgreSQL gracefully"""
        try:
            if self.pool:
                await self.pool.close()

            self.is_connected = False
            self.pool = None

            self.logger.info("Disconnected from PostgreSQL")

        except Exception as e:
            self.logger.error(f"Error during PostgreSQL disconnect: {e}")

    async def _perform_health_check(self) -> bool:
        """
        Perform health check on PostgreSQL connection.

        Returns:
            True if healthy
        """
        try:
            if not self.pool:
                return False

            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")

                if result == 1:
                    self.last_health_check = time.time()
                    self.logger.debug("PostgreSQL health check passed")
                    return True
                else:
                    self.logger.warning("PostgreSQL health check returned unexpected result")
                    return False

        except Exception as e:
            self.logger.error(f"PostgreSQL health check error: {e}")
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

    async def execute_query(self, query: str, params: List[Any] = None) -> Any:
        """
        Execute a query and return the result.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Query result

        Raises:
            PostgreSQLQueryError: If query fails
        """
        if not await self.ensure_connected():
            raise PostgreSQLQueryError("Not connected to PostgreSQL")

        query_start = time.time()
        params = params or []

        try:
            self.stats['query_operations'] += 1

            async with self.pool.acquire() as conn:
                result = await conn.fetch(query, *params)

            # Update statistics
            query_duration = (time.time() - query_start) * 1000  # Convert to ms
            self.stats['total_queries_executed'] += 1
            self.stats['last_query_time'] = time.time()

            # Track latency
            self.query_latencies.append(query_duration)
            if len(self.query_latencies) > self.max_latency_samples:
                self.query_latencies = self.query_latencies[-self.max_latency_samples:]

            # Calculate average latency
            if self.query_latencies:
                self.stats['avg_query_latency_ms'] = sum(self.query_latencies) / len(self.query_latencies)

            self.logger.debug(f"Query executed in {query_duration:.2f}ms, returned {len(result)} rows")
            return result

        except Exception as e:
            self.stats['query_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Query failed: {e}")
            self.logger.debug(f"Failed query: {query[:200]}...")
            raise PostgreSQLQueryError(f"Query execution failed: {e}")

    async def execute_command(self, command: str, params: List[Any] = None) -> str:
        """
        Execute a command (INSERT/UPDATE/DELETE) and return status.

        Args:
            command: SQL command string
            params: Command parameters

        Returns:
            Command status string

        Raises:
            PostgreSQLQueryError: If command fails
        """
        if not await self.ensure_connected():
            raise PostgreSQLQueryError("Not connected to PostgreSQL")

        command_start = time.time()
        params = params or []

        try:
            self.stats['query_operations'] += 1

            async with self.pool.acquire() as conn:
                result = await conn.execute(command, *params)

            # Update statistics
            command_duration = (time.time() - command_start) * 1000
            self.stats['total_queries_executed'] += 1
            self.stats['last_query_time'] = time.time()

            self.logger.debug(f"Command executed in {command_duration:.2f}ms: {result}")
            return result

        except Exception as e:
            self.stats['query_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Command failed: {e}")
            self.logger.debug(f"Failed command: {command[:200]}...")
            raise PostgreSQLQueryError(f"Command execution failed: {e}")

    async def execute_transaction(self, commands: List[Tuple[str, List[Any]]]) -> bool:
        """
        Execute multiple commands in a transaction.

        Args:
            commands: List of (command, params) tuples

        Returns:
            True if transaction successful

        Raises:
            PostgreSQLQueryError: If transaction fails
        """
        if not await self.ensure_connected():
            raise PostgreSQLQueryError("Not connected to PostgreSQL")

        transaction_start = time.time()

        try:
            self.stats['transaction_operations'] += 1

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    for command, params in commands:
                        await conn.execute(command, *params)

            transaction_duration = (time.time() - transaction_start) * 1000
            self.logger.debug(f"Transaction with {len(commands)} commands executed in {transaction_duration:.2f}ms")
            return True

        except Exception as e:
            self.stats['transaction_errors'] += 1
            self.stats['last_error_time'] = time.time()
            self.logger.error(f"Transaction failed: {e}")
            raise PostgreSQLQueryError(f"Transaction failed: {e}")

    async def insert_record(self, table: str, data: Dict[str, Any]) -> str:
        """
        Insert a single record into a table.

        Args:
            table: Table name
            data: Dictionary of column -> value

        Returns:
            Insert result status

        Raises:
            PostgreSQLQueryError: If insert fails
        """
        if not data:
            raise PostgreSQLQueryError("No data provided for insert")

        # Build INSERT query
        columns = list(data.keys())
        placeholders = [f"${i + 1}" for i in range(len(columns))]
        values = [data[col] for col in columns]

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        return await self.execute_command(query, values)

    async def insert_records_batch(self, table: str, records: List[Dict[str, Any]]) -> str:
        """
        Insert multiple records in a single batch operation.

        Args:
            table: Table name
            records: List of dictionaries with column -> value

        Returns:
            Insert result status

        Raises:
            PostgreSQLQueryError: If batch insert fails
        """
        if not records:
            return "INSERT 0 0"

        # Get columns from first record
        columns = list(records[0].keys())

        # Validate all records have same columns
        for record in records:
            if set(record.keys()) != set(columns):
                raise PostgreSQLQueryError("All records must have the same columns")

        # Build batch INSERT query
        placeholders = []
        values = []

        for i, record in enumerate(records):
            record_placeholders = []
            for j, col in enumerate(columns):
                param_num = i * len(columns) + j + 1
                record_placeholders.append(f"${param_num}")
                values.append(record[col])

            placeholders.append(f"({', '.join(record_placeholders)})")

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES {', '.join(placeholders)}
        """

        return await self.execute_command(query, values)

    async def update_record(self, table: str, data: Dict[str, Any],
                            where_clause: str, where_params: List[Any] = None) -> str:
        """
        Update records in a table.

        Args:
            table: Table name
            data: Dictionary of column -> new_value
            where_clause: WHERE clause (without WHERE keyword)
            where_params: Parameters for WHERE clause

        Returns:
            Update result status

        Raises:
            PostgreSQLQueryError: If update fails
        """
        if not data:
            raise PostgreSQLQueryError("No data provided for update")

        where_params = where_params or []

        # Build SET clause
        set_clauses = []
        values = []
        param_num = 1

        for col, value in data.items():
            set_clauses.append(f"{col} = ${param_num}")
            values.append(value)
            param_num += 1

        # Add WHERE parameters
        for param in where_params:
            values.append(param)

        # Adjust WHERE clause parameter numbers
        adjusted_where = where_clause
        for i in range(len(where_params)):
            old_param = f"${i + 1}"
            new_param = f"${param_num + i}"
            adjusted_where = adjusted_where.replace(old_param, new_param)

        query = f"""
            UPDATE {table}
            SET {', '.join(set_clauses)}
            WHERE {adjusted_where}
        """

        return await self.execute_command(query, values)

    async def select_records(self, table: str, columns: List[str] = None,
                             where_clause: str = None, where_params: List[Any] = None,
                             order_by: str = None, limit: int = None) -> List[Dict[str, Any]]:
        """
        Select records from a table.

        Args:
            table: Table name
            columns: List of columns to select (None for all)
            where_clause: WHERE clause (without WHERE keyword)
            where_params: Parameters for WHERE clause
            order_by: ORDER BY clause (without ORDER BY keyword)
            limit: LIMIT value

        Returns:
            List of records as dictionaries

        Raises:
            PostgreSQLQueryError: If select fails
        """
        where_params = where_params or []

        # Build SELECT query
        columns_str = ', '.join(columns) if columns else '*'
        query = f"SELECT {columns_str} FROM {table}"

        if where_clause:
            query += f" WHERE {where_clause}"

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        rows = await self.execute_query(query, where_params)

        # Convert to list of dictionaries
        return [dict(row) for row in rows]

    async def create_table_if_not_exists(self, table_schema: str) -> bool:
        """
        Create table if it doesn't exist.

        Args:
            table_schema: Complete CREATE TABLE statement

        Returns:
            True if successful
        """
        try:
            await self.execute_command(table_schema)
            self.logger.info(f"Table creation command executed successfully")
            return True
        except Exception as e:
            # Check if error is because table already exists
            if "already exists" in str(e).lower():
                self.logger.debug("Table already exists")
                return True
            else:
                self.logger.error(f"Failed to create table: {e}")
                return False

    async def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table information
        """
        # Get column information
        columns_query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
        """

        columns = await self.execute_query(columns_query, [table_name])

        # Get table size
        size_query = """
            SELECT 
                pg_size_pretty(pg_total_relation_size($1)) as total_size,
                pg_size_pretty(pg_relation_size($1)) as table_size
        """

        try:
            size_info = await self.execute_query(size_query, [table_name])
            size_data = dict(size_info[0]) if size_info else {}
        except:
            size_data = {}

        return {
            'table_name': table_name,
            'columns': [dict(col) for col in columns],
            'column_count': len(columns),
            'size_info': size_data
        }

    def get_connection_status(self) -> Dict[str, Any]:
        """Get detailed connection status"""
        pool_info = {}

        if self.pool:
            pool_info = {
                'size': self.pool.get_size(),
                'max_size': self.pool.get_max_size(),
                'min_size': self.pool.get_min_size(),
                'idle_size': self.pool.get_idle_size()
            }

        return {
            'is_connected': self.is_connected,
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'username': self.username,
            'last_health_check': self.last_health_check,
            'health_check_interval': self.health_check_interval,
            'pool_info': pool_info,
            'statistics': self.stats.copy(),
            'avg_query_latency_ms': self.stats['avg_query_latency_ms']
        }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        current_time = time.time()

        # Calculate rates
        uptime = current_time - (self.stats.get('first_connection_time', current_time))
        query_rate = self.stats['total_queries_executed'] / max(uptime, 1)
        error_rate = self.stats['query_errors'] / max(self.stats['query_operations'], 1)

        return {
            'query_operations_total': self.stats['query_operations'],
            'query_errors_total': self.stats['query_errors'],
            'transaction_operations_total': self.stats['transaction_operations'],
            'transaction_errors_total': self.stats['transaction_errors'],
            'queries_executed_total': self.stats['total_queries_executed'],
            'query_rate_per_second': query_rate,
            'error_rate_percent': error_rate * 100,
            'avg_query_latency_ms': self.stats['avg_query_latency_ms'],
            'recent_latencies_ms': self.query_latencies[-10:] if self.query_latencies else [],
            'last_query_time': self.stats['last_query_time'],
            'last_error_time': self.stats['last_error_time']
        }


# Demo and testing
async def test_postgres_client():
    """Test PostgreSQL client functionality"""
    print("TESTING POSTGRESQL CLIENT")
    print("=" * 50)

    if not ASYNCPG_AVAILABLE:
        print("❌ AsyncPG library not available")
        print("Install with: pip install asyncpg")
        return False

    try:
        # Initialize client
        client = PostgreSQLClient()

        # Test connection
        print("Testing connection...")
        await client.connect()
        print("✅ Connected to PostgreSQL")

        # Test table creation
        test_table_schema = """
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(15,6),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """

        print("Testing table creation...")
        success = await client.create_table_if_not_exists(test_table_schema)
        print(f"✅ Table creation: {success}")

        # Test single record insert
        test_data = {
            'name': 'test_record',
            'value': 42.5
        }

        print("Testing single record insert...")
        result = await client.insert_record('test_table', test_data)
        print(f"✅ Single insert: {result}")

        # Test batch insert
        batch_data = [
            {'name': 'batch_1', 'value': 10.5},
            {'name': 'batch_2', 'value': 20.5},
            {'name': 'batch_3', 'value': 30.5}
        ]

        print("Testing batch insert...")
        result = await client.insert_records_batch('test_table', batch_data)
        print(f"✅ Batch insert: {result}")

        # Test select
        print("Testing select...")
        records = await client.select_records(
            'test_table',
            columns=['id', 'name', 'value'],
            order_by='id DESC',
            limit=3
        )
        print(f"✅ Select: {len(records)} records returned")

        # Test update
        print("Testing update...")
        result = await client.update_record(
            'test_table',
            {'value': 99.9},
            'name = $1',
            ['test_record']
        )
        print(f"✅ Update: {result}")

        # Test transaction
        transaction_commands = [
            ("INSERT INTO test_table (name, value) VALUES ($1, $2)", ['trans_1', 100.0]),
            ("INSERT INTO test_table (name, value) VALUES ($1, $2)", ['trans_2', 200.0])
        ]

        print("Testing transaction...")
        success = await client.execute_transaction(transaction_commands)
        print(f"✅ Transaction: {success}")

        # Test health check
        print("Testing health check...")
        healthy = await client._perform_health_check()
        print(f"✅ Health check: {healthy}")

        # Get table info
        print("Testing table info...")
        table_info = await client.get_table_info('test_table')
        print(f"✅ Table info: {table_info['column_count']} columns")

        # Show status
        status = client.get_connection_status()
        print(f"\nConnection Status:")
        print(f"  Connected: {status['is_connected']}")
        print(f"  Database: {status['database']}")
        print(f"  Pool Size: {status['pool_info'].get('size', 'N/A')}")
        print(f"  Query Operations: {status['statistics']['query_operations']}")
        print(f"  Avg Latency: {status['avg_query_latency_ms']:.2f}ms")

        # Show performance metrics
        metrics = client.get_performance_metrics()
        print(f"\nPerformance Metrics:")
        print(f"  Query Rate: {metrics['query_rate_per_second']:.2f}/sec")
        print(f"  Error Rate: {metrics['error_rate_percent']:.2f}%")
        print(f"  Recent Latencies: {metrics['recent_latencies_ms']}")

        # Cleanup test table
        await client.execute_command("DROP TABLE IF EXISTS test_table")

        # Test disconnect
        await client.disconnect()
        print("✅ Disconnected from PostgreSQL")

        print("\n✅ PostgreSQL client test completed!")
        return True

    except Exception as e:
        print(f"❌ PostgreSQL client test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(test_postgres_client())
        if not success:
            exit(1)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test execution failed: {e}")
        exit(1)