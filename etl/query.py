#!/usr/bin/env python3
"""
Complete Query Module - Get available tickers and market data from InfluxDB
"""

import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from influxdb_client import InfluxDBClient
import pytz
from datetime import datetime, timedelta

# Load environment
env_file = find_dotenv()
if env_file:
    load_dotenv(env_file, override=True)


def get_available_tickers(source=None):
    """
    Get all available tickers from InfluxDB market data

    Args:
        source: 'kalshi', 'polymarket', or None for both

    Returns:
        list of unique tickers found in the data
    """

    client = InfluxDBClient(
        url=os.getenv('INFLUXDB_URL', 'http://localhost:8086'),
        token=os.getenv('INFLUXDB_TOKEN'),
        org=os.getenv('INFLUXDB_ORG'),
        timeout=None
    )

    query_api = client.query_api()
    bucket = os.getenv('INFLUXDB_BUCKET', 'arbs')

    # Build query to get unique tickers
    if source:
        source_filter = f'|> filter(fn: (r) => r.source == "{source}")'
    else:
        source_filter = ''

    query = f'''
    from(bucket: "{bucket}")
    |> range(start: 1970-01-01T00:00:00Z)
    |> filter(fn: (r) => r._measurement == "market_snapshot")
    {source_filter}
    |> keep(columns: ["ticker", "source"])
    |> unique(column: "ticker")
    '''

    try:
        result = query_api.query(query)
        tickers = []

        for table in result:
            for record in table.records:
                ticker = record.values.get('ticker')
                source_val = record.values.get('source')
                if ticker:
                    tickers.append({'ticker': ticker, 'source': source_val})

        client.close()
        return tickers

    except Exception as e:
        print(f"InfluxDB query failed: {e}")
        client.close()
        return []


def get_ticker_data(ticker, exchange, start_date=None, end_date=None, start_hours=0, end_hours=0, period=None):
    """
    Get market data for a specific ticker from a specific exchange

    Args:
        ticker: Specific ticker to query
        exchange: Exchange to query ('kalshi' or 'polymarket')
        start_date: Start date (YYYY-MM-DD format or datetime object), optional
        end_date: End date (YYYY-MM-DD format or datetime object), optional
        start_hours: Hours to add to start_date (can be negative to subtract), default 0
        end_hours: Hours to add to end_date (can be negative to subtract), default 0
        period: Aggregation period ('1m', '5m', '1h', '24h'), optional - if None returns all raw data

    Returns:
        pandas.DataFrame: DataFrame with price data for the ticker/exchange, timestamp as index
    """

    if exchange not in ['kalshi', 'polymarket']:
        raise ValueError("Exchange must be 'kalshi' or 'polymarket'")

    client = InfluxDBClient(
        url=os.getenv('INFLUXDB_URL', 'http://localhost:8086'),
        token=os.getenv('INFLUXDB_TOKEN'),
        org=os.getenv('INFLUXDB_ORG'),
        timeout=None
    )

    query_api = client.query_api()
    bucket = os.getenv('INFLUXDB_BUCKET', 'arbs')
    cst = pytz.timezone('America/Chicago')

    # Process start_date with hours adjustment
    if start_date:
        if isinstance(start_date, str):
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start_dt = start_date

        # Add hours offset
        start_dt = start_dt + timedelta(hours=start_hours)
        start_formatted = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        start_formatted = None

    # Process end_date with hours adjustment
    if end_date:
        if isinstance(end_date, str):
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end_dt = end_date

        # Add hours offset
        end_dt = end_dt + timedelta(hours=end_hours)
        end_formatted = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        end_formatted = None

    # Build time range
    if start_formatted and end_formatted:
        range_clause = f'range(start: {start_formatted}, stop: {end_formatted})'
    elif start_formatted:
        range_clause = f'range(start: {start_formatted})'
    elif end_formatted:
        range_clause = f'range(start: 1970-01-01T00:00:00Z, stop: {end_formatted})'
    else:
        range_clause = 'range(start: 1970-01-01T00:00:00Z)'

    # Build aggregation
    if period:
        # Map period to InfluxDB window
        period_map = {
            '1m': '1m',
            '5m': '5m',
            '1h': '1h',
            '24h': '24h'
        }
        window = period_map.get(period, '1m')
        aggregation = f'|> aggregateWindow(every: {window}, fn: last, createEmpty: false)'
    else:
        aggregation = ''

    # Query for specific ticker and exchange
    query = f'''
    from(bucket: "{bucket}")
    |> {range_clause}
    |> filter(fn: (r) => r._measurement == "market_snapshot")
    |> filter(fn: (r) => r._field == "full_orderbook")
    |> filter(fn: (r) => r.ticker == "{ticker}")
    |> filter(fn: (r) => r.source == "{exchange}")
    {aggregation}
    |> sort(columns: ["_time"], desc: false)
    '''

    try:
        result = query_api.query(query)

        prices = []

        for table in result:
            for record in table.records:
                timestamp = record.get_time().astimezone(cst)
                source = record.values.get('source')
                ticker_name = record.values.get('ticker')
                orderbook_json = record.get_value()

                try:
                    orderbook = json.loads(orderbook_json)

                    if source == 'kalshi':
                        yes_levels = orderbook.get('yes', [])

                        if yes_levels:
                            # Convert to decimal and find highest YES price
                            yes_prices = [level[0] / 100 for level in yes_levels if len(level) >= 2]

                            yes_price = max(yes_prices) if yes_prices else np.nan
                            # Calculate NO price as complement (1 - YES)
                            no_price = 1.0 - yes_price if not pd.isna(yes_price) else np.nan

                            prices.append({
                                'timestamp': timestamp,
                                'ticker': ticker_name,
                                'yes': yes_price,
                                'no': no_price
                            })

                    elif source == 'polymarket':
                        if 'orderbook' in orderbook:
                            ob = orderbook['orderbook']
                            bids = ob.get('bids', [])
                            asks = ob.get('asks', [])

                            if bids and asks:
                                # Get highest bid and lowest ask
                                bid_prices = [bid['price'] for bid in bids if isinstance(bid, dict)]
                                ask_prices = [ask['price'] for ask in asks if isinstance(ask, dict)]

                                yes_bid = max(bid_prices) if bid_prices else np.nan
                                yes_ask = min(ask_prices) if ask_prices else np.nan

                                # Use midpoint for clean yes/no prices
                                yes_mid = (yes_bid + yes_ask) / 2
                                no_mid = 1.0 - yes_mid

                                # Polymarket might be trading the opposite contract
                                # If Polymarket YES is much higher than expected, swap them
                                # to match Kalshi's contract direction
                                if yes_mid > 0.5:
                                    # Polymarket is trading the "NO" contract, so swap
                                    yes_price = no_mid  # What poly calls "yes" is actually "no"
                                    no_price = yes_mid  # What poly calls "no" is actually "yes"
                                else:
                                    yes_price = yes_mid
                                    no_price = no_mid

                                prices.append({
                                    'timestamp': timestamp,
                                    'ticker': ticker_name,
                                    'yes': yes_price,
                                    'no': no_price
                                })

                except Exception:
                    continue

        client.close()

        if not prices:
            print(f"No data found for ticker '{ticker}' on exchange '{exchange}'")
            print("Please check:")
            print(f"  - Ticker is correct: {ticker}")
            print(f"  - Exchange is correct: {exchange}")
            print("  - Ticker exists on the specified exchange")
            print("  - Data exists for the time range specified")
            return pd.DataFrame()

        # Create DataFrame and set timestamp as index
        df = pd.DataFrame(prices)
        df.set_index('timestamp', inplace=True)

        return df

    except Exception as e:
        print(f"Query failed: {e}")
        client.close()
        return pd.DataFrame()
