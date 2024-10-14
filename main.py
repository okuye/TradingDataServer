from fastapi import FastAPI, HTTPException, Query
import pandas as pd
import os
import json
from typing import Optional
from datetime import datetime

# Initialize FastAPI app
app = FastAPI()


# Define a function to convert the array to a dictionary (similar to a Trade case class)
def from_array(arr):
    try:
        return {
            'symbol': str(arr[0]),
            'date': pd.to_datetime(str(arr[1]), format='%Y-%m-%d'),
            'hour': int(arr[2]),
            'openbid': float(arr[3]),
            'highbid': float(arr[4]),
            'lowbid': float(arr[5]),
            'closebid': float(arr[6]),
            'openask': float(arr[7]),
            'highask': float(arr[8]),
            'lowask': float(arr[9]),
            'closeask': float(arr[10]),
            'totalticks': int(arr[11])
        }
    except (ValueError, IndexError) as e:
        raise ValueError(f"Error converting array to Trade: {e}")


# Function to handle multiple concatenated JSON objects by splitting
def parse_json_objects(file_path: str):
    with open(file_path, 'r') as f:
        content = f.read()

    # Handle multiple concatenated JSON objects by splitting at '}{'
    raw_json_objects = content.split('}{')
    parsed_objects = []

    for i, obj in enumerate(raw_json_objects):
        if i > 0:  # Add back the leading '{' for all but the first object
            obj = '{' + obj
        if i < len(raw_json_objects) - 1:  # Add back the trailing '}' for all but the last object
            obj = obj + '}'

        try:
            parsed_obj = json.loads(obj)
            parsed_objects.append(parsed_obj)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON object {i}: {e}")

    return parsed_objects


# Function to load trades from parsed JSON objects
def load_trades_from_file(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found.")

    trades = []
    for json_obj in parse_json_objects(file_path):
        if "datatable" in json_obj:
            trades_data = json_obj['datatable']['data']
            trades.extend([from_array(row) for row in trades_data])

    # Print the number of trades parsed for debug
    print(f"Parsed {len(trades)} trades.")

    columns = [
        'symbol', 'date', 'hour', 'openbid', 'highbid', 'lowbid',
        'closebid', 'openask', 'highask', 'lowask', 'closeask', 'totalticks'
    ]

    return pd.DataFrame(trades, columns=columns)


# Function to filter trades based on date range and symbol
def filter_trades(trades: pd.DataFrame, start_date: str, end_date: str, symbol: Optional[str] = None) -> pd.DataFrame:
    start_date = pd.to_datetime(start_date, format='%Y-%m-%d')
    end_date = pd.to_datetime(end_date, format='%Y-%m-%d')

    # Filter by date range and symbol (if provided)
    filtered_trades = trades[
        (trades['date'] >= start_date) &
        (trades['date'] <= end_date)
        ]

    if symbol:
        filtered_trades = filtered_trades[filtered_trades['symbol'] == symbol]

    # Print number of filtered trades for debug
    print(f"Filtered {len(filtered_trades)} trades.")

    return filtered_trades


# Function to convert filtered trades to response format
def convert_trades_to_response(trades: pd.DataFrame) -> dict:
    columns = [
        {'name': 'symbol', 'type': 'String'},
        {'name': 'date', 'type': 'Date'},
        {'name': 'hour', 'type': 'Integer'},
        {'name': 'openbid', 'type': 'double'},
        {'name': 'highbid', 'type': 'double'},
        {'name': 'lowbid', 'type': 'double'},
        {'name': 'closebid', 'type': 'double'},
        {'name': 'openask', 'type': 'double'},
        {'name': 'highask', 'type': 'double'},
        {'name': 'lowask', 'type': 'double'},
        {'name': 'closeask', 'type': 'double'},
        {'name': 'totalticks', 'type': 'Integer'}
    ]

    trade_data = trades.to_dict(orient='records')

    return {
        'datatable': {
            'data': trade_data,
            'columns': columns
        },
        'meta': {'next_cursor_id': None}
    }


# Load trades data once when the API starts (to avoid reloading every request)
TRADES_DF = load_trades_from_file(
    "/Users/olakunlekuye/Documents/Dev/KLX/TradingDataServer/Data/nasdaq-data-2023-eur-usd.json")

# API key for validation
VALID_API_KEY = "Ee-osjmRSwyXkPA3QBFe"


# Define the GET /trades endpoint
@app.get("/trades")
def get_trades(startDate: str, endDate: str, symbol: Optional[str] = None, api_key: Optional[str] = Query(None)):
    # Validate API key
    if api_key != VALID_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    # Validate date format
    try:
        datetime.strptime(startDate, '%Y-%m-%d')
        datetime.strptime(endDate, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Expected 'YYYY-MM-DD'")

    # Filter trades by date range and symbol
    try:
        filtered_trades = filter_trades(TRADES_DF, startDate, endDate, symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering trades: {e}")

    # Convert the filtered trades to the response format
    response = convert_trades_to_response(filtered_trades)

    return response
