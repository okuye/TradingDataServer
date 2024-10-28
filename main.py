from fastapi import FastAPI, HTTPException, Query
import pandas as pd
import os
import json
from typing import Optional
from datetime import datetime

app = FastAPI()


# Function to convert the array to a dictionary while handling missing fields
def from_array(arr):
    # Define the default values for each field
    defaults = {
        'symbol': None,
        'date': None,
        'hour': None,
        'openbid': None,
        'highbid': None,
        'lowbid': None,
        'closebid': None,
        'openask': None,
        'highask': None,
        'lowask': None,
        'closeask': None,
        'totalticks': None
    }

    # Map the incoming array values to the expected fields, using default values for missing fields
    try:
        return {
            'symbol': str(arr[0]) if len(arr) > 0 else defaults['symbol'],
            'date': pd.to_datetime(arr[1], format='%Y-%m-%d', errors='coerce') if len(arr) > 1 else defaults['date'],
            'hour': int(arr[2]) if len(arr) > 2 and arr[2] is not None else defaults['hour'],
            'openbid': float(arr[3]) if len(arr) > 3 and arr[3] is not None else defaults['openbid'],
            'highbid': float(arr[4]) if len(arr) > 4 and arr[4] is not None else defaults['highbid'],
            'lowbid': float(arr[5]) if len(arr) > 5 and arr[5] is not None else defaults['lowbid'],
            'closebid': float(arr[6]) if len(arr) > 6 and arr[6] is not None else defaults['closebid'],
            'openask': float(arr[7]) if len(arr) > 7 and arr[7] is not None else defaults['openask'],
            'highask': float(arr[8]) if len(arr) > 8 and arr[8] is not None else defaults['highask'],
            'lowask': float(arr[9]) if len(arr) > 9 and arr[9] is not None else defaults['lowask'],
            'closeask': float(arr[10]) if len(arr) > 10 and arr[10] is not None else defaults['closeask'],
            'totalticks': int(arr[11]) if len(arr) > 11 and arr[11] is not None else defaults['totalticks'],
        }
    except (ValueError, IndexError) as e:
        raise ValueError(f"Error converting array to Trade: {e}")


# Function to load trades from parsed JSON objects
def load_trades_from_file(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found.")

    with open(file_path, 'r') as f:
        content = json.load(f)

    trades_data = content['datatable']['data']

    # Use the from_array function to handle each row
    trades = [from_array(row) for row in trades_data]

    # Convert to DataFrame
    columns = [
        'symbol', 'date', 'hour', 'openbid', 'highbid', 'lowbid',
        'closebid', 'openask', 'highask', 'lowask', 'closeask', 'totalticks'
    ]

    return pd.DataFrame(trades, columns=columns)


# Function to load all JSON files in a directory and concatenate into a single DataFrame
def load_all_trades(directory_path: str) -> pd.DataFrame:
    all_trades = pd.DataFrame()

    # Initialize a list to store file names
    file_names = []

    # Iterate over all .json files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            file_path = os.path.join(directory_path, filename)
            file_names.append(filename)  # Add the file name to the list

            try:
                trades_df = load_trades_from_file(file_path)
                all_trades = pd.concat([all_trades, trades_df], ignore_index=True)
            except Exception as e:
                print(f"Error loading {file_path}: {e}")

    # Print all the files that were read
    print(f"Files read from directory {directory_path}: {file_names}")

    # Print the total number of trades loaded
    print(f"Total trades loaded: {len(all_trades)}")

    # Print the date range available in the dataset
    if not all_trades.empty:
        print(f"Date range in dataset: {all_trades['date'].min()} to {all_trades['date'].max()}")

    # Print available symbols in the dataset
    print(f"Available symbols: {all_trades['symbol'].unique() if not all_trades.empty else 'No data'}")

    return all_trades


# Load all trades once when the app starts
TRADES_DF = load_all_trades("/Users/olakunlekuye/Documents/Dev/KLX/Nasdaq-Data")


# Function to filter trades based on date range and symbol
def filter_trades(trades: pd.DataFrame, start_date: str, end_date: str, symbol: Optional[str] = None) -> pd.DataFrame:
    start_date = pd.to_datetime(start_date, format='%Y-%m-%d')
    end_date = pd.to_datetime(end_date, format='%Y-%m-%d')

    filtered_trades = trades[(trades['date'] >= start_date) & (trades['date'] <= end_date)]

    if symbol:
        filtered_trades = filtered_trades[filtered_trades['symbol'] == symbol]

    return filtered_trades


@app.get("/trades")
def get_trades(startDate: str, endDate: str, symbol: Optional[str] = None, api_key: Optional[str] = Query(None)):
    # Validate API key
    if api_key != "Ee-osjmRSwyXkPA3QBFe":
        raise HTTPException(status_code=403, detail="Invalid API key")

    # Validate date format
    try:
        datetime.strptime(startDate, '%Y-%m-%d')
        datetime.strptime(endDate, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Expected 'YYYY-MM-DD'")

    # Filter trades
    filtered_trades = filter_trades(TRADES_DF, startDate, endDate, symbol)

    # Return filtered trades
    return filtered_trades.to_dict(orient='records')

