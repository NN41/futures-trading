#%%
 
import requests
import datetime
import time
import os
import csv

# ==============================================================================
# SCRIPT CONFIGURATION (Specs V0.1)
# ==============================================================================
START_DATE = "2025-07-10" # "YYYY-MM-DD"
END_DATE = "2025-07-17" # "YYYY-MM-DD" (non-inclusive)
INSTRUMENT_NAME = "BTC-PERPETUAL"
DATA_TYPE = "funding_rate_history"
OUTPUT_DIRECTORY = "./data/raw"

CHUNK_SIZE_DAYS = 1 # Limit is 30 days for funding rate history
SLEEP_INTERVAL_SECONDS = 0.5
# ==============================================================================

def _date_to_timestamp_ms(date_string: str) -> int:
    """Converts a 'YYYY-MM-DD' date string into a Unix timestamp in milliseconds.

    Args:
        date_string (str): The date in "YYYY-MM-DD" format.

    Returns:
        int: The corresponding Unix timestamp in milliseconds.
    """
    dt_obj = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    return int(dt_obj.timestamp() * 1000)

def _generate_unique_filename(instrument: str, data_type: str, start_date: str, end_date: str) -> str:
    """Creates a standardized, descriptive, and unique filename.

    Args:
        instrument (str): The instrument name.
        data_type (str): The type of data being downloaded.
        start_date (str): The start date of the data range ("YYYY-MM-DD").
        end_date (str): The end date of the data range ("YYYY-MM-DD").

    Returns:
        str: A formatted string for use as the CSV filename.
    """
    run_timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"DERIBIT_{instrument}_{data_type}_from_{start_date}_to_{end_date}_at_{run_timestamp}.csv"

def _save_data_to_csv(data: list[dict], file_path: str):
    """Saves a list of dictionaries to a CSV file.

    Args:
        data (list[dict]): The data to be saved. The list should not be empty.
        file_path (str): The complete path where the CSV file will be stored.
    """
    if not data:
        print("No data to save.")
        return

    # Ensure the output directory exists
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Write the data
    with open(file_path, 'w', newline='') as csvfile:
        # Use the keys from the first data point as headers
        headers = list(data[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
    print(f"Successfully saved data to: {file_path}")


def _get_funding_rate_history_chunk(instrument_name: str, start_timestamp_ms: int, end_timestamp_ms: int) -> list[dict] | None:
    """Makes a single API call to fetch a chunk of funding rate history.

    Args:
        instrument_name (str): The instrument to query.
        start_timestamp_ms (int): The query start time as a Unix timestamp (ms).
        end_timestamp_ms (int): The query end time as a Unix timestamp (ms).

    Returns:
        list[dict] | None: A list of data points from the API, or None if the request fails.
    """
    API_URL = "https://www.deribit.com/api/v2/public/get_funding_rate_history"
    params = {
        "instrument_name": instrument_name,
        "start_timestamp": start_timestamp_ms,
        "end_timestamp": end_timestamp_ms
    }
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        response_json = response.json()
        if 'result' in response_json:
            return response_json['result']
        elif 'error' in response_json:
            print(f"  - API Error: {response_json['error']['message']}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  - Request failed: {e}")
        return None
    return None


def _fetch_data_in_chunks(instrument_name, start_timestamp_ms, end_timestamp_ms, chunk_size_days, sleep_interval_seconds):
    """Fetches data for a date range by breaking it into smaller chunks.

    Args:
        instrument_name (str): The Deribit instrument name.
        start_timestamp_ms (int): The start of the total date range (milliseconds).
        end_timestamp_ms (int): The end of the total date range (milliseconds).
        chunk_size_days (int): The size of each data request in days.
        sleep_interval_seconds (float): Time to pause between requests.

    Returns:
        list[dict]: A list containing all aggregated data records.
    """
    all_data = []
    chunk_size_ms = chunk_size_days * 24 * 60 * 60 * 1000
    
    current_start_ts = start_timestamp_ms
    while current_start_ts < end_timestamp_ms:
        current_end_ts = min(current_start_ts + chunk_size_ms, end_timestamp_ms)
        
        start_dt = datetime.datetime.fromtimestamp(current_start_ts / 1000)
        end_dt = datetime.datetime.fromtimestamp(current_end_ts / 1000)
        print(f"Fetching chunk: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}...")

        chunk_data = _get_funding_rate_history_chunk(instrument_name, current_start_ts, current_end_ts)
        
        if chunk_data:
            all_data.extend(chunk_data)
            print(f"  + Retrieved {len(chunk_data)} records for this chunk.")
        else:
            print("  - Failed to retrieve data for this chunk.")
            
        current_start_ts += chunk_size_ms
        time.sleep(sleep_interval_seconds)
        
    return all_data


def download_historical_data():
    """Main function to orchestrate the entire data download and saving process."""
    print("--- Starting Deribit Data Download ---")
    print(f"Instrument: {INSTRUMENT_NAME}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print("-" * 35)

    # Convert dates to timestamps
    start_ts = _date_to_timestamp_ms(START_DATE)
    end_ts = _date_to_timestamp_ms(END_DATE)

    # Fetch all data by iterating through chunks
    final_data = _fetch_data_in_chunks(
        instrument_name=INSTRUMENT_NAME,
        start_timestamp_ms=start_ts,
        end_timestamp_ms=end_ts,
        chunk_size_days=CHUNK_SIZE_DAYS,
        sleep_interval_seconds=SLEEP_INTERVAL_SECONDS
    )

    print("-" * 35)
    
    # Save the data if any was downloaded
    if final_data:
        print(f"Total records downloaded: {len(final_data)}")
        # Generate a unique filename
        filename = _generate_unique_filename(INSTRUMENT_NAME, DATA_TYPE, START_DATE, END_DATE)
        full_path = os.path.join(OUTPUT_DIRECTORY, filename)
        
        # Save to CSV
        _save_data_to_csv(final_data, full_path)
    else:
        print("No data was downloaded. Check parameters and API status.")
        
    print("--- Download Process Finished ---")


if __name__ == "__main__":
    download_historical_data()