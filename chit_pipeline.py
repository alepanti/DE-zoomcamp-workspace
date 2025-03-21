import dlt
import requests
import os
from datetime import datetime, UTC

# Chicago Data Portal
API_URL = "https://data.cityofchicago.org/resource/kf7e-cur8.json"

# Default start date for backfill
DEFAULT_START_DATE = "2025-03-21T15:00:00.000"

# Get backfill date from Kestra (if provided)
BACKFILL_DATE = os.getenv("BACKFILL_DATE", None)

def get_api_url(since_time):
    """Generate API URL for incremental or full load."""
    return f"{API_URL}?$where=time>'{since_time}'"

@dlt.resource(name="chicago_traffic", write_disposition="append")
def fetch_traffic_data(pipeline):
    """Fetch all data from API in batches using pagination."""

    last_timestamp = BACKFILL_DATE or pipeline.state.get("last_timestamp", DEFAULT_START_DATE)
    offset = 0
    limit = 1000  # API allows a max of 1000 rows per request

    while True:
        # Construct paginated API URL
        url = f"{API_URL}?$where=time>'{last_timestamp}'&$limit={limit}&$offset={offset}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if not data:
            break  # Stop if no more data

        print("Sample row:", data[0])
        print(f"DEBUG: Extracted {len(data)} rows (Offset: {offset})")  # Debugging output

        yield from data  # dlt expects iterable of dicts

        # Get the latest timestamp and update state
        latest_timestamp = max(entry["time"] for entry in data)
        pipeline.state["last_timestamp"] = latest_timestamp

        # Move to the next page
        offset += limit

# Initialize pipeline **ONCE**
pipeline = dlt.pipeline(
    pipeline_name="chit_pipeline",
    destination="filesystem",  # GCS is configured in secrets.toml
    dataset_name="chicago_traffic_data",
)

# Run the pipeline with fetch_traffic_data()
load_info = pipeline.run(fetch_traffic_data(pipeline))

print(f"Load Info: {load_info}")
