import json
import os
import toml
import requests
import dlt
from dlt.sources.filesystem import filesystem, read_csv
from google.cloud import storage

# Load the TOML file
# the TOML file should follow below format:
#[credentials]
#project_id = "your project id"
#private_key = "your sevice account key"
#client_email = "email"
config = toml.load(".dlt/secrets.toml")

# Set environment variables
#os.environ["CREDENTIALS__PROJECT_ID"] = config["destination.bigquery.credentials"]["project_id"]
#os.environ["CREDENTIALS__PRIVATE_KEY"] = config["destination.bigquery.credentials"]["private_key"]
#os.environ["CREDENTIALS__CLIENT_EMAIL"] = config["destination.bigquery.credentials"]["client_email"]

# Initialize GCS client
storage_client = storage.Client()
bucket_name = "dlt-taxi-data"  # Replace with your GCS bucket name
bucket = storage_client.bucket(bucket_name)

# Function to generate URLs based on user input for the date range and trip color
def generate_urls(color, start_year, end_year, start_month, end_month):
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
    urls = []

# Generate the list of URLs based on the specified date range and color

    for year in range(start_year, end_year + 1):
        for month in range(start_month, end_month + 1):
            # Format the month to ensure two digits
            month_str = f"{month:02d}"
            url = f"{base_url}{color}/{color}_tripdata_{year}-{month_str}.csv.gz"
            #'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_',
            #'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_',
            #'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_'
            urls.append(url)

    return urls

# User input for time range and trip color
color = input("Enter type (green, yellow, fhv): ").lower()  
start_year = int(input("Enter the start year (e.g., 2019): "))
end_year = int(input("Enter the end year (e.g., 2022): "))
start_month = int(input("Enter the start month (1-12): "))
end_month = int(input("Enter the end month (1-12): "))

# Generate URLs based on user input
urls = generate_urls(color, start_year, end_year, start_month, end_month)


# Debug: Print generated URLs
print("Generated URLs:")
for url in urls:
    print(url)

# Download files and upload them to GCS
gcs_files = []
for url in urls:
    file_name = url.split("/")[-1]  # Extract the file name from the URL
    gcs_blob = bucket.blob(f"{color}/{file_name}")

    print(f"Downloading {url} and uploading to GCS as {file_name}")
    response = requests.get(url)
    gcs_blob.upload_from_string(response.content)
    gcs_files.append(f"gs://{bucket_name}/{color}/{file_name}")

@dlt.resource(name=f"{color}_taxi_rides", write_disposition="replace")
def csv_source():
    # Use filesystem to load files from GCS and apply read_parquet transformation
    files = filesystem(bucket_url=f"gs://dlt-taxi-data/{color}", file_glob="*.csv.gz")
    for f in files:
        print(f)
    reader = (files | read_csv()).with_name("tripdata")

    # Iterate through the rows from the reader and yield them
    row_count = 0
    for row in reader:
        row_count += 1
        if row_count <= 5:  # debugging
            print(f"Yielding row: {row}")
        yield row
    print(f"Total rows yielded: {row_count}")

# Create the pipeline
pipeline = dlt.pipeline(
    pipeline_name="taxi",
    dataset_name="taxi_data",
    destination="bigquery"
)

# Run the pipeline
info = pipeline.run(csv_source())
print(info)

