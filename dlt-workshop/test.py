import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdbtest
from google.colab import data_table

def paginated_getter():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

ny_taxi = []

for page_data in paginated_getter():
    ny_taxi.append(page_data)

print(len(ny_taxi))

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

load_info = pipeline.run(ny_taxi, table_name="rides", write_disposition="replace")
print(load_info)

data_table.enable_dataframe_formatter()
conn = duckdbtest.connect(f"ny_taxi_pipeline.duckdb")
# Set search path to the dataset
conn.sql(f"SET search_path = 'ny_taxi_pipeline'")

# Describe the dataset
print(conn.sql("DESCRIBE").df())