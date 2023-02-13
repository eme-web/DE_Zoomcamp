from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data\{color}\{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"{gcs_path}")

    
@task()
def transform(path: Path) -> pd.DataFrame:
   """Read data into a DataFrame"""
   df = pd.read_parquet(path)
   print(f"rows: {len(df)}")
#    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
#    df['passenger_count'].fillna(0, inplace=True)
#    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
   return df

@task()
def write_bq(df: pd.DataFrame ) -> None:
   """write DataFrame to BigQuery"""

   gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

   df.to_gbq(
      destination_table="taxi_trips_data_all.yellow_taxi_trips",
      project_id="ferrous-phoenix-376516",
      credentials=gcp_credentials_block.get_credentials_from_service_account(),
      chunksize=500_000,
      if_exists="append",
      )


@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str):
   """Main ETL to load data into Big Query"""
   path = extract_from_gcs(color, year, month)
   df = transform(path)
   write_bq(df)
 

@flow()
def etl_bq_flow(months: list[int] = [2,3], year: int = 2019, color: str = "yellow"):
    total_rows = 0

    for month in months:
        rows = etl_gcs_to_bq(year, month, color)
        total_rows += rows

        print(total_rows)


if __name__ == '__main__':
   color = "yellow"
   months = [2,3]
   year = 2019
   etl_bq_flow(months,year, color)