import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """write DataFrame out locally as csv.gz file"""
    data_dir = f"data2/{color}"
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f"{data_dir}/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return



@flow()
def el_web_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    
    df = fetch(dataset_url)
    #df_clean = clean(df)
    path = write_local(df, color, dataset_file)
    write_gcs(path)

@flow()
def el_parent_flow(months: list[int] = [1,2,3], year: int = 2019, color: str = "fhv"):
    for month in months:
        el_web_gcs(year, month, color)

   
if __name__ == '__main__':
    color = "fhv"
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    el_parent_flow(months,year, color)


