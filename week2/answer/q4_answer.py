from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os
from datetime import timedelta

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    
    # create directory if not exist
    dir = f"./data/{color}"
    if not os.path.isdir(dir):
        os.makedirs(dir)

    path = Path(f"{dir}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_block = GcsBucket.load("prefect-de-zoomcamp-gcs")
    gcp_block.upload_from_path(from_path=path, to_path=path, timeout=120)
    return


@flow()
def etl_web_to_gcs(month, year, color) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    path = write_local(df, color, dataset_file)
    write_gcs(path)
    return len(df)

@flow(log_prints=True)
def etl_web_to_gcs_parent(months, year, color):
    total_rows = 0
    for month in months:
        total_rows += etl_web_to_gcs(month, year, color)
    print("total rows ", total_rows)

if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_web_to_gcs_parent(months, year, color)
